use base64::{Engine as _, engine::general_purpose};
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{Stream, StreamExt, stream};
use http::HeaderMap;
use reqwest::{Client, ClientBuilder};
use std::fmt::Debug;
use std::time::Duration;
use std::{pin::Pin, sync::Arc};
use tokio::sync::RwLock;
use url::Url;

use crate::engineio::generator::StreamGenerator;
use crate::engineio::tls::{self, TlsConfig, apply_reqwest_tls};
use crate::engineio::{Error, error::Result};

/// An asynchronous polling type. Makes use of the nonblocking reqwest types and
/// methods.
#[derive(Clone)]
pub struct PollingTransport {
    client: Client,
    base_url: Arc<RwLock<Url>>,
    generator: StreamGenerator<Bytes>,
}

impl PollingTransport {
    pub fn new(
        base_url: Url,
        tls_config: Option<TlsConfig>,
        opening_headers: Option<HeaderMap>,
    ) -> Result<Self> {
        Self::with_timeout(base_url, tls_config, opening_headers, None)
    }

    /// Like [`PollingTransport::new`], but every request (connect, send, body) fails after
    /// `timeout`, so a silent peer or a half-open connection cannot hang a request forever.
    pub(crate) fn with_timeout(
        base_url: Url,
        tls_config: Option<TlsConfig>,
        opening_headers: Option<HeaderMap>,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        tls::check_scheme(&base_url)?;
        let mut builder = apply_reqwest_tls!(ClientBuilder::new(), tls_config.as_ref());
        if let Some(map) = opening_headers {
            builder = builder.default_headers(map);
        }
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        let client = builder.build()?;

        let mut url = base_url;
        url.query_pairs_mut().append_pair("transport", "polling");

        Ok(PollingTransport {
            client: client.clone(),
            base_url: Arc::new(RwLock::new(url.clone())),
            generator: StreamGenerator::new(Self::stream(url, client)),
        })
    }

    fn stream(
        url: Url,
        client: Client,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + 'static + Send>> {
        stream::try_unfold((url, client), |(url, client)| async move {
            let response = client
                .get(crate::engineio::util::with_timestamp(url.clone()))
                .send()
                .await?;
            // A non-200 reply (e.g. 400 "Session ID unknown") carries an error body, not a
            // payload: report it instead of handing it to the decoder.
            let status = response.status().as_u16();
            if status != 200 {
                return Err(Error::HttpStatus { status });
            }
            // One polling response is one engine.io payload. Read the whole body: yielding
            // the chunks of `bytes_stream()` one by one hands partial payloads to the decoder.
            let body = response.bytes().await?;
            Ok(Some((body, (url, client))))
        })
        .boxed()
    }

    pub(crate) async fn emit(&self, data: Bytes, is_binary_att: bool) -> Result<()> {
        let data_to_send = if is_binary_att {
            // the binary attachment gets `base64` encoded
            let encoded = general_purpose::STANDARD.encode(data);
            let mut packet_bytes = BytesMut::with_capacity(encoded.len() + 1);
            packet_bytes.put_u8(b'b');
            packet_bytes.put(encoded.as_bytes());
            packet_bytes.freeze()
        } else {
            data
        };

        let address = crate::engineio::util::with_timestamp(self.base_url().await?);
        let status = self
            .client
            .post(address)
            .body(data_to_send)
            .send()
            .await?
            .status()
            .as_u16();

        if status != 200 {
            return Err(Error::HttpStatus { status });
        }
        Ok(())
    }

    pub async fn base_url(&self) -> Result<Url> {
        Ok(self.base_url.read().await.clone())
    }

    pub async fn set_base_url(&self, base_url: Url) -> Result<()> {
        let mut url = base_url;
        if !url
            .query_pairs()
            .any(|(k, v)| k == "transport" && v == "polling")
        {
            url.query_pairs_mut().append_pair("transport", "polling");
        }
        *self.base_url.write().await = url;
        Ok(())
    }
}

impl Stream for PollingTransport {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.generator.poll_next_unpin(cx)
    }
}

impl Debug for PollingTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PollingTransport")
            .field("client", &self.client)
            .field("base_url", &self.base_url)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn polling_transport_base_url() -> Result<()> {
        let url = crate::engineio::test::engine_io_server()?.to_string();
        let transport = PollingTransport::new(Url::from_str(&url[..]).unwrap(), None, None)?;
        assert_eq!(
            transport.base_url().await?.to_string(),
            url.clone() + "?transport=polling"
        );
        transport
            .set_base_url(Url::parse("https://127.0.0.1")?)
            .await?;
        assert_eq!(
            transport.base_url().await?.to_string(),
            "https://127.0.0.1/?transport=polling"
        );
        assert_ne!(transport.base_url().await?.to_string(), url);

        transport
            .set_base_url(Url::parse("http://127.0.0.1/?transport=polling")?)
            .await?;
        assert_eq!(
            transport.base_url().await?.to_string(),
            "http://127.0.0.1/?transport=polling"
        );
        assert_ne!(transport.base_url().await?.to_string(), url);
        Ok(())
    }

    #[tokio::test]
    async fn polling_transport_reports_http_error_status() {
        let mut url = crate::engineio::test::engine_io_polling_server().unwrap();
        url.set_path("/engine.io/");
        url.query_pairs_mut()
            .append_pair("EIO", "4")
            .append_pair("sid", "unknown-session");
        let mut transport = PollingTransport::new(url, None, None).unwrap();

        // engine.io answers an unknown sid with 400 and a JSON error body.
        match transport.next().await {
            Some(Err(Error::HttpStatus { status: 400 })) => {}
            other => panic!("expected IncompleteHttp(400), got {other:?}"),
        }
    }
}
