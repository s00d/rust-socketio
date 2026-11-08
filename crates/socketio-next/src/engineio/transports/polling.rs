use base64::{Engine as _, engine::general_purpose};
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{Stream, StreamExt, stream};
use http::HeaderMap;
use reqwest::{Client, RequestBuilder};
use std::fmt::Debug;
use std::time::Duration;
use std::{pin::Pin, sync::Arc};
use tokio::sync::RwLock;
use url::Url;

use crate::engineio::generator::StreamGenerator;
use crate::engineio::http::Http;
use crate::engineio::tls::{self, TlsConfig};
use crate::engineio::{Error, error::Result};

/// Largest polling response body read.
const MAX_BODY: usize = 64 << 20;

/// An asynchronous polling type. Makes use of the nonblocking reqwest types and
/// methods.
#[derive(Clone)]
pub struct PollingTransport {
    client: Client,
    headers: HeaderMap,
    base_url: Arc<RwLock<Url>>,
    generator: StreamGenerator<Bytes>,
}

impl PollingTransport {
    pub fn new(
        base_url: Url,
        tls_config: Option<TlsConfig>,
        opening_headers: Option<HeaderMap>,
    ) -> Result<Self> {
        let http = Http::new(tls_config.as_ref(), None)?;
        Self::with_client(base_url, &http, opening_headers.unwrap_or_default(), None)
    }

    /// A transport on the shared `http` client. Each long-poll GET fails after
    /// `poll_timeout`, so a silent peer or a half-open connection cannot hang it forever;
    /// POSTs have no total bound (a large upload may take longer than a heartbeat).
    pub(crate) fn with_client(
        base_url: Url,
        http: &Http,
        headers: HeaderMap,
        poll_timeout: Option<Duration>,
    ) -> Result<Self> {
        tls::check_scheme(&base_url)?;
        let client = http.client.clone();
        let mut url = base_url;
        url.query_pairs_mut().append_pair("transport", "polling");
        let get = Get {
            client: client.clone(),
            headers: headers.clone(),
            timeout: poll_timeout,
        };
        Ok(PollingTransport {
            client,
            headers,
            base_url: Arc::new(RwLock::new(url.clone())),
            generator: StreamGenerator::new(Self::stream(url, get)),
        })
    }

    fn stream(url: Url, get: Get) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + 'static + Send>> {
        stream::try_unfold((url, get), |(url, get)| async move {
            let response = get.request(&url).send().await?;
            // A non-200 reply (e.g. 400 "Session ID unknown") carries an error body, not a
            // payload: report it instead of handing it to the decoder.
            let status = response.status().as_u16();
            if status != 200 {
                return Err(http_status(response).await);
            }
            // One polling response is one engine.io payload. Read the whole body: yielding
            // the chunks of `bytes_stream()` one by one hands partial payloads to the decoder.
            // Bounded like a websocket message (tungstenite's 64 MiB default): a server that
            // streams without end must not exhaust memory.
            let mut response = response;
            let mut body = bytes::BytesMut::new();
            while let Some(chunk) = response.chunk().await? {
                if body.len() + chunk.len() > MAX_BODY {
                    return Err(Error::Protocol(crate::protocol::Error::InvalidPacket(
                        "polling response larger than 64 MiB",
                    )));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(Some((body.freeze(), (url, get))))
        })
        .boxed()
    }

    fn post(&self, url: Url, body: Bytes) -> RequestBuilder {
        self.client
            .post(url)
            .headers(self.headers.clone())
            .body(body)
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
        let response = self.post(address, data_to_send).send().await?;
        if response.status().as_u16() != 200 {
            return Err(http_status(response).await);
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

/// The long-poll GET: headers of this attempt, bounded by the heartbeat.
#[derive(Clone)]
struct Get {
    client: Client,
    headers: HeaderMap,
    timeout: Option<Duration>,
}

impl Get {
    fn request(&self, url: &Url) -> RequestBuilder {
        let request = self
            .client
            .get(crate::engineio::util::with_timestamp(url.clone()))
            .headers(self.headers.clone());
        match self.timeout {
            Some(timeout) => request.timeout(timeout),
            None => request,
        }
    }
}

/// `HttpStatus` with what the body says (at most 4 KiB of it is read).
async fn http_status(mut response: reqwest::Response) -> Error {
    let status = response.status().as_u16();
    let mut body = Vec::new();
    while body.len() < 4096 {
        match response.chunk().await {
            Ok(Some(chunk)) => body.extend_from_slice(&chunk),
            _ => break,
        }
    }
    body.truncate(4096);
    Error::HttpStatus {
        status,
        message: crate::engineio::error::body_message(&body),
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
        // The URL carries query tokens and the client its default headers: neither is printed.
        let url = self.base_url.try_read().map_or_else(
            |_| "<busy>".to_owned(),
            |url| crate::redact::url(&url, true),
        );
        f.debug_struct("PollingTransport")
            .field("base_url", &url)
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

    /// Only the long-poll GET is bounded by the heartbeat; a POST (an upload) is not.
    #[test]
    fn only_the_long_poll_has_a_total_timeout() {
        let http = Http::new(None, None).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-token", "t".parse().unwrap());
        let url = Url::parse("http://h/engine.io/").unwrap();
        let timeout = Duration::from_millis(500);
        let transport =
            PollingTransport::with_client(url.clone(), &http, headers.clone(), Some(timeout))
                .unwrap();
        let get = Get {
            client: http.client.clone(),
            headers,
            timeout: Some(timeout),
        }
        .request(&url)
        .build()
        .unwrap();
        assert_eq!(get.timeout(), Some(&timeout));
        assert_eq!(get.headers()["x-token"], "t");
        let post = transport
            .post(url, Bytes::from_static(b"x"))
            .build()
            .unwrap();
        assert_eq!(post.timeout(), None);
        assert_eq!(post.headers()["x-token"], "t");
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
            Some(Err(Error::HttpStatus {
                status: 400,
                message: Some(message),
            })) => assert_eq!(message, "Session ID unknown"),
            other => panic!("expected IncompleteHttp(400), got {other:?}"),
        }
    }
}
