use crate::error::{Error, Result};
use crate::tls::{self, apply_reqwest_tls, TlsConfig};
use crate::transport::Transport;
use base64::{engine::general_purpose, Engine as _};
use bytes::{BufMut, Bytes, BytesMut};
use reqwest::{
    blocking::{Client, ClientBuilder},
    header::HeaderMap,
};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone)]
pub struct PollingTransport {
    client: Arc<Client>,
    base_url: Arc<RwLock<Url>>,
}

impl PollingTransport {
    /// Creates an instance of `PollingTransport`.
    pub fn new(
        base_url: Url,
        tls_config: Option<TlsConfig>,
        opening_headers: Option<HeaderMap>,
    ) -> Result<Self> {
        tls::check_scheme(&base_url)?;
        let mut builder = apply_reqwest_tls!(ClientBuilder::new(), tls_config.as_ref());
        if let Some(map) = opening_headers {
            builder = builder.default_headers(map);
        }
        let client = builder.build()?;

        let mut url = base_url;
        url.query_pairs_mut().append_pair("transport", "polling");

        Ok(PollingTransport {
            client: Arc::new(client),
            base_url: Arc::new(RwLock::new(url)),
        })
    }
}

impl Transport for PollingTransport {
    fn emit(&self, data: Bytes, is_binary_att: bool) -> Result<()> {
        let data_to_send = if is_binary_att {
            // the binary attachment gets `base64` encoded
            let mut packet_bytes = BytesMut::with_capacity(data.len() + 1);
            packet_bytes.put_u8(b'b');

            let encoded_data = general_purpose::STANDARD.encode(data);
            packet_bytes.put(encoded_data.as_bytes());

            packet_bytes.freeze()
        } else {
            data
        };
        let status = self
            .client
            .post(self.address()?)
            .body(data_to_send)
            .send()?
            .status()
            .as_u16();

        if status != 200 {
            let error = Error::IncompleteHttp(status);
            return Err(error);
        }

        Ok(())
    }

    fn poll(&self, timeout: Duration) -> Result<Bytes> {
        let response = self.client.get(self.address()?).timeout(timeout).send()?;
        // An error status carries an error body, not a payload (same rule as the async transport).
        let status = response.status().as_u16();
        if status != 200 {
            return Err(Error::IncompleteHttp(status));
        }
        Ok(response.bytes()?)
    }

    fn base_url(&self) -> Result<Url> {
        Ok(self.base_url.read()?.clone())
    }

    fn set_base_url(&self, base_url: Url) -> Result<()> {
        let mut url = base_url;
        if !url
            .query_pairs()
            .any(|(k, v)| k == "transport" && v == "polling")
        {
            url.query_pairs_mut().append_pair("transport", "polling");
        }
        *self.base_url.write()? = url;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr;
    #[test]
    fn polling_transport_base_url() -> Result<()> {
        let url = crate::test::engine_io_server()?.to_string();
        let transport = PollingTransport::new(Url::from_str(&url[..]).unwrap(), None, None)?;
        assert_eq!(
            transport.base_url()?.to_string(),
            url.clone() + "?transport=polling"
        );
        transport.set_base_url(Url::parse("https://127.0.0.1")?)?;
        assert_eq!(
            transport.base_url()?.to_string(),
            "https://127.0.0.1/?transport=polling"
        );
        assert_ne!(transport.base_url()?.to_string(), url);

        transport.set_base_url(Url::parse("http://127.0.0.1/?transport=polling")?)?;
        assert_eq!(
            transport.base_url()?.to_string(),
            "http://127.0.0.1/?transport=polling"
        );
        assert_ne!(transport.base_url()?.to_string(), url);
        Ok(())
    }

    #[test]
    fn transport_debug() -> Result<()> {
        let mut url = crate::test::engine_io_server()?;
        let transport =
            PollingTransport::new(Url::from_str(&url.to_string()[..]).unwrap(), None, None)?;
        url.query_pairs_mut().append_pair("transport", "polling");
        assert_eq!(format!("PollingTransport {{ client: {:?}, base_url: RwLock {{ data: {:?}, poisoned: false, .. }} }}", transport.client, url), format!("{:?}", transport));
        let test: Box<dyn Transport> = Box::new(transport);
        assert_eq!(
            format!("Transport(base_url: Ok({:?}))", url),
            format!("{:?}", test)
        );
        Ok(())
    }
}
