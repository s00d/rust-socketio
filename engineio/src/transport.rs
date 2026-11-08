use super::transports::{PollingTransport, WebsocketTransport};
use crate::error::Result;
use bytes::Bytes;
use std::time::Duration;
use url::Url;

pub trait Transport {
    /// Sends a packet to the server. This optionally handles sending of a
    /// socketio binary attachment via the boolean attribute `is_binary_att`.
    fn emit(&self, data: Bytes, is_binary_att: bool) -> Result<()>;

    /// Performs the server long polling procedure as long as the client is
    /// connected. This should run separately at all time to ensure proper
    /// response handling from the server.
    fn poll(&self, timeout: Duration) -> Result<Bytes>;

    /// Returns start of the url. ex. http://localhost:2998/engine.io/?EIO=4&transport=polling
    /// Must have EIO and transport already set.
    fn base_url(&self) -> Result<Url>;

    /// Used to update the base path, like when adding the sid.
    fn set_base_url(&self, base_url: Url) -> Result<()>;

    /// Full query address
    fn address(&self) -> Result<Url> {
        Ok(crate::util::with_timestamp(self.base_url()?))
    }
}

#[derive(Debug)]
pub enum TransportType {
    Polling(PollingTransport),
    Websocket(WebsocketTransport),
}

impl From<PollingTransport> for TransportType {
    fn from(transport: PollingTransport) -> Self {
        TransportType::Polling(transport)
    }
}

impl From<WebsocketTransport> for TransportType {
    fn from(transport: WebsocketTransport) -> Self {
        TransportType::Websocket(transport)
    }
}

impl TransportType {
    pub fn as_transport(&self) -> &dyn Transport {
        match self {
            TransportType::Polling(transport) => transport,
            TransportType::Websocket(transport) => transport,
        }
    }
}

impl std::fmt::Debug for dyn Transport {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_fmt(format_args!("Transport(base_url: {:?})", self.base_url(),))
    }
}
