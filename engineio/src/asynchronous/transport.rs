use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use url::Url;

use super::async_transports::{PollingTransport, WebsocketTransport};
use crate::error::Result;

/// The transport of an async engine.io socket (enum dispatch, no boxed trait objects).
#[derive(Debug, Clone)]
pub enum AsyncTransportType {
    Polling(PollingTransport),
    Websocket(WebsocketTransport),
}

impl From<PollingTransport> for AsyncTransportType {
    fn from(transport: PollingTransport) -> Self {
        AsyncTransportType::Polling(transport)
    }
}

impl From<WebsocketTransport> for AsyncTransportType {
    fn from(transport: WebsocketTransport) -> Self {
        AsyncTransportType::Websocket(transport)
    }
}

impl AsyncTransportType {
    /// Sends one encoded packet; `is_binary_att` marks raw binary data.
    pub async fn emit(&self, data: Bytes, is_binary_att: bool) -> Result<()> {
        match self {
            AsyncTransportType::Polling(t) => t.emit(data, is_binary_att).await,
            AsyncTransportType::Websocket(t) => t.emit(data, is_binary_att).await,
        }
    }

    /// The transport URL (with `EIO`, `transport` and, after the handshake, `sid`).
    pub async fn base_url(&self) -> Result<Url> {
        match self {
            AsyncTransportType::Polling(t) => t.base_url().await,
            AsyncTransportType::Websocket(t) => t.base_url().await,
        }
    }

    pub async fn set_base_url(&self, url: Url) -> Result<()> {
        match self {
            AsyncTransportType::Polling(t) => t.set_base_url(url).await,
            AsyncTransportType::Websocket(t) => t.set_base_url(url).await,
        }
    }

    pub(crate) fn is_polling(&self) -> bool {
        matches!(self, AsyncTransportType::Polling(_))
    }
}

impl Stream for AsyncTransportType {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            AsyncTransportType::Polling(t) => t.poll_next_unpin(cx),
            AsyncTransportType::Websocket(t) => t.poll_next_unpin(cx),
        }
    }
}
