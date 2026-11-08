use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use url::Url;

use crate::engineio::error::Result;
use crate::engineio::transports::{PollingTransport, WebsocketTransport};

/// Which transport a session runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TransportKind {
    /// HTTP long polling.
    Polling,
    /// Websocket (`ws://` or `wss://`).
    Websocket,
}

impl TransportKind {
    /// The engine.io name (`polling`, `websocket`).
    pub fn as_str(self) -> &'static str {
        match self {
            TransportKind::Polling => "polling",
            TransportKind::Websocket => "websocket",
        }
    }

    pub(crate) fn is_polling(self) -> bool {
        self == TransportKind::Polling
    }
}

impl fmt::Display for TransportKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One unit read from a transport: a polling body or a websocket text frame (encoded
/// packets), or a websocket binary frame (raw data of one binary message).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Frame {
    /// Encoded engine.io packet(s).
    Text(Bytes),
    /// The data of one binary message (websocket only).
    Binary(Bytes),
}

impl From<Bytes> for Frame {
    fn from(bytes: Bytes) -> Self {
        Frame::Text(bytes)
    }
}

/// The transport of an engine.io session (enum dispatch, no boxed trait objects).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Transport {
    /// HTTP long polling.
    Polling(PollingTransport),
    /// Websocket.
    Websocket(WebsocketTransport),
}

impl From<PollingTransport> for Transport {
    fn from(transport: PollingTransport) -> Self {
        Transport::Polling(transport)
    }
}

impl From<WebsocketTransport> for Transport {
    fn from(transport: WebsocketTransport) -> Self {
        Transport::Websocket(transport)
    }
}

impl Transport {
    /// Sends one encoded packet; `is_binary_att` marks raw binary data.
    pub async fn emit(&self, data: Bytes, is_binary_att: bool) -> Result<()> {
        match self {
            Transport::Polling(t) => t.emit(data, is_binary_att).await,
            Transport::Websocket(t) => t.emit(data, is_binary_att).await,
        }
    }

    /// The transport URL (with `EIO`, `transport` and, after the handshake, `sid`).
    pub async fn base_url(&self) -> Result<Url> {
        match self {
            Transport::Polling(t) => t.base_url().await,
            Transport::Websocket(t) => t.base_url().await,
        }
    }

    /// Replaces the transport URL.
    pub async fn set_base_url(&self, url: Url) -> Result<()> {
        match self {
            Transport::Polling(t) => t.set_base_url(url).await,
            Transport::Websocket(t) => t.set_base_url(url).await,
        }
    }

    /// Polling or websocket.
    pub fn kind(&self) -> TransportKind {
        match self {
            Transport::Polling(_) => TransportKind::Polling,
            Transport::Websocket(_) => TransportKind::Websocket,
        }
    }
}

impl Stream for Transport {
    type Item = Result<Frame>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Transport::Polling(t) => t
                .poll_next_unpin(cx)
                .map(|item| item.map(|r| r.map(Frame::Text))),
            Transport::Websocket(t) => t.poll_next_unpin(cx),
        }
    }
}
