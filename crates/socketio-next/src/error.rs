use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::{engineio, protocol};

/// Everything that can go wrong in the socket.io client.
///
/// Typed so callers never have to match on message text. Messages say what happened and what
/// to check; they never contain query values (tokens) of the server URL.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The server URL does not parse.
    #[error("invalid server URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
    /// The engine.io layer failed: connection refused, DNS, TLS, HTTP status, websocket,
    /// handshake, ping timeout. [`engineio::Error`] has the details.
    #[error(transparent)]
    Engine(#[from] engineio::Error),
    /// The server sent a packet that breaks the socket.io protocol.
    #[error(transparent)]
    Protocol(#[from] protocol::Error),
    /// The connect attempt (engine.io handshake, upgrade and namespace CONNECT) took longer
    /// than the builder's `timeout`.
    #[error(
        "connect attempt timed out after {0:?} (engine.io handshake + namespace CONNECT); check the URL, the path and that a socket.io v3+ server listens there"
    )]
    ConnectTimeout(Duration),
    /// The server refused the namespace (CONNECT_ERROR): a middleware rejected the auth, or the
    /// namespace does not exist. socket.io-client's `connect_error` with `err.message` /
    /// `err.data`. The client does not reconnect after this.
    #[error("the server rejected the namespace: {message}")]
    ConnectRejected {
        /// The error message from the server middleware (`next(new Error(message))`).
        message: String,
        /// `err.data` from the server middleware, if any.
        data: Option<Value>,
    },
    /// The server answered CONNECT without a session id: a Socket.IO v2 server, which this
    /// v3+/v4 client cannot talk to.
    #[error(
        "the server answered CONNECT without a sid: it looks like a Socket.IO v2 server, which a v4 client cannot use"
    )]
    IncompatibleServer,
    /// `emit` while not connected, without `buffer_while_disconnected`.
    #[error("not connected: the emit was dropped (enable buffer_while_disconnected to queue it)")]
    NotConnected,
    /// The client was closed (`disconnect()`, a rejection, reconnect attempts exhausted).
    #[error("the client is closed")]
    Closed,
    /// No ack within the timeout given to `emit_with_ack`.
    #[error("no ack from the server within {0:?}")]
    AckTimeout(Duration),
    /// The connection was lost while an ack was pending.
    #[error("the connection was lost before the ack arrived")]
    Disconnected,
    /// The event name is reserved (`connect`, `connect_error`, `disconnect`, `disconnecting`,
    /// `newListener`, `removeListener`): socket.io-client throws on these too.
    #[error("`{0}` is a reserved event name")]
    ReservedEvent(String),
    /// A value could not be converted to or from JSON.
    #[error("JSON conversion failed: {0}")]
    Json(#[source] Arc<serde_json::Error>),
    /// A header name or value is not valid HTTP.
    #[error("invalid HTTP header: {0}")]
    InvalidHeader(String),
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Json(Arc::new(err))
    }
}

/// Result alias of this crate.
pub type Result<T> = std::result::Result<T, Error>;
