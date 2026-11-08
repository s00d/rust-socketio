use std::fmt;
use std::time::Duration;

use tokio::time::Instant;

use crate::Error;
use crate::engineio::TransportKind;

/// Why a connection ended. The first six are socket.io-client's `disconnect` reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// The server disconnected the socket (`socket.disconnect()` on the server). No reconnect
    /// unless `reconnect_on_disconnect` is on.
    IoServerDisconnect,
    /// [`Client::disconnect`](crate::Client::disconnect) was called.
    IoClientDisconnect,
    /// No ping from the server within pingInterval + pingTimeout.
    PingTimeout,
    /// The connection was closed (the server went away, the network dropped it).
    TransportClose,
    /// The transport failed (HTTP error, websocket error, a write that failed).
    TransportError,
    /// The server sent a packet that does not decode.
    ParseError,
    /// The server refused the namespace (CONNECT_ERROR). Final.
    ConnectRejected,
    /// The reconnect attempts ran out (`reconnection_attempts`). Final.
    ReconnectFailed,
    /// Every [`Client`](crate::Client) handle was dropped. Final.
    Dropped,
}

impl DisconnectReason {
    /// The socket.io-client string (`"io server disconnect"`, `"ping timeout"`, ...).
    pub fn as_str(self) -> &'static str {
        match self {
            DisconnectReason::IoServerDisconnect => "io server disconnect",
            DisconnectReason::IoClientDisconnect => "io client disconnect",
            DisconnectReason::PingTimeout => "ping timeout",
            DisconnectReason::TransportClose => "transport close",
            DisconnectReason::TransportError => "transport error",
            DisconnectReason::ParseError => "parse error",
            DisconnectReason::ConnectRejected => "connect rejected",
            DisconnectReason::ReconnectFailed => "reconnect failed",
            DisconnectReason::Dropped => "client dropped",
        }
    }
}

impl fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The connection lifecycle, published on every transition.
///
/// A network loss goes `Connected` -> `Disconnected` -> `Reconnecting` -> `Connecting` ->
/// (`Reconnecting` -> `Connecting` per failed attempt) -> `Connected`. `Closed` is final.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ConnectionState {
    /// A connect attempt is running (`attempt` 0 is the initial connect).
    #[non_exhaustive]
    Connecting {
        /// 0 for the initial connect, then 1, 2, ... per reconnect attempt.
        attempt: u32,
    },
    /// The namespace is joined.
    #[non_exhaustive]
    Connected {
        /// The socket id the server assigned (`socket.id`).
        sid: String,
        /// Connection state recovery restored the session (`socket.recovered`).
        recovered: bool,
    },
    /// The connection was lost; a reconnect follows.
    #[non_exhaustive]
    Disconnected {
        /// Why.
        reason: DisconnectReason,
        /// The error behind it (`WebsocketClosed { code, reason }`, `PingTimeout`, ...).
        error: Option<Error>,
    },
    /// Waiting `delay` before reconnect attempt `attempt`.
    #[non_exhaustive]
    Reconnecting {
        /// 1, 2, ... (socket.io-client `reconnect_attempt`).
        attempt: u32,
        /// The backoff delay before this attempt.
        delay: Duration,
        /// Why the previous attempt failed (`None` before the first attempt).
        error: Option<Error>,
    },
    /// Final: no more connects or events.
    #[non_exhaustive]
    Closed {
        /// Why.
        reason: DisconnectReason,
        /// The error behind it (rejection, last failed attempt).
        error: Option<Error>,
    },
}

impl ConnectionState {
    /// The namespace is joined.
    pub fn is_connected(&self) -> bool {
        matches!(self, ConnectionState::Connected { .. })
    }

    /// Final state.
    pub fn is_closed(&self) -> bool {
        matches!(self, ConnectionState::Closed { .. })
    }
}

/// A snapshot of the link for health checks and debugging.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct LinkInfo {
    /// The socket id (`socket.id`) while connected.
    pub sid: Option<String>,
    /// The engine.io session id while connected.
    pub engine_sid: Option<String>,
    /// The transport in use while connected.
    pub transport: Option<TransportKind>,
    /// pingInterval from the last handshake.
    pub ping_interval: Option<Duration>,
    /// pingTimeout from the last handshake.
    pub ping_timeout: Option<Duration>,
    /// maxPayload from the last handshake.
    pub max_payload: Option<u64>,
    /// When the current connection was established.
    pub connected_at: Option<Instant>,
    /// When the last engine.io packet of any kind arrived.
    pub last_packet_at: Option<Instant>,
    /// When the last ping arrived.
    pub last_ping_at: Option<Instant>,
    /// When the last connect attempt started.
    pub last_attempt_at: Option<Instant>,
    /// Reconnect attempts since the last successful connect.
    pub reconnect_attempts: u32,
    /// Acks waiting for the server.
    pub pending_acks: usize,
    /// Emits queued while disconnected (`buffer_while_disconnected`).
    pub buffered_emits: usize,
    /// Engine.io message packets sent (socket.io packets and their binary attachments).
    pub packets_out: u64,
    /// Engine.io message packets received.
    pub packets_in: u64,
    /// Bytes of those packets sent (encoded socket.io text plus raw attachments).
    pub bytes_out: u64,
    /// Bytes of those packets received.
    pub bytes_in: u64,
    /// Successful connects (the first one and every reconnect) of this client.
    pub sessions: u32,
}
