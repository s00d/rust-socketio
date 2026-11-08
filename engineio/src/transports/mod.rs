mod polling;
mod websocket;

pub use self::polling::PollingTransport;
pub use self::websocket::WebsocketTransport;

/// `ws` and `wss` are one transport now; the scheme follows the URL.
#[deprecated(
    since = "0.7.0",
    note = "use WebsocketTransport, it handles wss:// too"
)]
pub type WebsocketSecureTransport = WebsocketTransport;
