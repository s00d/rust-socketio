/// Flavor of Engine.IO transport.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum TransportType {
    /// Handshakes with polling, upgrades if possible
    #[default]
    Any,
    /// Handshakes with websocket. Does not use polling.
    Websocket,
    /// Handshakes with polling, errors if upgrade fails
    WebsocketUpgrade,
    /// Handshakes with polling
    Polling,
}
