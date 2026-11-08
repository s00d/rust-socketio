use std::sync::Arc;
use std::time::Duration;

use tokio_tungstenite::tungstenite;

/// Everything that can go wrong in the engine.io client.
///
/// Errors never contain the request URL: query strings often carry tokens, so `reqwest`
/// errors are stored without their URL.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A packet broke the engine.io wire format.
    #[error(transparent)]
    Protocol(#[from] crate::protocol::Error),
    /// The server URL does not parse.
    #[error("invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
    /// The URL scheme is not http, https, ws or wss.
    #[error("unsupported URL scheme `{0}` (expected http, https, ws or wss)")]
    InvalidUrlScheme(String),
    /// An HTTP request (polling transport) failed: connection refused, DNS, TLS, timeout, ...
    #[error("HTTP request failed: {0}")]
    Http(#[source] Arc<reqwest::Error>),
    /// The server answered a polling request with a non-200 status (400 "Session ID unknown"
    /// after a server restart, 403 from a proxy, ...).
    #[error("server answered HTTP {status}")]
    HttpStatus {
        /// The HTTP status code.
        status: u16,
    },
    /// The websocket failed to open or broke.
    #[error("websocket error: {0}")]
    Websocket(#[source] Arc<tungstenite::Error>),
    /// The handshake (`0{...}` open packet) is missing or malformed. An Engine.IO v3 server
    /// answers in a format this v4 client does not speak.
    #[error("invalid engine.io handshake: {0}")]
    InvalidHandshake(String),
    /// `emit` before `connect` or after the connection closed.
    #[error("the engine.io connection is not open")]
    NotConnected,
    /// The handshake did not offer `websocket` in `upgrades` (server or proxy allows polling only).
    #[error("the server does not offer a websocket upgrade")]
    UpgradeUnavailable,
    /// The websocket upgrade probe (`2probe` / `3probe`) failed.
    #[error("websocket upgrade probe failed: {0}")]
    UpgradeFailed(&'static str),
    /// No ping from the server within pingInterval + pingTimeout: the link is dead.
    #[error("no ping from the server within pingInterval + pingTimeout")]
    PingTimeout,
    /// The handshake or websocket connect took longer than the timeout.
    #[error("timed out after {0:?}")]
    Timeout(Duration),
    /// `https`/`wss` without a TLS feature.
    #[error(
        "`{0}` needs TLS, but socketio-next was built without the `rustls` or `native-tls` feature"
    )]
    TlsNotEnabled(String),
    /// Another crate in the build turned on reqwest's `rustls-no-provider` feature (e.g.
    /// `tauri-plugin-updater`), so reqwest wants a rustls crypto provider that nobody installed.
    #[error(
        "reqwest was built with rustls by another crate but has no crypto provider: enable the `rustls` feature of socketio-next or install a rustls CryptoProvider before connecting"
    )]
    TlsProviderMissing,
    /// The TLS configuration could not be built (bad PEM, unsupported protocol versions).
    #[error("invalid TLS configuration: {0}")]
    InvalidTlsConfig(String),
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::Http(Arc::new(err.without_url()))
    }
}

impl From<tungstenite::Error> for Error {
    fn from(err: tungstenite::Error) -> Self {
        Error::Websocket(Arc::new(err))
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Error::Protocol(err.into())
    }
}

/// Result alias of this crate.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn http_errors_drop_the_url() {
        // nothing listens on port 9 of localhost
        let client = crate::engineio::tls::apply_reqwest_tls!(reqwest::Client::builder(), None)
            .build()
            .unwrap();
        let err = client
            .get("http://127.0.0.1:9/engine.io/?token=SECRET")
            .send()
            .await
            .unwrap_err();
        let err = Error::from(err);
        let text = format!("{err} {err:?}");
        assert!(!text.contains("SECRET"), "{text}");
    }
}
