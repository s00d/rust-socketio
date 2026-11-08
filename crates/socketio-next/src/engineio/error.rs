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
    #[error("server answered HTTP {status}{}", message.as_ref().map(|m| format!(": {m}")).unwrap_or_default())]
    #[non_exhaustive]
    HttpStatus {
        /// The HTTP status code.
        status: u16,
        /// engine.io's error message when the body is its `{"code": n, "message": ".."}` JSON
        /// (at most 200 characters, URLs and queries removed); `None` for any other body.
        message: Option<String>,
    },
    /// The HTTP proxy refused or broke the `CONNECT` tunnel (never shows the proxy URL).
    #[error("proxy: {0}")]
    ProxyFailed(String),
    /// The server closed the websocket with a close frame (code and reason as sent).
    #[error("the server closed the websocket (code {code}{})", if reason.is_empty() { String::new() } else { format!(": {reason}") })]
    #[non_exhaustive]
    WebsocketClosed {
        /// The close code (1000 normal, 1001 going away, 4xxx application codes, ...).
        code: u16,
        /// The close reason, possibly empty.
        reason: String,
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
        match err {
            // a refused handshake (3xx, 4xx): its response may carry a `Location` with the
            // query; only the status is kept
            tungstenite::Error::Http(response) => Error::HttpStatus {
                status: response.status().as_u16(),
                message: None,
            },
            err => Error::Websocket(Arc::new(err)),
        }
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Error::Protocol(err.into())
    }
}

/// The message of a non-200 polling body (read up to 4 KiB). Only engine.io's own error
/// JSON counts: an object with a numeric `code` and a string `message`; that message, at most
/// 200 characters, with URL-, path- and query-like words removed. Any other body (an HTML
/// page, text, other JSON) is `None`: it may echo the request, a header or a token in forms no
/// filter can know.
pub(crate) fn body_message(body: &[u8]) -> Option<String> {
    let json: serde_json::Value = serde_json::from_slice(body).ok()?;
    json.get("code")?.as_u64()?;
    let message = json.get("message")?.as_str()?;
    let text = message
        .split_whitespace()
        .map(|word| {
            let lower = word.to_ascii_lowercase();
            if word.contains("://") {
                "<url>"
            } else if word.starts_with('/')
                || word.starts_with('?')
                || ((lower.contains('?') || lower.contains('&')) && lower.contains('='))
            {
                "<redacted>"
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    let text: String = text.chars().take(200).collect();
    (!text.is_empty()).then_some(text)
}

/// Result alias of this crate.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_messages() {
        // engine.io's error JSON: its message is kept (URLs and queries in it removed)
        for (body, kept) in [
            (
                &br#"{"code":1,"message":"Session ID unknown"}"#[..],
                "Session ID unknown",
            ),
            (br#"{"code":4,"message":"Forbidden"}"#, "Forbidden"),
            (br#"{"code":4,"message":"token expired"}"#, "token expired"),
            (
                br#"{"code":4,"message":"denied by middleware: try again later"}"#,
                "denied by middleware: try again later",
            ),
            (
                br#"{"code":3,"message":"Bad request ?sid=x&token=S see https://h/?t=S"}"#,
                "Bad request <redacted> see <url>",
            ),
        ] {
            assert_eq!(body_message(body).as_deref(), Some(kept), "{body:?}");
        }
        let long = format!(r#"{{"code":1,"message":"{}"}}"#, "a".repeat(500));
        assert_eq!(body_message(long.as_bytes()).unwrap().len(), 200);
        // anything else says nothing: no filter knows every way a body can carry a secret
        for body in [
            &b"X-Auth-Token: S"[..],
            b"access-token: S",
            b"Authorization: Token S",
            b"Authorization: ApiKey S",
            b"Authorization: Digest username=\"u\", response=\"S\"",
            br#"{"error":"denied","token":"S"}"#,
            br#"{"message":"no code here","token":"S"}"#,
            br#"{"code":"1","message":"string code"}"#,
            b"token%3DS",
            b"token = S",
            b"password : S",
            b"Cookie: a=b; sid=S",
            b"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.S",
            b"<b>password</b> S",
            b"Cannot GET /socket.io/?EIO=4&token=S",
            b"<h1>Forbidden</h1>",
            b"  ",
        ] {
            assert_eq!(body_message(body), None, "{body:?}");
        }
        let e = Error::HttpStatus {
            status: 400,
            message: Some("Session ID unknown".into()),
        };
        assert_eq!(
            e.to_string(),
            "server answered HTTP 400: Session ID unknown"
        );
        let e = Error::WebsocketClosed {
            code: 4001,
            reason: "bye".into(),
        };
        assert_eq!(
            e.to_string(),
            "the server closed the websocket (code 4001: bye)"
        );
    }

    #[tokio::test]
    async fn http_errors_drop_the_url() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
        })
        .await
        .expect("test `http_errors_drop_the_url` timed out")
    }
}
