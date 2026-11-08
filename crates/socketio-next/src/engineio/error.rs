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
        /// What the body says: engine.io's `{"code":..,"message":..}` message, or the start of
        /// a text body (at most 200 characters, URLs replaced by `<url>`).
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

/// The message of a non-200 polling body (read up to 4 KiB): engine.io's JSON `message`, else
/// the text. Either way at most 200 characters, and words that may carry a secret are replaced
/// (a proxy or server page may echo the request line with its query).
pub(crate) fn body_message(body: &[u8]) -> Option<String> {
    let raw = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|json| json.get("message")?.as_str().map(str::to_owned))
        .unwrap_or_else(|| String::from_utf8_lossy(body).into_owned());
    let text = scrub(&raw);
    let text: String = text.chars().take(200).collect();
    (!text.is_empty()).then_some(text)
}

/// Words that name a credential on their own (`password: x`, `token x`): the next word is a
/// secret. Ambiguous ones ("session", "key", "auth") only count in a `name=value` word.
const CREDENTIAL_NAMES: &[&str] = &[
    "token",
    "access_token",
    "refresh_token",
    "id_token",
    "apikey",
    "api_key",
    "api-key",
    "x-api-key",
    "secret",
    "client_secret",
    "password",
    "passwd",
    "pwd",
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "jwt",
    "bearer",
    "basic",
];

/// Credential names that may appear as part of a `name=value` key (`apiKey=`, `x_token=`).
const CREDENTIAL_PARTS: &[&str] = &[
    "token", "key", "secret", "password", "passwd", "auth", "session", "jwt",
];

fn is_credential_name(word: &str) -> bool {
    let name = word
        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
        .to_ascii_lowercase();
    CREDENTIAL_NAMES.contains(&name.as_str())
}

/// `<url>` for URLs, `<redacted>` for paths, queries and the values of credentials
/// (`name=value`, `name: value`, `name value`, `Bearer value`); other words stay
/// ("Unauthorized", "Forbidden").
fn scrub(text: &str) -> String {
    let words: Vec<&str> = text.split_whitespace().collect();
    let mut out: Vec<String> = Vec::with_capacity(words.len());
    let mut redact_next = 0usize;
    for word in words {
        if redact_next > 0 {
            redact_next -= 1;
            out.push("<redacted>".into());
            // `Authorization: Bearer xyz`: the scheme is followed by the secret
            if is_credential_name(word)
                && matches!(word.to_ascii_lowercase().as_str(), "bearer" | "basic")
            {
                redact_next = 1;
            }
            continue;
        }
        let lower = word.to_ascii_lowercase();
        if word.contains("://") {
            out.push("<url>".into());
        } else if word.starts_with('/')
            || word.starts_with('?')
            || ((lower.contains('?') || lower.contains('&')) && lower.contains('='))
        {
            // a path or a query
            out.push("<redacted>".into());
        } else if let Some((name, _)) = word.split_once(['=', ':']).filter(|(_, v)| !v.is_empty()) {
            // `name=value` / `name:value` in one word
            let lname = name.to_ascii_lowercase();
            if is_credential_name(name) || CREDENTIAL_PARTS.iter().any(|p| lname.contains(p)) {
                out.push(format!("{name}=<redacted>"));
            } else {
                out.push(word.to_owned());
            }
        } else if is_credential_name(word) {
            // `password: hunter2`, `token abc`, `Bearer eyJ...`
            out.push(word.to_owned());
            redact_next = 1;
        } else {
            out.push(word.to_owned());
        }
    }
    out.join(" ")
}

/// Result alias of this crate.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_messages() {
        assert_eq!(
            body_message(br#"{"code":1,"message":"Session ID unknown"}"#).as_deref(),
            Some("Session ID unknown")
        );
        assert_eq!(
            body_message(b"<h1>Forbidden</h1> see https://proxy/x?token=s").as_deref(),
            Some("<h1>Forbidden</h1> see <url>")
        );
        // an echoed request line, in a text body and in the JSON message, and credential
        // values in every usual form
        for body in [
            &b"Cannot GET /socket.io/?EIO=4&token=SECRET"[..],
            br#"{"code":3,"message":"Bad request ?sid=x&token=SECRET"}"#,
            b"denied: apiKey=SECRET",
            b"x a&b=SECRET",
            b"Authorization: Bearer SECRET",
            b"password: SECRET please",
            b"token SECRET",
            b"bad credentials (password=SECRET)",
            b"Proxy-Authorization: Basic SECRET",
        ] {
            let message = body_message(body).unwrap();
            assert!(!message.contains("SECRET"), "{message}");
        }
        // ordinary words stay
        for (body, kept) in [
            (&b"Unauthorized"[..], "Unauthorized"),
            (
                b"Forbidden: authentication required",
                "Forbidden: authentication required",
            ),
            (br#"{"message":"Session ID unknown"}"#, "Session ID unknown"),
            (b"Bad handshake method", "Bad handshake method"),
        ] {
            assert_eq!(body_message(body).as_deref(), Some(kept));
        }
        let long_json = format!(r#"{{"message":"{}"}}"#, "a".repeat(500));
        assert_eq!(body_message(long_json.as_bytes()).unwrap().len(), 200);
        assert_eq!(body_message(b"  "), None);
        let long = body_message(&[b'a'; 500]).unwrap();
        assert_eq!(long.len(), 200);
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
