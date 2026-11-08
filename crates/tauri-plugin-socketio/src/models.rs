//! What crosses the IPC: connect options, payloads, states, link info and channel messages.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, Mutex};

use socketio_next::{
    ClientBuilder, ConnectParams, ConnectionState, LinkInfo, Payload, TransportType,
};

use crate::error::ErrorPayload;

/// Who may drive a registered client from the webview.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Access {
    /// The webview may emit, ack and disconnect (when its capabilities allow those commands).
    Full,
    /// The webview may only observe (subscribe, state, link info), whatever its capabilities
    /// say. For clients that Rust code owns.
    ReadOnly,
}

/// `transports` of the JS options.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Transports {
    /// Polling, upgraded to websocket when possible (socket.io-client's default).
    #[default]
    Any,
    Polling,
    Websocket,
    /// Polling, then the upgrade must succeed.
    WebsocketUpgrade,
}

/// The JS `connect` options (socket.io-client names; durations in milliseconds).
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ConnectOptions {
    pub url: String,
    pub namespace: Option<String>,
    pub path: Option<String>,
    #[serde(default)]
    pub query: BTreeMap<String, String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub auth: Option<Value>,
    #[serde(default)]
    pub transports: Transports,
    pub timeout: Option<u64>,
    pub reconnection: Option<bool>,
    pub reconnection_attempts: Option<u32>,
    pub reconnection_delay: Option<u64>,
    pub reconnection_delay_max: Option<u64>,
    pub randomization_factor: Option<f64>,
    pub reconnect_on_disconnect: Option<bool>,
    pub buffer_while_disconnected: Option<bool>,
    /// PEM certificates to trust instead of the default roots (a private CA).
    pub ca: Option<String>,
}

/// Query, header and auth values are secrets as often as not: only their keys are shown.
impl std::fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("namespace", &self.namespace)
            .field("path", &self.path)
            .field("query_keys", &self.query.keys().collect::<Vec<_>>())
            .field("header_keys", &self.headers.keys().collect::<Vec<_>>())
            .field("auth", &self.auth.as_ref().map(|_| "***"))
            .field("transports", &self.transports)
            .field("ca", &self.ca.is_some())
            .finish_non_exhaustive()
    }
}

/// Serde's message without the values it quotes (a token passed where a number belongs).
pub(crate) fn invalid(err: &serde_json::Error) -> crate::Error {
    let text = err.to_string();
    let mut out = String::with_capacity(text.len());
    let mut quoted = false;
    for c in text.chars() {
        match c {
            '"' if !quoted => {
                quoted = true;
                out.push_str("\"***");
            }
            '"' => {
                quoted = false;
                out.push('"');
            }
            _ if quoted => {}
            c => out.push(c),
        }
    }
    crate::Error::InvalidOptions(out)
}

impl ConnectOptions {
    /// From the webview's JSON; a bad field is [`Error::InvalidOptions`](crate::Error).
    pub(crate) fn parse(value: Value) -> crate::Result<Self> {
        let options: Self = serde_json::from_value(value).map_err(|e| invalid(&e))?;
        check_headers(&options.headers)?;
        Ok(options)
    }

    /// The builder, plus the values the webview may change later (`setAuth`, `setHeaders`,
    /// `setQuery`): they are read again before every connect attempt.
    pub(crate) fn into_builder(self) -> crate::Result<(ClientBuilder, Arc<Mutex<Dynamic>>)> {
        let ms = Duration::from_millis;
        let dynamic = Arc::new(Mutex::new(Dynamic {
            auth: self.auth,
            headers: self.headers,
            query: self.query,
        }));
        let mut b = ClientBuilder::new(self.url).transport_type(match self.transports {
            Transports::Any => TransportType::Any,
            Transports::Polling => TransportType::Polling,
            Transports::Websocket => TransportType::Websocket,
            Transports::WebsocketUpgrade => TransportType::WebsocketUpgrade,
        });
        if let Some(v) = self.namespace {
            b = b.namespace(v);
        }
        if let Some(v) = self.path {
            b = b.path(v);
        }
        let (auth, params) = (Arc::clone(&dynamic), Arc::clone(&dynamic));
        b = b
            .auth_with(move || {
                let auth = lock(&auth).auth.clone();
                async move { auth.unwrap_or_else(|| Value::Object(Default::default())) }
            })
            .connect_params_with(move || {
                let d = lock(&params);
                let mut p = ConnectParams::new();
                for (k, v) in &d.query {
                    p = p.query(k, v);
                }
                for (k, v) in &d.headers {
                    p = p.header(k, v);
                }
                async move { p }
            });
        if let Some(pem) = self.ca {
            let tls = socketio_next::TlsConfig::with_root_certificates_pem(pem.as_bytes())
                .map_err(|e| crate::Error::SocketIo(e.into()))?;
            #[cfg(any(feature = "rustls", feature = "native-tls"))]
            {
                b = b.tls_config(tls);
            }
            #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
            let _ = tls; // unreachable: without TLS the PEM is refused above
        }
        if let Some(v) = self.timeout {
            b = b.timeout(ms(v));
        }
        if let Some(v) = self.reconnection {
            b = b.reconnection(v);
        }
        if let Some(v) = self.reconnection_attempts {
            b = b.reconnection_attempts(v);
        }
        if let Some(v) = self.reconnection_delay {
            b = b.reconnection_delay(ms(v));
        }
        if let Some(v) = self.reconnection_delay_max {
            b = b.reconnection_delay_max(ms(v));
        }
        if let Some(v) = self.randomization_factor {
            b = b.randomization_factor(v);
        }
        if let Some(v) = self.reconnect_on_disconnect {
            b = b.reconnect_on_disconnect(v);
        }
        if let Some(v) = self.buffer_while_disconnected {
            b = b.buffer_while_disconnected(v);
        }
        Ok((b, dynamic))
    }
}

/// Headers a webview may not set: they belong to the HTTP / websocket machinery or to the
/// proxy, and cookies come from the jar. Rust's own clients are not limited.
const FORBIDDEN_HEADERS: &[&str] = &[
    "host",
    "connection",
    "upgrade",
    "content-length",
    "transfer-encoding",
    "cookie",
];

/// `InvalidOptions` naming the first forbidden header (never its value).
pub(crate) fn check_headers(headers: &BTreeMap<String, String>) -> crate::Result<()> {
    for name in headers.keys() {
        let lower = name.to_ascii_lowercase();
        if FORBIDDEN_HEADERS.contains(&lower.as_str())
            || lower.starts_with("sec-websocket-")
            || lower.starts_with("proxy-")
        {
            return Err(crate::Error::InvalidOptions(format!(
                "the header `{name}` may not be set from the webview"
            )));
        }
    }
    Ok(())
}

/// Connect values a webview may replace on a client it connected; used from the next attempt.
#[derive(Default)]
pub(crate) struct Dynamic {
    pub auth: Option<Value>,
    pub headers: BTreeMap<String, String>,
    pub query: BTreeMap<String, String>,
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Event arguments over the IPC: JSON `args`, and the binary attachments that the
/// `{"_placeholder":true,"num":n}` objects in `args` stand for (socket.io-parser's encoding).
/// Bytes travel as arrays of numbers; guest-js turns them into `Uint8Array`s and back.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Args {
    /// The JSON arguments, placeholders included.
    #[serde(default)]
    pub args: Vec<Value>,
    /// The attachments; empty when the arguments have no binary data.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binary: Vec<Vec<u8>>,
}

impl From<Payload> for Args {
    fn from(payload: Payload) -> Self {
        match payload {
            Payload::Text(args) => Args {
                args,
                binary: Vec::new(),
            },
            Payload::Binary(bytes) => Args {
                args: vec![Payload::placeholder(0)],
                binary: vec![bytes.to_vec()],
            },
            Payload::Mixed { args, attachments } => Args {
                args,
                binary: attachments.into_iter().map(|b| b.to_vec()).collect(),
            },
            // Payload is non_exhaustive: a future shape is forwarded by its JSON part
            other => Args {
                args: other.args().to_vec(),
                binary: other.binary().iter().map(|b| b.to_vec()).collect(),
            },
        }
    }
}

impl From<Args> for Payload {
    /// Without attachments placeholders are plain JSON; with them, the socket.io binary shape.
    fn from(args: Args) -> Self {
        if args.binary.is_empty() {
            Payload::Text(args.args)
        } else {
            Payload::Mixed {
                args: args.args,
                attachments: args.binary.into_iter().map(Into::into).collect(),
            }
        }
    }
}

/// [`ConnectionState`] for JS: `{ status, ... }` with socket.io-client's reason strings and
/// delays in milliseconds.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum State {
    /// A connect attempt runs (`attempt` 0 is the first connect).
    Connecting {
        /// 0, then 1, 2, ... per reconnect attempt.
        attempt: u32,
    },
    /// The namespace is joined.
    Connected {
        /// `socket.id`.
        sid: String,
        /// Connection state recovery restored the session.
        recovered: bool,
    },
    /// The connection was lost; a reconnect follows.
    Disconnected {
        /// `"transport close"`, `"ping timeout"`, ...
        reason: String,
        /// The error behind it (e.g. `WebsocketClosed` with the close code).
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<ErrorPayload>,
    },
    /// Waiting `delay` ms before reconnect attempt `attempt`.
    Reconnecting {
        /// 1, 2, ...
        attempt: u32,
        /// Milliseconds.
        delay: u64,
        /// Why the previous attempt failed.
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<ErrorPayload>,
    },
    /// Final.
    Closed {
        /// `"io client disconnect"`, `"connect rejected"`, `"reconnect failed"`, ...
        reason: String,
        /// The error behind it.
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<ErrorPayload>,
    },
}

impl From<&ConnectionState> for State {
    fn from(state: &ConnectionState) -> Self {
        let err = |e: &Option<socketio_next::Error>| e.as_ref().map(ErrorPayload::from_core);
        match state {
            ConnectionState::Connecting { attempt, .. } => State::Connecting { attempt: *attempt },
            ConnectionState::Connected { sid, recovered, .. } => State::Connected {
                sid: sid.clone(),
                recovered: *recovered,
            },
            ConnectionState::Disconnected { reason, error, .. } => State::Disconnected {
                reason: reason.as_str().to_owned(),
                error: err(error),
            },
            ConnectionState::Reconnecting {
                attempt,
                delay,
                error,
                ..
            } => State::Reconnecting {
                attempt: *attempt,
                delay: millis(*delay),
                error: err(error),
            },
            ConnectionState::Closed { reason, error, .. } => State::Closed {
                reason: reason.as_str().to_owned(),
                error: err(error),
            },
            // ConnectionState is non_exhaustive; a new state shows as a connect attempt
            _ => State::Connecting { attempt: 0 },
        }
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// [`LinkInfo`] for JS: durations in ms, instants as Unix epoch ms.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Link {
    /// `socket.id` while connected.
    pub sid: Option<String>,
    /// The engine.io session id while connected.
    pub engine_sid: Option<String>,
    /// `"polling"` or `"websocket"` while connected.
    pub transport: Option<&'static str>,
    /// pingInterval (ms) of the last handshake.
    pub ping_interval: Option<u64>,
    /// pingTimeout (ms) of the last handshake.
    pub ping_timeout: Option<u64>,
    /// maxPayload of the last handshake.
    pub max_payload: Option<u64>,
    /// When the current connection was established (epoch ms).
    pub connected_at: Option<u64>,
    /// When the last engine.io packet arrived (epoch ms).
    pub last_packet_at: Option<u64>,
    /// When the last ping arrived (epoch ms).
    pub last_ping_at: Option<u64>,
    /// When the last connect attempt started (epoch ms).
    pub last_attempt_at: Option<u64>,
    /// Reconnect attempts since the last connect.
    pub reconnect_attempts: u32,
    /// Acks waiting for the server.
    pub pending_acks: usize,
    /// Emits queued while disconnected.
    pub buffered_emits: usize,
    /// Engine.io message packets sent / received.
    pub packets_out: u64,
    /// See `packets_out`.
    pub packets_in: u64,
    /// Bytes of those packets sent / received.
    pub bytes_out: u64,
    /// See `bytes_out`.
    pub bytes_in: u64,
    /// Successful connects of this client (first one and reconnects).
    pub sessions: u32,
}

impl From<&LinkInfo> for Link {
    fn from(l: &LinkInfo) -> Self {
        let now = tokio::time::Instant::now();
        let wall = SystemTime::now();
        let epoch = |i: Option<tokio::time::Instant>| {
            i.and_then(|i| wall.checked_sub(now.saturating_duration_since(i)))
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(millis)
        };
        Link {
            sid: l.sid.clone(),
            engine_sid: l.engine_sid.clone(),
            transport: l.transport.map(|t| t.as_str()),
            ping_interval: l.ping_interval.map(millis),
            ping_timeout: l.ping_timeout.map(millis),
            max_payload: l.max_payload,
            connected_at: epoch(l.connected_at),
            last_packet_at: epoch(l.last_packet_at),
            last_ping_at: epoch(l.last_ping_at),
            last_attempt_at: epoch(l.last_attempt_at),
            reconnect_attempts: l.reconnect_attempts,
            pending_acks: l.pending_acks,
            buffered_emits: l.buffered_emits,
            packets_out: l.packets_out,
            packets_in: l.packets_in,
            bytes_out: l.bytes_out,
            bytes_in: l.bytes_in,
            sessions: l.sessions,
        }
    }
}

/// One registered client, as `list()` reports it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientInfo {
    /// The registry id.
    pub id: String,
    /// Who may drive it from the webview.
    pub access: Access,
    /// The namespace.
    pub namespace: String,
    /// The current state.
    pub state: State,
}

/// What `connect` and `subscribe` resolve with in JS.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Subscribed {
    pub subscription: u64,
    /// The snapshot also sent first on the channel.
    pub state: State,
}

/// What a subscription's channel carries.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Message {
    /// A state transition (the first message is the state at subscribe time).
    State {
        /// The new state.
        state: State,
        /// The state at subscribe time (the subscription's first state message); state
        /// messages queued before it are older.
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        snapshot: bool,
    },
    /// An event from the server.
    Event {
        /// The event name.
        event: String,
        /// Its arguments.
        #[serde(flatten)]
        args: Args,
        /// Set when the server wants an ack: answer with the `ack` command.
        #[serde(rename = "ackId", skip_serializing_if = "Option::is_none")]
        ack_id: Option<u64>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn payload_shapes_round_trip() {
        let text: Args = Payload::Text(vec![json!(1), json!("a")]).into();
        assert_eq!(
            serde_json::to_value(&text).unwrap(),
            json!({ "args": [1, "a"] })
        );
        let bin: Args = Payload::Binary(vec![1u8, 2].into()).into();
        assert_eq!(
            serde_json::to_value(&bin).unwrap(),
            json!({ "args": [{ "_placeholder": true, "num": 0 }], "binary": [[1, 2]] })
        );
        let back: Payload = bin.into();
        assert!(
            matches!(back, Payload::Mixed { ref attachments, .. } if attachments[0].as_ref() == [1, 2])
        );
        let plain: Payload =
            serde_json::from_value::<Args>(json!({ "args": [{ "_placeholder": true, "num": 0 }] }))
                .unwrap()
                .into();
        assert!(
            matches!(plain, Payload::Text(_)),
            "no attachments: placeholders stay JSON"
        );
    }

    /// The JS shape of states (the conversion from socketio-next's states is exercised by
    /// the IPC tests: reasons, delays, errors of real transitions).
    #[test]
    fn states_serialize_with_js_reasons() {
        let s = State::Disconnected {
            reason: "transport close".into(),
            error: None,
        };
        assert_eq!(
            serde_json::to_value(s).unwrap(),
            json!({ "status": "disconnected", "reason": "transport close" })
        );
        let s = State::Reconnecting {
            attempt: 2,
            delay: 1500,
            error: Some(ErrorPayload {
                kind: "NotConnected",
                message: "x".into(),
                data: None,
            }),
        };
        let v = serde_json::to_value(s).unwrap();
        assert_eq!(v["delay"], 1500);
        assert_eq!(v["error"]["kind"], "NotConnected");
    }

    #[test]
    fn event_message_shape() {
        let m = Message::Event {
            event: "e".into(),
            args: Args {
                args: vec![json!(1)],
                binary: vec![],
            },
            ack_id: Some(7),
        };
        assert_eq!(
            serde_json::to_value(m).unwrap(),
            json!({ "type": "event", "event": "e", "args": [1], "ackId": 7 })
        );
    }

    #[test]
    fn options_errors_are_typed_and_hide_values() {
        let err =
            ConnectOptions::parse(json!({ "url": "http://x", "timeout": "s3cr3t" })).unwrap_err();
        let crate::Error::InvalidOptions(text) = &err else {
            panic!("{err:?}")
        };
        assert!(!text.contains("s3cr3t") && text.contains("u64"), "{text}");
        assert!(ConnectOptions::parse(json!({ "url": "http://x", "timeout": 1.5 })).is_err());
        let opts = ConnectOptions::parse(json!({
            "url": "http://x?token=q", "auth": { "token": "a" }, "query": { "k": "qv" }, "headers": { "h": "hv" }
        }))
        .unwrap();
        let dbg = format!("{opts:?}");
        for secret in ["token=q", "\"a\"", "qv", "hv"] {
            assert!(!dbg.contains(secret), "{dbg}");
        }
    }

    #[test]
    fn webview_headers_are_limited() {
        for name in [
            "Host",
            "cookie",
            "Sec-WebSocket-Key",
            "Proxy-Authorization",
            "Upgrade",
        ] {
            let err =
                ConnectOptions::parse(json!({ "url": "http://x", "headers": { name: "s3cr3t" } }))
                    .err()
                    .unwrap_or_else(|| panic!("{name} accepted"));
            let text = err.to_string();
            assert!(text.contains(name) && !text.contains("s3cr3t"), "{text}");
        }
        assert!(
            ConnectOptions::parse(
                json!({ "url": "http://x", "headers": { "authorization": "Bearer x" } })
            )
            .is_ok()
        );
    }

    #[test]
    fn options_reject_unknown_fields() {
        let err = serde_json::from_value::<ConnectOptions>(
            json!({ "url": "http://x", "reconection": true }),
        )
        .unwrap_err();
        assert!(err.to_string().contains("reconection"));
        let ok: ConnectOptions =
            serde_json::from_value(json!({ "url": "http://x", "transports": "websocket-upgrade" }))
                .unwrap();
        assert!(matches!(ok.transports, Transports::WebsocketUpgrade));
    }
}
