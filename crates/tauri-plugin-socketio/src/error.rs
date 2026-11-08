use serde::{Serialize, Serializer};
use serde_json::{Value, json};
use socketio_next::engineio;

/// Errors of the plugin. Over the IPC they become `{ kind, message, data? }` (see
/// [`ErrorPayload`]); guest-js throws them as `SocketIoError`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The socket.io client failed (connect, emit, ack timeout, ...).
    #[error(transparent)]
    SocketIo(#[from] socketio_next::Error),
    /// No client is registered under this id.
    #[error("no socket.io client with id `{0}`")]
    UnknownClient(String),
    /// A client is registered under this id already (remove it first).
    #[error("a socket.io client with id `{0}` is registered already")]
    AlreadyExists(String),
    /// The client is [`Access::ReadOnly`](crate::Access::ReadOnly): the webview may not emit,
    /// ack or disconnect it.
    #[error("socket.io client `{0}` is read-only for the webview")]
    AccessDenied(String),
    /// The server ack was answered already, expired, or belongs to another client.
    #[error("no pending server ack {0} for this client (answered, expired or unknown)")]
    UnknownAck(u64),
}

/// Result alias of the plugin.
pub type Result<T> = std::result::Result<T, Error>;

/// An error as JS sees it: `kind` is the variant name (`ConnectRejected`, `NotConnected`,
/// `AckTimeout`, `ConnectTimeout`, `AccessDenied`, `UnknownClient`, ..., engine.io failures by
/// their own name such as `HttpStatus` or `TlsProviderMissing`), `message` the text (never a
/// URL or query value), `data` extra fields (`ConnectRejected`: the server's `err.data`;
/// `HttpStatus`: `{ status }`).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorPayload {
    /// The variant name.
    pub kind: &'static str,
    /// The message.
    pub message: String,
    /// Extra data, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl ErrorPayload {
    pub(crate) fn from_core(err: &socketio_next::Error) -> Self {
        use socketio_next::Error as E;
        let mut data = None;
        let kind = match err {
            E::InvalidUrl(_) => "InvalidUrl",
            E::Engine(e) => engine_kind(e, &mut data),
            E::Protocol(_) => "Protocol",
            E::ConnectTimeout(_) => "ConnectTimeout",
            E::ConnectRejected { data: d, .. } => {
                data.clone_from(d);
                "ConnectRejected"
            }
            E::IncompatibleServer => "IncompatibleServer",
            E::InvalidAuth(_) => "InvalidAuth",
            E::NotConnected => "NotConnected",
            E::Closed => "Closed",
            E::AckTimeout(_) => "AckTimeout",
            E::Disconnected => "Disconnected",
            E::ReservedEvent(_) => "ReservedEvent",
            E::Json(_) => "Json",
            E::InvalidHeader(_) => "InvalidHeader",
            _ => "SocketIo",
        };
        ErrorPayload {
            kind,
            message: err.to_string(),
            data,
        }
    }
}

fn engine_kind(err: &engineio::Error, data: &mut Option<Value>) -> &'static str {
    use engineio::Error as E;
    match err {
        E::Protocol(_) => "Protocol",
        E::InvalidUrl(_) => "InvalidUrl",
        E::InvalidUrlScheme(_) => "InvalidUrlScheme",
        E::Http(_) => "Http",
        E::HttpStatus { status } => {
            *data = Some(json!({ "status": status }));
            "HttpStatus"
        }
        E::Websocket(_) => "Websocket",
        E::InvalidHandshake(_) => "InvalidHandshake",
        E::NotConnected => "NotConnected",
        E::UpgradeUnavailable => "UpgradeUnavailable",
        E::UpgradeFailed(_) => "UpgradeFailed",
        E::PingTimeout => "PingTimeout",
        E::Timeout(_) => "Timeout",
        E::TlsNotEnabled(_) => "TlsNotEnabled",
        E::TlsProviderMissing => "TlsProviderMissing",
        E::InvalidTlsConfig(_) => "InvalidTlsConfig",
        _ => "Engine",
    }
}

impl From<&Error> for ErrorPayload {
    fn from(err: &Error) -> Self {
        let kind = match err {
            Error::SocketIo(e) => return ErrorPayload::from_core(e),
            Error::UnknownClient(_) => "UnknownClient",
            Error::AlreadyExists(_) => "AlreadyExists",
            Error::AccessDenied(_) => "AccessDenied",
            Error::UnknownAck(_) => "UnknownAck",
        };
        ErrorPayload {
            kind,
            message: err.to_string(),
            data: None,
        }
    }
}

impl Serialize for Error {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        ErrorPayload::from(self).serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_and_data() {
        let rejected = Error::SocketIo(socketio_next::Error::ConnectRejected {
            message: "not authorized".into(),
            data: Some(json!({ "reason": "test" })),
        });
        assert_eq!(
            serde_json::to_value(&rejected).unwrap(),
            json!({
                "kind": "ConnectRejected",
                "message": "the server rejected the namespace: not authorized",
                "data": { "reason": "test" }
            })
        );
        let status = Error::SocketIo(engineio::Error::HttpStatus { status: 403 }.into());
        let v = serde_json::to_value(&status).unwrap();
        assert_eq!(v["kind"], "HttpStatus");
        assert_eq!(v["data"]["status"], 403);
        let denied = serde_json::to_value(Error::AccessDenied("core".into())).unwrap();
        assert_eq!(denied["kind"], "AccessDenied");
        assert!(denied.get("data").is_none());
    }
}
