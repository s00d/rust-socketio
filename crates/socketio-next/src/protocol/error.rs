use std::str::Utf8Error;

/// A packet that does not follow the Engine.IO v4 / Socket.IO v5 wire format.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The first character is not a known packet type.
    #[error("unknown packet type {0:?}")]
    InvalidPacketType(char),
    /// The packet ends before a required part (type, attachment count, namespace).
    #[error("incomplete packet")]
    IncompletePacket,
    /// The packet breaks a protocol rule; the text says which.
    #[error("invalid packet: {0}")]
    InvalidPacket(&'static str),
    /// A text packet is not valid UTF-8.
    #[error("packet is not valid UTF-8: {0}")]
    InvalidUtf8(#[from] Utf8Error),
    /// A `b`-prefixed (base64) engine.io packet does not decode.
    #[error("invalid base64 in a binary packet: {0}")]
    InvalidBase64(#[from] base64::DecodeError),
    /// The JSON payload does not parse.
    #[error("invalid JSON payload: {0}")]
    InvalidJson(#[from] serde_json::Error),
}

/// Result alias of this crate.
pub type Result<T> = std::result::Result<T, Error>;
