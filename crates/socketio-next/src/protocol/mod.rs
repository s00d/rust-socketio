//! Packet codecs of the [Engine.IO v4](https://github.com/socketio/engine.io-protocol) and
//! [Socket.IO v5](https://github.com/socketio/socket.io-protocol) protocols, with the validation
//! rules of the reference implementations (engine.io-parser 5.2, socket.io-parser 4.2).
//!
//! No I/O: this module only turns bytes into packets and back.

pub mod engineio;
mod error;
mod payload;
pub mod socketio;

pub use error::{Error, Result};
pub use payload::Payload;

#[cfg(test)]
mod engineio_js_parity;
#[cfg(test)]
mod socketio_js_parity;
