//! A [Socket.IO](https://socket.io) v4 client for Rust, with the behaviour of the reference
//! JavaScript client (socket.io-client 4.8 / engine.io-client 6.6): namespaces, auth, acks in
//! both directions, binary data anywhere in the arguments, reconnection with the same backoff,
//! connection state recovery, and polling / websocket transports over rustls or native-tls.
//!
//! ```no_run
//! use std::time::Duration;
//! use socketio_next::{Client, ClientBuilder, ConnectionState, Event};
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> socketio_next::Result<()> {
//!     let client = ClientBuilder::new("http://localhost:3000")
//!         .auth(json!({ "token": "abc" }))
//!         .on("greeting", |event: Event, _client: Client| async move {
//!             println!("server says {:?}", event.payload());
//!         })
//!         .on_state_change(|state: ConnectionState, _client: Client| async move {
//!             println!("state: {state:?}");
//!         })
//!         .connect()
//!         .await?;
//!
//!     client.emit("hello", json!({ "from": "rust" }))?;
//!     let reply = client
//!         .emit_with_ack("add", vec![json!(1), json!(2)], Duration::from_secs(5))
//!         .await?;
//!     println!("1 + 2 = {}", reply.arg::<i64>(0)?);
//!
//!     client.disconnect().await
//! }
//! ```
//!
//! - [`ClientBuilder`] configures and connects; [`Client`] emits, acks, adds handlers at
//!   runtime and reports [`ConnectionState`] / [`LinkInfo`].
//! - [`blocking`] (feature `blocking`) is the same client without async.
//! - [`engineio`] is the transport layer, [`protocol`] the packet codecs (no I/O).
#![cfg_attr(docsrs, feature(doc_cfg))]

mod actor;
mod backoff;
#[cfg(feature = "blocking")]
#[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
pub mod blocking;
mod builder;
mod client;
pub mod engineio;
mod error;
mod event;
mod handler;
pub mod protocol;
mod redact;
mod state;
mod transport_type;

pub use builder::{ClientBuilder, ConnectParams, Direction};
pub use client::Client;
pub use engineio::{TlsConfig, TransportKind};
pub use error::{Error, Result};
pub use event::{AckSender, Event};
pub use handler::{Handler, ListenerId};
pub use protocol::Payload;
pub use state::{ConnectionState, DisconnectReason, LinkInfo};
pub use transport_type::TransportType;
