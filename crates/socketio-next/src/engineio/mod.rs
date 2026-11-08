//! An async [Engine.IO v4](https://github.com/socketio/engine.io-protocol) client: HTTP long
//! polling and websocket transports, the upgrade probe, the heartbeat, and TLS through
//! rustls (default) or native-tls.
//!
//! This is the transport layer of the socket.io client; use it directly to talk raw engine.io.
//!
//! ```no_run
//! use socketio_next::engineio::{ClientBuilder, Packet, PacketId};
//! use futures_util::StreamExt;
//! use url::Url;
//!
//! # async fn run() -> socketio_next::engineio::Result<()> {
//! let mut client = ClientBuilder::new(Url::parse("http://localhost:4201")?)
//!     .build()
//!     .await?;
//! client.connect().await?;
//! client.emit(Packet::new(PacketId::Message, "hello")).await?;
//! while let Some(packet) = client.next().await {
//!     println!("{:?}", packet?);
//! }
//! # Ok(()) }
//! ```

mod builder;
mod callback;
mod client;
mod error;
mod generator;
pub(crate) mod http;
mod socket;
pub mod tls;
mod transport;
pub mod transports;
mod util;

#[cfg(test)]
mod js_parity;

pub use crate::protocol::engineio::{HandshakePacket, Packet, PacketId};
pub use builder::ClientBuilder;
pub use client::Client;
pub use error::{Error, Result};
pub use tls::TlsConfig;
pub use transport::{Frame, Transport, TransportKind};

/// The engine.io protocol revision this client speaks (`EIO=4`).
pub const ENGINE_IO_VERSION: i32 = 4;

/// Default bound for the handshake and the websocket connect (socket.io-client `timeout`, 20 s).
pub const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use url::Url;

    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    const CERT_PATH: &str = "../../ci/cert/ca.crt";

    /// Trusts the test CA from `ci/cert` (`CA_CERT_PATH` overrides the path).
    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    pub(crate) fn tls_config() -> Result<TlsConfig> {
        let cert_path = std::env::var("CA_CERT_PATH").unwrap_or_else(|_| CERT_PATH.to_owned());
        let pem = std::fs::read(&cert_path)
            .map_err(|e| Error::InvalidTlsConfig(format!("{cert_path}: {e}")))?;
        TlsConfig::with_root_certificates_pem(&pem)
    }

    fn url(var: &str, default: &str) -> Result<Url> {
        Ok(Url::parse(
            &std::env::var(var).unwrap_or_else(|_| default.to_owned()),
        )?)
    }

    /// The `engine.io` server for testing runs on port 4201
    pub(crate) fn engine_io_server() -> Result<Url> {
        url("ENGINE_IO_SERVER", "http://localhost:4201")
    }

    /// The `engine.io` server that refuses upgrades runs on port 4203
    pub(crate) fn engine_io_polling_server() -> Result<Url> {
        url("ENGINE_IO_POLLING_SERVER", "http://localhost:4203")
    }

    #[allow(dead_code)]
    pub(crate) fn engine_io_server_secure() -> Result<Url> {
        url("ENGINE_IO_SECURE_SERVER", "https://localhost:4202")
    }
}
