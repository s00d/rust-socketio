//! Stories: TLS with a private CA (rustls or native-tls) against the Node TLS server.

use std::time::Duration;

use socketio_next::{ClientBuilder, Error, TransportType};

use crate::common::*;

/// `cookies`: the engine.io `io` cookie set on the handshake goes back on every later polling
/// request and on the websocket upgrade.
#[cfg(any(feature = "rustls", feature = "native-tls"))]
/// A private CA through `tls_config`: the TLS handshake with the test server passes (4202 is an
/// engine.io server: its greeting then fails to parse as socket.io, a protocol error).
#[tokio::test]
async fn tls_config_trusts_a_private_ca() {
    let tls = socketio_next::TlsConfig::with_root_certificates_pem(&ca_cert_pem()).unwrap();
    let result = ClientBuilder::new("https://localhost:4202")
        .tls_config(tls)
        .path("/engine.io/")
        .transport_type(TransportType::Polling)
        .timeout(Duration::from_secs(10))
        .connect()
        .await;
    assert!(matches!(result, Err(Error::Protocol(_))), "{result:?}");
}

/// The bundled roots only: native-tls and `platform-verifier` ask the OS, whose store may trust
/// a CA a developer installed.
#[cfg(all(feature = "rustls", not(feature = "platform-verifier")))]
#[tokio::test]
async fn without_the_ca_tls_fails() {
    let result = ClientBuilder::new("https://localhost:4202")
        .path("/engine.io/")
        .transport_type(TransportType::Polling)
        .timeout(Duration::from_secs(10))
        .connect()
        .await;
    assert!(matches!(result, Err(Error::Engine(_))), "{result:?}");
}
