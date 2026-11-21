//! Helpers shared by the test modules; each module uses some of them.
#![allow(dead_code)]

use std::time::Duration;

use socketio_next::{Client, ClientBuilder, ConnectionState, Event};
use tokio::sync::mpsc;

pub use socketio_next_testing::*;

pub const WAIT: Duration = Duration::from_secs(10);

/// Collects every event the client receives.
pub fn collect(builder: ClientBuilder) -> (ClientBuilder, mpsc::UnboundedReceiver<Event>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let builder = builder.on_any(move |event: Event, _: Client| {
        let tx = tx.clone();
        async move {
            let _ = tx.send(event);
        }
    });
    (builder, rx)
}

/// Collects every state transition.
pub fn states(builder: ClientBuilder) -> (ClientBuilder, mpsc::UnboundedReceiver<ConnectionState>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let builder = builder.on_state_change(move |state: ConnectionState, _: Client| {
        let tx = tx.clone();
        async move {
            let _ = tx.send(state);
        }
    });
    (builder, rx)
}

/// The next event named `name` (others are skipped).
pub async fn next_named(rx: &mut mpsc::UnboundedReceiver<Event>, name: &str) -> Event {
    tokio::time::timeout(WAIT, async {
        loop {
            let event = rx.recv().await.expect("event channel closed");
            if event.name() == name {
                return event;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no `{name}` event within {WAIT:?}"))
}

/// The next state matching `pred` (others are skipped).
pub async fn next_state(
    rx: &mut mpsc::UnboundedReceiver<ConnectionState>,
    pred: impl Fn(&ConnectionState) -> bool,
) -> ConnectionState {
    tokio::time::timeout(WAIT, async {
        loop {
            let state = rx.recv().await.expect("state channel closed");
            if pred(&state) {
                return state;
            }
        }
    })
    .await
    .expect("state not reached in time")
}

/// Logs to the test output with `RUST_LOG` (default off).
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// The socket.io server with the `ci/socket-io.js` fixtures (in-process fake unless
/// `SOCKET_IO_SERVER` names another).
pub fn server() -> String {
    socket_io_server()
}

/// `builder.connect()`, bounded by [`WAIT`]; panics on failure.
pub async fn connect(builder: ClientBuilder) -> Client {
    tokio::time::timeout(WAIT, builder.connect())
        .await
        .expect("connect hung")
        .expect("connect failed")
}

/// The engine.io echo server (`ci/engine-io.js`), path `/engine.io/` left to the builder.
pub fn engine_io_url() -> url::Url {
    url::Url::parse(&engine_io_server()).unwrap()
}

/// The polling-only engine.io echo server (`ci/engine-io-polling.js`).
pub fn engine_io_polling_url() -> url::Url {
    url::Url::parse(&engine_io_polling_server()).unwrap()
}

/// The TLS engine.io server (Node, `ci/engine-io-secure.js`).
pub fn engine_io_secure_url() -> url::Url {
    url::Url::parse(
        &std::env::var("ENGINE_IO_SECURE_SERVER")
            .unwrap_or_else(|_| "https://localhost:4202".to_owned()),
    )
    .unwrap()
}

/// Trusts the test CA (`ci/cert/ca.crt`).
#[cfg(any(feature = "rustls", feature = "native-tls"))]
pub fn test_tls_config() -> socketio_next::TlsConfig {
    socketio_next::TlsConfig::with_root_certificates_pem(&ca_cert_pem()).unwrap()
}
