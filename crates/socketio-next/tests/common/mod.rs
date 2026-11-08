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
