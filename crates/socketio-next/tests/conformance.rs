//! Differential tests: the same scenario with socket.io-client 4.8 (Node, `ci/conformance/
//! client.js`) and with this client against one recording socket.io 4.8 server
//! (`ci/conformance/server.js`); the engine.io message packets the server saw must be equal.
//! Pings, pongs, noops and upgrades are left out of the logs (timing, not client logic).

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::*;
use serde_json::{Value, json};
use socketio_next::{Client, ClientBuilder, Event, Payload, TransportType};

/// A tag no other test (or earlier run) uses: the clock alone repeats across parallel tests.
fn tag(name: &str) -> String {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{name}-{}-{nanos}-{n}", std::process::id())
}

fn builder(tag: &str, polling: bool) -> ClientBuilder {
    let transport = if polling {
        TransportType::Polling
    } else {
        TransportType::Any
    };
    ClientBuilder::new(conformance_server())
        .transport_type(transport)
        .query("tag", tag)
        .auth(json!({"token": "t"}))
        .reconnection(false)
}

/// `name` followed by the arguments of `payload`, binary kept.
fn prepend(name: &str, payload: Payload) -> Payload {
    let name = Value::String(name.to_owned());
    match payload {
        Payload::Binary(bytes) => Payload::Mixed {
            args: vec![name, Payload::placeholder(0)],
            attachments: vec![bytes],
        },
        Payload::Mixed {
            mut args,
            attachments,
        } => {
            args.insert(0, name);
            Payload::Mixed { args, attachments }
        }
        other => {
            let mut args = other.args().to_vec();
            args.insert(0, name);
            Payload::Text(args)
        }
    }
}

async fn compare(scenario: &str, rust_tag: &str, polling: bool) {
    let node_tag = tag(&format!("node-{scenario}"));
    run_node_client(scenario, &node_tag, polling.then_some("polling")).await;
    let node = recorded_packets(&node_tag).await;
    let rust = recorded_packets(rust_tag).await;
    assert!(!node.is_empty(), "the Node client left no log");
    assert_eq!(
        rust, node,
        "wire differs from socket.io-client for `{scenario}`"
    );
}

#[tokio::test]
async fn basic_matches_socket_io_client() {
    basic(false).await;
}

#[tokio::test]
async fn basic_over_polling_matches_socket_io_client() {
    basic(true).await;
}

async fn basic(polling: bool) {
    let rust_tag = tag("rust-basic");
    let (b, mut events) = collect(builder(&rust_tag, polling));
    let client = b.connect().await.unwrap();

    client
        .emit("echo", vec![json!(1), json!("two"), json!({"three": 3})])
        .unwrap();
    next_named(&mut events, "echo-back").await;
    let reply = client
        .emit_with_ack("echo", vec![json!("ack-me"), json!([1, 2])], WAIT)
        .await
        .unwrap();
    // emitWithAck resolves with the first argument
    let first: Value = reply.arg(0).unwrap();
    client
        .emit(
            "echo",
            Payload::Mixed {
                args: vec![Payload::placeholder(0), json!("x"), first],
                attachments: vec![bytes::Bytes::from_static(&[1, 2, 3])],
            },
        )
        .unwrap();
    next_named(&mut events, "echo-back").await;
    client.on("question", |event: Event, _: Client| {
        let q: Value = event.arg(0).unwrap();
        event.ack(vec![json!("because"), q]).unwrap();
    });
    client.emit("ask", ()).unwrap();
    next_named(&mut events, "answered").await;
    client.emit("bin", ()).unwrap();
    let mixed = next_named(&mut events, "binary-mixed").await;
    client.emit("echo", mixed.into_payload()).unwrap();
    next_named(&mut events, "echo-back").await;
    client.disconnect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    compare("basic", &rust_tag, polling).await;
}

#[tokio::test]
async fn received_shapes_match_socket_io_client() {
    shapes(false).await;
}

#[tokio::test]
async fn received_shapes_over_polling_match_socket_io_client() {
    shapes(true).await;
}

async fn shapes(polling: bool) {
    let rust_tag = tag("rust-shapes");
    // the echo handler first, so `seen(done)` is queued before the test reacts to `done`
    let b = builder(&rust_tag, polling).on_any(|event: Event, client: Client| async move {
        let name = event.name().to_owned();
        client
            .emit("seen", prepend(&name, event.into_payload()))
            .unwrap();
    });
    let (b, mut events) = collect(b);
    let client = b.connect().await.unwrap();
    client.emit("shapes", ()).unwrap();
    next_named(&mut events, "done").await;
    client.emit_with_ack("echo", "sync", WAIT).await.unwrap();
    client.disconnect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    compare("shapes", &rust_tag, polling).await;
}

/// Connection state recovery (socket.io 4.6+): after a transport close the server restores
/// the session; `recovered` is true and the id stays.
#[tokio::test]
async fn connection_state_recovery() {
    let (b, mut events) = collect(
        ClientBuilder::new(recovery_server()).reconnection_delay(Duration::from_millis(50)),
    );
    let client = b.connect().await.unwrap();
    let id = client.id().unwrap();
    assert!(!client.recovered());
    client.emit("tick", 1).unwrap();
    let tock = next_named(&mut events, "tock").await;
    assert_eq!(tock.arg::<String>(1).unwrap(), "offset-1");
    client.emit("drop", ()).unwrap();
    client.wait_for(|s| !s.is_connected()).await;
    let state = tokio::time::timeout(WAIT, client.wait_for(|s| s.is_connected()))
        .await
        .unwrap();
    assert!(
        matches!(state, socketio_next::ConnectionState::Connected { recovered: true, ref sid } if *sid == id),
        "{state:?} (was {id})"
    );
    assert!(client.recovered());
    client.disconnect().await.unwrap();
}
