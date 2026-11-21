//! Stories: handlers. Runtime listeners, order, panics, `disconnect()` from a handler, the
//! end of a client (drop, `WeakClient`, `reconnect()` after a close).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::json;
use socketio_next::{
    Client, ClientBuilder, ConnectionState, DisconnectReason, Error, Event, TransportType,
};

use crate::common::*;

/// `disconnect()` from inside a handler does not deadlock, and no handler runs after it.
#[tokio::test]
async fn disconnect_from_a_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let builder = ClientBuilder::new(server()).on_any({
        let calls = Arc::clone(&calls);
        move |_: Event, client: Client| {
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                client.disconnect().await.unwrap();
            }
        }
    });
    let client = connect(builder).await;
    let closed = tokio::time::timeout(WAIT, client.wait_for(ConnectionState::is_closed))
        .await
        .unwrap();
    assert!(matches!(
        closed,
        ConnectionState::Closed {
            reason: DisconnectReason::IoClientDisconnect,
            ..
        }
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    // the greetings after the first one are not delivered any more
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// No handler starts after `disconnect()` returned.
#[tokio::test]
async fn no_handler_after_disconnect_returns() {
    let calls = Arc::new(AtomicUsize::new(0));
    let builder = ClientBuilder::new(server()).on_any({
        let calls = Arc::clone(&calls);
        move |_: Event, _: Client| {
            calls.fetch_add(1, Ordering::SeqCst);
        }
    });
    let client = connect(builder).await;
    client.disconnect().await.unwrap();
    let after = calls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(calls.load(Ordering::SeqCst), after);
}

/// Runtime listeners: on / once / off / off_listener / listener_count.
#[tokio::test]
async fn runtime_listeners() {
    let client = connect(ClientBuilder::new(server())).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(String, i64)>();
    let on_id = client.on("echo-back", {
        let tx = tx.clone();
        move |event: Event, _: Client| {
            let _ = tx.send(("on".into(), event.arg(0).unwrap()));
        }
    });
    client.once("echo-back", {
        let tx = tx.clone();
        move |event: Event, _: Client| {
            let _ = tx.send(("once".into(), event.arg(0).unwrap()));
        }
    });
    assert_eq!(client.listener_count("echo-back"), 2);
    client.emit("echo", 1).unwrap();
    let mut got = vec![rx.recv().await.unwrap(), rx.recv().await.unwrap()];
    got.sort();
    assert_eq!(got, [("on".into(), 1), ("once".into(), 1)]);
    assert_eq!(client.listener_count("echo-back"), 1);
    assert!(client.off_listener(on_id));
    assert!(!client.off_listener(on_id));
    assert_eq!(client.listener_count("echo-back"), 0);
    client.on("echo-back", move |event: Event, _: Client| {
        let _ = tx.send(("again".into(), event.arg(0).unwrap()));
    });
    client.emit("echo", 2).unwrap();
    assert_eq!(rx.recv().await.unwrap(), ("again".into(), 2));
    assert_eq!(client.off("echo-back"), 1);
    client.disconnect().await.unwrap();
}

/// Handlers run in arrival order even when they await.
#[tokio::test]
async fn handlers_run_in_order() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let builder = ClientBuilder::new(server()).on("echo-back", move |event: Event, _: Client| {
        let tx = tx.clone();
        async move {
            let n: i64 = event.arg(0).unwrap();
            // later events would overtake a slow handler if handlers ran concurrently
            tokio::time::sleep(Duration::from_millis((10 - n as u64) * 5)).await;
            let _ = tx.send(n);
        }
    });
    let client = connect(builder).await;
    for i in 0..10 {
        client.emit("echo", i).unwrap();
    }
    for i in 0..10 {
        assert_eq!(
            tokio::time::timeout(WAIT, rx.recv())
                .await
                .unwrap()
                .unwrap(),
            i
        );
    }
    client.disconnect().await.unwrap();
}

/// A panicking handler does not stop the event task.
#[tokio::test]
async fn panicking_handler_is_contained() {
    let (builder, mut events) = collect(ClientBuilder::new(server()));
    let builder = builder
        .on("echo-back", |_: Event, _: Client| async move {
            panic!("handler bug");
        })
        // a panic in the closure body, before it returns its future
        .on("echo-back", |event: Event, _: Client| {
            let n: i64 = event.arg(0).unwrap();
            assert!(n != 1, "sync handler bug");
            async move {}
        });
    let client = connect(builder).await;
    client.emit("echo", 1).unwrap();
    client.emit("echo", 2).unwrap();
    next_named(&mut events, "echo-back").await;
    assert_eq!(
        next_named(&mut events, "echo-back")
            .await
            .arg::<i64>(0)
            .unwrap(),
        2
    );
    client.disconnect().await.unwrap();
}

/// Dropping every handle closes the connection, also when a handler holds a `WeakClient`.
#[tokio::test]
async fn drop_closes_the_connection() {
    let proxy = Proxy::start(&host_port(&server())).await;
    let client =
        connect(ClientBuilder::new(proxy.url()).transport_type(TransportType::Websocket)).await;
    let weak = client.downgrade();
    client.on("never", move |_: Event, _: Client| {
        let _ = weak.upgrade();
    });
    let weak = client.downgrade();
    assert!(weak.upgrade().is_some());
    assert!(proxy.wait_open(1, WAIT).await);
    drop(client);
    assert!(weak.upgrade().is_none());
    assert!(
        proxy.wait_open(0, WAIT).await,
        "connection still open after drop"
    );
}

/// A closed client (here: the server disconnected it, which is final by default) connects
/// again with `reconnect()`, keeping its handlers.
#[tokio::test]
async fn reconnect_after_closed_keeps_the_handlers() {
    let (builder, mut events) = collect(ClientBuilder::new(server()));
    let client = connect(builder).await;
    client.emit("server_disconnect", ()).unwrap();
    let closed = client.wait_for(ConnectionState::is_closed).await;
    assert!(
        matches!(
            closed,
            ConnectionState::Closed {
                reason: DisconnectReason::IoServerDisconnect,
                ..
            }
        ),
        "{closed:?}"
    );
    assert!(matches!(client.emit("echo", 1), Err(Error::Closed)));
    client.reconnect().await.unwrap();
    assert!(client.connected());
    client.emit("echo", json!("again")).unwrap();
    let echo = next_named(&mut events, "echo-back").await;
    assert_eq!(echo.arg::<String>(0).unwrap(), "again");
    // a no-op while connected
    client.reconnect().await.unwrap();
    // after disconnect() too
    client.disconnect().await.unwrap();
    client.reconnect().await.unwrap();
    client.emit("echo", json!("third")).unwrap();
    assert_eq!(
        next_named(&mut events, "echo-back")
            .await
            .arg::<String>(0)
            .unwrap(),
        "third"
    );
    client.disconnect().await.unwrap();
}

/// Given a runtime, when a client connects, works and is dropped, then every task it started
/// ends (socket.io-client-java `ExecutionTest`: the process can exit).
#[tokio::test(flavor = "multi_thread")]
async fn no_task_outlives_the_client() {
    let metrics = tokio::runtime::Handle::current().metrics();
    let before = metrics.num_alive_tasks();
    for transport in [TransportType::Polling, TransportType::Websocket] {
        let client = connect(ClientBuilder::new(server()).transport_type(transport)).await;
        client.emit_with_ack("echo", 1, WAIT).await.unwrap();
        client.disconnect().await.unwrap();
        client.reconnect().await.unwrap();
        drop(client);
    }
    // tasks end asynchronously once their channels close: no event to wait for
    let deadline = tokio::time::Instant::now() + WAIT;
    while metrics.num_alive_tasks() > before && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(metrics.num_alive_tasks(), before);
}
