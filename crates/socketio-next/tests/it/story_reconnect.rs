//! Stories: losing the connection and getting it back: server disconnects, transport closes,
//! parse errors, bounded attempts, emits while offline (fail fast or buffered), a server
//! restart, connect values computed per attempt.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use socketio_next::{
    Client, ClientBuilder, ConnectParams, ConnectionState, DisconnectReason, Error, TransportType,
};

use crate::common::*;

/// A server namespace disconnect is final by default (socket.io-client).
#[tokio::test]
async fn server_disconnect_is_final_by_default() {
    let (builder, mut states) = states(ClientBuilder::new(server()));
    let client = connect(builder).await;
    client.emit("server_disconnect", ()).unwrap();
    let closed = next_state(&mut states, ConnectionState::is_closed).await;
    assert!(matches!(
        closed,
        ConnectionState::Closed {
            reason: DisconnectReason::IoServerDisconnect,
            ..
        }
    ));
    assert!(!client.connected());
}

#[tokio::test]
async fn server_disconnect_reconnects_when_asked() {
    let (builder, mut states) = states(
        ClientBuilder::new(server())
            .reconnect_on_disconnect(true)
            .reconnection_delay(Duration::from_millis(50)),
    );
    let client = connect(builder).await;
    client.emit("server_disconnect", ()).unwrap();
    next_state(&mut states, |s| {
        matches!(
            s,
            ConnectionState::Disconnected {
                reason: DisconnectReason::IoServerDisconnect,
                ..
            }
        )
    })
    .await;
    next_state(&mut states, |s| {
        matches!(s, ConnectionState::Reconnecting { attempt: 1, .. })
    })
    .await;
    next_state(&mut states, ConnectionState::is_connected).await;
    client.disconnect().await.unwrap();
}

/// The server closes the engine.io connection: Disconnected(transport close), then reconnect.
#[tokio::test]
async fn transport_close_reconnects_with_state_transitions() {
    let (builder, mut states) =
        states(ClientBuilder::new(server()).reconnection_delay(Duration::from_millis(50)));
    let client = connect(builder).await;
    let first_sid = client.id().unwrap();
    next_state(&mut states, ConnectionState::is_connected).await;
    client.emit("close_transport", ()).unwrap();
    let lost = next_state(&mut states, |s| {
        matches!(s, ConnectionState::Disconnected { .. })
    })
    .await;
    assert!(
        matches!(
            lost,
            ConnectionState::Disconnected {
                reason: DisconnectReason::TransportClose | DisconnectReason::TransportError,
                ..
            }
        ),
        "{lost:?}"
    );
    next_state(&mut states, |s| {
        matches!(
            s,
            ConnectionState::Reconnecting {
                attempt: 1,
                error: None,
                ..
            }
        )
    })
    .await;
    next_state(&mut states, |s| {
        matches!(s, ConnectionState::Connecting { attempt: 1, .. })
    })
    .await;
    next_state(&mut states, ConnectionState::is_connected).await;
    assert_ne!(client.id().unwrap(), first_sid);
    assert_eq!(client.link_info().reconnect_attempts, 0);
    client.disconnect().await.unwrap();
}

/// A packet socket.io-parser rejects closes the connection with `parse error`.
#[tokio::test]
async fn invalid_packet_is_a_parse_error() {
    let (builder, mut states) = states(ClientBuilder::new(server()).reconnection(false));
    let client = connect(builder).await;
    client.emit("invalid_packet", ()).unwrap();
    let closed = next_state(&mut states, ConnectionState::is_closed).await;
    assert!(
        matches!(
            closed,
            ConnectionState::Closed {
                reason: DisconnectReason::ParseError,
                error: Some(_),
                ..
            }
        ),
        "{closed:?}"
    );
}

/// connection.ts "should try to reconnect twice and fail when requested two attempts"
#[tokio::test]
async fn reconnect_attempts_are_bounded() {
    let proxy = Proxy::start(&host_port(&server())).await;
    let (builder, mut states) = states(
        ClientBuilder::new(proxy.url())
            .transport_type(TransportType::Websocket)
            .reconnection_attempts(2)
            .reconnection_delay(Duration::from_millis(20))
            .reconnection_delay_max(Duration::from_millis(40)),
    );
    let client = connect(builder).await;
    proxy.refuse(true);
    proxy.cut();
    let mut attempts = Vec::new();
    let closed = tokio::time::timeout(WAIT, async {
        loop {
            match states.recv().await.unwrap() {
                ConnectionState::Reconnecting { attempt, .. } => attempts.push(attempt),
                state @ ConnectionState::Closed { .. } => return state,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(attempts, [1, 2]);
    assert!(
        matches!(
            closed,
            ConnectionState::Closed {
                reason: DisconnectReason::ReconnectFailed,
                error: Some(_),
                ..
            }
        ),
        "{closed:?}"
    );
    assert!(matches!(client.emit("x", ()), Err(Error::Closed)));
}

/// Fail fast by default: emits while the link is down are refused.
#[tokio::test]
async fn emit_while_disconnected_fails_fast() {
    let proxy = Proxy::start(&host_port(&server())).await;
    let client = connect(
        ClientBuilder::new(proxy.url())
            .transport_type(TransportType::Websocket)
            .reconnection_delay(Duration::from_millis(200)),
    )
    .await;
    proxy.refuse(true);
    proxy.cut();
    client.wait_for(|s| !s.is_connected()).await;
    assert!(matches!(client.emit("echo", 1), Err(Error::NotConnected)));
    assert!(matches!(
        client.emit_with_ack("echo", 1, WAIT).await,
        Err(Error::NotConnected)
    ));
    proxy.refuse(false);
    client.wait_for(ConnectionState::is_connected).await;
    client.emit("echo", 1).unwrap();
    client.disconnect().await.unwrap();
}

/// Opt-in buffering (socket.io-client `sendBuffer`): emits during the outage go out after the
/// reconnect, in order, and their acks resolve.
#[tokio::test]
async fn buffer_while_disconnected_delivers_after_reconnect() {
    let proxy = Proxy::start(&host_port(&server())).await;
    let (builder, mut events) = collect(
        ClientBuilder::new(proxy.url())
            .transport_type(TransportType::Websocket)
            .buffer_while_disconnected(true)
            .reconnection_delay(Duration::from_millis(100)),
    );
    let client = connect(builder).await;
    proxy.refuse(true);
    proxy.cut();
    client.wait_for(|s| !s.is_connected()).await;
    client.emit("echo", "a").unwrap();
    client.emit("echo", "b").unwrap();
    let acked = {
        let client = client.clone();
        tokio::spawn(async move { client.emit_with_ack("echo", "c", WAIT).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(client.link_info().buffered_emits, 3);
    proxy.refuse(false);
    assert_eq!(
        next_named(&mut events, "echo-back")
            .await
            .arg::<String>(0)
            .unwrap(),
        "a"
    );
    assert_eq!(
        next_named(&mut events, "echo-back")
            .await
            .arg::<String>(0)
            .unwrap(),
        "b"
    );
    assert_eq!(acked.await.unwrap().unwrap().arg::<String>(0).unwrap(), "c");
    client.disconnect().await.unwrap();
}

/// A dropped link fails the pending acks with `Disconnected`.
#[tokio::test]
async fn pending_ack_fails_when_the_link_drops() {
    let proxy = Proxy::start(&host_port(&server())).await;
    let client = connect(
        ClientBuilder::new(proxy.url())
            .transport_type(TransportType::Websocket)
            .reconnection(false),
    )
    .await;
    let pending = {
        let client = client.clone();
        tokio::spawn(async move { client.emit_with_ack("ack_late", (), WAIT).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    proxy.cut();
    let result = pending.await.unwrap();
    assert!(matches!(result, Err(Error::Disconnected)), "{result:?}");
}

/// The restart server goes away for 2 s: the client reconnects by itself.
#[tokio::test]
async fn reconnects_after_a_server_restart() {
    let (builder, mut events) = collect(
        ClientBuilder::new(new_socket_io_restart_server())
            .reconnection_delay(Duration::from_millis(200))
            .reconnection_delay_max(Duration::from_millis(500)),
    );
    let (builder, mut states) = states(builder);
    let client = connect(builder).await;
    next_named(&mut events, "message").await;
    client.emit("restart_server", ()).unwrap();
    next_state(&mut states, |s| {
        matches!(s, ConnectionState::Disconnected { .. })
    })
    .await;
    let reconnected = tokio::time::timeout(
        Duration::from_secs(15),
        client.wait_for(ConnectionState::is_connected),
    )
    .await
    .unwrap();
    assert!(reconnected.is_connected());
    next_named(&mut events, "message").await;
    client.disconnect().await.unwrap();
}

/// `auth_with` returning a non-object on a reconnect closes the client (no endless loop).
#[tokio::test]
async fn bad_auth_on_reconnect_is_final() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (builder, mut states) = states(
        ClientBuilder::new(server())
            .reconnect_on_disconnect(true)
            .reconnection_delay(Duration::from_millis(20))
            .auth_with({
                let calls = Arc::clone(&calls);
                move || {
                    let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                    async move {
                        if first {
                            json!({"ok": true})
                        } else {
                            json!("not an object")
                        }
                    }
                }
            }),
    );
    let client = connect(builder).await;
    client.emit("server_disconnect", ()).unwrap();
    let closed = next_state(&mut states, ConnectionState::is_closed).await;
    assert!(
        matches!(
            closed,
            ConnectionState::Closed {
                reason: DisconnectReason::ConnectRejected,
                error: Some(Error::InvalidAuth(_)),
                ..
            }
        ),
        "{closed:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

/// Query parameters and headers are computed again for every connect attempt (a token that
/// expires, a signed timestamp): the reconnect after a cut sends the new values.
#[tokio::test]
async fn connect_params_are_recomputed_per_attempt() {
    let proxy = Proxy::start(&host_port(&server())).await;
    let n = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&n);
    let client = connect(
        ClientBuilder::new(proxy.url())
            .transport_type(TransportType::Websocket)
            .query("token", "static")
            .reconnection_delay(Duration::from_millis(20))
            .connect_params_with(move || {
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                async move { ConnectParams::new().query("token", format!("t{n}")) }
            }),
    )
    .await;
    let query = |client: Client| async move {
        let reply = client
            .emit_with_ack("handshake_query", (), WAIT)
            .await
            .unwrap();
        reply.arg::<Value>(0).unwrap()["token"].clone()
    };
    assert_eq!(query(client.clone()).await, "t1");
    proxy.cut();
    client.wait_for(|s| !s.is_connected()).await;
    client.wait_for(ConnectionState::is_connected).await;
    assert_eq!(query(client.clone()).await, "t2");
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn connect_headers_are_recomputed_per_attempt() {
    let proxy = Proxy::start(&host_port(&socket_io_restart_server())).await;
    let n = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&n);
    let (builder, mut events) = collect(
        ClientBuilder::new(proxy.url())
            .transport_type(TransportType::Websocket)
            .header("message_back", "static")
            .reconnection_delay(Duration::from_millis(20))
            .connect_params_with(move || {
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                async move { ConnectParams::new().header("message_back", format!("h{n}")) }
            }),
    );
    let client = connect(builder).await;
    assert_eq!(
        next_named(&mut events, "message")
            .await
            .arg::<String>(0)
            .unwrap(),
        "h1"
    );
    proxy.cut();
    assert_eq!(
        next_named(&mut events, "message")
            .await
            .arg::<String>(0)
            .unwrap(),
        "h2"
    );
    client.disconnect().await.unwrap();
}
