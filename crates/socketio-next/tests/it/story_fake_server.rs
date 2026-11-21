//! Stories: a programmable fake server (`socketio_next_testing::fake`) driven by the client:
//! custom namespaces, a refusing `on_connect`, auth, acks both ways, binary, delayed work, a
//! server push, a restart, polling only on another path.

use std::time::Duration;

use bytes::Bytes;
use serde_json::json;
use socketio_next::{ClientBuilder, ConnectionState, Error, Payload, TransportType};
use socketio_next_testing::fake::{Connect, Data, Namespace, Server};

use crate::common::*;

/// Given a server whose `/chat` namespace checks a token and greets, answers `sum` through the
/// ack and asks the client back, when clients connect with and without the token, then the
/// wrong one is refused with the server's message and data, and the right one gets the
/// greeting, its ack, and the answer to the server's question.
#[tokio::test]
async fn a_custom_namespace_with_auth_acks_and_a_question() {
    let server = Server::builder()
        .namespace(
            "/chat",
            Namespace::new()
                .on_connect(|s| {
                    if s.auth().and_then(|a| a.get("token")) != Some(&json!("t1")) {
                        return Connect::Reject {
                            message: "bad token".into(),
                            data: Some(json!({ "code": 401 })),
                        };
                    }
                    s.emit(
                        "welcome",
                        Data::default().arg(s.query("room").unwrap_or("-")),
                    );
                    Connect::Accept
                })
                .on("sum", |s, e| {
                    let total: i64 = e.data().values().iter().filter_map(|v| v.as_i64()).sum();
                    s.ack(&e, Data::default().arg(total));
                })
                .on("ask", |s, _| {
                    s.emit_with_ack("question", Data::default().arg("name?"), |s, reply| {
                        s.emit("got", reply);
                    });
                }),
        )
        .start();

    let refused = ClientBuilder::new(format!("{}/chat", server.url()))
        .auth(json!({ "token": "nope" }))
        .connect()
        .await
        .unwrap_err();
    assert!(
        matches!(&refused, Error::ConnectRejected { message, data, .. }
            if message == "bad token" && *data == Some(json!({ "code": 401 }))),
        "{refused:?}"
    );

    let (builder, mut events) = collect(
        ClientBuilder::new(format!("{}/chat", server.url()))
            .auth(json!({ "token": "t1" }))
            .query("room", "r7")
            .on("question", |event: socketio_next::Event, _| async move {
                event.ack("rust").unwrap();
            }),
    );
    let client = connect(builder).await;
    let welcome = next_named(&mut events, "welcome").await;
    assert_eq!(welcome.arg::<String>(0).unwrap(), "r7");
    let sum = client
        .emit_with_ack("sum", vec![json!(2), json!(3), json!(5)], WAIT)
        .await
        .unwrap();
    assert_eq!(sum.arg::<i64>(0).unwrap(), 10);
    client.emit("ask", ()).unwrap();
    let got = next_named(&mut events, "got").await;
    assert_eq!(got.arg::<String>(0).unwrap(), "rust");
    // a namespace the server does not serve
    let err = ClientBuilder::new(format!("{}/other", server.url()))
        .connect()
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::ConnectRejected { ref message, .. } if message == "Invalid namespace")
    );
    client.disconnect().await.unwrap();
}

/// Given a server that answers binary with binary nested in JSON, sends a malformed packet on
/// demand and pushes events, when the client exchanges binary, then the nested buffers arrive
/// numbered for the client; a broadcast from the server handle reaches every connected client;
/// a raw invalid packet is a parse error.
#[tokio::test]
async fn binary_broadcast_and_raw_packets() {
    let server = Server::builder()
        .namespace(
            "/",
            Namespace::new()
                .on("upload", |s, e| {
                    let bytes = e.data().bytes_of(&e.data().values()[0]).unwrap().clone();
                    let mut reply = Data::default();
                    let wrapped = reply.binary(bytes);
                    s.ack(&e, reply.arg(json!({ "file": wrapped, "ok": true })));
                })
                .on("break", |s, _| s.send_raw(r#"2[true]"#)),
        )
        .start();
    let (builder, mut events) = collect(ClientBuilder::new(server.url()).reconnection(false));
    let (builder, mut states) = states(builder);
    let client = connect(builder).await;
    let reply = client
        .emit_with_ack("upload", Bytes::from_static(b"\x00\x1e\xff"), WAIT)
        .await
        .unwrap();
    assert_eq!(
        reply,
        Payload::Mixed {
            args: vec![json!({ "file": Payload::placeholder(0), "ok": true })],
            attachments: vec![Bytes::from_static(b"\x00\x1e\xff")],
        }
    );
    server.broadcast("/", "news", Data::default().arg("flash"));
    assert_eq!(
        next_named(&mut events, "news")
            .await
            .arg::<String>(0)
            .unwrap(),
        "flash"
    );
    client.emit("break", ()).unwrap();
    let closed = next_state(&mut states, ConnectionState::is_closed).await;
    assert!(
        matches!(
            closed,
            ConnectionState::Closed {
                reason: socketio_next::DisconnectReason::ParseError,
                ..
            }
        ),
        "{closed:?}"
    );
}

/// Given a polling-only server on another path with delayed work and a restart, when a client
/// connects, then it stays on polling, a delayed emit arrives, and after the restart it
/// reconnects by itself.
#[tokio::test]
async fn polling_only_on_a_custom_path_with_delays_and_a_restart() {
    let server = Server::builder()
        .path("/rt/")
        .polling_only()
        .namespace(
            "/",
            Namespace::new()
                .on_connect(|s| {
                    s.after(Duration::from_millis(30), |s| {
                        s.emit("later", Data::default().arg(1));
                    });
                    Connect::Accept
                })
                .on("restart", |s, _| {
                    s.restart_server(Duration::from_millis(300))
                }),
        )
        .start();
    let (builder, mut events) = collect(
        ClientBuilder::new(server.url())
            .path("/rt/")
            .reconnection_delay(Duration::from_millis(50))
            .reconnection_delay_max(Duration::from_millis(100)),
    );
    let client = connect(builder).await;
    assert_eq!(
        client.link_info().transport,
        Some(socketio_next::TransportKind::Polling)
    );
    next_named(&mut events, "later").await;
    client.emit("restart", ()).unwrap();
    client.wait_for(|s| !s.is_connected()).await;
    let back = tokio::time::timeout(
        Duration::from_secs(10),
        client.wait_for(ConnectionState::is_connected),
    )
    .await
    .unwrap();
    assert!(back.is_connected());
    next_named(&mut events, "later").await;
    // the handle restarts it too
    server.restart(Duration::from_millis(100));
    client.wait_for(|s| !s.is_connected()).await;
    client.wait_for(ConnectionState::is_connected).await;
    client.disconnect().await.unwrap();
    // a websocket is refused on a polling-only server
    let ws = ClientBuilder::new(server.url())
        .path("/rt/")
        .transport_type(TransportType::Websocket)
        .reconnection(false)
        .timeout(Duration::from_secs(2))
        .connect()
        .await;
    assert!(ws.is_err(), "{ws:?}");
}

/// Given a server that records disconnect reasons, answers acks twice, reads a header in any
/// case, schedules work per socket and panics on demand, served on a path given without a
/// trailing slash and with only `/admin` registered, when a client joins, leaves and rejoins,
/// then: `/` is served too; `emit_to` reaches the socket by id; work scheduled for the first
/// socket does not run for the second; only the first ack answer counts; a panicking handler
/// closes the connection; the reasons arrive in order; after `shutdown` the port refuses.
#[tokio::test]
async fn hooks_ids_ack_once_panics_and_shutdown() {
    let (left_tx, mut left) = tokio::sync::mpsc::unbounded_channel::<String>();
    let (sid_tx, mut sids) = tokio::sync::mpsc::unbounded_channel::<String>();
    let server = Server::builder()
        .path("rt")
        .namespace(
            "/admin",
            Namespace::new()
                .on_connect(move |s| {
                    let _ = sid_tx.send(s.id().to_owned());
                    s.emit("ua", Data::default().arg(s.header("X-Test").unwrap_or("-")));
                    // runs for this socket only, not for a later one
                    let scheduled = s.id().to_owned();
                    s.after(Duration::from_millis(150), move |s| {
                        let ran_on = s.id().to_owned();
                        s.emit("late", Data::default().arg(scheduled).arg(ran_on));
                    });
                    Connect::Accept
                })
                .on("twice", |s, e| {
                    s.ack(&e, Data::default().arg(1));
                    s.ack(&e, Data::default().arg(2));
                })
                .on("boom", |_, _| panic!("handler bug (expected in this test)"))
                .on("kick", |s, _| s.disconnect())
                .on_disconnect(move |_, reason| {
                    let _ = left_tx.send(reason.to_owned());
                }),
        )
        .start();
    let url = format!("{}/admin", server.url());
    let root = ClientBuilder::new(server.url())
        .path("/rt/")
        .connect()
        .await;
    assert!(root.is_ok(), "`/` is served by default: {root:?}");
    root.unwrap().disconnect().await.unwrap();

    let (builder, mut events) = collect(
        ClientBuilder::new(url.clone())
            .path("/rt/")
            .header("x-test", "yes")
            .reconnect_on_disconnect(true)
            .reconnection_delay(Duration::from_millis(20)),
    );
    let client = connect(builder).await;
    assert_eq!(
        next_named(&mut events, "ua")
            .await
            .arg::<String>(0)
            .unwrap(),
        "yes"
    );
    let first_sid = sids.recv().await.unwrap();
    assert_eq!(client.id().as_deref(), Some(first_sid.as_str()));
    server.emit_to("/admin", &first_sid, "dm", Data::default().arg("hi"));
    assert_eq!(
        next_named(&mut events, "dm")
            .await
            .arg::<String>(0)
            .unwrap(),
        "hi"
    );
    let reply = client.emit_with_ack("twice", (), WAIT).await.unwrap();
    assert_eq!(reply.arg::<i64>(0).unwrap(), 1);

    // the server kicks the socket; the client rejoins with a new one before the timer fires
    client.emit("kick", ()).unwrap();
    assert_eq!(left.recv().await.unwrap(), "server namespace disconnect");
    let second_sid = sids.recv().await.unwrap();
    assert_ne!(second_sid, first_sid);
    client.wait_for(ConnectionState::is_connected).await;
    // the first socket's timer (due first) was dropped with it: the first `late` to arrive is
    // the second socket's own
    let late = next_named(&mut events, "late").await;
    let (scheduled, ran_on): (String, String) = late.payload().deserialize().unwrap();
    assert_eq!(scheduled, second_sid);
    assert_eq!(ran_on, second_sid);

    // a panicking handler closes the connection
    client.emit("boom", ()).unwrap();
    assert_eq!(left.recv().await.unwrap(), "transport close");
    client.disconnect().await.unwrap();

    server.shutdown();
    let refused = ClientBuilder::new(url)
        .path("/rt/")
        .reconnection(false)
        .timeout(Duration::from_secs(2))
        .connect()
        .await;
    assert!(matches!(refused, Err(Error::Engine(_))), "{refused:?}");
}
