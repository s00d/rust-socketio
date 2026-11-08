//! The async client against the Node servers in `ci/` (socket.io 4.x). Several cases are ports
//! of socket.io-client 4.8 tests (`test/socket.ts`, `test/connection.ts`), named after them.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use serde_json::{Value, json};
use socketio_next::{
    Client, ClientBuilder, ConnectParams, ConnectionState, DisconnectReason, Error, Event, Payload,
    TransportType,
};

fn server() -> String {
    socket_io_server()
}

async fn connect(builder: ClientBuilder) -> Client {
    tokio::time::timeout(WAIT, builder.connect())
        .await
        .expect("connect hung")
        .expect("connect failed")
}

#[tokio::test]
async fn connects_and_receives_the_greetings() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        init_tracing();
        let (builder, mut events) = collect(ClientBuilder::new(server()));
        let client = connect(builder).await;
        assert!(client.connected());
        assert!(client.id().is_some());
        assert!(client.state().is_connected());

        // `socket.emit("Hello from the message event!")`: a lone name is an event without arguments
        let hello = next_named(&mut events, "Hello from the message event!").await;
        assert!(hello.payload().is_empty());
        let test = next_named(&mut events, "test").await;
        assert_eq!(test.arg::<String>(0).unwrap(), "Hello from the test event!");
        // `emit(Buffer)` without a name is delivered as `message` (rust_socketio compatibility)
        let binary = next_named(&mut events, "message").await;
        assert_eq!(
            binary.payload(),
            &Payload::Binary(Bytes::from_static(&[4, 5, 6]))
        );
        let test_binary = next_named(&mut events, "test").await;
        assert_eq!(
            test_binary.payload(),
            &Payload::Binary(Bytes::from_static(&[1, 2, 3]))
        );
        let multi = next_named(&mut events, "This is the first argument").await;
        assert_eq!(multi.payload().len(), 2);
        assert_eq!(multi.arg::<Value>(1).unwrap(), json!({"argCount": 3}));
        let abc = next_named(&mut events, "on_abc_event").await;
        let (empty, obj): (String, Value) = abc.payload().deserialize().unwrap();
        assert_eq!((empty.as_str(), obj["abc"].clone()), ("", json!(0)));

        let info = client.link_info();
        assert!(info.engine_sid.is_some() && info.transport.is_some());
        assert_eq!(info.ping_interval, Some(Duration::from_millis(25000)));
        assert!(info.last_packet_at.is_some());

        client.disconnect().await.unwrap();
        assert!(matches!(
            client.state(),
            ConnectionState::Closed {
                reason: DisconnectReason::IoClientDisconnect,
                ..
            }
        ));
        assert!(matches!(client.emit("test", "late"), Err(Error::Closed)));
    })
    .await
    .expect("test `connects_and_receives_the_greetings` timed out")
}

/// socket.ts "should emit and ack multiple arguments" / "should work with false"
#[tokio::test]
async fn ack_roundtrip_with_multiple_arguments() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = connect(ClientBuilder::new(server())).await;
        let args = vec![
            json!(1),
            json!("2"),
            json!({"3": [false]}),
            json!(false),
            json!(null),
        ];
        let reply = client
            .emit_with_ack("echo", args.clone(), WAIT)
            .await
            .unwrap();
        assert_eq!(reply, Payload::Text(args));
        let reply = client.emit_with_ack("test", "x", WAIT).await.unwrap();
        assert_eq!(reply.arg::<String>(0).unwrap(), "woot");
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `ack_roundtrip_with_multiple_arguments` timed out")
}

/// socket.ts "should receive utf8 multibyte characters"
#[tokio::test]
async fn utf8_roundtrip() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = connect(ClientBuilder::new(server())).await;
        for text in [
            "てすと",
            "Я Б Г Д Ж Й",
            "Ä ä Ü ü ß",
            "utf8 — string",
            "😀🎉",
        ] {
            let reply = client.emit_with_ack("echo", text, WAIT).await.unwrap();
            assert_eq!(reply.arg::<String>(0).unwrap(), text);
        }
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `utf8_roundtrip` timed out")
}

/// socket.ts "should timeout when the server does not acknowledge the event in time"
#[tokio::test]
async fn ack_timeout_is_an_error() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = connect(ClientBuilder::new(server())).await;
        let err = client
            .emit_with_ack("ack_late", (), Duration::from_millis(100))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::AckTimeout(d) if d == Duration::from_millis(100)));
        // the late ack (500 ms) is ignored; the client keeps working
        tokio::time::sleep(Duration::from_millis(600)).await;
        let reply = client.emit_with_ack("echo", 1, WAIT).await.unwrap();
        assert_eq!(reply.arg::<i64>(0).unwrap(), 1);
        assert_eq!(client.link_info().pending_acks, 0);
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `ack_timeout_is_an_error` timed out")
}

/// socket.ts "should not ack upon disconnection": a pending ack fails ("socket has been
/// disconnected") when the client disconnects.
#[tokio::test]
async fn pending_ack_fails_on_disconnect() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = connect(ClientBuilder::new(server())).await;
        let pending = {
            let client = client.clone();
            tokio::spawn(async move { client.emit_with_ack("ack_late", (), WAIT).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        client.disconnect().await.unwrap();
        let result = pending.await.unwrap();
        assert!(matches!(result, Err(Error::Disconnected)), "{result:?}");
    })
    .await
    .expect("test `pending_ack_fails_on_disconnect` timed out")
}

#[tokio::test]
async fn reserved_event_names_are_refused() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = connect(ClientBuilder::new(server())).await;
        for name in [
            "connect",
            "connect_error",
            "disconnect",
            "disconnecting",
            "newListener",
            "removeListener",
        ] {
            assert!(matches!(client.emit(name, ()), Err(Error::ReservedEvent(n)) if n == name));
        }
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `reserved_event_names_are_refused` timed out")
}

/// The server emits with a callback; the client answers through `Event::ack`.
#[tokio::test]
async fn answers_server_acks() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let (builder, mut events) = collect(ClientBuilder::new(server()));
        let builder = builder.on("question", |event: Event, _: Client| async move {
            assert!(event.wants_ack());
            let q: String = event.arg(0).unwrap();
            event
                .ack(vec![json!(format!("re: {q}")), json!(42)])
                .unwrap();
            // a second ack is ignored, like socket.io-client
            event.ack("twice").unwrap();
        });
        let client = connect(builder).await;
        client.emit("ask_me", "why").unwrap();
        let answered = next_named(&mut events, "answered").await;
        assert_eq!(
            answered.payload(),
            &Payload::Text(vec![json!("re: why"), json!(42)])
        );
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `answers_server_acks` timed out")
}

/// A binary ack reply (BINARY_ACK).
#[tokio::test]
async fn answers_server_acks_with_binary() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let (builder, mut events) = collect(ClientBuilder::new(server()));
        let builder = builder.on("question", |event: Event, _: Client| async move {
            event.ack(Bytes::from_static(b"bin")).unwrap();
        });
        let client = connect(builder).await;
        client.emit("ask_me", "x").unwrap();
        let answered = next_named(&mut events, "answered").await;
        assert_eq!(
            answered.payload(),
            &Payload::Binary(Bytes::from_static(b"bin"))
        );
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `answers_server_acks_with_binary` timed out")
}

/// connection.ts "should send events with ArrayBuffers in the correct order", binary acks,
/// binary that contains the polling separator, mixed and nested binary.
#[tokio::test]
async fn binary_in_every_position() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        for transport in [TransportType::Polling, TransportType::Websocket] {
            let (builder, mut events) =
                collect(ClientBuilder::new(server()).transport_type(transport));
            let client = connect(builder).await;

            client.emit("binary", Bytes::from_static(&[9, 8])).unwrap();
            let echoed = next_named(&mut events, "binary-received").await;
            assert_eq!(
                echoed.payload(),
                &Payload::Binary(Bytes::from_static(&[9, 8]))
            );

            let reply = client
                .emit_with_ack("binary", Bytes::from_static(&[1]), WAIT)
                .await
                .unwrap();
            assert_eq!(reply, Payload::Binary(Bytes::from_static(&[1, 2, 3])));

            client.emit("binary_delimiter", ()).unwrap();
            let delim = next_named(&mut events, "binary-delimiter").await;
            assert_eq!(
                delim.payload(),
                &Payload::Binary(Bytes::from_static(&[0x1e, 1, 0x1e]))
            );

            client.emit("binary_with_args", ()).unwrap();
            let mixed = next_named(&mut events, "binary-args").await;
            assert_eq!(
                mixed.payload(),
                &Payload::Mixed {
                    args: vec![json!("extra"), Payload::placeholder(0)],
                    attachments: vec![Bytes::from_static(&[9])],
                }
            );

            client.emit("binary_mixed", ()).unwrap();
            let nested = next_named(&mut events, "binary-mixed").await;
            match nested.payload() {
                Payload::Mixed { args, attachments } => {
                    assert_eq!(attachments.len(), 2);
                    assert_eq!(args[1], json!("tail"));
                    let a = &args[0]["a"];
                    let b = &args[0]["b"][0];
                    let pick = |p: &Value| attachments[p["num"].as_u64().unwrap() as usize].clone();
                    assert_eq!(pick(a), Bytes::from_static(&[1]));
                    assert_eq!(pick(b), Bytes::from_static(&[2, 3]));
                }
                other => panic!("expected Mixed, got {other:?}"),
            }

            // Mixed out: the echo comes back the same
            let out = Payload::Mixed {
                args: vec![
                    json!({"file": Payload::placeholder(0)}),
                    json!(7),
                    Payload::placeholder(1),
                ],
                attachments: vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")],
            };
            let back = client
                .emit_with_ack("echo", out.clone(), WAIT)
                .await
                .unwrap();
            assert_eq!(back, out);
            client.disconnect().await.unwrap();
        }
    })
    .await
    .expect("test `binary_in_every_position` timed out")
}

/// socket.ts "should emit events in order"
#[tokio::test]
async fn emits_keep_their_order() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let (builder, mut events) = collect(ClientBuilder::new(server()));
        let client = connect(builder).await;
        for i in 0..20 {
            client.emit("echo", i).unwrap();
        }
        for i in 0..20 {
            let event = next_named(&mut events, "echo-back").await;
            assert_eq!(event.arg::<i64>(0).unwrap(), i);
        }
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `emits_keep_their_order` timed out")
}

/// socket.ts "query option: should properly encode the parameters"
#[tokio::test]
async fn query_is_encoded() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = connect(
            ClientBuilder::new(server())
                .query("key1", "1")
                .query("key2", "a&b=c")
                .query("key3", "Б"),
        )
        .await;
        let query = client
            .emit_with_ack("handshake_query", (), WAIT)
            .await
            .unwrap();
        let query: Value = query.arg(0).unwrap();
        assert_eq!(query["key1"], "1");
        assert_eq!(query["key2"], "a&b=c");
        assert_eq!(query["key3"], "Б");
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `query_is_encoded` timed out")
}

#[tokio::test]
async fn auth_is_sent_in_connect() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        for (password, expect) in [("123", "success"), ("nope", "failed")] {
            let (builder, mut events) = collect(
                ClientBuilder::new(socket_io_auth_server())
                    .namespace("admin")
                    .auth(json!({ "password": password })),
            );
            let client = connect(builder).await;
            assert_eq!(client.namespace(), "/admin");
            let auth = next_named(&mut events, "auth").await;
            assert_eq!(auth.arg::<String>(0).unwrap(), expect);
            client.disconnect().await.unwrap();
        }
    })
    .await
    .expect("test `auth_is_sent_in_connect` timed out")
}

#[tokio::test]
async fn auth_with_is_called_per_connect() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let calls = Arc::new(AtomicUsize::new(0));
        let (builder, mut events) =
            collect(ClientBuilder::new(socket_io_auth_server()).auth_with({
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async { json!({"password": "123"}) }
                }
            }));
        let client = connect(builder).await;
        assert_eq!(
            next_named(&mut events, "auth")
                .await
                .arg::<String>(0)
                .unwrap(),
            "success"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `auth_with_is_called_per_connect` timed out")
}

/// The URL path is the namespace, like socket.io-client (`io("http://host/admin")`).
#[tokio::test]
async fn url_path_is_the_namespace() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = connect(ClientBuilder::new(format!("{}/admin", server()))).await;
        assert_eq!(client.namespace(), "/admin");
        assert!(client.connected());
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `url_path_is_the_namespace` timed out")
}

/// socket.ts "should fire a connect_error event with error data on middleware failure"
#[tokio::test]
async fn connect_rejected_carries_message_and_data() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let err = ClientBuilder::new(server())
            .namespace("/reject")
            .connect()
            .await
            .unwrap_err();
        match err {
            Error::ConnectRejected { message, data, .. } => {
                assert_eq!(message, "not authorized");
                assert_eq!(data, Some(json!({"reason": "test"})));
            }
            other => panic!("expected ConnectRejected, got {other:?}"),
        }
    })
    .await
    .expect("test `connect_rejected_carries_message_and_data` timed out")
}

/// An unknown namespace is refused by the server.
#[tokio::test]
async fn unknown_namespace_is_rejected() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
    let err = ClientBuilder::new(server())
        .namespace("/does-not-exist")
        .connect()
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::ConnectRejected { ref message, .. } if message == "Invalid namespace"),
        "{err:?}"
    );
})
    .await
    .expect("test `unknown_namespace_is_rejected` timed out")
}

/// socket.ts "should fire a connect_error event when the connection cannot be established"
#[tokio::test]
async fn connect_error_when_nothing_listens() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let err = ClientBuilder::new("http://127.0.0.1:9/?token=SECRET")
            .reconnection(false)
            .connect()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Engine(_)), "{err:?}");
        let text = format!("{err} {err:?}");
        assert!(!text.contains("SECRET"), "secret leaked: {text}");
    })
    .await
    .expect("test `connect_error_when_nothing_listens` timed out")
}

#[tokio::test]
async fn connect_timeout_on_a_silent_server() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", silent.local_addr().unwrap());
        let started = std::time::Instant::now();
        let err = ClientBuilder::new(url)
            .timeout(Duration::from_millis(300))
            .connect()
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::ConnectTimeout(_)
                    | Error::Engine(socketio_next::engineio::Error::Timeout(_))
                    | Error::Engine(socketio_next::engineio::Error::Http(_))
            ),
            "{err:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(3));
    })
    .await
    .expect("test `connect_timeout_on_a_silent_server` timed out")
}

/// The slow namespace answers CONNECT after 200 ms: `connect()` waits for it.
#[tokio::test]
async fn connect_waits_for_the_namespace() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = connect(ClientBuilder::new(server()).namespace("/slow")).await;
        assert!(client.connected());
        let reply = client.emit_with_ack("echo", "first", WAIT).await.unwrap();
        assert_eq!(reply.arg::<String>(0).unwrap(), "first");
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `connect_waits_for_the_namespace` timed out")
}

/// A server namespace disconnect is final by default (socket.io-client).
#[tokio::test]
async fn server_disconnect_is_final_by_default() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `server_disconnect_is_final_by_default` timed out")
}

#[tokio::test]
async fn server_disconnect_reconnects_when_asked() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `server_disconnect_reconnects_when_asked` timed out")
}

/// The server closes the engine.io connection: Disconnected(transport close), then reconnect.
#[tokio::test]
async fn transport_close_reconnects_with_state_transitions() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `transport_close_reconnects_with_state_transitions` timed out")
}

/// A packet socket.io-parser rejects closes the connection with `parse error`.
#[tokio::test]
async fn invalid_packet_is_a_parse_error() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `invalid_packet_is_a_parse_error` timed out")
}

/// connection.ts "should try to reconnect twice and fail when requested two attempts"
#[tokio::test]
async fn reconnect_attempts_are_bounded() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `reconnect_attempts_are_bounded` timed out")
}

/// Fail fast by default: emits while the link is down are refused.
#[tokio::test]
async fn emit_while_disconnected_fails_fast() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `emit_while_disconnected_fails_fast` timed out")
}

/// Opt-in buffering (socket.io-client `sendBuffer`): emits during the outage go out after the
/// reconnect, in order, and their acks resolve.
#[tokio::test]
async fn buffer_while_disconnected_delivers_after_reconnect() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `buffer_while_disconnected_delivers_after_reconnect` timed out")
}

/// A dropped link fails the pending acks with `Disconnected`.
#[tokio::test]
async fn pending_ack_fails_when_the_link_drops() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `pending_ack_fails_when_the_link_drops` timed out")
}

/// Dropping every handle closes the connection.
#[tokio::test]
async fn drop_closes_the_connection() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let proxy = Proxy::start(&host_port(&server())).await;
        let client =
            connect(ClientBuilder::new(proxy.url()).transport_type(TransportType::Websocket)).await;
        assert!(proxy.wait_open(1, WAIT).await);
        drop(client);
        assert!(
            proxy.wait_open(0, WAIT).await,
            "connection still open after drop"
        );
    })
    .await
    .expect("test `drop_closes_the_connection` timed out")
}

/// `disconnect()` from inside a handler does not deadlock, and no handler runs after it.
#[tokio::test]
async fn disconnect_from_a_handler() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `disconnect_from_a_handler` timed out")
}

/// No handler starts after `disconnect()` returned.
#[tokio::test]
async fn no_handler_after_disconnect_returns() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `no_handler_after_disconnect_returns` timed out")
}

/// Runtime listeners: on / once / off / off_listener / listener_count.
#[tokio::test]
async fn runtime_listeners() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `runtime_listeners` timed out")
}

/// Handlers run in arrival order even when they await.
#[tokio::test]
async fn handlers_run_in_order() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let builder =
            ClientBuilder::new(server()).on("echo-back", move |event: Event, _: Client| {
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
    })
    .await
    .expect("test `handlers_run_in_order` timed out")
}

/// A panicking handler does not stop the event task.
#[tokio::test]
async fn panicking_handler_is_contained() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let (builder, mut events) = collect(ClientBuilder::new(server()));
        let builder = builder.on("echo-back", |_: Event, _: Client| async move {
            panic!("handler bug");
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
    })
    .await
    .expect("test `panicking_handler_is_contained` timed out")
}

#[tokio::test]
async fn every_transport_type_connects() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        for transport in [
            TransportType::Any,
            TransportType::Polling,
            TransportType::Websocket,
            TransportType::WebsocketUpgrade,
        ] {
            let client = connect(ClientBuilder::new(server()).transport_type(transport)).await;
            let reply = client.emit_with_ack("echo", "x", WAIT).await.unwrap();
            assert_eq!(reply.arg::<String>(0).unwrap(), "x", "{transport:?}");
            let expected = match transport {
                TransportType::Polling => socketio_next::TransportKind::Polling,
                _ => socketio_next::TransportKind::Websocket,
            };
            assert_eq!(
                client.link_info().transport,
                Some(expected),
                "{transport:?}"
            );
            client.disconnect().await.unwrap();
        }
    })
    .await
    .expect("test `every_transport_type_connects` timed out")
}

/// The packet inspector sees both directions.
#[tokio::test]
async fn packet_inspector() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let client = connect(ClientBuilder::new(server()).inspect_packets({
            let seen = Arc::clone(&seen);
            move |dir, packet| {
                seen.lock()
                    .unwrap()
                    .push((dir, packet.packet_type, packet.data.clone()));
            }
        }))
        .await;
        client.emit_with_ack("echo", "probe", WAIT).await.unwrap();
        client.disconnect().await.unwrap();
        let seen = seen.lock().unwrap();
        use socketio_next::Direction::*;
        use socketio_next::protocol::socketio::PacketId;
        assert!(
            seen.iter()
                .any(|(d, t, _)| *d == Outgoing && *t == PacketId::Connect)
        );
        assert!(
            seen.iter()
                .any(|(d, t, _)| *d == Incoming && *t == PacketId::Connect)
        );
        assert!(seen.iter().any(|(d, t, data)| *d == Outgoing
            && *t == PacketId::Event
            && data.as_deref() == Some(r#"["echo","probe"]"#)));
        assert!(
            seen.iter()
                .any(|(d, t, _)| *d == Incoming && *t == PacketId::Ack)
        );
        assert!(
            seen.iter()
                .any(|(d, t, _)| *d == Outgoing && *t == PacketId::Disconnect)
        );
    })
    .await
    .expect("test `packet_inspector` timed out")
}

#[cfg(any(feature = "rustls", feature = "native-tls"))]
#[tokio::test]
async fn invalid_header_is_reported() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let err = ClientBuilder::new(server())
            .header("bad header", "x")
            .connect()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidHeader(_)));
    })
    .await
    .expect("test `invalid_header_is_reported` timed out")
}

/// Headers reach the server (the restart server echoes `message_back`).
#[tokio::test]
async fn headers_reach_the_server() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let (builder, mut events) = collect(
            ClientBuilder::new(socket_io_restart_server()).header("message_back", "from a header"),
        );
        let client = connect(builder).await;
        let message = next_named(&mut events, "message").await;
        assert_eq!(message.arg::<String>(0).unwrap(), "from a header");
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `headers_reach_the_server` timed out")
}

/// The restart server goes away for 2 s: the client reconnects by itself.
#[tokio::test]
async fn reconnects_after_a_server_restart() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let (builder, mut events) = collect(
            ClientBuilder::new(socket_io_restart_server())
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
    })
    .await
    .expect("test `reconnects_after_a_server_restart` timed out")
}

/// Query values never reach the logs (only the engine.io parameters stay visible).
#[tokio::test]
async fn logs_redact_query_values() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        use std::io::Write;
        #[derive(Clone, Default)]
        struct Buf(Arc<std::sync::Mutex<Vec<u8>>>);
        impl Write for Buf {
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(data);
                Ok(data.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Buf::default();
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let client = connect(ClientBuilder::new(server()).query("token", "SECRET-TOKEN")).await;
        client.emit_with_ack("echo", 1, WAIT).await.unwrap();
        client.disconnect().await.unwrap();
        let _ = ClientBuilder::new("http://127.0.0.1:9")
            .query("token", "SECRET-TOKEN")
            .reconnection(false)
            .connect()
            .await;

        let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("token=***"),
            "no redacted url in the logs:\n{logs}"
        );
        assert!(logs.contains("connected"), "{logs}");
        assert!(
            !logs.contains("SECRET-TOKEN"),
            "secret in the logs:\n{logs}"
        );
    })
    .await
    .expect("test `logs_redact_query_values` timed out")
}

/// A blackholed link (TCP open, nothing gets through) is detected by the heartbeat:
/// Disconnected(ping timeout), and `last_ping_at` showed the pings before.
#[tokio::test]
async fn ping_timeout_on_a_blackholed_link() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let proxy = Proxy::start(&host_port(&fast_ping_server())).await;
        let (builder, mut states) = states(
            ClientBuilder::new(proxy.url())
                .transport_type(TransportType::Websocket)
                .reconnection(false),
        );
        let client = connect(builder).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(client.link_info().last_ping_at.is_some(), "no ping seen");
        assert_eq!(
            client.link_info().ping_interval,
            Some(Duration::from_millis(300))
        );
        proxy.freeze();
        let closed = next_state(&mut states, ConnectionState::is_closed).await;
        assert!(
            matches!(
                closed,
                ConnectionState::Closed {
                    reason: DisconnectReason::PingTimeout,
                    ..
                }
            ),
            "{closed:?}"
        );
        proxy.cut();
    })
    .await
    .expect("test `ping_timeout_on_a_blackholed_link` timed out")
}

/// `disconnect()` on a dead link returns promptly (goodbye packets are bounded).
#[tokio::test]
async fn disconnect_is_bounded_on_a_dead_link() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let proxy = Proxy::start(&host_port(&server())).await;
        let client =
            connect(ClientBuilder::new(proxy.url()).transport_type(TransportType::Polling)).await;
        proxy.freeze();
        let started = std::time::Instant::now();
        client.disconnect().await.unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "{:?}",
            started.elapsed()
        );
        assert!(client.state().is_closed());
        proxy.cut();
    })
    .await
    .expect("test `disconnect_is_bounded_on_a_dead_link` timed out")
}

/// A proxy that blackholes websocket upgrades: `TransportType::Any` stays on polling within
/// the connect timeout (engine.io-client keeps the polling session after a failed probe).
#[tokio::test]
async fn blackholed_websocket_falls_back_to_polling() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let proxy = Proxy::start(&host_port(&server())).await;
        proxy.block_websocket(true);
        let started = std::time::Instant::now();
        let client = connect(ClientBuilder::new(proxy.url()).timeout(Duration::from_secs(4))).await;
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(
            client.link_info().transport,
            Some(socketio_next::TransportKind::Polling)
        );
        let reply = client
            .emit_with_ack("echo", "over polling", WAIT)
            .await
            .unwrap();
        assert_eq!(reply.arg::<String>(0).unwrap(), "over polling");
        client.disconnect().await.unwrap();
        proxy.cut();
    })
    .await
    .expect("test `blackholed_websocket_falls_back_to_polling` timed out")
}

/// Untrusted input in user hands: a huge ack timeout, a non-object auth.
#[tokio::test]
async fn input_edge_cases_do_not_break_the_client() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = connect(ClientBuilder::new(server())).await;
        let reply = client
            .emit_with_ack("echo", 1, Duration::MAX)
            .await
            .unwrap();
        assert_eq!(reply.arg::<i64>(0).unwrap(), 1);
        assert!(client.connected());
        client.disconnect().await.unwrap();
        let err = ClientBuilder::new(server())
            .auth(json!("token"))
            .connect()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidAuth(_)), "{err:?}");
    })
    .await
    .expect("test `input_edge_cases_do_not_break_the_client` timed out")
}

/// A panicking packet inspector is contained.
#[tokio::test]
async fn panicking_inspector_is_contained() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = connect(ClientBuilder::new(server()).inspect_packets(|_, packet| {
            if packet.data.as_deref() == Some(r#"["echo","boom"]"#) {
                panic!("inspector bug");
            }
        }))
        .await;
        let reply = client.emit_with_ack("echo", "boom", WAIT).await.unwrap();
        assert_eq!(reply.arg::<String>(0).unwrap(), "boom");
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `panicking_inspector_is_contained` timed out")
}

/// `auth_with` returning a non-object on a reconnect closes the client (no endless loop).
#[tokio::test]
async fn bad_auth_on_reconnect_is_final() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `bad_auth_on_reconnect_is_final` timed out")
}

/// Query parameters and headers are computed again for every connect attempt (a token that
/// expires, a signed timestamp): the reconnect after a cut sends the new values.
#[tokio::test]
async fn connect_params_are_recomputed_per_attempt() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `connect_params_are_recomputed_per_attempt` timed out")
}

#[tokio::test]
async fn connect_headers_are_recomputed_per_attempt() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `connect_headers_are_recomputed_per_attempt` timed out")
}

#[tokio::test]
async fn a_panicking_params_function_fails_the_attempt() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let result = ClientBuilder::new(server())
            .connect_params_with(|| async { panic!("boom") })
            .connect()
            .await;
        assert!(matches!(result, Err(Error::InvalidHeader(_))), "{result:?}");
    })
    .await
    .expect("test `a_panicking_params_function_fails_the_attempt` timed out")
}

/// A closed client (here: the server disconnected it, which is final by default) connects
/// again with `reconnect()`, keeping its handlers.
#[tokio::test]
async fn reconnect_after_closed_keeps_the_handlers() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
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
    })
    .await
    .expect("test `reconnect_after_closed_keeps_the_handlers` timed out")
}

/// `.proxy()`: polling as absolute-form requests, the websocket through a `CONNECT` tunnel,
/// both with the proxy credentials.
#[tokio::test]
async fn an_explicit_proxy_carries_polling_and_the_websocket() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let proxy = HttpProxy::start().await;
        let with_auth = proxy.url().replace("http://", "http://user:p%40ss@");
        let client = connect(
            ClientBuilder::new(server())
                .proxy(with_auth)
                .transport_type(TransportType::WebsocketUpgrade),
        )
        .await;
        let reply = client
            .emit_with_ack("echo", json!("via proxy"), WAIT)
            .await
            .unwrap();
        assert_eq!(reply.arg::<String>(0).unwrap(), "via proxy");
        let log = proxy.log();
        let auth = "Basic dXNlcjpwQHNz"; // user:p@ss
        assert!(
            log.iter()
                .any(|l| l.starts_with("GET /socket.io/") && l.ends_with(auth)),
            "{log:?}"
        );
        let target = host_port(&server());
        assert!(
            log.iter().any(|l| l == &format!("CONNECT {target} {auth}")),
            "{log:?}"
        );
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `an_explicit_proxy_carries_polling_and_the_websocket` timed out")
}

#[tokio::test]
async fn a_proxy_that_refuses_is_a_typed_error() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let result = ClientBuilder::new(server())
            .proxy("http://127.0.0.1:9")
            .transport_type(TransportType::Websocket)
            .reconnection(false)
            .connect()
            .await;
        match result {
            Err(Error::Engine(socketio_next::engineio::Error::ProxyFailed(text))) => {
                assert!(!text.contains("127.0.0.1"), "{text}")
            }
            other => panic!("{other:?}"),
        }
    })
    .await
    .expect("test `a_proxy_that_refuses_is_a_typed_error` timed out")
}

/// One HTTP client per socket.io client: the handshake connection is reused for the polls
/// (two clients, as before, meant a third TCP connection).
#[tokio::test]
async fn one_http_client_for_every_request() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let tcp = Proxy::start(&host_port(&server())).await;
        let client =
            connect(ClientBuilder::new(tcp.url()).transport_type(TransportType::Polling)).await;
        client.emit_with_ack("echo", 1, WAIT).await.unwrap();
        client.emit_with_ack("echo", 2, WAIT).await.unwrap();
        assert!(tcp.accepted() <= 2, "{} connections", tcp.accepted());
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `one_http_client_for_every_request` timed out")
}

/// `cookies`: the engine.io `io` cookie set on the handshake goes back on every later polling
/// request and on the websocket upgrade.
#[cfg(feature = "cookies")]
#[tokio::test]
async fn cookies_reach_polling_and_the_websocket() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        for (transport, kind) in [
            (TransportType::Polling, "polling"),
            (TransportType::WebsocketUpgrade, "websocket"),
        ] {
            // the caller's own cookie and the jar's (the sticky one) both go out
            let client = connect(
                ClientBuilder::new(cookie_server())
                    .header("Cookie", "user=1")
                    .transport_type(transport),
            )
            .await;
            client.emit_with_ack("echo", 1, WAIT).await.unwrap();
            let sid = client.link_info().engine_sid.unwrap();
            let seen = cookies_seen(&sid).await;
            let cookie = format!("io={sid}");
            assert!(
                seen.iter().any(|l| l.starts_with(&format!("{kind} "))
                    && l.contains(&cookie)
                    && l.contains("user=1")),
                "{kind}: {seen:?}"
            );
            client.disconnect().await.unwrap();
        }
    })
    .await
    .expect("test `cookies_reach_polling_and_the_websocket` timed out")
}

#[cfg(any(feature = "rustls", feature = "native-tls"))]
/// A private CA through `tls_config`: the TLS handshake with the test server passes (4202 is an
/// engine.io server: its greeting then fails to parse as socket.io, a protocol error).
#[tokio::test]
async fn tls_config_trusts_a_private_ca() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let tls = socketio_next::TlsConfig::with_root_certificates_pem(&ca_cert_pem()).unwrap();
        let result = ClientBuilder::new("https://localhost:4202")
            .tls_config(tls)
            .path("/engine.io/")
            .transport_type(TransportType::Polling)
            .timeout(Duration::from_secs(10))
            .connect()
            .await;
        assert!(matches!(result, Err(Error::Protocol(_))), "{result:?}");
    })
    .await
    .expect("test `tls_config_trusts_a_private_ca` timed out")
}

/// The bundled roots only: native-tls and `platform-verifier` ask the OS, whose store may trust
/// a CA a developer installed.
#[cfg(all(feature = "rustls", not(feature = "platform-verifier")))]
#[tokio::test]
async fn without_the_ca_tls_fails() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let result = ClientBuilder::new("https://localhost:4202")
            .path("/engine.io/")
            .transport_type(TransportType::Polling)
            .timeout(Duration::from_secs(10))
            .connect()
            .await;
        assert!(matches!(result, Err(Error::Engine(_))), "{result:?}");
    })
    .await
    .expect("test `without_the_ca_tls_fails` timed out")
}

/// IPv6: a `[::1]` server through a proxy (the `CONNECT` line keeps the brackets), and a proxy
/// on `[::1]` (the connect address drops them).
#[tokio::test]
async fn ipv6_targets_and_proxies() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let port = host_port(&server()).rsplit(':').next().unwrap().to_owned();
        let v6 = format!("http://[::1]:{port}");
        for proxy in [
            HttpProxy::start().await,
            HttpProxy::start_on("[::1]:0").await,
        ] {
            let client = connect(
                ClientBuilder::new(v6.clone())
                    .proxy(proxy.url())
                    .transport_type(TransportType::Websocket),
            )
            .await;
            client.emit_with_ack("echo", 1, WAIT).await.unwrap();
            assert!(
                proxy.log().contains(&format!("CONNECT [::1]:{port}")),
                "{:?}",
                proxy.log()
            );
            client.disconnect().await.unwrap();
        }
    })
    .await
    .expect("test `ipv6_targets_and_proxies` timed out")
}

/// `no_proxy()` wins over the environment for both transports (the environment is not touched
/// here: an unreachable explicit proxy would fail, `no_proxy` after it connects directly).
#[tokio::test]
async fn no_proxy_connects_directly() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = connect(
            ClientBuilder::new(server())
                .proxy("http://127.0.0.1:9")
                .no_proxy()
                .transport_type(TransportType::WebsocketUpgrade),
        )
        .await;
        client.emit_with_ack("echo", 1, WAIT).await.unwrap();
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `no_proxy_connects_directly` timed out")
}
