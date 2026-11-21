//! Stories: events and acks in both directions, text and binary anywhere in the arguments,
//! order, timeouts, reserved names, the packet inspector.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};
use socketio_next::{Client, ClientBuilder, Error, Event, Payload, TransportType};

use crate::common::*;

/// socket.ts "should emit and ack multiple arguments" / "should work with false"
#[tokio::test]
async fn ack_roundtrip_with_multiple_arguments() {
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
}

/// socket.ts "should receive utf8 multibyte characters"
#[tokio::test]
async fn utf8_roundtrip() {
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
}

/// socket.ts "should timeout when the server does not acknowledge the event in time"
#[tokio::test]
async fn ack_timeout_is_an_error() {
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
}

/// socket.ts "should not ack upon disconnection": a pending ack fails ("socket has been
/// disconnected") when the client disconnects.
#[tokio::test]
async fn pending_ack_fails_on_disconnect() {
    let client = connect(ClientBuilder::new(server())).await;
    let pending = {
        let client = client.clone();
        tokio::spawn(async move { client.emit_with_ack("ack_late", (), WAIT).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.disconnect().await.unwrap();
    let result = pending.await.unwrap();
    assert!(matches!(result, Err(Error::Disconnected)), "{result:?}");
}

#[tokio::test]
async fn reserved_event_names_are_refused() {
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
}

/// The server emits with a callback; the client answers through `Event::ack`.
#[tokio::test]
async fn answers_server_acks() {
    let (builder, mut events) = collect(ClientBuilder::new(server()));
    let builder = builder.on("question", |event: Event, _: Client| async move {
        assert!(event.wants_ack());
        let q: String = event.arg(0).unwrap();
        // an ack that cannot be encoded (11 attachments) is not counted as sent
        let too_many = Payload::Mixed {
            args: (0..11).map(Payload::placeholder).collect(),
            attachments: vec![Bytes::from_static(b"x"); 11],
        };
        assert!(event.ack(too_many).is_err());
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
}

/// A binary ack reply (BINARY_ACK).
#[tokio::test]
async fn answers_server_acks_with_binary() {
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
}

/// socket.ts "should emit events in order"
#[tokio::test]
async fn emits_keep_their_order() {
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
}

/// The packet inspector sees both directions.
#[tokio::test]
async fn packet_inspector() {
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
}

/// A panicking packet inspector is contained.
#[tokio::test]
async fn panicking_inspector_is_contained() {
    let client = connect(ClientBuilder::new(server()).inspect_packets(|_, packet| {
        if packet.data.as_deref() == Some(r#"["echo","boom"]"#) {
            panic!("inspector bug");
        }
    }))
    .await;
    let reply = client.emit_with_ack("echo", "boom", WAIT).await.unwrap();
    assert_eq!(reply.arg::<String>(0).unwrap(), "boom");
    client.disconnect().await.unwrap();
}

/// connection.ts "should send events with ArrayBuffers in the correct order", binary acks,
/// binary that contains the polling separator, mixed and nested binary.
#[tokio::test]
async fn binary_in_every_position() {
    for transport in [TransportType::Polling, TransportType::Websocket] {
        let (builder, mut events) = collect(ClientBuilder::new(server()).transport_type(transport));
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
}

/// Given a connected client, when 32 tasks each send 20 acked emits at once (half of them to
/// `ack_late` with a short timeout), then every reply reaches its own caller (no ack id mix-up)
/// and every late one times out alone. Like socket.io-client-csharp
/// `EmitAsyncAck_ConcurrentEmit_PacketIdAndAckHandlerCountAreCorrect`.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_acks_reach_their_own_callers() {
    let client = connect(ClientBuilder::new(server())).await;
    // collected: a lazy iterator would spawn each task only when the previous one ended
    let tasks: Vec<_> = (0..32)
        .map(|task| {
            let client = client.clone();
            tokio::spawn(async move {
                for i in 0..20 {
                    if i % 2 == 0 {
                        let sent = json!({ "task": task, "i": i });
                        let reply = client.emit_with_ack("echo", sent.clone(), WAIT).await;
                        assert_eq!(reply.unwrap(), Payload::Text(vec![sent]));
                    } else {
                        let late = client
                            .emit_with_ack("ack_late", (), Duration::from_millis(50))
                            .await;
                        assert!(matches!(late, Err(Error::AckTimeout(_))), "{late:?}");
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    client.disconnect().await.unwrap();
}

/// Given a connected client, when it sends 900 KiB of text and 600 KiB of binary (800 KiB as
/// base64 over polling) over polling and over the websocket, then the echo comes back
/// unchanged (one polling request each, under the server's 1 MB maxPayload).
#[tokio::test]
async fn large_payloads_in_both_transports() {
    for transport in [TransportType::Polling, TransportType::Websocket] {
        let client = connect(ClientBuilder::new(server()).transport_type(transport)).await;
        let text = Payload::Text(vec![json!("é".repeat(450 * 1024))]);
        let binary = Payload::Binary(Bytes::from(vec![7u8; 600 * 1024]));
        for sent in [text, binary] {
            let reply = client.emit_with_ack("echo", sent.clone(), WAIT).await;
            match reply {
                Ok(reply) => assert!(reply == sent, "{transport:?}: the echo differs"),
                Err(e) => panic!("{transport:?}: {e}"),
            }
        }
        client.disconnect().await.unwrap();
    }
}
