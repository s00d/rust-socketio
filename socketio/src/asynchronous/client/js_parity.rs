//! Ports of socket.io-client 4.8 tests (`test/connection.ts`, `test/socket.ts`) against the ci/
//! servers. Cases tied to APIs this crate does not have (Manager / namespace multiplexing,
//! socket.id, volatile, onAnyOutgoing, promise acks, autoConnect, manual reconnect after
//! disconnect, Date / Blob / ArrayBuffer types, timers) are listed as N/A in the parity report.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use bytes::Bytes;
use futures_util::FutureExt;
use serde_json::{json, Value};
use tokio::{sync::mpsc, time::timeout};

use crate::{
    asynchronous::{client::client::Client, ClientBuilder},
    Event, Payload, TransportType,
};

const WAIT: Duration = Duration::from_secs(5);

fn url_with_path(addr: &str) -> String {
    format!("http://{addr}/socket.io/")
}

/// Connects and waits for the namespace CONNECT.
async fn connected(builder: ClientBuilder) -> Client {
    let (tx, mut rx) = mpsc::channel(1);
    let client = builder
        .on(Event::Connect, move |_, _| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(()).await;
            }
            .boxed()
        })
        .connect()
        .await
        .unwrap();
    timeout(WAIT, rx.recv()).await.unwrap().unwrap();
    client
}

/// Emits with an ack and returns the ack arguments (None on timeout).
///
/// Deliberate API difference: the Rust ack callback gets the whole argument list as one JSON
/// array (`Payload::Text(vec![[arg1, arg2]])`), where socket.io-client passes the arguments;
/// unwrap that level so the JS expectations apply unchanged.
async fn ack(client: &Client, event: &str, data: impl Into<Payload>) -> Option<Payload> {
    let (tx, mut rx) = mpsc::channel(1);
    client
        .emit_with_ack(event, data, WAIT, move |payload, _| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(payload).await;
            }
            .boxed()
        })
        .await
        .unwrap();
    match timeout(WAIT, rx.recv()).await.ok().flatten() {
        Some(Payload::Text(mut values)) if values.len() == 1 && values[0].is_array() => {
            let Value::Array(args) = values.remove(0) else {
                unreachable!()
            };
            Some(Payload::Text(args))
        }
        other => other,
    }
}

fn text(values: Vec<Value>) -> Option<Payload> {
    Some(Payload::Text(values))
}

#[tokio::test]
async fn should_work_with_false() {
    let client = connected(ClientBuilder::new(crate::test::socket_io_server())).await;
    assert_eq!(
        ack(&client, "echo", json!(false)).await,
        text(vec![json!(false)])
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn should_receive_utf8_multibyte_characters() {
    let client = connected(ClientBuilder::new(crate::test::socket_io_server())).await;
    for s in [
        "てすと",
        "Я Б Г Д Ж Й",
        "Ä ä Ü ü ß",
        "utf8 — string",
        "😀🎉",
    ] {
        assert_eq!(
            ack(&client, "echo", json!(s)).await,
            text(vec![json!(s)]),
            "{s}"
        );
    }
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn should_emit_and_ack_multiple_arguments() {
    let client = connected(ClientBuilder::new(crate::test::socket_io_server())).await;
    let args = vec![json!(1), json!("2"), json!({"a": [3]})];
    assert_eq!(ack(&client, "echo", args.clone()).await, text(args));
    client.disconnect().await.unwrap();
}

/// socket.ts "should emit events in order"
#[tokio::test]
async fn should_emit_events_in_order() {
    let (tx, mut rx) = mpsc::channel(8);
    let client = connected(ClientBuilder::new(crate::test::socket_io_server()).on(
        "echo-back",
        move |payload, _| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(payload).await;
            }
            .boxed()
        },
    ))
    .await;
    for i in 0..5 {
        client.emit("echo", json!(i)).await.unwrap();
    }
    for i in 0..5 {
        let payload = timeout(WAIT, rx.recv()).await.unwrap().unwrap();
        assert_eq!(Some(payload), text(vec![json!(i)]));
    }
    client.disconnect().await.unwrap();
}

/// connection.ts "should send events with ArrayBuffers in the correct order"
#[tokio::test]
async fn should_send_binary_and_text_events_in_the_correct_order() {
    let client = connected(ClientBuilder::new(crate::test::socket_io_server())).await;
    let (tx, mut rx) = mpsc::channel(4);
    let payloads = [
        Payload::Binary(Bytes::from_static(&[1])),
        Payload::Text(vec![json!("two")]),
        Payload::Binary(Bytes::from_static(&[3])),
    ];
    for (i, payload) in payloads.into_iter().enumerate() {
        let tx = tx.clone();
        client
            .emit_with_ack("echo", payload, WAIT, move |_, _| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(i).await;
                }
                .boxed()
            })
            .await
            .unwrap();
    }
    for i in 0..3 {
        assert_eq!(timeout(WAIT, rx.recv()).await.ok().flatten(), Some(i));
    }
    client.disconnect().await.unwrap();
}

/// connection.ts "should get base64 data as a last resort" (binary over polling)
#[tokio::test]
async fn should_get_binary_data_over_polling() {
    let (tx, mut rx) = mpsc::channel(4);
    let client = connected(
        ClientBuilder::new(crate::test::socket_io_server())
            .transport_type(TransportType::Polling)
            .on("test", move |payload, _| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(payload).await;
                }
                .boxed()
            }),
    )
    .await;
    // ci/socket-io.js emits `test` twice on connection: a string, then Buffer [1, 2, 3].
    let mut binary = None;
    while let Ok(Some(payload)) = timeout(WAIT, rx.recv()).await {
        if let Payload::Binary(bytes) = payload {
            binary = Some(bytes);
            break;
        }
    }
    assert_eq!(binary, Some(Bytes::from_static(&[1, 2, 3])));
    client.disconnect().await.unwrap();
}

/// socket.ts "query option: should properly encode the parameters"
#[tokio::test]
async fn query_option_should_properly_encode_the_parameters() {
    let mut url = crate::test::socket_io_server();
    url.query_pairs_mut()
        .append_pair("token", "a&b=c")
        .append_pair("name", "Тест");
    let client = connected(ClientBuilder::new(url)).await;
    let (tx, mut rx) = mpsc::channel(1);
    client
        .emit_with_ack(
            "handshake_query",
            Payload::Text(vec![]),
            WAIT,
            move |p, _| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(p).await;
                }
                .boxed()
            },
        )
        .await
        .unwrap();
    let Some(Payload::Text(values)) = timeout(WAIT, rx.recv()).await.ok().flatten() else {
        panic!("no ack");
    };
    // ack arguments arrive as one array (see `ack`)
    assert_eq!(values[0][0]["token"], json!("a&b=c"));
    assert_eq!(values[0][0]["name"], json!("Тест"));
    client.disconnect().await.unwrap();
}

/// socket.ts "fire a connect_error event when the connection cannot be established": the Rust
/// client reports it as an Err from connect().
#[tokio::test]
async fn connect_error_when_the_connection_cannot_be_established() {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = closed.local_addr().unwrap();
    drop(closed);
    let result = timeout(
        WAIT,
        ClientBuilder::new(url_with_path(&addr.to_string())).connect(),
    )
    .await
    .expect("connect hung");
    assert!(result.is_err());
}

/// socket.ts "should timeout when the server does not acknowledge the event in time": a late ack
/// must not run the callback.
#[tokio::test]
async fn should_not_call_the_ack_callback_after_its_timeout() {
    let client = connected(ClientBuilder::new(crate::test::socket_io_server())).await;
    let called = Arc::new(AtomicUsize::new(0));
    let flag = called.clone();
    client
        .emit_with_ack(
            "ack_late",
            json!(1),
            Duration::from_millis(200),
            move |_, _| {
                flag.fetch_add(1, Ordering::SeqCst);
                async {}.boxed()
            },
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(called.load(Ordering::SeqCst), 0);
    client.disconnect().await.unwrap();
}

/// socket.ts "should not ack upon disconnection (callback)"
#[tokio::test]
async fn should_not_ack_upon_disconnection() {
    let client = connected(ClientBuilder::new(crate::test::socket_io_server())).await;
    let called = Arc::new(AtomicUsize::new(0));
    let flag = called.clone();
    client
        .emit_with_ack("ack_late", json!(1), WAIT, move |_, _| {
            flag.fetch_add(1, Ordering::SeqCst);
            async {}.boxed()
        })
        .await
        .unwrap();
    client.disconnect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(called.load(Ordering::SeqCst), 0);
}

/// socket.ts "should fire a connect_error event with error data on middleware failure"
#[tokio::test]
async fn connect_error_with_error_data_on_middleware_failure() {
    let (tx, mut rx) = mpsc::channel(4);
    let _client = ClientBuilder::new(crate::test::socket_io_server())
        .namespace("/reject")
        .on(Event::Error, move |payload, _| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(payload).await;
            }
            .boxed()
        })
        .connect()
        .await
        .unwrap();
    let Some(Payload::Text(values)) = timeout(WAIT, rx.recv()).await.ok().flatten() else {
        panic!("no connect_error");
    };
    let message = values[0].as_str().unwrap_or_default().to_owned();
    assert!(message.contains("not authorized"), "{message}");
    assert!(message.contains(r#""reason":"test""#), "{message}");
}
