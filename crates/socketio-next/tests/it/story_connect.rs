//! Stories: connecting. A client joins a namespace (URL path or option) with query, headers,
//! auth or `auth_with`; the server greets it, rejects it, or never answers.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};
use socketio_next::{
    ClientBuilder, ConnectParams, ConnectionState, DisconnectReason, Error, Payload,
};

use crate::common::*;

#[tokio::test]
async fn connects_and_receives_the_greetings() {
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
}

/// socket.ts "query option: should properly encode the parameters"
#[tokio::test]
async fn query_is_encoded() {
    let client = connect(
        ClientBuilder::new(server())
            .query("key1", "1")
            .query("key2", "a&b=c")
            .query("key3", "Б")
            // engine.io's own parameters stay the client's (a caller cannot hijack a
            // session with `sid` or change the protocol with `EIO`)
            .query("EIO", "3")
            .query("sid", "someone-else")
            .query("transport", "websocket"),
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
    assert_eq!(query["EIO"], "4");
    assert!(query.get("sid").is_none(), "{query}");
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn auth_is_sent_in_connect() {
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
}

#[tokio::test]
async fn auth_with_is_called_per_connect() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (builder, mut events) = collect(ClientBuilder::new(socket_io_auth_server()).auth_with({
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
}

/// The URL path is the namespace, like socket.io-client (`io("http://host/admin")`).
#[tokio::test]
async fn url_path_is_the_namespace() {
    let client = connect(ClientBuilder::new(format!("{}/admin", server()))).await;
    assert_eq!(client.namespace(), "/admin");
    assert!(client.connected());
    client.disconnect().await.unwrap();
}

/// socket.ts "should fire a connect_error event with error data on middleware failure"
#[tokio::test]
async fn connect_rejected_carries_message_and_data() {
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
}

/// An unknown namespace is refused by the server.
#[tokio::test]
async fn unknown_namespace_is_rejected() {
    let err = ClientBuilder::new(server())
        .namespace("/does-not-exist")
        .connect()
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::ConnectRejected { ref message, .. } if message == "Invalid namespace"),
        "{err:?}"
    );
}

/// socket.ts "should fire a connect_error event when the connection cannot be established"
#[tokio::test]
async fn connect_error_when_nothing_listens() {
    let err = ClientBuilder::new("http://127.0.0.1:9/?token=SECRET")
        .reconnection(false)
        .connect()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Engine(_)), "{err:?}");
    let text = format!("{err} {err:?}");
    assert!(!text.contains("SECRET"), "secret leaked: {text}");
}

#[tokio::test]
async fn connect_timeout_on_a_silent_server() {
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
}

/// The slow namespace answers CONNECT after 200 ms: `connect()` waits for it.
#[tokio::test]
async fn connect_waits_for_the_namespace() {
    let client = connect(ClientBuilder::new(server()).namespace("/slow")).await;
    assert!(client.connected());
    let reply = client.emit_with_ack("echo", "first", WAIT).await.unwrap();
    assert_eq!(reply.arg::<String>(0).unwrap(), "first");
    client.disconnect().await.unwrap();
}

#[cfg(any(feature = "rustls", feature = "native-tls"))]
#[tokio::test]
async fn invalid_header_is_reported() {
    let err = ClientBuilder::new(server())
        .header("bad header", "x")
        .connect()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidHeader(_)));
}

/// Headers reach the server (the restart server echoes `message_back`).
#[tokio::test]
async fn headers_reach_the_server() {
    let (builder, mut events) = collect(
        ClientBuilder::new(socket_io_restart_server()).header("message_back", "from a header"),
    );
    let client = connect(builder).await;
    let message = next_named(&mut events, "message").await;
    assert_eq!(message.arg::<String>(0).unwrap(), "from a header");
    client.disconnect().await.unwrap();
}

/// Untrusted input in user hands: a huge ack timeout, a non-object auth.
#[tokio::test]
async fn input_edge_cases_do_not_break_the_client() {
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
}

#[tokio::test]
async fn a_panicking_params_function_fails_the_attempt() {
    let result = ClientBuilder::new(server())
        .connect_params_with(|| async { panic!("boom") })
        .connect()
        .await;
    assert!(matches!(result, Err(Error::InvalidHeader(_))), "{result:?}");
    // a panic before the future is returned fails the attempt the same way
    #[allow(unreachable_code)]
    let result = ClientBuilder::new(server())
        .connect_params_with(|| -> std::future::Ready<ConnectParams> { panic!("boom") })
        .connect()
        .await;
    assert!(matches!(result, Err(Error::InvalidHeader(_))), "{result:?}");
    #[allow(unreachable_code)]
    let result = ClientBuilder::new(server())
        .auth_with(|| -> std::future::Ready<Value> { panic!("boom") })
        .connect()
        .await;
    assert!(matches!(result, Err(Error::InvalidAuth(_))), "{result:?}");
}
