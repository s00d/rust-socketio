//! Stories: transports and the network: every transport type, a blackholed link or websocket,
//! proxies (explicit, refusing, IPv6, none), one HTTP client per socket, log redaction.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use socketio_next::{ClientBuilder, ConnectionState, DisconnectReason, Error, TransportType};

use crate::common::*;

#[tokio::test]
async fn every_transport_type_connects() {
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
}

/// Query values never reach the logs (only the engine.io parameters stay visible).
#[tokio::test]
async fn logs_redact_query_values() {
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
}

/// A blackholed link (TCP open, nothing gets through) is detected by the heartbeat:
/// Disconnected(ping timeout), and `last_ping_at` showed the pings before.
#[tokio::test]
async fn ping_timeout_on_a_blackholed_link() {
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
}

/// `disconnect()` on a dead link returns promptly (goodbye packets are bounded).
#[tokio::test]
async fn disconnect_is_bounded_on_a_dead_link() {
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
}

/// A proxy that blackholes websocket upgrades: `TransportType::Any` stays on polling within
/// the connect timeout (engine.io-client keeps the polling session after a failed probe).
#[tokio::test]
async fn blackholed_websocket_falls_back_to_polling() {
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
}

/// `.proxy()`: polling as absolute-form requests, the websocket through a `CONNECT` tunnel,
/// both with the proxy credentials.
#[tokio::test]
async fn an_explicit_proxy_carries_polling_and_the_websocket() {
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
}

#[tokio::test]
async fn a_proxy_that_refuses_is_a_typed_error() {
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
}

/// One HTTP client per socket.io client: the handshake connection is reused for the polls
/// (two clients, as before, meant a third TCP connection).
#[tokio::test]
async fn one_http_client_for_every_request() {
    let tcp = Proxy::start(&host_port(&server())).await;
    let client =
        connect(ClientBuilder::new(tcp.url()).transport_type(TransportType::Polling)).await;
    client.emit_with_ack("echo", 1, WAIT).await.unwrap();
    client.emit_with_ack("echo", 2, WAIT).await.unwrap();
    assert!(tcp.accepted() <= 2, "{} connections", tcp.accepted());
    client.disconnect().await.unwrap();
}

/// IPv6: a `[::1]` server through a proxy (the `CONNECT` line keeps the brackets), and a proxy
/// on `[::1]` (the connect address drops them).
#[tokio::test]
async fn ipv6_targets_and_proxies() {
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
}

/// `no_proxy()` wins over the environment for both transports (the environment is not touched
/// here: an unreachable explicit proxy would fail, `no_proxy` after it connects directly).
#[tokio::test]
async fn no_proxy_connects_directly() {
    let client = connect(
        ClientBuilder::new(server())
            .proxy("http://127.0.0.1:9")
            .no_proxy()
            .transport_type(TransportType::WebsocketUpgrade),
    )
    .await;
    client.emit_with_ack("echo", 1, WAIT).await.unwrap();
    client.disconnect().await.unwrap();
}
