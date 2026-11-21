//! Stories: the engine.io client on its own (`socketio_next::engineio`): handshake over
//! polling or websocket, upgrade and fallback, messages in both directions (text, binary,
//! large, UTF-8), timeouts, misuse. Includes ports of engine.io-client 6.6 tests
//! (`test/connection.js`, `test/binary-fallback.js`); browser-only cases (workers,
//! beforeunload, Blob) and options without a Rust counterpart (agent, perMessageDeflate,
//! rememberUpgrade) are not ported.

use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use http::HeaderMap;
use socketio_next::engineio::{
    Client, ClientBuilder, DEFAULT_TIMEOUT as HANDSHAKE_TIMEOUT, Error, Packet, PacketId, Result,
};
use url::Url;

use crate::common::*;

async fn next_message(client: &mut Client) -> Packet {
    loop {
        let packet = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("no packet in 5 s")
            .expect("stream ended")
            .expect("stream error");
        if matches!(
            packet.packet_id,
            PacketId::Message | PacketId::MessageBinary
        ) {
            return packet;
        }
    }
}

async fn polling_client() -> Client {
    let url = engine_io_polling_url();
    let mut client = ClientBuilder::new(url).build_polling().await.unwrap();
    client.connect().await.unwrap();
    assert_eq!(
        next_message(&mut client).await.data,
        Bytes::from("hello client")
    );
    client
}

#[tokio::test]
async fn should_receive_multibyte_utf8_strings_with_polling() {
    let mut client = polling_client().await;
    client
        .emit(Packet::new(PacketId::Message, "utf8"))
        .await
        .unwrap();
    let packet = next_message(&mut client).await;
    assert_eq!(packet.data, Bytes::from("cash money €€€ てすと"));
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn should_receive_emoji() {
    let mut client = polling_client().await;
    client
        .emit(Packet::new(PacketId::Message, "emoji"))
        .await
        .unwrap();
    let packet = next_message(&mut client).await;
    assert_eq!(packet.data, Bytes::from("😀🎉"));
    client.disconnect().await.unwrap();
}

/// binary-fallback.js: binary over polling arrives base64-encoded and is decoded.
#[tokio::test]
async fn should_be_able_to_receive_binary_data_over_polling() {
    let mut client = polling_client().await;
    client
        .emit(Packet::new(PacketId::Message, "binary"))
        .await
        .unwrap();
    let packet = next_message(&mut client).await;
    assert_eq!(packet.packet_id, PacketId::MessageBinary);
    assert_eq!(packet.data, Bytes::from_static(&[1, 2, 3]));
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn should_not_send_packets_if_socket_closes() {
    let client = polling_client().await;
    client.disconnect().await.unwrap();
    let result = client.emit(Packet::new(PacketId::Message, "late")).await;
    assert!(matches!(result, Err(Error::NotConnected)));
}

#[tokio::test]
async fn should_connect_to_localhost_ws() {
    let url = engine_io_url();
    let mut client = ClientBuilder::new(url).build_websocket().await.unwrap();
    client.connect().await.unwrap();
    assert_eq!(
        next_message(&mut client).await.data,
        Bytes::from("hello client")
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_cloneable() -> Result<()> {
    let url = engine_io_url();

    let mut sut = builder(url).build().await?;
    let mut cloned = sut.clone();

    sut.connect().await?;

    // when the underlying socket is connected, the
    // state should also change on the cloned one
    assert!(sut.is_connected());
    assert!(cloned.is_connected());

    // both clients should reflect the same messages.
    assert_eq!(
        sut.next().await.unwrap()?,
        Packet::new(PacketId::Message, "hello client")
    );

    sut.emit(Packet::new(PacketId::Message, "respond")).await?;

    assert_eq!(
        cloned.next().await.unwrap()?,
        Packet::new(PacketId::Message, "Roger Roger")
    );

    cloned.disconnect().await?;

    // when the underlying socket is disconnected, the
    // state should also change on the cloned one
    assert!(!sut.is_connected());
    assert!(!cloned.is_connected());

    Ok(())
}

/// A polling response bigger than one HTTP body chunk must still be decoded as one payload.
#[tokio::test]
async fn test_polling_large_message() -> Result<()> {
    let url = engine_io_polling_url();
    let mut socket = builder(url).build_polling().await?;
    socket.connect().await?;

    assert_eq!(
        socket.next().await.unwrap()?,
        Packet::new(PacketId::Message, "hello client")
    );

    socket
        .emit(Packet::new(PacketId::Message, "respond_large"))
        .await?;

    assert_eq!(
        socket.next().await.unwrap()?,
        Packet::new(PacketId::Message, "x".repeat(64 * 1024))
    );

    socket.disconnect().await
}

/// A peer that accepts TCP but never answers (blackholed proxy, half-open link) must not hang
/// the polling handshake forever.
#[tokio::test(start_paused = true)]
async fn test_polling_handshake_times_out() {
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = Url::parse(&format!("http://{}/", silent.local_addr().unwrap())).unwrap();

    let result = tokio::time::timeout(
        HANDSHAKE_TIMEOUT * 2,
        ClientBuilder::new(url).build_polling(),
    )
    .await
    .expect("polling handshake hung past its timeout");
    assert!(result.is_err());
}

/// After disconnect() the packet stream ends instead of polling a closed session.
#[tokio::test]
async fn test_stream_ends_after_disconnect() {
    for url in [engine_io_polling_url(), engine_io_url()] {
        let mut socket = builder(url).build().await.unwrap();
        socket.connect().await.unwrap();
        socket.disconnect().await.unwrap();
        assert!(!socket.is_connected());

        let next = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
            .await
            .expect("stream still waiting after disconnect");
        assert!(next.is_none(), "stream yielded {next:?} after disconnect");
    }
}

#[tokio::test(start_paused = true)]
async fn test_websocket_connect_times_out() {
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = Url::parse(&format!("http://{}/", silent.local_addr().unwrap())).unwrap();

    let result = tokio::time::timeout(
        HANDSHAKE_TIMEOUT * 2,
        ClientBuilder::new(url).build_websocket(),
    )
    .await
    .expect("websocket connect hung past its timeout");
    assert!(result.is_err());
}

#[tokio::test]
async fn test_illegal_actions() -> Result<()> {
    let url = engine_io_url();
    let mut sut = builder(url.clone()).build().await?;

    assert!(
        sut.emit(Packet::new(PacketId::Close, Bytes::new()))
            .await
            .is_err()
    );

    sut.connect().await?;

    assert!(sut.next().await.unwrap().is_ok());

    assert!(
        builder(Url::parse("fake://fake.fake").unwrap())
            .build_websocket()
            .await
            .is_err()
    );

    sut.disconnect().await?;

    Ok(())
}

use reqwest::header::HOST;

fn builder(url: Url) -> ClientBuilder {
    ClientBuilder::new(url)
        .on_open(|_| async {
            println!("Open event!");
        })
        .on_packet(|packet| async move {
            println!("Received packet: {:?}", packet);
        })
        .on_data(|data| async move {
            println!("Received data: {:?}", std::str::from_utf8(&data));
        })
        .on_close(|_| async {
            println!("Close event!");
        })
        .on_error(|error| async move {
            println!("Error {}", error);
        })
}

async fn test_connection(socket: Client) -> Result<()> {
    let mut socket = socket;

    socket.connect().await.unwrap();

    assert_eq!(
        socket.next().await.unwrap()?,
        Packet::new(PacketId::Message, "hello client")
    );
    println!("received msg, about to send");

    socket
        .emit(Packet::new(PacketId::Message, "respond"))
        .await?;

    println!("send msg");

    assert_eq!(
        socket.next().await.unwrap()?,
        Packet::new(PacketId::Message, "Roger Roger")
    );
    println!("received 2");

    socket.close().await
}

#[tokio::test]
async fn test_connection_long() -> Result<()> {
    // Long lived socket to receive pings
    let url = engine_io_url();
    let mut socket = builder(url).build().await?;

    socket.connect().await?;

    // hello client
    assert!(matches!(
        socket.next().await.unwrap()?,
        Packet {
            packet_id: PacketId::Message,
            ..
        }
    ));
    // Ping
    assert!(matches!(
        socket.next().await.unwrap()?,
        Packet {
            packet_id: PacketId::Ping,
            ..
        }
    ));

    socket.disconnect().await?;

    assert!(!socket.is_connected());

    Ok(())
}

#[tokio::test]
async fn test_connection_dynamic() -> Result<()> {
    let url = engine_io_url();
    let socket = builder(url).build().await?;
    test_connection(socket).await?;

    let url = engine_io_polling_url();
    let socket = builder(url).build().await?;
    test_connection(socket).await
}

#[tokio::test]
async fn test_connection_fallback() -> Result<()> {
    let url = engine_io_url();
    let socket = builder(url).build_with_fallback().await?;
    test_connection(socket).await?;

    let url = engine_io_polling_url();
    let socket = builder(url).build_with_fallback().await?;
    test_connection(socket).await
}

#[cfg(any(feature = "rustls", feature = "native-tls"))]
#[tokio::test]
async fn test_connection_dynamic_secure() -> Result<()> {
    let url = engine_io_secure_url();
    let mut socket_builder = builder(url);
    socket_builder = socket_builder.tls_config(test_tls_config());
    let socket = socket_builder.build().await?;
    test_connection(socket).await
}

#[tokio::test]
async fn test_connection_polling() -> Result<()> {
    let url = engine_io_url();
    let socket = builder(url).build_polling().await?;
    test_connection(socket).await
}

#[cfg(any(feature = "rustls", feature = "native-tls"))]
#[tokio::test]
async fn test_connection_wss() -> Result<()> {
    let url = engine_io_polling_url();
    assert!(builder(url).build_websocket_with_upgrade().await.is_err());

    let host = std::env::var("ENGINE_IO_SECURE_HOST").unwrap_or_else(|_| "localhost".to_owned());
    let mut url = engine_io_secure_url();

    let mut headers = HeaderMap::default();
    headers.insert(HOST, host.parse().unwrap());
    let mut builder = builder(url.clone());

    builder = builder.tls_config(test_tls_config());
    builder = builder.headers(headers.clone());
    let socket = builder.clone().build_websocket_with_upgrade().await?;

    test_connection(socket).await?;

    let socket = builder.build_websocket().await?;

    test_connection(socket).await?;

    url.set_scheme("wss").unwrap();

    let builder = self::builder(url)
        .tls_config(test_tls_config())
        .headers(headers);
    let socket = builder.clone().build_websocket().await?;

    test_connection(socket).await?;

    assert!(builder.build_websocket_with_upgrade().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_connection_ws() -> Result<()> {
    let url = engine_io_polling_url();
    assert!(builder(url.clone()).build_websocket().await.is_err());
    assert!(builder(url).build_websocket_with_upgrade().await.is_err());

    let mut url = engine_io_url();

    let builder = builder(url.clone());
    let socket = builder.clone().build_websocket().await?;
    test_connection(socket).await?;

    let socket = builder.build_websocket_with_upgrade().await?;
    test_connection(socket).await?;

    url.set_scheme("ws").unwrap();

    let builder = self::builder(url);
    let socket = builder.clone().build_websocket().await?;

    test_connection(socket).await?;

    assert!(builder.build_websocket_with_upgrade().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_open_invariants() -> Result<()> {
    let url = engine_io_url();
    let illegal_url = "this is illegal";

    assert!(Url::parse(illegal_url).is_err());

    let invalid_protocol = "file:///tmp/foo";
    assert!(
        builder(Url::parse(invalid_protocol).unwrap())
            .build()
            .await
            .is_err()
    );

    let sut = builder(url.clone()).build().await?;
    let _error = sut
        .emit(Packet::new(PacketId::Close, Bytes::new()))
        .await
        .expect_err("error");
    assert!(matches!(_error, Error::NotConnected));

    // test missing match arm in socket constructor
    let mut headers = HeaderMap::default();
    let host = std::env::var("ENGINE_IO_SECURE_HOST").unwrap_or_else(|_| "localhost".to_owned());
    headers.insert(HOST, host.parse().unwrap());

    let _ = builder(url.clone()).build().await?;
    let _ = builder(url).headers(headers).build().await?;
    Ok(())
}
