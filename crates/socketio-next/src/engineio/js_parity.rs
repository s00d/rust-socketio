//! Ports of engine.io-client 6.6 tests (`test/connection.js`, `test/binary-fallback.js`) against
//! the ci/ servers. Browser-only cases (workers, beforeunload, Blob), transport options without a
//! Rust counterpart (agent, perMessageDeflate, rememberUpgrade, cookies) and client-side
//! batching by `maxPayload` (this client sends one request per packet) are not ported.
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;

use crate::engineio::{Client, ClientBuilder, Error, Packet, PacketId};

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
    let url = crate::engineio::test::engine_io_polling_server().unwrap();
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
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let mut client = polling_client().await;
        client
            .emit(Packet::new(PacketId::Message, "utf8"))
            .await
            .unwrap();
        let packet = next_message(&mut client).await;
        assert_eq!(packet.data, Bytes::from("cash money €€€ てすと"));
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `should_receive_multibyte_utf8_strings_with_polling` timed out")
}

#[tokio::test]
async fn should_receive_emoji() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let mut client = polling_client().await;
        client
            .emit(Packet::new(PacketId::Message, "emoji"))
            .await
            .unwrap();
        let packet = next_message(&mut client).await;
        assert_eq!(packet.data, Bytes::from("😀🎉"));
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `should_receive_emoji` timed out")
}

/// binary-fallback.js: binary over polling arrives base64-encoded and is decoded.
#[tokio::test]
async fn should_be_able_to_receive_binary_data_over_polling() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let mut client = polling_client().await;
        client
            .emit(Packet::new(PacketId::Message, "binary"))
            .await
            .unwrap();
        let packet = next_message(&mut client).await;
        assert_eq!(packet.packet_id, PacketId::MessageBinary);
        assert_eq!(packet.data, Bytes::from_static(&[1, 2, 3]));
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `should_be_able_to_receive_binary_data_over_polling` timed out")
}

#[tokio::test]
async fn should_not_send_packets_if_socket_closes() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let client = polling_client().await;
        client.disconnect().await.unwrap();
        let result = client.emit(Packet::new(PacketId::Message, "late")).await;
        assert!(matches!(result, Err(Error::NotConnected)));
    })
    .await
    .expect("test `should_not_send_packets_if_socket_closes` timed out")
}

#[tokio::test]
async fn should_connect_to_localhost_ws() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        let url = crate::engineio::test::engine_io_server().unwrap();
        let mut client = ClientBuilder::new(url).build_websocket().await.unwrap();
        client.connect().await.unwrap();
        assert_eq!(
            next_message(&mut client).await.data,
            Bytes::from("hello client")
        );
        client.disconnect().await.unwrap();
    })
    .await
    .expect("test `should_connect_to_localhost_ws` timed out")
}
