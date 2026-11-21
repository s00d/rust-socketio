//! Model test: the engine.io packet and payload codec (engine.io-parser v4).
//!
//! proptest builds random payloads of text packets (every packet type, unicode data without the
//! 0x1e separator) and binary messages. The model writes the expected payload: each packet as
//! its type digit and data, a binary message as `b` and its base64, joined by 0x1e. The codec
//! must produce exactly it and decode it back to the same packets.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use proptest::prelude::*;
use socketio_next::protocol::engineio::{Batch, Packet, PacketId};

fn text_kind() -> impl Strategy<Value = (PacketId, u8)> {
    prop_oneof![
        Just((PacketId::Open, b'0')),
        Just((PacketId::Close, b'1')),
        Just((PacketId::Ping, b'2')),
        Just((PacketId::Pong, b'3')),
        Just((PacketId::Message, b'4')),
        Just((PacketId::Upgrade, b'5')),
        Just((PacketId::Noop, b'6')),
    ]
}

/// A packet and the model's encoding of it.
fn packet() -> impl Strategy<Value = (Packet, String)> {
    prop_oneof![
        (text_kind(), "[^\u{1e}]{0,24}").prop_map(|((id, digit), data)| {
            let model = format!("{}{data}", digit as char);
            (Packet::new(id, Bytes::from(data)), model)
        }),
        proptest::collection::vec(any::<u8>(), 0..32).prop_map(|data| {
            let model = format!("b{}", BASE64.encode(&data));
            (
                Packet::new(PacketId::MessageBinary, Bytes::from(data)),
                model,
            )
        }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn payloads_encode_as_the_model_says_and_decode_back(
        packets in proptest::collection::vec(packet(), 1..8),
    ) {
        let model: Vec<String> = packets.iter().map(|(_, m)| m.clone()).collect();
        let packets: Vec<Packet> = packets.into_iter().map(|(p, _)| p).collect();
        let encoded = Bytes::try_from(Batch(packets.clone())).unwrap();
        prop_assert_eq!(std::str::from_utf8(&encoded).unwrap(), model.join("\u{1e}"));
        let decoded: Vec<Packet> = Batch::try_from(encoded).unwrap().into_iter().collect();
        prop_assert_eq!(decoded, packets);
    }

    #[test]
    fn single_packets_round_trip((packet, model) in packet()) {
        let encoded = Bytes::from(packet.clone());
        prop_assert_eq!(std::str::from_utf8(&encoded).unwrap(), model);
        prop_assert_eq!(Packet::try_from(encoded).unwrap(), packet);
    }

    /// Bytes that are not a valid payload are an error, never a panic.
    #[test]
    fn any_bytes_decode_or_err(bytes in proptest::collection::vec(any::<u8>(), 0..48)) {
        let _ = Batch::try_from(Bytes::from(bytes.clone()));
        let _ = Packet::try_from(Bytes::from(bytes));
    }
}
