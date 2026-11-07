//! Ports of engine.io-parser 5.2 tests (`test/index.ts`, `test/node.ts`). The encoder/decoder
//! stream cases (WebTransport framing) and typed-array / ArrayBuffer variants have no Rust
//! equivalent.
use super::*;

fn decode(raw: &'static [u8]) -> std::result::Result<Packet, String> {
    Packet::try_from(Bytes::from_static(raw)).map_err(|e| e.to_string())
}

#[test]
fn should_encode_decode_a_string() {
    let packet = Packet::new(PacketId::Message, "test");
    assert_eq!(Bytes::from(packet.clone()), Bytes::from_static(b"4test"));
    assert_eq!(decode(b"4test").unwrap(), packet);
}

#[test]
fn should_fail_to_decode_a_malformed_packet() {
    assert!(decode(b"").is_err());
    assert!(decode(b"a123").is_err());
}

#[test]
fn should_encode_decode_all_packet_types() {
    let packets = vec![
        Packet::new(PacketId::Open, ""),
        Packet::new(PacketId::Close, ""),
        Packet::new(PacketId::Ping, "probe"),
        Packet::new(PacketId::Pong, "probe"),
        Packet::new(PacketId::Message, "test"),
    ];
    let raw = Bytes::from_static(b"0\x1e1\x1e2probe\x1e3probe\x1e4test");
    assert_eq!(Bytes::try_from(Payload(packets.clone())).unwrap(), raw);
    assert_eq!(Payload::try_from(raw).unwrap().0, packets);
}

#[test]
fn should_fail_to_decode_a_malformed_payload() {
    for raw in [&b"{"[..], b"{}", br#"["a123", "a456"]"#] {
        assert!(
            Payload::try_from(Bytes::from_static(raw)).is_err(),
            "{raw:?}"
        );
    }
}

#[test]
fn should_encode_decode_a_buffer_as_base64() {
    let packet = Packet::new(PacketId::MessageBinary, Bytes::from_static(&[1, 2, 3, 4]));
    assert_eq!(
        Bytes::from(packet.clone()),
        Bytes::from_static(b"bAQIDBA==")
    );
    assert_eq!(decode(b"bAQIDBA==").unwrap(), packet);
}

#[test]
fn should_encode_decode_a_string_and_buffer_payload() {
    let packets = vec![
        Packet::new(PacketId::Message, "test"),
        Packet::new(PacketId::MessageBinary, Bytes::from_static(&[1, 2, 3, 4])),
    ];
    let raw = Bytes::from_static(b"4test\x1ebAQIDBA==");
    assert_eq!(Bytes::try_from(Payload(packets.clone())).unwrap(), raw);
    assert_eq!(Payload::try_from(raw).unwrap().0, packets);
}
