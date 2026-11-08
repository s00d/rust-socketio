//! Ports of socket.io-parser 4.2.x tests (`test/parser.js`, `test/buffer.js`): wire vectors that
//! the JS decoder accepts or rejects. Encoder-only cases (circular objects, custom revivers,
//! Blob / ArrayBuffer / nested buffers) have no Rust equivalent: the client builds packets from
//! typed values with one top-level attachment.
use crate::protocol::socketio::*;
use bytes::Bytes;

fn decode(raw: &str) -> std::result::Result<Packet, String> {
    Packet::try_from(&Bytes::from(raw.to_owned())).map_err(|e| e.to_string())
}

/// Decodes `raw`, checks the fields and that encoding gives `raw` back.
fn roundtrip(raw: &str, packet_type: PacketId, nsp: &str, data: Option<&str>, id: Option<u64>) {
    let packet = decode(raw).unwrap_or_else(|e| panic!("{raw}: {e}"));
    assert_eq!(packet.packet_type, packet_type, "{raw}");
    assert_eq!(packet.nsp, nsp, "{raw}");
    assert_eq!(packet.data.as_deref(), data, "{raw}");
    assert_eq!(packet.id, id, "{raw}");
    assert_eq!(Bytes::from(&packet), Bytes::from(raw.to_owned()), "{raw}");
}

#[test]
fn encodes_connection() {
    let data = r#"{"token":"123"}"#;
    roundtrip(
        &format!("0/woot,{data}"),
        PacketId::Connect,
        "/woot",
        Some(data),
        None,
    );
}

#[test]
fn encodes_disconnection() {
    roundtrip("1/woot,", PacketId::Disconnect, "/woot", None, None);
}

#[test]
fn encodes_an_event() {
    roundtrip(
        r#"2["a",1,{}]"#,
        PacketId::Event,
        "/",
        Some(r#"["a",1,{}]"#),
        None,
    );
}

#[test]
fn encodes_an_event_with_an_integer_as_event_name() {
    roundtrip(
        r#"2[1,"a",{}]"#,
        PacketId::Event,
        "/",
        Some(r#"[1,"a",{}]"#),
        None,
    );
}

#[test]
fn encodes_an_event_with_ack() {
    roundtrip(
        r#"2/test,1["a",1,{}]"#,
        PacketId::Event,
        "/test",
        Some(r#"["a",1,{}]"#),
        Some(1),
    );
}

#[test]
fn encodes_an_ack() {
    roundtrip(
        r#"3123["a",1,{}]"#,
        PacketId::Ack,
        "/",
        Some(r#"["a",1,{}]"#),
        Some(123),
    );
}

#[test]
fn encodes_a_connect_error() {
    roundtrip(
        r#"4"Unauthorized""#,
        PacketId::ConnectError,
        "/",
        Some(r#""Unauthorized""#),
        None,
    );
}

#[test]
fn encodes_a_connect_error_with_object() {
    let data = r#"{"message":"Unauthorized"}"#;
    roundtrip(
        &format!("4{data}"),
        PacketId::ConnectError,
        "/",
        Some(data),
        None,
    );
}

#[test]
fn decodes_a_bad_binary_packet() {
    assert!(decode("5").is_err());
}

#[test]
fn throws_an_error_upon_parsing_error() {
    // invalid payload
    for raw in [
        r#"442["some","data""#,
        r#"0/admin,"invalid""#,
        "0[]",
        "1/admin,{}",
        r#"2/admin,"invalid"#,
        "2/admin,{}",
        r#"2[{"toString":"foo"}]"#,
        r#"2[true,"foo"]"#,
        r#"2[null,"bar"]"#,
        r#"2["connect"]"#,
        r#"2["disconnect","123"]"#,
    ] {
        assert!(decode(raw).is_err(), "accepted invalid payload {raw}");
    }
    // illegal attachments
    for raw in ["5", "51", "50-", "5a-", "51.23-"] {
        assert!(
            decode(raw).is_err(),
            "accepted illegal attachment count {raw}"
        );
    }
    // unknown packet type
    assert!(decode("999").is_err());
}

#[test]
fn reserved_event_names_are_rejected() {
    for name in [
        "connect",
        "connect_error",
        "disconnect",
        "disconnecting",
        "newListener",
        "removeListener",
    ] {
        assert!(decode(&format!(r#"2["{name}"]"#)).is_err(), "{name}");
        assert!(
            decode(&format!(r#"51-["{name}",{{"_placeholder":true,"num":0}}]"#)).is_err(),
            "binary {name}"
        );
    }
}

#[test]
fn encodes_a_buffer() {
    // buffer.js "encodes a Buffer": an event with one attachment
    let raw = r#"51-["a",{"_placeholder":true,"num":0}]"#;
    let packet = decode(raw).unwrap();
    assert_eq!(packet.packet_type, PacketId::BinaryEvent);
    assert_eq!(packet.attachment_count, 1);
    assert_eq!(packet.binary_event_name(), "a".to_owned());
}
