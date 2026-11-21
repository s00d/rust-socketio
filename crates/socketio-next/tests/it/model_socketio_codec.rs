//! Model test: the socket.io packet codec (socket.io-parser).
//!
//! proptest builds random events and acks: a namespace, an ack id, an event name (unicode,
//! digits, separators), JSON arguments nested in arrays and objects, and binary buffers
//! anywhere in them (0 to 10, socket.io-parser's limit). The model writes the expected wire
//! text from the protocol description (type, `<n>-`, `<nsp>,`, id, JSON with placeholders
//! numbered depth-first); the codec must produce exactly it, decode it back to the same
//! packet, and give the same arguments back. More than 10 buffers is an error.

use bytes::Bytes;
use proptest::prelude::*;
use serde_json::{Value, json};
use socketio_next::Payload;
use socketio_next::protocol::socketio::{Packet, PacketId};

/// An argument: JSON, or a buffer (`Bin`), nested.
#[derive(Debug, Clone)]
enum Arg {
    Json(Value),
    Bin(Vec<u8>),
    List(Vec<Arg>),
    Map(Vec<(String, Arg)>),
}

fn scalar() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        // finite floats that survive a JSON round trip unchanged
        (-1e9f64..1e9).prop_map(|f| json!((f * 1000.0).round() / 1000.0)),
        "\\PC{0,12}".prop_map(Value::String),
    ]
}

fn arg() -> impl Strategy<Value = Arg> {
    let leaf = prop_oneof![
        4 => scalar().prop_map(Arg::Json),
        1 => proptest::collection::vec(any::<u8>(), 0..16).prop_map(Arg::Bin),
    ];
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Arg::List),
            // object keys are unique: JSON objects keep the last of equal keys
            proptest::collection::btree_map("[a-z_]{1,6}", inner, 0..4)
                .prop_map(|m| Arg::Map(m.into_iter().collect())),
        ]
    })
}

/// The JSON of `arg` with each buffer as a placeholder numbered depth-first, and the buffers
/// in that order (socket.io-parser `deconstructPacket`).
fn deconstruct(arg: &Arg, buffers: &mut Vec<Bytes>) -> Value {
    match arg {
        Arg::Json(v) => v.clone(),
        Arg::Bin(b) => {
            buffers.push(Bytes::from(b.clone()));
            json!({ "_placeholder": true, "num": buffers.len() - 1 })
        }
        Arg::List(items) => Value::Array(items.iter().map(|a| deconstruct(a, buffers)).collect()),
        Arg::Map(entries) => Value::Object(
            entries
                .iter()
                .map(|(k, a)| (k.clone(), deconstruct(a, buffers)))
                .collect(),
        ),
    }
}

/// The model's wire text of a packet (socket.io-parser `encodeAsString`).
fn model_text(kind: u8, attachments: usize, nsp: &str, id: Option<u64>, data: &Value) -> String {
    let mut text = kind.to_string();
    if kind == 5 || kind == 6 {
        text.push_str(&format!("{attachments}-"));
    }
    if nsp != "/" {
        text.push_str(nsp);
        text.push(',');
    }
    if let Some(id) = id {
        text.push_str(&id.to_string());
    }
    text.push_str(&data.to_string());
    text
}

/// What the receiving side hands out for these arguments: one lone buffer is `Binary`,
/// buffers elsewhere `Mixed`, no buffers `Text`.
fn expected_payload(args: Vec<Value>, buffers: Vec<Bytes>) -> Payload {
    if buffers.is_empty() {
        return Payload::Text(args);
    }
    if buffers.len() == 1 && args.len() == 1 && args[0].get("_placeholder").is_some() {
        return Payload::Binary(buffers[0].clone());
    }
    Payload::Mixed {
        args,
        attachments: buffers,
    }
}

fn nsp() -> impl Strategy<Value = String> {
    prop_oneof![Just("/".to_owned()), "/[a-z0-9_-]{1,8}"]
}

/// Event names socket.io accepts: not reserved, anything else (separators included).
fn event_name() -> impl Strategy<Value = String> {
    "\\PC{1,12}".prop_filter("reserved", |n| {
        !socketio_next::protocol::socketio::is_reserved_event(n)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn events_encode_as_the_model_says_and_decode_back(
        args in proptest::collection::vec(arg(), 0..4),
        name in event_name(),
        nsp in nsp(),
        id in proptest::option::of(0u64..1_000_000),
    ) {
        let mut buffers = Vec::new();
        let json_args: Vec<Value> = args.iter().map(|a| deconstruct(a, &mut buffers)).collect();
        let payload = expected_payload(json_args.clone(), buffers.clone());
        let result = Packet::new_event(payload.clone(), &name, &nsp, id);
        if buffers.len() > 10 {
            prop_assert!(result.is_err(), "more than 10 attachments must be refused");
            return Ok(());
        }
        let packet = result.unwrap();

        let kind = if buffers.is_empty() { 2 } else { 5 };
        let mut data = vec![json!(name)];
        data.extend(json_args);
        let text = Bytes::from(&packet);
        prop_assert_eq!(
            std::str::from_utf8(&text).unwrap(),
            model_text(kind, buffers.len(), &nsp, id, &Value::Array(data))
        );
        prop_assert_eq!(packet.attachments.clone().unwrap_or_default(), buffers.clone());

        let mut decoded = Packet::try_from(text).unwrap();
        prop_assert_eq!(decoded.packet_type, if buffers.is_empty() { PacketId::Event } else { PacketId::BinaryEvent });
        prop_assert_eq!(&decoded.nsp, &nsp);
        prop_assert_eq!(decoded.id, id);
        prop_assert_eq!(decoded.attachment_count as usize, buffers.len());
        decoded.attachments = (!buffers.is_empty()).then(|| buffers.clone());
        let (got_name, got) = decoded.event_payload().unwrap();
        prop_assert_eq!(got_name, name);
        prop_assert_eq!(got, payload);
    }

    #[test]
    fn acks_encode_as_the_model_says_and_decode_back(
        args in proptest::collection::vec(arg(), 0..4),
        nsp in nsp(),
        id in 0u64..1_000_000,
    ) {
        let mut buffers = Vec::new();
        let json_args: Vec<Value> = args.iter().map(|a| deconstruct(a, &mut buffers)).collect();
        let payload = expected_payload(json_args.clone(), buffers.clone());
        let Ok(packet) = Packet::new_ack(payload.clone(), &nsp, id) else {
            prop_assert!(buffers.len() > 10);
            return Ok(());
        };
        let kind = if buffers.is_empty() { 3 } else { 6 };
        let text = Bytes::from(&packet);
        prop_assert_eq!(
            std::str::from_utf8(&text).unwrap(),
            model_text(kind, buffers.len(), &nsp, Some(id), &Value::Array(json_args))
        );
        let mut decoded = Packet::try_from(text).unwrap();
        decoded.attachments = (!buffers.is_empty()).then(|| buffers.clone());
        prop_assert_eq!(decoded.ack_payload().unwrap(), payload);
    }

    /// Hostile headers are errors, never panics or overflows: an ack id past u64, an
    /// attachment count with many digits (python-socketio `test_decode_id_too_long`,
    /// `test_decode_attachment_count_too_long`).
    #[test]
    fn oversized_numbers_are_refused(digits in 20usize..120) {
        let big = "9".repeat(digits);
        let id = Packet::try_from(Bytes::from(format!("2{big}[\"x\"]")));
        let count = Packet::try_from(Bytes::from(format!("5{big}-[\"x\"]")));
        prop_assert!(id.is_err(), "ack id accepted");
        prop_assert!(count.is_err(), "attachment count accepted");
    }

    /// Text that is not a valid packet is an error, never a panic.
    #[test]
    fn any_text_decodes_or_errs(text in "\\PC{0,40}") {
        let _ = Packet::try_from(Bytes::from(text));
    }
}
