//! The socket.io packet codec (socket.io-parser 4.2 rules), without I/O.
//!
//! `data` always holds the JSON payload exactly as it goes over the wire. For BINARY_EVENT and
//! BINARY_ACK that JSON contains `{"_placeholder":true,"num":n}` objects that refer to
//! `attachments[n]` (socket.io-parser `deconstructPacket` / `reconstructPacket`).

use crate::protocol::Payload;
use crate::protocol::error::{Error, Result};
use bytes::Bytes;
use serde_json::{Value, json};

use std::convert::TryFrom;
use std::fmt::Write;
use std::str::from_utf8 as str_from_utf8;

/// socket.io-parser `Decoder` default `maxAttachments`.
pub const MAX_ATTACHMENTS: u8 = 10;

/// An enumeration of the different `Packet` types in the `socket.io` protocol.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PacketId {
    Connect = 0,
    Disconnect = 1,
    Event = 2,
    Ack = 3,
    ConnectError = 4,
    BinaryEvent = 5,
    BinaryAck = 6,
}

/// A packet which gets sent or received during in the `socket.io` protocol.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Packet {
    pub packet_type: PacketId,
    pub nsp: String,
    /// The JSON payload as text, placeholders included.
    pub data: Option<String>,
    pub id: Option<i32>,
    pub attachment_count: u8,
    pub attachments: Option<Vec<Bytes>>,
}

/// The placeholder for `attachments[num]`.
pub fn placeholder(num: usize) -> Value {
    json!({ "_placeholder": true, "num": num })
}

fn is_placeholder(value: &Value) -> bool {
    value.get("_placeholder") == Some(&Value::Bool(true))
}

/// Every placeholder refers to an attachment that exists (socket.io-parser throws
/// "illegal attachments" otherwise).
fn placeholders_valid(value: &Value, attachments: usize) -> bool {
    if is_placeholder(value) {
        return value
            .get("num")
            .and_then(Value::as_u64)
            .is_some_and(|num| (num as usize) < attachments);
    }
    match value {
        Value::Array(items) => items.iter().all(|v| placeholders_valid(v, attachments)),
        Value::Object(map) => map.values().all(|v| placeholders_valid(v, attachments)),
        _ => true,
    }
}

/// Splits a payload into its JSON arguments and binary attachments.
fn deconstruct(payload: Payload) -> Result<(Vec<Value>, Vec<Bytes>)> {
    match payload {
        Payload::Text(args) => Ok((args, Vec::new())),
        Payload::Binary(bytes) => Ok((vec![placeholder(0)], vec![bytes])),
        Payload::Mixed { args, attachments } => {
            if attachments.len() > usize::from(MAX_ATTACHMENTS) {
                return Err(Error::InvalidPacket("more than 10 attachments"));
            }
            if !args
                .iter()
                .all(|v| placeholders_valid(v, attachments.len()))
            {
                return Err(Error::InvalidPacket("placeholder without an attachment"));
            }
            Ok((args, attachments))
        }
        #[allow(deprecated)]
        Payload::String(text) => Ok((vec![Payload::string_to_value(text)], Vec::new())),
    }
}

/// The payload for received arguments: a lone buffer stays [`Payload::Binary`] (0.6 behaviour),
/// anything else with attachments is [`Payload::Mixed`].
fn reconstruct(args: Vec<Value>, mut attachments: Vec<Bytes>) -> Result<Payload> {
    if attachments.is_empty() {
        return Ok(Payload::Text(args));
    }
    if !args
        .iter()
        .all(|v| placeholders_valid(v, attachments.len()))
    {
        return Err(Error::InvalidPacket("placeholder without an attachment"));
    }
    if attachments.len() == 1 && args.len() == 1 && is_placeholder(&args[0]) {
        return Ok(Payload::Binary(attachments.remove(0)));
    }
    Ok(Payload::Mixed { args, attachments })
}

impl Packet {
    /// An EVENT (or BINARY_EVENT when the payload carries buffers) for `event`.
    pub fn new_event(payload: Payload, event: &str, nsp: &str, id: Option<i32>) -> Result<Packet> {
        let (args, attachments) = deconstruct(payload)?;
        let mut data = Vec::with_capacity(args.len() + 1);
        data.push(Value::String(event.to_owned()));
        data.extend(args);
        Ok(Self::with_attachments(
            (PacketId::Event, PacketId::BinaryEvent),
            nsp,
            Value::Array(data),
            id,
            attachments,
        ))
    }

    /// The ACK (or BINARY_ACK) that answers the server's event `id`.
    pub fn new_ack(payload: Payload, nsp: &str, id: i32) -> Result<Packet> {
        let (args, attachments) = deconstruct(payload)?;
        Ok(Self::with_attachments(
            (PacketId::Ack, PacketId::BinaryAck),
            nsp,
            Value::Array(args),
            Some(id),
            attachments,
        ))
    }

    fn with_attachments(
        (text, binary): (PacketId, PacketId),
        nsp: &str,
        data: Value,
        id: Option<i32>,
        attachments: Vec<Bytes>,
    ) -> Packet {
        let (packet_type, count, attachments) = if attachments.is_empty() {
            (text, 0, None)
        } else {
            // deconstruct() bounds the count by MAX_ATTACHMENTS
            (binary, attachments.len() as u8, Some(attachments))
        };
        Packet::new(
            packet_type,
            nsp.to_owned(),
            Some(data.to_string()),
            id,
            count,
            attachments,
        )
    }

    /// The JSON arguments of an EVENT/ACK (empty when there is no payload).
    fn args(&self) -> Result<Vec<Value>> {
        match self.data.as_deref() {
            None => Ok(Vec::new()),
            Some(data) => match serde_json::from_str::<Value>(data)? {
                Value::Array(args) => Ok(args),
                _ => Err(Error::InvalidPacket("EVENT/ACK payload is not an array")),
            },
        }
    }

    fn attachment_list(&self) -> Vec<Bytes> {
        self.attachments.clone().unwrap_or_default()
    }

    /// Event name and arguments of an EVENT / BINARY_EVENT.
    ///
    /// The first argument is the name when it is a string (or a number, which socket.io-parser
    /// also accepts). Deliberate difference from socket.io-client, kept from rust_socketio: a
    /// binary event without a name (`emit(buffer)` on the server) is named `"message"`.
    pub fn event_payload(&self) -> Result<(String, Payload)> {
        let mut args = self.args()?;
        let event = match args.first() {
            Some(Value::String(name)) => Some(name.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            _ => None,
        };
        let event = match event {
            Some(event) => {
                args.remove(0);
                event
            }
            None => "message".to_owned(),
        };
        Ok((event, reconstruct(args, self.attachment_list())?))
    }

    /// The arguments of an ACK / BINARY_ACK.
    pub fn ack_payload(&self) -> Result<Payload> {
        reconstruct(self.args()?, self.attachment_list())
    }

    /// Event name of an EVENT / BINARY_EVENT (see [`Packet::event_payload`]).
    #[cfg(test)]
    pub(crate) fn binary_event_name(&self) -> String {
        self.event_payload()
            .map(|(event, _)| event)
            .unwrap_or_else(|_| "message".to_owned())
    }
}

impl Default for Packet {
    fn default() -> Self {
        Self {
            packet_type: PacketId::Event,
            nsp: String::from("/"),
            data: None,
            id: None,
            attachment_count: 0,
            attachments: None,
        }
    }
}

impl TryFrom<u8> for PacketId {
    type Error = Error;
    fn try_from(b: u8) -> Result<Self> {
        PacketId::try_from(b as char)
    }
}

impl TryFrom<char> for PacketId {
    type Error = Error;
    fn try_from(b: char) -> Result<Self> {
        match b {
            '0' => Ok(PacketId::Connect),
            '1' => Ok(PacketId::Disconnect),
            '2' => Ok(PacketId::Event),
            '3' => Ok(PacketId::Ack),
            '4' => Ok(PacketId::ConnectError),
            '5' => Ok(PacketId::BinaryEvent),
            '6' => Ok(PacketId::BinaryAck),
            _ => Err(Error::InvalidPacketType(b)),
        }
    }
}

impl Packet {
    /// Creates an instance.
    pub const fn new(
        packet_type: PacketId,
        nsp: String,
        data: Option<String>,
        id: Option<i32>,
        attachment_count: u8,
        attachments: Option<Vec<Bytes>>,
    ) -> Self {
        Packet {
            packet_type,
            nsp,
            data,
            id,
            attachment_count,
            attachments,
        }
    }
}

impl From<Packet> for Bytes {
    fn from(packet: Packet) -> Self {
        Bytes::from(&packet)
    }
}

impl From<&Packet> for Bytes {
    /// socket.io-parser `encodeAsString`: type, `<attachments>-`, `<nsp>,` (unless `/`), id,
    /// JSON. The attachments themselves are sent by the socket as separate binary messages.
    fn from(packet: &Packet) -> Bytes {
        let mut buffer = String::new();
        buffer.push((packet.packet_type as u8 + b'0') as char);

        if let PacketId::BinaryAck | PacketId::BinaryEvent = packet.packet_type {
            let _ = write!(buffer, "{}-", packet.attachment_count);
        }

        if packet.nsp != "/" {
            buffer.push_str(&packet.nsp);
            buffer.push(',');
        }

        if let Some(id) = packet.id {
            let _ = write!(buffer, "{id}");
        }

        if let Some(data) = packet.data.as_ref() {
            buffer.push_str(data);
        }

        Bytes::from(buffer)
    }
}

impl TryFrom<Bytes> for Packet {
    type Error = Error;
    fn try_from(value: Bytes) -> Result<Self> {
        Packet::try_from(&value)
    }
}

impl TryFrom<&Bytes> for Packet {
    type Error = Error;
    /// socket.io-parser `decodeString`. The attachments of a binary packet arrive as separate
    /// engine.io messages; the socket fills `attachments` in.
    fn try_from(payload: &Bytes) -> Result<Packet> {
        let mut payload = str_from_utf8(payload).map_err(Error::InvalidUtf8)?;
        let mut packet = Packet::default();

        // packet_type
        let id_char = payload.chars().next().ok_or(Error::IncompletePacket)?;
        packet.packet_type = PacketId::try_from(id_char)?;
        payload = &payload[id_char.len_utf8()..];

        // attachment_count: a positive integer up to MAX_ATTACHMENTS, then '-'
        if let PacketId::BinaryAck | PacketId::BinaryEvent = packet.packet_type {
            let (prefix, rest) = payload.split_once('-').ok_or(Error::IncompletePacket)?;
            payload = rest;
            if prefix.is_empty() || !prefix.bytes().all(|b| b.is_ascii_digit()) {
                return Err(Error::InvalidPacket("attachment count is not a number"));
            }
            let count: u32 = prefix
                .parse()
                .map_err(|_| Error::InvalidPacket("attachment count is not a number"))?;
            if count == 0 || count > u32::from(MAX_ATTACHMENTS) {
                return Err(Error::InvalidPacket("attachment count must be 1..=10"));
            }
            packet.attachment_count = count as u8;
        }

        // namespace: up to ',' or the end
        if payload.starts_with('/') {
            let (nsp, rest) = payload.split_once(',').unwrap_or((payload, ""));
            packet.nsp = nsp.to_owned();
            payload = rest;
        }

        // id
        let digits = payload.bytes().take_while(u8::is_ascii_digit).count();
        if digits > 0 {
            let (id, rest) = payload.split_at(digits);
            packet.id = Some(
                id.parse()
                    .map_err(|_| Error::InvalidPacket("ack id out of range"))?,
            );
            payload = rest;
        }

        if payload.is_empty() {
            return Ok(packet);
        }

        // validate json, then the payload shape for the packet type (socket.io-parser's
        // isPayloadValid): a server event must not pose as a reserved client event like `connect`
        let json = serde_json::from_str::<Value>(payload).map_err(Error::InvalidJson)?;
        if !is_payload_valid(packet.packet_type, &json) {
            return Err(Error::InvalidPacket(
                "payload shape not allowed for this packet type",
            ));
        }
        packet.data = Some(payload.to_owned());

        Ok(packet)
    }
}

/// Event names the socket.io client and server reserve (socket.io-parser `RESERVED_EVENTS`).
const RESERVED_EVENTS: [&str; 6] = [
    "connect",
    "connect_error",
    "disconnect",
    "disconnecting",
    "newListener",
    "removeListener",
];

fn is_payload_valid(packet_type: PacketId, payload: &Value) -> bool {
    match packet_type {
        PacketId::Connect => payload.is_object(),
        PacketId::Disconnect => false,
        PacketId::ConnectError => payload.is_string() || payload.is_object(),
        PacketId::Event | PacketId::BinaryEvent => match payload.as_array().and_then(|a| a.first())
        {
            Some(Value::Number(_)) => true,
            Some(Value::String(name)) => !RESERVED_EVENTS.contains(&name.as_str()),
            // Deliberate difference from socket.io-client: a binary event without a name
            // (`emit(buffer)` on the server) is delivered as `message`, as in 0.6.0.
            Some(first) => packet_type == PacketId::BinaryEvent && is_placeholder(first),
            None => false,
        },
        PacketId::Ack | PacketId::BinaryAck => payload.is_array(),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn decode(raw: &str) -> Result<Packet> {
        Packet::try_from(&Bytes::from(raw.to_owned()))
    }

    fn encode(packet: &Packet) -> String {
        String::from_utf8(Bytes::from(packet).to_vec()).unwrap()
    }

    #[test]
    fn protocol_examples_roundtrip() {
        // https://github.com/socketio/socket.io-protocol examples
        let cases = [
            (
                "0{\"token\":\"123\"}",
                PacketId::Connect,
                "/",
                Some("{\"token\":\"123\"}"),
                None,
            ),
            (
                "0/admin™,{\"token™\":\"123\"}",
                PacketId::Connect,
                "/admin™",
                Some("{\"token™\":\"123\"}"),
                None,
            ),
            ("1/admin,", PacketId::Disconnect, "/admin", None, None),
            (
                "2[\"hello\",1]",
                PacketId::Event,
                "/",
                Some("[\"hello\",1]"),
                None,
            ),
            (
                "2/admin,456[\"project:delete\",123]",
                PacketId::Event,
                "/admin",
                Some("[\"project:delete\",123]"),
                Some(456),
            ),
            (
                "3/admin,456[]",
                PacketId::Ack,
                "/admin",
                Some("[]"),
                Some(456),
            ),
            (
                "4/admin,{\"message\":\"Not authorized\"}",
                PacketId::ConnectError,
                "/admin",
                Some("{\"message\":\"Not authorized\"}"),
                None,
            ),
        ];
        for (raw, packet_type, nsp, data, id) in cases {
            let packet = decode(raw).unwrap();
            assert_eq!(
                packet,
                Packet::new(
                    packet_type,
                    nsp.to_owned(),
                    data.map(str::to_owned),
                    id,
                    0,
                    None
                ),
                "{raw}"
            );
            assert_eq!(encode(&packet), raw);
        }
    }

    #[test]
    fn namespace_without_comma_runs_to_the_end() {
        // socket.io-parser reads the namespace up to ',' or the end of the string
        let packet = decode("1/admin").unwrap();
        assert_eq!(packet.packet_type, PacketId::Disconnect);
        assert_eq!(packet.nsp, "/admin");
    }

    #[test]
    fn binary_packets_keep_placeholders() {
        let raw = "51-[\"hello\",{\"_placeholder\":true,\"num\":0}]";
        let mut packet = decode(raw).unwrap();
        assert_eq!(packet.packet_type, PacketId::BinaryEvent);
        assert_eq!(packet.attachment_count, 1);
        assert_eq!(
            packet.data.as_deref(),
            Some("[\"hello\",{\"_placeholder\":true,\"num\":0}]")
        );
        packet.attachments = Some(vec![Bytes::from_static(&[1, 2, 3])]);
        assert_eq!(encode(&packet), raw);
        let (event, payload) = packet.event_payload().unwrap();
        assert_eq!(event, "hello".to_owned());
        assert_eq!(payload, Payload::Binary(Bytes::from_static(&[1, 2, 3])));

        let raw = "61-/admin,456[{\"_placeholder\":true,\"num\":0}]";
        let packet = decode(raw).unwrap();
        assert_eq!(packet.packet_type, PacketId::BinaryAck);
        assert_eq!((packet.nsp.as_str(), packet.id), ("/admin", Some(456)));
        assert_eq!(encode(&packet), raw);
    }

    #[test]
    fn nested_and_multiple_attachments() {
        // socket.io-parser "encodes a deep binary" / "encodes mixed binary and json"
        let raw = r#"52-["ev",{"a":[{"_placeholder":true,"num":0}],"b":"x"},{"_placeholder":true,"num":1},3]"#;
        let mut packet = decode(raw).unwrap();
        assert_eq!(packet.attachment_count, 2);
        let a = Bytes::from_static(b"one");
        let b = Bytes::from_static(b"two");
        packet.attachments = Some(vec![a.clone(), b.clone()]);
        let (event, payload) = packet.event_payload().unwrap();
        assert_eq!(event, "ev".to_owned());
        let expected = Payload::Mixed {
            args: vec![
                json!({"a": [placeholder(0)], "b": "x"}),
                placeholder(1),
                json!(3),
            ],
            attachments: vec![a, b],
        };
        assert_eq!(payload, expected);
        // and back
        let encoded = Packet::new_event(expected, "ev", "/", None).unwrap();
        assert_eq!(encode(&encoded), raw);
        assert_eq!(encoded.attachment_count, 2);
    }

    #[test]
    fn placeholder_out_of_range_is_rejected() {
        let mut packet = decode(r#"51-["ev",{"_placeholder":true,"num":1}]"#).unwrap();
        packet.attachments = Some(vec![Bytes::from_static(b"x")]);
        assert!(packet.event_payload().is_err());
        let bad = Payload::Mixed {
            args: vec![placeholder(2)],
            attachments: vec![Bytes::new()],
        };
        assert!(Packet::new_event(bad, "ev", "/", None).is_err());
    }

    #[test]
    fn attachment_count_rules() {
        // socket.io-parser: integer >= 1, at most maxAttachments (10), followed by '-'
        for raw in [
            "5",
            "51",
            "50-",
            "5a-",
            "51.23-",
            "511-[\"a\"]",
            "5+1-[\"a\"]",
        ] {
            assert!(decode(raw).is_err(), "{raw}");
        }
        assert!(decode(r#"510-["a"]"#).is_ok());
        let too_many = Payload::Mixed {
            args: vec![],
            attachments: vec![Bytes::new(); 11],
        };
        assert!(Packet::new_event(too_many, "ev", "/", None).is_err());
    }

    #[test]
    fn event_names() {
        let name = |raw: &str| decode(raw).unwrap().event_payload().unwrap();
        // a lone name is the event, with no arguments (socket.io-client)
        assert_eq!(
            name(r#"2["ERROR_VERSION"]"#),
            ("ERROR_VERSION".to_owned(), Payload::Text(vec![]))
        );
        // a numeric name is accepted by socket.io-parser
        assert_eq!(
            name("2[42,\"x\"]"),
            ("42".to_owned(), Payload::Text(vec![json!("x")]))
        );
        // the binary event without a name (0.6 compatibility) is `message`
        let mut packet = decode(r#"51-[{"_placeholder":true,"num":0}]"#).unwrap();
        packet.attachments = Some(vec![Bytes::from_static(b"b")]);
        assert_eq!(
            packet.event_payload().unwrap(),
            (
                "message".to_owned(),
                Payload::Binary(Bytes::from_static(b"b"))
            )
        );
        // extra arguments around the buffer
        let mut packet =
            decode(r#"51-["tts:audio","extra",{"_placeholder":true,"num":0}]"#).unwrap();
        packet.attachments = Some(vec![Bytes::from_static(b"b")]);
        assert_eq!(packet.binary_event_name(), "tts:audio".to_owned());
    }

    #[test]
    fn ack_arguments_are_the_array_items() {
        let packet = decode("3/admin,1[\"woot\",2]").unwrap();
        assert_eq!(
            packet.ack_payload().unwrap(),
            Payload::Text(vec![json!("woot"), json!(2)])
        );
    }

    #[test]
    fn encode_from_payloads() {
        let ev = || "ev";
        let p = Packet::new_event(
            Payload::Binary(Bytes::from_static(&[0, 4, 9])),
            ev(),
            "/admin",
            None,
        )
        .unwrap();
        assert_eq!(
            encode(&p),
            r#"51-/admin,["ev",{"_placeholder":true,"num":0}]"#
        );
        assert_eq!(p.attachments, Some(vec![Bytes::from_static(&[0, 4, 9])]));
        // with an ack id it is still BINARY_EVENT, never BINARY_ACK (#422)
        let p = Packet::new_event(
            Payload::Binary(Bytes::from_static(b"x")),
            "upload",
            "/",
            Some(3),
        )
        .unwrap();
        assert_eq!(
            encode(&p),
            r#"51-3["upload",{"_placeholder":true,"num":0}]"#
        );
        let p = Packet::new_event(
            Payload::Text(vec![json!({"a":1}), json!("b")]),
            ev(),
            "/",
            Some(7),
        )
        .unwrap();
        assert_eq!(encode(&p), r#"27["ev",{"a":1},"b"]"#);
        #[allow(deprecated)]
        let p = Packet::new_event(Payload::String("plain".into()), ev(), "/", None).unwrap();
        assert_eq!(encode(&p), r#"2["ev","plain"]"#);
        #[allow(deprecated)]
        let p = Packet::new_event(Payload::String("{\"j\":1}".into()), ev(), "/", None).unwrap();
        assert_eq!(encode(&p), r#"2["ev",{"j":1}]"#);
        // acks
        let p = Packet::new_ack(Payload::Text(vec![json!("ok")]), "/", 5).unwrap();
        assert_eq!(encode(&p), r#"35["ok"]"#);
        let p = Packet::new_ack(Payload::Binary(Bytes::from_static(b"x")), "/n", 5).unwrap();
        assert_eq!(encode(&p), r#"61-/n,5[{"_placeholder":true,"num":0}]"#);
    }

    #[test]
    fn illegal_packet_id() {
        assert!(matches!(decode("7"), Err(Error::InvalidPacketType('7'))));
        assert!(decode("").is_err());
    }
}
