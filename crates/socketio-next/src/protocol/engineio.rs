use base64::{Engine as _, engine::general_purpose};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::char;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt::{Display, Formatter, Result as FmtResult, Write};
use std::ops::Index;

use crate::protocol::error::{Error, Result};
/// Enumeration of the `engine.io` `Packet` types.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum PacketId {
    Open,
    Close,
    Ping,
    Pong,
    Message,
    // A type of message that is base64 encoded
    MessageBinary,
    Upgrade,
    Noop,
}

impl PacketId {
    /// Returns the byte that represents the [`PacketId`] as a [`char`].
    fn to_string_byte(self) -> u8 {
        match self {
            Self::MessageBinary => b'b',
            _ => u8::from(self) + b'0',
        }
    }
}

impl Display for PacketId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_char(self.to_string_byte() as char)
    }
}

impl From<PacketId> for u8 {
    fn from(packet_id: PacketId) -> Self {
        match packet_id {
            PacketId::Open => 0,
            PacketId::Close => 1,
            PacketId::Ping => 2,
            PacketId::Pong => 3,
            PacketId::Message => 4,
            PacketId::MessageBinary => 4,
            PacketId::Upgrade => 5,
            PacketId::Noop => 6,
        }
    }
}

impl TryFrom<u8> for PacketId {
    type Error = Error;
    /// Converts a byte into the corresponding `PacketId`.
    fn try_from(b: u8) -> Result<PacketId> {
        match b {
            0 | b'0' => Ok(PacketId::Open),
            1 | b'1' => Ok(PacketId::Close),
            2 | b'2' => Ok(PacketId::Ping),
            3 | b'3' => Ok(PacketId::Pong),
            4 | b'4' => Ok(PacketId::Message),
            5 | b'5' => Ok(PacketId::Upgrade),
            6 | b'6' => Ok(PacketId::Noop),
            _ => Err(Error::InvalidPacketType(b as char)),
        }
    }
}

/// A `Packet` sent via the `engine.io` protocol.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Packet {
    pub packet_id: PacketId,
    pub data: Bytes,
}

/// Data which gets exchanged in a handshake as defined by the server.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct HandshakePacket {
    pub sid: String,
    pub upgrades: Vec<String>,
    #[serde(rename = "pingInterval")]
    pub ping_interval: u64,
    #[serde(rename = "pingTimeout")]
    pub ping_timeout: u64,
    /// Largest payload the server accepts in one request (engine.io `maxPayload`, bytes).
    #[serde(
        rename = "maxPayload",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub max_payload: Option<u64>,
}

impl TryFrom<Packet> for HandshakePacket {
    type Error = Error;
    fn try_from(packet: Packet) -> Result<HandshakePacket> {
        Ok(serde_json::from_slice(packet.data[..].as_ref())?)
    }
}

impl Packet {
    /// The text encoding of a packet with string data (`2probe`, `5`).
    pub(crate) fn text(packet_id: PacketId, data: &str) -> String {
        format!("{packet_id}{data}")
    }

    /// Creates a new `Packet`.
    pub fn new<T: Into<Bytes>>(packet_id: PacketId, data: T) -> Self {
        Packet {
            packet_id,
            data: data.into(),
        }
    }
}

impl TryFrom<Bytes> for Packet {
    type Error = Error;
    /// Decodes a single `Packet` from an `u8` byte stream.
    fn try_from(
        bytes: Bytes,
    ) -> std::result::Result<Self, <Self as std::convert::TryFrom<Bytes>>::Error> {
        if bytes.is_empty() {
            return Err(Error::IncompletePacket);
        }

        let is_base64 = *bytes.first().ok_or(Error::IncompletePacket)? == b'b';

        // only 'messages' packets could be encoded
        let packet_id = if is_base64 {
            PacketId::MessageBinary
        } else {
            (*bytes.first().ok_or(Error::IncompletePacket)?).try_into()?
        };

        if bytes.len() == 1 && packet_id == PacketId::Message {
            return Err(Error::IncompletePacket);
        }

        let data: Bytes = bytes.slice(1..);

        Ok(Packet {
            packet_id,
            data: if is_base64 {
                Bytes::from(general_purpose::STANDARD.decode(data.as_ref())?)
            } else {
                data
            },
        })
    }
}

impl From<Packet> for Bytes {
    /// Encodes a `Packet` into an `u8` byte stream.
    fn from(packet: Packet) -> Self {
        let mut result = BytesMut::with_capacity(packet.data.len() + 1);
        result.put_u8(packet.packet_id.to_string_byte());
        if packet.packet_id == PacketId::MessageBinary {
            result.extend(general_purpose::STANDARD.encode(packet.data).into_bytes());
        } else {
            result.put(packet.data);
        }
        result.freeze()
    }
}

#[derive(Debug, Clone)]
pub struct Batch(pub Vec<Packet>);

impl Batch {
    // see https://en.wikipedia.org/wiki/Delimiter#ASCII_delimited_text
    const SEPARATOR: char = '\x1e';

    /// Number of packets.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// No packets.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A websocket frame is exactly one packet (whose binary data may contain the separator).
impl From<Packet> for Batch {
    fn from(packet: Packet) -> Self {
        Self(vec![packet])
    }
}

impl TryFrom<Bytes> for Batch {
    type Error = Error;
    /// Decodes a `payload` which in the `engine.io` context means a chain of normal
    /// packets separated by a certain SEPARATOR, in this case the delimiter `\x30`.
    fn try_from(payload: Bytes) -> Result<Self> {
        payload
            .split(|&c| c as char == Self::SEPARATOR)
            .map(|slice| Packet::try_from(payload.slice_ref(slice)))
            .collect::<Result<Vec<_>>>()
            .map(Self)
    }
}

impl TryFrom<Batch> for Bytes {
    type Error = Error;
    /// Encodes a payload. Batch in the `engine.io` context means a chain of
    /// normal `packets` separated by a SEPARATOR, in this case the delimiter
    /// `\x30`.
    fn try_from(packets: Batch) -> Result<Self> {
        let mut buf = BytesMut::new();
        for packet in packets {
            // at the moment no base64 encoding is used
            buf.extend(Bytes::from(packet.clone()));
            buf.put_u8(Batch::SEPARATOR as u8);
        }

        // remove the last separator
        let _ = buf.split_off(buf.len() - 1);
        Ok(buf.freeze())
    }
}

#[derive(Clone, Debug)]
pub struct IntoIter {
    iter: std::vec::IntoIter<Packet>,
}

impl Iterator for IntoIter {
    type Item = Packet;
    fn next(&mut self) -> std::option::Option<<Self as std::iter::Iterator>::Item> {
        self.iter.next()
    }
}

impl IntoIterator for Batch {
    type Item = Packet;
    type IntoIter = IntoIter;
    fn into_iter(self) -> <Self as std::iter::IntoIterator>::IntoIter {
        IntoIter {
            iter: self.0.into_iter(),
        }
    }
}

impl Index<usize> for Batch {
    type Output = Packet;
    fn index(&self, index: usize) -> &Packet {
        &self.0[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_error() {
        let err = Packet::try_from(BytesMut::with_capacity(10).freeze());
        assert!(err.is_err())
    }

    #[test]
    fn test_is_reflexive() {
        let data = Bytes::from_static(b"1Hello World");
        let packet = Packet::try_from(data).unwrap();

        assert_eq!(packet.packet_id, PacketId::Close);
        assert_eq!(packet.data, Bytes::from_static(b"Hello World"));

        let data = Bytes::from_static(b"1Hello World");
        assert_eq!(Bytes::from(packet), data);
    }

    #[test]
    fn test_binary_packet() {
        // SGVsbG8= is the encoded string for 'Hello'
        let data = Bytes::from_static(b"bSGVsbG8=");
        let packet = Packet::try_from(data.clone()).unwrap();

        assert_eq!(packet.packet_id, PacketId::MessageBinary);
        assert_eq!(packet.data, Bytes::from_static(b"Hello"));

        assert_eq!(Bytes::from(packet), data);
    }

    #[test]
    fn test_decode_payload() -> Result<()> {
        let data = Bytes::from_static(b"1Hello\x1e1HelloWorld");
        let packets = Batch::try_from(data)?;

        assert_eq!(packets[0].packet_id, PacketId::Close);
        assert_eq!(packets[0].data, Bytes::from_static(b"Hello"));
        assert_eq!(packets[1].packet_id, PacketId::Close);
        assert_eq!(packets[1].data, Bytes::from_static(b"HelloWorld"));

        let data = "1Hello\x1e1HelloWorld".to_owned().into_bytes();
        assert_eq!(Bytes::try_from(packets).unwrap(), data);

        Ok(())
    }

    #[test]
    fn test_binary_payload() {
        let data = Bytes::from_static(b"bSGVsbG8=\x1ebSGVsbG9Xb3JsZA==\x1ebSGVsbG8=");
        let packets = Batch::try_from(data.clone()).unwrap();

        assert!(packets.len() == 3);
        assert_eq!(packets[0].packet_id, PacketId::MessageBinary);
        assert_eq!(packets[0].data, Bytes::from_static(b"Hello"));
        assert_eq!(packets[1].packet_id, PacketId::MessageBinary);
        assert_eq!(packets[1].data, Bytes::from_static(b"HelloWorld"));
        assert_eq!(packets[2].packet_id, PacketId::MessageBinary);
        assert_eq!(packets[2].data, Bytes::from_static(b"Hello"));

        assert_eq!(Bytes::try_from(packets).unwrap(), data);
    }

    #[test]
    fn test_packet_id_conversion_and_incompl_packet() -> Result<()> {
        let sut = Packet::try_from(Bytes::from_static(b"4"));
        assert!(sut.is_err());
        let _sut = sut.unwrap_err();
        assert!(matches!(Error::IncompletePacket, _sut));

        assert_eq!(PacketId::MessageBinary.to_string(), "b");

        let sut = PacketId::try_from(b'0')?;
        assert_eq!(sut, PacketId::Open);
        assert_eq!(sut.to_string(), "0");

        let sut = PacketId::try_from(b'1')?;
        assert_eq!(sut, PacketId::Close);
        assert_eq!(sut.to_string(), "1");

        let sut = PacketId::try_from(b'2')?;
        assert_eq!(sut, PacketId::Ping);
        assert_eq!(sut.to_string(), "2");

        let sut = PacketId::try_from(b'3')?;
        assert_eq!(sut, PacketId::Pong);
        assert_eq!(sut.to_string(), "3");

        let sut = PacketId::try_from(b'4')?;
        assert_eq!(sut, PacketId::Message);
        assert_eq!(sut.to_string(), "4");

        let sut = PacketId::try_from(b'5')?;
        assert_eq!(sut, PacketId::Upgrade);
        assert_eq!(sut.to_string(), "5");

        let sut = PacketId::try_from(b'6')?;
        assert_eq!(sut, PacketId::Noop);
        assert_eq!(sut.to_string(), "6");

        let sut = PacketId::try_from(42);
        assert!(sut.is_err());
        assert!(matches!(sut.unwrap_err(), Error::InvalidPacketType('*')));

        Ok(())
    }

    #[test]
    fn test_handshake_packet() {
        assert!(
            HandshakePacket::try_from(Packet::new(PacketId::Message, Bytes::from("test"))).is_err()
        );
        let packet = HandshakePacket {
            ping_interval: 10000,
            ping_timeout: 1000,
            sid: "Test".to_owned(),
            upgrades: vec!["websocket".to_owned(), "test".to_owned()],
            max_payload: Some(1_000_000),
        };
        let encoded: String = serde_json::to_string(&packet).unwrap();

        assert_eq!(
            packet,
            HandshakePacket::try_from(Packet::new(PacketId::Message, Bytes::from(encoded)))
                .unwrap()
        );
    }
}
