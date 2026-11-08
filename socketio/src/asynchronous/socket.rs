use super::generator::{end_after_error, StreamGenerator};
use crate::{
    error::Result,
    packet::{Packet, PacketId},
    Error, Event, Payload,
};
use bytes::Bytes;
use futures_util::{stream, Stream, StreamExt};
use rust_engineio::{
    asynchronous::Client as EngineClient, Packet as EnginePacket, PacketId as EnginePacketId,
};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct Socket {
    engine_client: Arc<EngineClient>,
    connected: Arc<AtomicBool>,
    /// Packets emitted before the server answered CONNECT (socket.io-client `sendBuffer`).
    send_buffer: Arc<Mutex<SendBuffer>>,
    generator: StreamGenerator<Packet>,
}

#[derive(Default)]
struct SendBuffer {
    packets: Vec<Packet>,
    /// CONNECT_ERROR, server DISCONNECT or disconnect(): nothing is buffered any more.
    closed: bool,
}

impl Socket {
    /// Creates an instance of `Socket`.
    pub(super) fn new(engine_client: EngineClient) -> Result<Self> {
        let connected = Arc::new(AtomicBool::default());
        let send_buffer = Arc::new(Mutex::new(SendBuffer::default()));
        Ok(Socket {
            engine_client: Arc::new(engine_client.clone()),
            connected: connected.clone(),
            send_buffer: send_buffer.clone(),
            generator: StreamGenerator::new(Self::stream(engine_client, connected, send_buffer)),
        })
    }

    /// Connects to the server. This includes a connection of the underlying
    /// engine.io client and afterwards an opening socket.io request.
    pub async fn connect(&self) -> Result<()> {
        // The namespace counts as connected only once the server answers CONNECT; until then
        // send() buffers.
        self.engine_client.connect().await?;
        Ok(())
    }

    /// Disconnects from the server by sending a socket.io `Disconnect` packet. This results
    /// in the underlying engine.io transport to get closed as well.
    pub async fn disconnect(&self) -> Result<()> {
        self.send_buffer.lock().await.closed = true;
        if self.is_engineio_connected() {
            self.engine_client.disconnect().await?;
        }
        if self.connected.load(Ordering::Acquire) {
            self.connected.store(false, Ordering::Release);
        }
        Ok(())
    }

    /// Sends a `socket.io` packet to the server using the `engine.io` client.
    ///
    /// Before the server has answered CONNECT the packet is buffered and sent, in order, right
    /// after it (upstream #502: an event that arrives before the namespace is joined makes the
    /// server close the connection). After CONNECT_ERROR, a server DISCONNECT or disconnect()
    /// nothing is buffered: the send fails as before.
    pub async fn send(&self, packet: Packet) -> Result<()> {
        if !self.is_engineio_connected() {
            return Err(Error::IllegalActionBeforeOpen());
        }
        if packet.packet_type != PacketId::Connect {
            let mut buffer = self.send_buffer.lock().await;
            if !self.connected.load(Ordering::Acquire) {
                if buffer.closed {
                    return Err(Error::IllegalActionBeforeOpen());
                }
                buffer.packets.push(packet);
                return Ok(());
            }
        }
        Self::send_packet(&self.engine_client, packet).await
    }

    // same Error type as every other fn here; the baseline lint is not worth a new type
    #[allow(clippy::result_large_err)]
    async fn send_packet(engine_client: &EngineClient, packet: Packet) -> Result<()> {
        // the packet, encoded as an engine.io message packet
        let engine_packet = EnginePacket::new(EnginePacketId::Message, Bytes::from(&packet));
        engine_client.emit(engine_packet).await?;

        if let Some(attachments) = packet.attachments {
            for attachment in attachments {
                let engine_packet = EnginePacket::new(EnginePacketId::MessageBinary, attachment);
                engine_client.emit(engine_packet).await?;
            }
        }

        Ok(())
    }

    /// Takes the packets still waiting for CONNECT (dropped on CONNECT_ERROR / disconnect()).
    pub(crate) async fn take_buffered(&self) -> Vec<Packet> {
        std::mem::take(&mut self.send_buffer.lock().await.packets)
    }

    /// Emits to certain event with given data. The data needs to be JSON,
    /// otherwise this returns an `InvalidJson` error.
    pub async fn emit(&self, nsp: &str, event: Event, data: Payload) -> Result<()> {
        let socket_packet = Packet::new_from_payload(data, event, nsp, None)?;

        self.send(socket_packet).await
    }

    fn stream(
        client: EngineClient,
        is_connected: Arc<AtomicBool>,
        send_buffer: Arc<Mutex<SendBuffer>>,
    ) -> Pin<Box<dyn Stream<Item = Result<Packet>> + Send>> {
        let packets = stream::unfold(
            (client, is_connected, send_buffer),
            |(mut client, is_connected, send_buffer)| async move {
                loop {
                    let packet = match client.next().await? {
                        Ok(packet) => packet,
                        Err(err) => {
                            return Some((Err(err.into()), (client, is_connected, send_buffer)))
                        }
                    };
                    if packet.packet_id != EnginePacketId::Message
                        && packet.packet_id != EnginePacketId::MessageBinary
                    {
                        continue;
                    }
                    let result =
                        Self::handle_message(packet, &client, &is_connected, &send_buffer).await;
                    return Some((result, (client, is_connected, send_buffer)));
                }
            },
        );
        end_after_error(packets.boxed()).boxed()
    }

    /// Decodes one socket.io packet (with its attachments) and keeps the namespace state.
    async fn handle_message(
        packet: EnginePacket,
        client: &EngineClient,
        is_connected: &AtomicBool,
        send_buffer: &Mutex<SendBuffer>,
    ) -> Result<Packet> {
        let packet = Self::handle_engineio_packet(packet, client.clone()).await?;
        match packet.packet_type {
            PacketId::Connect => {
                // Flush under the buffer lock: a concurrent send() either got
                // buffered before this or sends after the flush, so order holds.
                let mut buffer = send_buffer.lock().await;
                is_connected.store(true, Ordering::Release);
                for buffered in std::mem::take(&mut buffer.packets) {
                    Self::send_packet(client, buffered).await?;
                }
            }
            PacketId::ConnectError | PacketId::Disconnect => {
                let mut buffer = send_buffer.lock().await;
                buffer.closed = true;
                is_connected.store(false, Ordering::Release);
            }
            _ => (),
        }
        Ok(packet)
    }

    /// Handles new incoming engineio packets
    async fn handle_engineio_packet(
        packet: EnginePacket,
        mut client: EngineClient,
    ) -> Result<Packet> {
        let mut socket_packet = Packet::try_from(&packet.data)?;

        // Only handle attachments if there are any
        if socket_packet.attachment_count > 0 {
            let mut attachments_left = socket_packet.attachment_count;
            let mut attachments = Vec::new();
            while attachments_left > 0 {
                // TODO: This is not nice! Find a different way to peek the next element while mapping the stream
                // The connection can end between the event and its attachments.
                let next = client.next().await.ok_or(Error::IncompletePacket())?;
                match next {
                    Err(err) => return Err(err.into()),
                    Ok(packet) => match packet.packet_id {
                        EnginePacketId::MessageBinary | EnginePacketId::Message => {
                            attachments.push(packet.data);
                            attachments_left -= 1;
                        }
                        _ => {
                            return Err(Error::InvalidAttachmentPacketType(
                                packet.packet_id.into(),
                            ));
                        }
                    },
                }
            }
            socket_packet.attachments = Some(attachments);
        }

        Ok(socket_packet)
    }

    pub(crate) fn is_engineio_connected(&self) -> bool {
        self.engine_client.is_connected()
    }

    /// Whether the namespace is connected (CONNECT seen, no DISCONNECT / CONNECT_ERROR since).
    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }
}

impl Stream for Socket {
    type Item = Result<Packet>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.generator.poll_next_unpin(cx)
    }
}

impl Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Socket")
            .field("engine_client", &self.engine_client)
            .field("connected", &self.connected)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rust_engineio::asynchronous::ClientBuilder as EngineClientBuilder;

    /// A stream that ends while attachments are still expected is an error, not a panic.
    #[tokio::test]
    async fn attachments_after_end_of_stream() {
        let mut url = crate::test::socket_io_server();
        url.set_path("/socket.io/");
        let client = EngineClientBuilder::new(url).build_polling().await.unwrap();
        client.connect().await.unwrap();
        client.disconnect().await.unwrap();

        let event = EnginePacket::new(
            EnginePacketId::Message,
            Bytes::from_static(b"51-[\"ev\",{\"_placeholder\":true,\"num\":0}]"),
        );
        let result = Socket::handle_engineio_packet(event, client).await;
        assert!(
            matches!(result, Err(Error::IncompletePacket())),
            "{result:?}"
        );
    }
}
