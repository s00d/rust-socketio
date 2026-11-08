use std::{
    fmt::Debug,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use futures_util::{stream, Stream, StreamExt};

use super::generator::end_after_error;
use tokio::{runtime::Handle, sync::Mutex, time::Instant};

use crate::{
    asynchronous::{callback::OptionalCallback, transport::AsyncTransportType},
    error::Result,
    packet::{HandshakePacket, Payload},
    Error, Packet, PacketId,
};

#[derive(Clone)]
pub struct Socket {
    handle: Handle,
    transport: Arc<Mutex<AsyncTransportType>>,
    transport_raw: AsyncTransportType,
    on_close: OptionalCallback<()>,
    on_data: OptionalCallback<Bytes>,
    on_error: OptionalCallback<String>,
    on_open: OptionalCallback<()>,
    on_packet: OptionalCallback<Packet>,
    connected: Arc<AtomicBool>,
    last_ping: Arc<Mutex<Instant>>,
    last_pong: Arc<Mutex<Instant>>,
    connection_data: Arc<HandshakePacket>,
    max_ping_timeout: u64,
}

impl Socket {
    pub(crate) fn new(
        transport: AsyncTransportType,
        handshake: HandshakePacket,
        on_close: OptionalCallback<()>,
        on_data: OptionalCallback<Bytes>,
        on_error: OptionalCallback<String>,
        on_open: OptionalCallback<()>,
        on_packet: OptionalCallback<Packet>,
    ) -> Self {
        let max_ping_timeout = handshake.ping_interval + handshake.ping_timeout;

        Socket {
            handle: Handle::current(),
            on_close,
            on_data,
            on_error,
            on_open,
            on_packet,
            transport: Arc::new(Mutex::new(transport.clone())),
            transport_raw: transport,
            connected: Arc::new(AtomicBool::default()),
            last_ping: Arc::new(Mutex::new(Instant::now())),
            last_pong: Arc::new(Mutex::new(Instant::now())),
            connection_data: Arc::new(handshake),
            max_ping_timeout,
        }
    }

    /// Opens the connection to a specified server. The first Pong packet is sent
    /// to the server to trigger the Ping-cycle.
    pub async fn connect(&self) -> Result<()> {
        // SAFETY: Has valid handshake due to type
        self.connected.store(true, Ordering::Release);

        if let Some(on_open) = self.on_open.as_ref() {
            let on_open = on_open.clone();
            self.handle.spawn(async move { on_open(()).await });
        }

        // The heartbeat deadline starts at the handshake. Like engine.io-client, nothing is sent
        // here: the client answers the server's pings and never pongs unasked.
        *self.last_ping.lock().await = Instant::now();

        Ok(())
    }

    /// A helper method that distributes
    pub(super) async fn handle_incoming_packet(&self, packet: Packet) -> Result<()> {
        // check for the appropriate action or callback
        self.handle_packet(packet.clone());
        match packet.packet_id {
            PacketId::MessageBinary => {
                self.handle_data(packet.data.clone());
            }
            PacketId::Message => {
                self.handle_data(packet.data.clone());
            }
            PacketId::Close => {
                self.handle_close();
            }
            PacketId::Upgrade => {
                // this is already checked during the handshake, so just do nothing here
            }
            PacketId::Ping => {
                self.pinged().await;
                self.emit(Packet::new(PacketId::Pong, Bytes::new())).await?;
            }
            PacketId::Pong | PacketId::Open => {
                // this will never happen as the pong and open
                // packets are only sent by the client
                return Err(Error::InvalidPacket());
            }
            PacketId::Noop => (),
        }
        Ok(())
    }

    /// Helper method that parses bytes and returns an iterator over the elements.
    /// A polling body joins packets with 0x1e; a websocket frame is a single packet.
    fn parse_payload(bytes: Bytes, is_polling: bool) -> Result<Vec<Packet>> {
        let payload = if is_polling {
            Payload::try_from(bytes)?
        } else {
            Payload::from(Packet::try_from(bytes)?)
        };
        Ok(payload.into_iter().collect())
    }

    /// The packet stream of the transport; it ends after the first error.
    fn stream(
        transport: AsyncTransportType,
    ) -> Pin<Box<dyn Stream<Item = Result<Packet>> + 'static + Send>> {
        let is_polling = transport.is_polling();
        let packets = transport.flat_map(move |payload| {
            let packets = match payload.and_then(|bytes| Self::parse_payload(bytes, is_polling)) {
                Ok(packets) => packets.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            };
            stream::iter(packets)
        });
        end_after_error(packets).boxed()
    }

    pub async fn disconnect(&self) -> Result<()> {
        if let Some(on_close) = self.on_close.as_ref() {
            let on_close = on_close.clone();
            self.handle.spawn(async move { on_close(()).await });
        }

        // On a dead link the Close packet cannot be sent; the socket is closed all the same, or
        // emits would keep going to the dead transport and the packet stream would never end.
        let result = self.emit(Packet::new(PacketId::Close, Bytes::new())).await;

        self.connected.store(false, Ordering::Release);

        result
    }

    /// Sends a packet to the server.
    pub async fn emit(&self, packet: Packet) -> Result<()> {
        if !self.connected.load(Ordering::Acquire) {
            let error = Error::IllegalActionBeforeOpen();
            self.call_error_callback(format!("{}", error));
            return Err(error);
        }

        let is_binary = packet.packet_id == PacketId::MessageBinary;

        // send a post request with the encoded payload as body
        // if this is a binary attachment, then send the raw bytes
        let data: Bytes = if is_binary {
            packet.data
        } else {
            packet.into()
        };

        let lock = self.transport.lock().await;
        if let Err(error) = lock.emit(data, is_binary).await {
            self.call_error_callback(error.to_string());
            return Err(error);
        }

        Ok(())
    }

    /// Calls the error callback with a given message.
    #[inline]
    fn call_error_callback(&self, text: String) {
        if let Some(on_error) = self.on_error.as_ref() {
            let on_error = on_error.clone();
            self.handle.spawn(async move { on_error(text).await });
        }
    }

    // Check if the underlying transport client is connected.
    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    pub(crate) async fn pinged(&self) {
        *self.last_ping.lock().await = Instant::now();
    }

    /// Returns the time in milliseconds that is left until a new ping must be received.
    /// This is used to detect whether we have been disconnected from the server.
    /// See https://socket.io/docs/v4/how-it-works/#disconnection-detection
    async fn time_to_next_ping(&self) -> u64 {
        match Instant::now().checked_duration_since(*self.last_ping.lock().await) {
            Some(since_last_ping) => {
                let since_last_ping = since_last_ping.as_millis() as u64;
                if since_last_ping > self.max_ping_timeout {
                    0
                } else {
                    self.max_ping_timeout - since_last_ping
                }
            }
            None => 0,
        }
    }

    pub(crate) fn handle_packet(&self, packet: Packet) {
        if let Some(on_packet) = self.on_packet.as_ref() {
            let on_packet = on_packet.clone();
            self.handle.spawn(async move { on_packet(packet).await });
        }
    }

    pub(crate) fn handle_data(&self, data: Bytes) {
        if let Some(on_data) = self.on_data.as_ref() {
            let on_data = on_data.clone();
            self.handle.spawn(async move { on_data(data).await });
        }
    }

    pub(crate) fn handle_close(&self) {
        if let Some(on_close) = self.on_close.as_ref() {
            let on_close = on_close.clone();
            self.handle.spawn(async move { on_close(()).await });
        }

        self.connected.store(false, Ordering::Release);
    }

    /// The packet stream with the heartbeat check: it ends when the socket closes and fails with
    /// [`Error::PingTimeout`] when no ping arrives within pingInterval + pingTimeout.
    pub(crate) fn as_stream(&self) -> Pin<Box<dyn Stream<Item = Result<Packet>> + Send + 'static>> {
        stream::unfold(
            (Self::stream(self.transport_raw.clone()), self.clone()),
            |(mut stream, socket)| async move {
                // A closed socket (disconnect() or a server Close packet) yields nothing more.
                if !socket.is_connected() {
                    return None;
                }
                let deadline = std::time::Duration::from_millis(socket.time_to_next_ping().await);
                match tokio::time::timeout(deadline, stream.next()).await {
                    Ok(result) => result.map(|result| (result, (stream, socket))),
                    // No ping in time: the connection is dead. Be nice and disconnect properly.
                    Err(_) => {
                        // The reason is the timeout even when the Close packet cannot be sent.
                        let _ = socket.disconnect().await;
                        Some((Err(Error::PingTimeout()), (stream, socket)))
                    }
                }
            },
        )
        .boxed()
    }
}

impl Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Socket")
            .field("transport", &self.transport)
            .field("on_close", &self.on_close)
            .field("on_data", &self.on_data)
            .field("on_error", &self.on_error)
            .field("on_open", &self.on_open)
            .field("on_packet", &self.on_packet)
            .field("connected", &self.connected)
            .field("last_ping", &self.last_ping)
            .field("last_pong", &self.last_pong)
            .field("connection_data", &self.connection_data)
            .finish()
    }
}
