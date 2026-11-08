use std::fmt::Debug;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::protocol::engineio::Batch;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use tokio::time::Instant;

use crate::engineio::callback::Callbacks;
use crate::engineio::error::Result;
use crate::engineio::generator::end_after_error;
use crate::engineio::transport::Transport;
use crate::engineio::{Error, HandshakePacket, Packet, PacketId};

/// The engine.io session: transport, heartbeat state and callbacks. Clones share all state.
#[derive(Clone)]
pub(crate) struct Socket {
    /// Serialises writes (one polling POST or websocket frame at a time).
    writer: Arc<tokio::sync::Mutex<()>>,
    transport: Transport,
    callbacks: Callbacks,
    connected: Arc<AtomicBool>,
    last_ping: Arc<Mutex<Instant>>,
    handshake: Arc<HandshakePacket>,
}

impl Socket {
    pub(crate) fn new(
        transport: Transport,
        handshake: HandshakePacket,
        callbacks: Callbacks,
    ) -> Self {
        Socket {
            writer: Arc::default(),
            transport,
            callbacks,
            connected: Arc::new(AtomicBool::default()),
            last_ping: Arc::new(Mutex::new(Instant::now())),
            handshake: Arc::new(handshake),
        }
    }

    pub(crate) fn handshake(&self) -> &HandshakePacket {
        &self.handshake
    }

    pub(crate) fn transport(&self) -> &Transport {
        &self.transport
    }

    /// Opens the session. Like engine.io-client nothing is sent: the client answers the server's
    /// pings and never pongs unasked. The heartbeat deadline starts now.
    pub(crate) fn connect(&self) {
        self.connected.store(true, Ordering::Release);
        *self
            .last_ping
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now();
        tracing::debug!(
            sid = %self.handshake.sid,
            transport = self.transport.kind().as_str(),
            ping_interval_ms = self.handshake.ping_interval,
            ping_timeout_ms = self.handshake.ping_timeout,
            "engine.io open"
        );
        Callbacks::fire(&self.callbacks.on_open, ());
    }

    /// Reacts to one received packet: pong for ping, close for close, callbacks.
    pub(crate) async fn handle_incoming_packet(&self, packet: Packet) -> Result<()> {
        tracing::trace!(kind = ?packet.packet_id, bytes = packet.data.len(), "engine.io <-");
        Callbacks::fire(&self.callbacks.on_packet, packet.clone());
        match packet.packet_id {
            PacketId::Message | PacketId::MessageBinary => {
                Callbacks::fire(&self.callbacks.on_data, packet.data);
            }
            PacketId::Close => {
                tracing::debug!(sid = %self.handshake.sid, "engine.io closed by the server");
                self.handle_close();
            }
            // Already checked during the handshake.
            PacketId::Upgrade | PacketId::Noop => {}
            PacketId::Ping => {
                *self
                    .last_ping
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Instant::now();
                self.emit(Packet::new(PacketId::Pong, Bytes::new())).await?;
            }
            // Only the client sends these.
            PacketId::Pong | PacketId::Open => {
                return Err(crate::protocol::Error::InvalidPacket(
                    "the server sent a client-only packet (open/pong)",
                )
                .into());
            }
        }
        Ok(())
    }

    /// A polling body joins packets with 0x1e; a websocket frame is a single packet.
    fn parse_payload(bytes: Bytes, is_polling: bool) -> Result<Vec<Packet>> {
        let batch = if is_polling {
            Batch::try_from(bytes)?
        } else {
            Batch::from(Packet::try_from(bytes)?)
        };
        Ok(batch.into_iter().collect())
    }

    /// The packet stream of the transport; it ends after the first error.
    fn stream(
        transport: Transport,
    ) -> Pin<Box<dyn Stream<Item = Result<Packet>> + 'static + Send>> {
        let is_polling = transport.kind().is_polling();
        let packets = transport.flat_map(move |payload| {
            let packets = match payload.and_then(|bytes| Self::parse_payload(bytes, is_polling)) {
                Ok(packets) => packets.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            };
            stream::iter(packets)
        });
        end_after_error(packets).boxed()
    }

    /// Sends a close packet (best effort) and marks the session closed. The session is closed
    /// even when the packet cannot be sent: emits would otherwise keep going to a dead transport.
    pub(crate) async fn disconnect(&self) -> Result<()> {
        if !self.connected.load(Ordering::Acquire) {
            return Ok(());
        }
        Callbacks::fire(&self.callbacks.on_close, ());
        // Like engine.io-client: a close packet over polling, a close frame over websocket.
        let result = match &self.transport {
            Transport::Polling(_) => self.emit(Packet::new(PacketId::Close, Bytes::new())).await,
            Transport::Websocket(ws) => {
                let _guard = self.writer.lock().await;
                ws.close().await
            }
        };
        self.connected.store(false, Ordering::Release);
        tracing::debug!(sid = %self.handshake.sid, "engine.io closed by the client");
        result
    }

    /// Sends one packet.
    pub(crate) async fn emit(&self, packet: Packet) -> Result<()> {
        if !self.connected.load(Ordering::Acquire) {
            let error = Error::NotConnected;
            Callbacks::fire(&self.callbacks.on_error, error.to_string());
            return Err(error);
        }
        tracing::trace!(kind = ?packet.packet_id, bytes = packet.data.len(), "engine.io ->");

        // A binary message goes out as raw bytes (websocket frame or base64 `b` polling packet).
        let is_binary = packet.packet_id == PacketId::MessageBinary;
        let data: Bytes = if is_binary {
            packet.data
        } else {
            packet.into()
        };

        let _guard = self.writer.lock().await;
        if let Err(error) = self.transport.emit(data, is_binary).await {
            Callbacks::fire(&self.callbacks.on_error, error.to_string());
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    pub(crate) fn last_ping(&self) -> Instant {
        *self
            .last_ping
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Time left until a ping must have arrived (pingInterval + pingTimeout after the last one).
    /// See <https://socket.io/docs/v4/how-it-works/#disconnection-detection>.
    fn time_to_next_ping(&self) -> Duration {
        let deadline = Duration::from_millis(
            self.handshake
                .ping_interval
                .saturating_add(self.handshake.ping_timeout),
        );
        deadline.saturating_sub(self.last_ping().elapsed())
    }

    fn handle_close(&self) {
        Callbacks::fire(&self.callbacks.on_close, ());
        self.connected.store(false, Ordering::Release);
    }

    /// The packet stream with the heartbeat check: it ends when the socket closes and fails with
    /// [`Error::PingTimeout`] when no ping arrives within pingInterval + pingTimeout.
    pub(crate) fn as_stream(&self) -> Pin<Box<dyn Stream<Item = Result<Packet>> + Send + 'static>> {
        stream::unfold(
            (Self::stream(self.transport.clone()), self.clone()),
            |(mut stream, socket)| async move {
                // A closed socket (disconnect() or a server Close packet) yields nothing more.
                if !socket.is_connected() {
                    return None;
                }
                match tokio::time::timeout(socket.time_to_next_ping(), stream.next()).await {
                    Ok(result) => result.map(|result| (result, (stream, socket))),
                    // No ping in time: the link is dead. Be nice and disconnect properly; the
                    // reason stays the timeout even when the close packet cannot be sent.
                    Err(_) => {
                        tracing::warn!(sid = %socket.handshake.sid, "engine.io ping timeout");
                        let _ =
                            tokio::time::timeout(Duration::from_secs(1), socket.disconnect()).await;
                        socket.connected.store(false, Ordering::Release);
                        Some((Err(Error::PingTimeout), (stream, socket)))
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
            .field("callbacks", &self.callbacks)
            .field("connected", &self.connected)
            .field("handshake", &self.handshake)
            .finish()
    }
}
