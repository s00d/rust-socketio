use std::fmt::Debug;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use crate::protocol::engineio::Batch;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use tokio::time::Instant;

use crate::engineio::callback::Callbacks;
use crate::engineio::error::Result;
use crate::engineio::generator::end_after_error;
use crate::engineio::transport::{Frame, Transport};
use crate::engineio::{Error, HandshakePacket, Packet, PacketId};

/// The engine.io session: transport, heartbeat state and callbacks. Clones share all state.
#[derive(Clone)]
pub(crate) struct Socket {
    /// Serialises writes (one polling POST or websocket frame at a time).
    writer: Arc<tokio::sync::Mutex<()>>,
    transport: Transport,
    callbacks: Callbacks,
    connected: Arc<AtomicBool>,
    /// The last ping (or the open), on the monotonic clock and on the wall clock: the
    /// monotonic one stops while the machine sleeps, the wall clock does not.
    last_ping: Arc<Mutex<(Instant, SystemTime)>>,
    /// Set when the wall clock says the deadline passed but the monotonic clock does not (a
    /// system sleep, or a clock step): the link must show life before this instant.
    wake_check: Arc<Mutex<Option<Instant>>>,
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
            last_ping: Arc::new(Mutex::new((Instant::now(), SystemTime::now()))),
            wake_check: Arc::default(),
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
            .unwrap_or_else(PoisonError::into_inner) = (Instant::now(), SystemTime::now());
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
                    .unwrap_or_else(PoisonError::into_inner) = (Instant::now(), SystemTime::now());
                // Answered off the read path: a write stuck on a dead link (holding the writer
                // lock) must not keep the stream, and so the heartbeat check, from running.
                let socket = self.clone();
                tokio::spawn(async move {
                    let _ = socket.emit(Packet::new(PacketId::Pong, Bytes::new())).await;
                });
            }
            PacketId::Pong => {
                return Err(crate::protocol::Error::InvalidPacket(
                    "unexpected pong from the server (the client answers pings, not the server)",
                )
                .into());
            }
            PacketId::Open => {
                return Err(crate::protocol::Error::InvalidPacket(
                    "unexpected second open packet in an established session",
                )
                .into());
            }
        }
        Ok(())
    }

    /// A polling body joins packets with 0x1e; a websocket text frame is one packet; a
    /// websocket binary frame is the data of one binary message.
    fn parse_payload(frame: Frame, is_polling: bool) -> Result<Vec<Packet>> {
        let batch = match frame {
            Frame::Text(bytes) if is_polling => Batch::try_from(bytes)?,
            Frame::Text(bytes) => Batch::from(Packet::try_from(bytes)?),
            Frame::Binary(data) => Batch::from(Packet::new(PacketId::MessageBinary, data)),
        };
        Ok(batch.into_iter().collect())
    }

    /// The packet stream of the transport; it ends after the first error.
    fn stream(
        transport: Transport,
    ) -> Pin<Box<dyn Stream<Item = Result<Packet>> + 'static + Send>> {
        let is_polling = transport.kind().is_polling();
        let packets = transport.flat_map(move |payload| {
            let packets = match payload.and_then(|frame| Self::parse_payload(frame, is_polling)) {
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

    /// Sends several packets: one POST per `maxPayload`-sized batch over polling (like
    /// engine.io-client's write buffer), one frame each over websocket.
    pub(crate) async fn emit_all(&self, packets: Vec<Packet>) -> Result<()> {
        if !self.transport.kind().is_polling() || packets.len() == 1 {
            for packet in packets {
                self.emit(packet).await?;
            }
            return Ok(());
        }
        if !self.connected.load(Ordering::Acquire) {
            let error = Error::NotConnected;
            Callbacks::fire(&self.callbacks.on_error, error.to_string());
            return Err(error);
        }
        let max = self.handshake.max_payload.unwrap_or(1_000_000) as usize;
        for chunk in split_by_payload(packets, max) {
            let body = Bytes::try_from(Batch(chunk))?;
            tracing::trace!(bytes = body.len(), "engine.io -> batch");
            let _guard = self.writer.lock().await;
            if let Err(error) = self.transport.emit(body, false).await {
                Callbacks::fire(&self.callbacks.on_error, error.to_string());
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    pub(crate) fn last_ping(&self) -> Instant {
        self.last_ping
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .0
    }

    fn deadline(&self) -> Duration {
        Duration::from_millis(
            self.handshake
                .ping_interval
                .saturating_add(self.handshake.ping_timeout),
        )
    }

    /// engine.io-client `_hasPingExpired`: the wall clock says the deadline passed (after a
    /// system sleep, when the monotonic timer has not caught up).
    fn wall_clock_expired(&self) -> bool {
        let wall = self
            .last_ping
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .1;
        wall.elapsed().is_ok_and(|gap| gap > self.deadline())
    }

    /// The link showed life (a packet, a probe answer): the wall clock counts from now and a
    /// pending wake check is over.
    fn alive(&self) {
        self.last_ping
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .1 = SystemTime::now();
        *self
            .wake_check
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// The pending wake check, or a new one (pingTimeout from now, with a probe) when the wall
    /// clock jumped past the deadline. `Some(instant)`: the link must show life before it.
    fn wake_deadline(&self) -> Option<Instant> {
        let mut check = self
            .wake_check
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if check.is_none() && self.wall_clock_expired() {
            let grace = Duration::from_millis(self.handshake.ping_timeout);
            *check = Some(Instant::now() + grace);
            tracing::debug!(sid = %self.handshake.sid, "wall clock past the ping deadline; probing the link");
            let socket = self.clone();
            tokio::spawn(async move {
                let _ = tokio::time::timeout(grace, socket.probe()).await;
            });
        }
        *check
    }

    /// A liveness probe. Polling: a noop through the normal, locked write path (two POSTs at
    /// once make engine.io close the session with "data request overlap"); its answer proves
    /// the session, so the link is alive. Websocket: a ping, whose pong arrives as a noop.
    async fn probe(&self) {
        if self.transport.kind().is_polling() {
            if self
                .emit(Packet::new(PacketId::Noop, Bytes::new()))
                .await
                .is_ok()
            {
                self.alive();
            }
        } else {
            let _ = self.transport.ws_ping().await;
        }
    }

    /// Moves the wall clock of the last ping back by `gap` (`forward`: a sleep or a forward
    /// step) or ahead (a backward step), as the system would.
    #[cfg(test)]
    pub(crate) fn shift_wall_clock(&self, gap: Duration, forward: bool) {
        let mut last = self
            .last_ping
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if forward {
            last.1 -= gap;
        } else {
            last.1 += gap;
        }
    }

    /// Time left until a ping must have arrived (pingInterval + pingTimeout after the last one).
    /// See <https://socket.io/docs/v4/how-it-works/#disconnection-detection>.
    fn time_to_next_ping(&self) -> Duration {
        self.deadline().saturating_sub(self.last_ping().elapsed())
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
                // Waits in slices so the wall clock is checked now and then: tokio's timer
                // does not count the time the machine slept. A wall-clock jump alone does not
                // close the link (an NTP step looks the same): it arms a probe and a short
                // deadline, and any packet before it proves the link alive.
                let outcome = {
                    let next = stream.next();
                    tokio::pin!(next);
                    loop {
                        let mut slice = socket.time_to_next_ping().min(WALL_CHECK);
                        if let Some(at) = socket.wake_deadline() {
                            slice = slice.min(at.saturating_duration_since(Instant::now()));
                        }
                        match tokio::time::timeout(slice, &mut next).await {
                            Ok(result) => {
                                if matches!(result, Some(Ok(_))) {
                                    socket.alive();
                                }
                                break Some(result);
                            }
                            Err(_) if socket.time_to_next_ping().is_zero() => break None,
                            Err(_) => {
                                if socket
                                    .wake_deadline()
                                    .is_some_and(|at| Instant::now() >= at)
                                {
                                    break None;
                                }
                            }
                        }
                    }
                };
                match outcome {
                    Some(result) => result.map(|result| (result, (stream, socket))),
                    // No ping in time: the link is dead. Be nice and disconnect properly; the
                    // reason stays the timeout even when the close packet cannot be sent.
                    None => {
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

/// How often a waiting reader compares the wall clock with the heartbeat deadline.
const WALL_CHECK: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(2)
};

/// engine.io-client `_getWritablePackets`: split before the packet that pushes the encoded size
/// over `max_payload` (base64 counts 4/3 for binary); a single oversized packet goes alone.
fn split_by_payload(packets: Vec<Packet>, max_payload: usize) -> Vec<Vec<Packet>> {
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut size = 1;
    for packet in packets {
        let len = if packet.packet_id == PacketId::MessageBinary {
            packet.data.len().div_ceil(3) * 4
        } else {
            packet.data.len()
        };
        if !current.is_empty() && size + len > max_payload {
            chunks.push(std::mem::take(&mut current));
            size = 1;
        }
        size += len + 2;
        current.push(packet);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The polling probe waits for the writer lock like every other write: it never runs
    /// next to another POST (engine.io answers an overlap with 400 and closes the session).
    #[tokio::test]
    async fn the_polling_probe_takes_the_writer_lock() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_server().unwrap();
            let client = crate::engineio::ClientBuilder::new(url)
                .build_polling()
                .await
                .unwrap();
            client.connect().await.unwrap();
            let socket = client.socket().clone();
            let held = socket.writer.lock().await;
            let probe = tokio::spawn({
                let socket = socket.clone();
                async move { socket.probe().await }
            });
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(
                !probe.is_finished(),
                "the probe wrote while another write held the lock"
            );
            drop(held);
            tokio::time::timeout(Duration::from_secs(5), probe)
                .await
                .unwrap()
                .unwrap();
            client.disconnect().await.unwrap();
        })
        .await
        .expect("test `the_polling_probe_takes_the_writer_lock` timed out")
    }

    #[test]
    fn batches_split_at_max_payload() {
        let p = |n: usize| Packet::new(PacketId::Message, Bytes::from(vec![b'x'; n]));
        let sizes = |chunks: Vec<Vec<Packet>>| chunks.iter().map(Vec::len).collect::<Vec<_>>();
        assert_eq!(sizes(split_by_payload(vec![p(10), p(10), p(10)], 100)), [3]);
        assert_eq!(
            sizes(split_by_payload(vec![p(40), p(40), p(40)], 100)),
            [2, 1]
        );
        // an oversized packet still goes, alone
        assert_eq!(sizes(split_by_payload(vec![p(500), p(1)], 100)), [1, 1]);
        let bin = Packet::new(PacketId::MessageBinary, Bytes::from(vec![0u8; 60]));
        assert_eq!(sizes(split_by_payload(vec![bin.clone(), bin], 100)), [1, 1]);
    }
}
