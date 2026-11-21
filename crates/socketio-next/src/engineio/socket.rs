use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use crate::protocol::engineio::Batch;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use tokio::time::Instant;

use crate::engineio::callback::{Callbacks, Dispatcher, Event};
use crate::engineio::error::Result;
use crate::engineio::generator::end_after_error;
use crate::engineio::transport::{Frame, Transport};
use crate::engineio::{Error, HandshakePacket, Packet, PacketId};

/// Where the session is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Built, `connect()` not called yet.
    Idle,
    Open,
    /// `disconnect()` is sending the close: no other write may go out after it.
    Closing,
    Closed,
}

/// The session state, owned by one struct behind one lock: the phase and the heartbeat. Only
/// [`Socket`]'s transitions below change it.
#[derive(Debug)]
struct SessionState {
    phase: Phase,
    /// The last ping (or the open), on the monotonic clock and on the wall clock: the
    /// monotonic one stops while the machine sleeps, the wall clock does not.
    last_ping: (Instant, SystemTime),
    /// Set when the wall clock says the deadline passed but the monotonic clock does not (a
    /// system sleep, or a clock step): the link must show life before this instant.
    wake_check: Option<Instant>,
}

/// The engine.io session: transport, state and callbacks. Clones share all of it.
#[derive(Clone)]
pub(crate) struct Socket {
    /// Serialises writes (one polling POST or websocket frame at a time). I/O order only; the
    /// session state is in `state`.
    writer: Arc<tokio::sync::Mutex<()>>,
    transport: Transport,
    callbacks: Dispatcher,
    state: Arc<Mutex<SessionState>>,
    handshake: Arc<HandshakePacket>,
    /// Packets that came with the handshake, handed out before the transport's.
    pending: Arc<[Packet]>,
}

impl Socket {
    pub(crate) fn new(
        transport: Transport,
        handshake: HandshakePacket,
        callbacks: Callbacks,
        pending: Vec<Packet>,
    ) -> Self {
        Socket {
            writer: Arc::default(),
            transport,
            callbacks: Dispatcher::start(callbacks),
            state: Arc::new(Mutex::new(SessionState {
                phase: Phase::Idle,
                last_ping: (Instant::now(), SystemTime::now()),
                wake_check: None,
            })),
            handshake: Arc::new(handshake),
            pending: pending.into(),
        }
    }

    fn state(&self) -> MutexGuard<'_, SessionState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
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
        {
            let mut state = self.state();
            if state.phase != Phase::Idle {
                return;
            }
            state.phase = Phase::Open;
            state.last_ping = (Instant::now(), SystemTime::now());
        }
        tracing::debug!(
            sid = %self.handshake.sid,
            transport = self.transport.kind().as_str(),
            ping_interval_ms = self.handshake.ping_interval,
            ping_timeout_ms = self.handshake.ping_timeout,
            "engine.io open"
        );
        self.callbacks.fire(Event::Open);
    }

    /// Reacts to one received packet: pong for ping, close for close, callbacks.
    pub(crate) async fn handle_incoming_packet(&self, packet: Packet) -> Result<()> {
        tracing::trace!(kind = ?packet.packet_id, bytes = packet.data.len(), "engine.io <-");
        self.callbacks.fire(Event::Packet(packet.clone()));
        match packet.packet_id {
            PacketId::Message | PacketId::MessageBinary => {
                self.callbacks.fire(Event::Data(packet.data));
            }
            PacketId::Close => {
                tracing::debug!(sid = %self.handshake.sid, "engine.io closed by the server");
                self.close_once();
            }
            // Already checked during the handshake.
            PacketId::Upgrade | PacketId::Noop => {}
            PacketId::Ping => {
                self.state().last_ping = (Instant::now(), SystemTime::now());
                // Answered off the read path: a write stuck on a dead link (holding the writer
                // lock) must not keep the stream, and so the heartbeat check, from running.
                let socket = self.clone();
                tokio::spawn(async move {
                    // a disconnect() before this task ran: nothing to answer, no error event
                    if socket.is_connected() {
                        let _ = socket.emit(Packet::new(PacketId::Pong, Bytes::new())).await;
                    }
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
    /// even when the packet cannot be sent. Only the first caller sends it; from its start no
    /// other write goes out, so nothing follows the close packet.
    pub(crate) async fn disconnect(&self) -> Result<()> {
        {
            let mut state = self.state();
            if state.phase != Phase::Open {
                return Ok(());
            }
            state.phase = Phase::Closing;
        }
        // Like engine.io-client: a close packet over polling, a close frame over websocket.
        let result = {
            let _guard = self.writer.lock().await;
            match &self.transport {
                Transport::Polling(_) => {
                    let close: Bytes = Packet::new(PacketId::Close, Bytes::new()).into();
                    self.transport.emit(close, false).await
                }
                Transport::Websocket(ws) => ws.close().await,
            }
        };
        self.close_once();
        tracing::debug!(sid = %self.handshake.sid, "engine.io closed by the client");
        result
    }

    /// `Err(NotConnected)` (reported to `on_error`) unless the session is open.
    fn check_open(&self) -> Result<()> {
        if self.state().phase == Phase::Open {
            return Ok(());
        }
        let error = Error::NotConnected;
        self.callbacks.fire(Event::Error(error.to_string()));
        Err(error)
    }

    /// Writes `data` under the writer lock, if the session is still open once the lock is
    /// held (a disconnect may have begun while this write waited).
    async fn write(&self, data: Bytes, is_binary: bool) -> Result<()> {
        let _guard = self.writer.lock().await;
        self.check_open()?;
        if let Err(error) = self.transport.emit(data, is_binary).await {
            self.callbacks.fire(Event::Error(error.to_string()));
            return Err(error);
        }
        Ok(())
    }

    /// Sends one packet.
    pub(crate) async fn emit(&self, packet: Packet) -> Result<()> {
        self.check_open()?;
        tracing::trace!(kind = ?packet.packet_id, bytes = packet.data.len(), "engine.io ->");
        // A binary message goes out as raw bytes (websocket frame or base64 `b` polling packet).
        let is_binary = packet.packet_id == PacketId::MessageBinary;
        let data: Bytes = if is_binary {
            packet.data
        } else {
            packet.into()
        };
        self.write(data, is_binary).await
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
        self.check_open()?;
        let max = self.handshake.max_payload.unwrap_or(1_000_000) as usize;
        for chunk in split_by_payload(packets, max) {
            let body = Bytes::try_from(Batch(chunk))?;
            tracing::trace!(bytes = body.len(), "engine.io -> batch");
            self.write(body, false).await?;
        }
        Ok(())
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.state().phase == Phase::Open
    }

    pub(crate) fn last_ping(&self) -> Instant {
        self.state().last_ping.0
    }

    fn deadline(&self) -> Duration {
        Duration::from_millis(
            self.handshake
                .ping_interval
                .saturating_add(self.handshake.ping_timeout),
        )
    }

    /// The link showed life (a packet, a probe answer): the wall clock counts from now and a
    /// pending wake check is over.
    fn alive(&self) {
        let mut state = self.state();
        state.last_ping.1 = SystemTime::now();
        state.wake_check = None;
    }

    /// The pending wake check, or a new one (pingTimeout from now, with a probe) when the wall
    /// clock jumped past the deadline (engine.io-client `_hasPingExpired`: after a system sleep
    /// the monotonic timer has not caught up). `Some(instant)`: the link must show life before
    /// it.
    fn wake_deadline(&self) -> Option<Instant> {
        let deadline = self.deadline();
        let grace = Duration::from_millis(self.handshake.ping_timeout);
        let armed = {
            let mut state = self.state();
            let expired = state.last_ping.1.elapsed().is_ok_and(|gap| gap > deadline);
            if state.wake_check.is_some() || !expired {
                return state.wake_check;
            }
            let at = Instant::now() + grace;
            state.wake_check = Some(at);
            at
        };
        tracing::debug!(sid = %self.handshake.sid, "wall clock past the ping deadline; probing the link");
        let socket = self.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(grace, socket.probe()).await;
        });
        Some(armed)
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
        let mut state = self.state();
        if forward {
            state.last_ping.1 -= gap;
        } else {
            state.last_ping.1 += gap;
        }
    }

    /// Time left until a ping must have arrived (pingInterval + pingTimeout after the last one).
    /// See <https://socket.io/docs/v4/how-it-works/#disconnection-detection>.
    fn time_to_next_ping(&self) -> Duration {
        self.deadline().saturating_sub(self.last_ping().elapsed())
    }

    /// Marks the session closed; `on_close` fires once, for whichever way the session ends (a
    /// disconnect, a server close packet, a transport that failed or ended, a ping timeout).
    fn close_once(&self) {
        let was = std::mem::replace(&mut self.state().phase, Phase::Closed);
        if matches!(was, Phase::Open | Phase::Closing) {
            self.callbacks.fire(Event::Close);
        }
    }

    /// The packet stream with the heartbeat check: it ends when the socket closes and fails with
    /// [`Error::PingTimeout`] when no ping arrives within pingInterval + pingTimeout.
    pub(crate) fn as_stream(&self) -> Pin<Box<dyn Stream<Item = Result<Packet>> + Send + 'static>> {
        let pending = stream::iter(self.pending.iter().cloned().map(Ok).collect::<Vec<_>>());
        let packets = pending.chain(Self::stream(self.transport.clone())).boxed();
        stream::unfold((packets, self.clone()), |(mut stream, socket)| async move {
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
                // a transport error or the end of the transport closes the session too
                // (a websocket closed without a close packet, `Session ID unknown`)
                Some(Some(Err(error))) => {
                    socket.close_once();
                    Some((Err(error), (stream, socket)))
                }
                Some(None) => {
                    socket.close_once();
                    None
                }
                Some(Some(Ok(packet))) => Some((Ok(packet), (stream, socket))),
                // No ping in time: the link is dead. Be nice and disconnect properly; the
                // reason stays the timeout even when the close packet cannot be sent.
                None => {
                    tracing::warn!(sid = %socket.handshake.sid, "engine.io ping timeout");
                    let _ = tokio::time::timeout(Duration::from_secs(1), socket.disconnect()).await;
                    socket.close_once();
                    Some((Err(Error::PingTimeout), (stream, socket)))
                }
            }
        })
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
            .field("phase", &self.state().phase)
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
