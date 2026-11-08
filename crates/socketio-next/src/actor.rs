//! The connection task: owns the engine.io connection, the acks, the reconnect loop and the
//! state. Handles talk to it through a command channel; events go to the dispatcher task.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use serde_json::{Map, Value};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until};
use tracing::Instrument;
use url::Url;

use crate::backoff::Backoff;
use crate::builder::{Config, Direction};
use crate::client::Shared;
use crate::engineio::{self, PacketId as EnginePacketId};
use crate::event::AckSender;
use crate::protocol::socketio::{Packet, PacketId};
use crate::{ConnectionState, DisconnectReason, Error, Event, Payload, Result, TransportType};

/// Bound for the goodbye packets (DISCONNECT, engine.io close) on a possibly dead link.
const GOODBYE_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) type AckReply = oneshot::Sender<Result<Payload>>;

/// From the handles to the actor.
pub(crate) enum Command {
    /// An event, optionally waiting for an ack.
    Emit {
        packet: Packet,
        ack: Option<(AckReply, Duration)>,
    },
    /// An ack reply to the server; dropped when not connected.
    Send { packet: Packet },
    /// Leave the namespace and stop.
    Disconnect { done: oneshot::Sender<()> },
}

/// From the actor to the dispatcher.
pub(crate) enum Dispatch {
    Event(Event),
    State(ConnectionState),
}

struct PendingAck {
    reply: AckReply,
    deadline: Instant,
    timeout: Duration,
}

/// One established connection.
struct Conn {
    engine: engineio::Client,
    writer_tx: Option<mpsc::UnboundedSender<engineio::Packet>>,
    writer: JoinHandle<()>,
    writer_failed: oneshot::Receiver<engineio::Error>,
    sid: String,
    pid: Option<String>,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.writer.abort();
    }
}

/// How `serve` ended.
enum Outcome {
    Lost(DisconnectReason, Option<Error>),
    Exit(DisconnectReason, Option<oneshot::Sender<()>>),
}

/// How an offline wait ended.
enum Step<T> {
    Done(T),
    Exit(DisconnectReason, Option<oneshot::Sender<()>>),
}

pub(crate) struct Actor {
    config: Arc<Config>,
    url: Url,
    log_url: String,
    nsp: Arc<str>,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    cmd_tx: mpsc::UnboundedSender<Command>,
    shutdown: oneshot::Receiver<()>,
    state: watch::Sender<ConnectionState>,
    dispatch: mpsc::UnboundedSender<Dispatch>,
    shared: Arc<Shared>,
    acks: HashMap<u64, PendingAck>,
    next_ack_id: u64,
    buffer: VecDeque<Packet>,
    /// Connection state recovery: the server's private session id and the last offset.
    pid: Option<String>,
    offset: Option<String>,
}

impl Actor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config: Config,
        url: Url,
        nsp: Arc<str>,
        cmd_rx: mpsc::UnboundedReceiver<Command>,
        cmd_tx: mpsc::UnboundedSender<Command>,
        shutdown: oneshot::Receiver<()>,
        state: watch::Sender<ConnectionState>,
        dispatch: mpsc::UnboundedSender<Dispatch>,
        shared: Arc<Shared>,
    ) -> Self {
        let log_url = crate::redact::url(&url, config.redact_query);
        Actor {
            config: Arc::new(config),
            url,
            log_url,
            nsp,
            cmd_rx,
            cmd_tx,
            shutdown,
            state,
            dispatch,
            shared,
            acks: HashMap::new(),
            next_ack_id: 0,
            buffer: VecDeque::new(),
            pid: None,
            offset: None,
        }
    }

    pub(crate) async fn run(mut self, ready: oneshot::Sender<Result<()>>) {
        let span = tracing::info_span!("socketio", nsp = %self.nsp, url = %self.log_url);
        async move {
            let mut conn = match self.connect_attempt(0).await {
                Step::Done(Ok(conn)) => {
                    // connected, sid and the Connected state are set before connect() returns
                    self.on_connected(&conn);
                    let _ = ready.send(Ok(()));
                    #[cfg(test)]
                    tests::after_ready().await;
                    conn
                }
                Step::Done(Err(err)) => {
                    tracing::warn!(error = %err, "connect failed");
                    let reason = reason_for_attempt_error(&err);
                    self.close(reason, Some(err.clone()));
                    let _ = ready.send(Err(err));
                    return;
                }
                Step::Exit(reason, done) => {
                    self.close(reason, None);
                    let _ = ready.send(Err(Error::Closed));
                    finish(done);
                    return;
                }
            };
            loop {
                let outcome = self.serve(&mut conn).await;
                match outcome {
                    Outcome::Exit(reason, done) => {
                        self.goodbye(conn, reason == DisconnectReason::IoClientDisconnect)
                            .await;
                        self.close(reason, None);
                        finish(done);
                        return;
                    }
                    Outcome::Lost(reason, error) => {
                        match &error {
                            Some(err) => tracing::warn!(%reason, error = %err, "disconnected"),
                            None => tracing::warn!(%reason, "disconnected"),
                        }
                        self.teardown(conn).await;
                        self.lost();
                        let reconnect = match reason {
                            DisconnectReason::IoServerDisconnect => {
                                self.config.reconnection && self.config.reconnect_on_disconnect
                            }
                            DisconnectReason::ConnectRejected => false,
                            _ => self.config.reconnection,
                        };
                        if !reconnect {
                            self.close(reason, error);
                            return;
                        }
                        self.set_state(ConnectionState::Disconnected { reason });
                        match self.reconnect().await {
                            Some(new_conn) => {
                                self.on_connected(&new_conn);
                                conn = new_conn;
                            }
                            None => return,
                        }
                    }
                }
            }
        }
        .instrument(span)
        .await;
    }

    /// The socket.io-client Manager reconnect loop. `None` when the client closed meanwhile.
    async fn reconnect(&mut self) -> Option<Conn> {
        let config = Arc::clone(&self.config);
        let mut backoff = Backoff::new(
            config.reconnection_delay.as_millis() as u64,
            config.reconnection_delay_max.as_millis() as u64,
            config.randomization_factor,
        );
        let mut last_error: Option<Error> = None;
        loop {
            if config
                .reconnection_attempts
                .is_some_and(|max| backoff.attempts() >= max)
            {
                tracing::warn!(
                    attempts = backoff.attempts(),
                    "reconnect attempts exhausted"
                );
                self.close(DisconnectReason::ReconnectFailed, last_error);
                return None;
            }
            let delay = backoff.duration();
            let attempt = backoff.attempts();
            self.shared.link().reconnect_attempts = attempt;
            tracing::info!(attempt, delay_ms = delay.as_millis() as u64, "reconnecting");
            self.set_state(ConnectionState::Reconnecting {
                attempt,
                delay,
                error: last_error.clone(),
            });
            if let Step::Exit(reason, done) = self.offline(sleep(delay)).await {
                self.close(reason, None);
                finish(done);
                return None;
            }
            match self.connect_attempt(attempt).await {
                Step::Done(Ok(conn)) => return Some(conn),
                Step::Done(Err(err)) => {
                    tracing::warn!(attempt, error = %err, "reconnect attempt failed");
                    if matches!(
                        err,
                        Error::ConnectRejected { .. }
                            | Error::IncompatibleServer
                            | Error::InvalidAuth(_)
                    ) {
                        self.close(DisconnectReason::ConnectRejected, Some(err));
                        return None;
                    }
                    last_error = Some(err);
                }
                Step::Exit(reason, done) => {
                    self.close(reason, None);
                    finish(done);
                    return None;
                }
            }
        }
    }

    /// One connect attempt while commands keep being served (offline).
    async fn connect_attempt(&mut self, attempt: u32) -> Step<Result<Conn>> {
        self.set_state(ConnectionState::Connecting { attempt });
        self.shared.link().last_attempt_at = Some(Instant::now());
        let fut = attempt_connect(
            Arc::clone(&self.config),
            self.url.clone(),
            Arc::clone(&self.nsp),
            self.pid.clone(),
            self.offset.clone(),
        );
        self.offline(fut).await
    }

    /// Waits for `fut` while serving commands, ack timeouts and the shutdown signal.
    async fn offline<F: Future>(&mut self, fut: F) -> Step<F::Output> {
        let mut fut = pin!(fut);
        loop {
            let ack_deadline = self.next_ack_deadline();
            tokio::select! {
                out = &mut fut => return Step::Done(out),
                _ = &mut self.shutdown => return Step::Exit(DisconnectReason::Dropped, None),
                cmd = self.cmd_rx.recv() => match cmd {
                    Some(Command::Emit { packet, ack }) => self.offline_emit(packet, ack),
                    Some(Command::Send { .. }) => {
                        tracing::debug!("ack reply dropped: not connected");
                    }
                    Some(Command::Disconnect { done }) => {
                        return Step::Exit(DisconnectReason::IoClientDisconnect, Some(done));
                    }
                    None => return Step::Exit(DisconnectReason::Dropped, None),
                },
                _ = sleep_until_opt(ack_deadline) => self.expire_acks(),
            }
        }
    }

    /// Serves one connection until it ends.
    async fn serve(&mut self, conn: &mut Conn) -> Outcome {
        let mut partial: Option<(Packet, Vec<Bytes>)> = None;
        loop {
            let ack_deadline = self.next_ack_deadline();
            tokio::select! {
                _ = &mut self.shutdown => return Outcome::Exit(DisconnectReason::Dropped, None),
                cmd = self.cmd_rx.recv() => match cmd {
                    Some(Command::Emit { mut packet, ack }) => {
                        if let Some((reply, timeout)) = ack {
                            packet.id = Some(self.register_ack(reply, timeout));
                        }
                        self.write(conn, packet);
                    }
                    Some(Command::Send { packet }) => self.write(conn, packet),
                    Some(Command::Disconnect { done }) => {
                        return Outcome::Exit(DisconnectReason::IoClientDisconnect, Some(done));
                    }
                    None => return Outcome::Exit(DisconnectReason::Dropped, None),
                },
                err = &mut conn.writer_failed => {
                    let err = err.ok().map(Error::from);
                    return Outcome::Lost(DisconnectReason::TransportError, err);
                }
                item = conn.engine.next() => {
                    let now = Instant::now();
                    match item {
                        None => return Outcome::Lost(DisconnectReason::TransportClose, None),
                        Some(Err(err)) => {
                            let reason = match err {
                                engineio::Error::PingTimeout => DisconnectReason::PingTimeout,
                                engineio::Error::Protocol(_) => DisconnectReason::ParseError,
                                _ => DisconnectReason::TransportError,
                            };
                            return Outcome::Lost(reason, Some(err.into()));
                        }
                        Some(Ok(packet)) => {
                            self.shared.link().last_packet_at = Some(now);
                            match packet.packet_id {
                                EnginePacketId::Ping => {
                                    self.shared.link().last_ping_at = Some(now);
                                }
                                EnginePacketId::Message | EnginePacketId::MessageBinary => {
                                    let binary = packet.packet_id == EnginePacketId::MessageBinary;
                                    if let Some(outcome) =
                                        self.on_message(packet.data, binary, &mut partial)
                                    {
                                        return outcome;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ = sleep_until_opt(ack_deadline) => self.expire_acks(),
            }
        }
    }

    fn on_connected(&mut self, conn: &Conn) {
        let recovered = conn.pid.is_some() && conn.pid == self.pid;
        if conn.pid.is_some() {
            self.pid.clone_from(&conn.pid);
        } else {
            self.pid = None;
            self.offset = None;
        }
        let handshake = conn.engine.handshake();
        {
            let mut link = self.shared.link();
            link.sid = Some(conn.sid.clone());
            link.engine_sid = Some(handshake.sid.clone());
            link.transport = Some(conn.engine.transport());
            link.ping_interval = Some(Duration::from_millis(handshake.ping_interval));
            link.ping_timeout = Some(Duration::from_millis(handshake.ping_timeout));
            link.max_payload = handshake.max_payload;
            link.connected_at = Some(Instant::now());
            link.reconnect_attempts = 0;
        }
        self.shared.connected.store(true, Ordering::Release);
        tracing::info!(
            sid = %conn.sid,
            transport = %conn.engine.transport(),
            recovered,
            "connected"
        );
        self.set_state(ConnectionState::Connected {
            sid: conn.sid.clone(),
            recovered,
        });
        // socket.io-client emitBuffered(): queued emits go out right after CONNECT.
        let buffered: Vec<Packet> = self.buffer.drain(..).collect();
        for packet in buffered {
            self.write(conn, packet);
        }
        self.update_counts();
    }

    /// One engine.io message: a socket.io packet (text) or one of its attachments (binary).
    /// Like socket.io-parser, text while attachments are pending and binary without a pending
    /// packet are protocol errors.
    fn on_message(
        &mut self,
        data: Bytes,
        binary: bool,
        partial: &mut Option<(Packet, Vec<Bytes>)>,
    ) -> Option<Outcome> {
        let parse_error = |what: &'static str| {
            Some(Outcome::Lost(
                DisconnectReason::ParseError,
                Some(crate::protocol::Error::InvalidPacket(what).into()),
            ))
        };
        match (partial.is_some(), binary) {
            (true, false) => return parse_error("got plaintext data when reconstructing a packet"),
            (false, true) => {
                return parse_error("got binary data when not reconstructing a packet");
            }
            _ => {}
        }
        if let Some((mut packet, mut attachments)) = partial.take() {
            attachments.push(data);
            if attachments.len() < usize::from(packet.attachment_count) {
                *partial = Some((packet, attachments));
                return None;
            }
            packet.attachments = Some(attachments);
            return self.on_packet(packet);
        }
        let packet = match Packet::try_from(&data) {
            Ok(packet) => packet,
            Err(err) => {
                return Some(Outcome::Lost(
                    DisconnectReason::ParseError,
                    Some(err.into()),
                ));
            }
        };
        if matches!(
            packet.packet_type,
            PacketId::BinaryEvent | PacketId::BinaryAck
        ) && packet.attachment_count > 0
        {
            *partial = Some((packet, Vec::new()));
            return None;
        }
        self.on_packet(packet)
    }

    fn on_packet(&mut self, packet: Packet) -> Option<Outcome> {
        tracing::trace!(
            kind = ?packet.packet_type,
            nsp = %packet.nsp,
            id = packet.id,
            bytes = packet.data.as_ref().map_or(0, String::len),
            attachments = packet.attachment_count,
            "socket.io <-"
        );
        inspect(&self.config, Direction::Incoming, &packet);
        if *packet.nsp != *self.nsp {
            return None;
        }
        match packet.packet_type {
            PacketId::Event | PacketId::BinaryEvent => {
                let (name, payload) = match packet.event_payload() {
                    Ok(event) => event,
                    Err(err) => {
                        return Some(Outcome::Lost(
                            DisconnectReason::ParseError,
                            Some(err.into()),
                        ));
                    }
                };
                if self.pid.is_some() {
                    if let Some(Value::String(offset)) = payload.args().last() {
                        self.offset = Some(offset.clone());
                    }
                }
                let ack = packet
                    .id
                    .map(|id| AckSender::new(id, Arc::clone(&self.nsp), self.cmd_tx.clone()));
                let _ = self
                    .dispatch
                    .send(Dispatch::Event(Event::new(name, payload, ack)));
            }
            PacketId::Ack | PacketId::BinaryAck => {
                if let Some(pending) = packet.id.and_then(|id| self.acks.remove(&id)) {
                    let _ = pending
                        .reply
                        .send(packet.ack_payload().map_err(Error::from));
                    self.update_counts();
                }
            }
            PacketId::Disconnect => {
                return Some(Outcome::Lost(DisconnectReason::IoServerDisconnect, None));
            }
            PacketId::ConnectError => {
                return Some(Outcome::Lost(
                    DisconnectReason::ConnectRejected,
                    Some(connect_error(packet.data.as_deref())),
                ));
            }
            // A second CONNECT on a joined namespace means nothing.
            PacketId::Connect => {}
        }
        None
    }

    fn register_ack(&mut self, reply: AckReply, timeout: Duration) -> u64 {
        // Unique per client like socket.io-client's counter (u64: never wraps in practice).
        let id = self.next_ack_id;
        self.next_ack_id += 1;
        self.acks.insert(
            id,
            PendingAck {
                reply,
                deadline: deadline(timeout),
                timeout,
            },
        );
        self.update_counts();
        id
    }

    fn offline_emit(&mut self, mut packet: Packet, ack: Option<(AckReply, Duration)>) {
        if self.config.buffer_while_disconnected {
            if let Some((reply, timeout)) = ack {
                packet.id = Some(self.register_ack(reply, timeout));
            }
            self.buffer.push_back(packet);
            self.update_counts();
        } else if let Some((reply, _)) = ack {
            let _ = reply.send(Err(Error::NotConnected));
        } else {
            tracing::debug!("emit dropped: not connected");
        }
    }

    // ponytail: O(pending acks) per loop turn; a deadline heap if thousands of acks are pending.
    fn next_ack_deadline(&self) -> Option<Instant> {
        self.acks.values().map(|ack| ack.deadline).min()
    }

    fn expire_acks(&mut self) {
        let now = Instant::now();
        let expired: Vec<u64> = self
            .acks
            .iter()
            .filter(|(_, ack)| ack.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(ack) = self.acks.remove(&id) {
                let _ = ack.reply.send(Err(Error::AckTimeout(ack.timeout)));
            }
            // socket.io-client drops a buffered packet whose ack timed out.
            self.buffer.retain(|packet| packet.id != Some(id));
        }
        self.update_counts();
    }

    /// The connection is gone: acks of sent packets fail (buffered ones keep waiting).
    fn lost(&mut self) {
        self.shared.connected.store(false, Ordering::Release);
        {
            let mut link = self.shared.link();
            link.sid = None;
            link.engine_sid = None;
            link.transport = None;
        }
        let buffered: Vec<u64> = self.buffer.iter().filter_map(|p| p.id).collect();
        let failed: Vec<u64> = self
            .acks
            .keys()
            .filter(|id| !buffered.contains(id))
            .copied()
            .collect();
        for id in failed {
            if let Some(ack) = self.acks.remove(&id) {
                let _ = ack.reply.send(Err(Error::Disconnected));
            }
        }
        self.update_counts();
    }

    /// Final: nothing is sent or dispatched after this.
    fn close(&mut self, reason: DisconnectReason, error: Option<Error>) {
        self.lost();
        for (_, ack) in self.acks.drain() {
            let _ = ack.reply.send(Err(Error::Closed));
        }
        self.buffer.clear();
        self.update_counts();
        self.shared.closed.store(true, Ordering::Release);
        tracing::info!(%reason, "closed");
        self.set_state(ConnectionState::Closed { reason, error });
    }

    fn set_state(&self, state: ConnectionState) {
        tracing::debug!(?state, "state");
        self.state.send_replace(state.clone());
        let _ = self.dispatch.send(Dispatch::State(state));
    }

    fn update_counts(&self) {
        let mut link = self.shared.link();
        link.pending_acks = self.acks.len();
        link.buffered_emits = self.buffer.len();
    }

    /// Queues `packet` (and its attachments) for the writer task.
    fn write(&self, conn: &Conn, packet: Packet) {
        tracing::trace!(
            kind = ?packet.packet_type,
            id = packet.id,
            bytes = packet.data.as_ref().map_or(0, String::len),
            attachments = packet.attachment_count,
            "socket.io ->"
        );
        inspect(&self.config, Direction::Outgoing, &packet);
        let Some(writer) = &conn.writer_tx else {
            return;
        };
        let _ = writer.send(engineio::Packet::new(
            EnginePacketId::Message,
            Bytes::from(&packet),
        ));
        for attachment in packet.attachments.into_iter().flatten() {
            let _ = writer.send(engineio::Packet::new(
                EnginePacketId::MessageBinary,
                attachment,
            ));
        }
    }

    /// Closing for good: DISCONNECT (when we leave on purpose) and engine.io close, bounded.
    async fn goodbye(&self, mut conn: Conn, leave: bool) {
        if leave {
            self.write(
                &conn,
                Packet::new(
                    PacketId::Disconnect,
                    self.nsp.to_string(),
                    None,
                    None,
                    0,
                    None,
                ),
            );
        }
        // Let the writer drain what is queued, then close the engine.
        drop(conn.writer_tx.take());
        let _ = tokio::time::timeout(GOODBYE_TIMEOUT, &mut conn.writer).await;
        let _ = tokio::time::timeout(GOODBYE_TIMEOUT, conn.engine.disconnect()).await;
    }

    /// After a loss: close the engine when it is still open (socket.io-client closes it too).
    async fn teardown(&self, conn: Conn) {
        conn.writer.abort();
        if conn.engine.is_connected() {
            let _ = tokio::time::timeout(GOODBYE_TIMEOUT, conn.engine.disconnect()).await;
        }
    }
}

/// `now + timeout`, saturating (a `Duration::MAX` timeout means "about never", not a panic).
fn deadline(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout)
        .unwrap_or_else(|| now + Duration::from_secs(86_400 * 365 * 30))
}

/// Runs the packet inspector; a panicking inspector is logged, not fatal.
fn inspect(config: &Config, direction: Direction, packet: &Packet) {
    if let Some(inspector) = &config.inspector {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            inspector(direction, packet)
        }))
        .is_err()
        {
            tracing::error!("the packet inspector panicked");
        }
    }
}

fn finish(done: Option<oneshot::Sender<()>>) {
    if let Some(done) = done {
        let _ = done.send(());
    }
}

async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

fn reason_for_attempt_error(err: &Error) -> DisconnectReason {
    match err {
        Error::ConnectRejected { .. } | Error::IncompatibleServer | Error::InvalidAuth(_) => {
            DisconnectReason::ConnectRejected
        }
        Error::Protocol(_) => DisconnectReason::ParseError,
        _ => DisconnectReason::TransportError,
    }
}

/// The CONNECT payload: socket.io-client sends `{pid, offset, ...auth}` once recovery is on.
fn connect_payload(auth: Option<Value>, pid: Option<&str>, offset: Option<&str>) -> Option<Value> {
    let Some(pid) = pid else {
        return auth;
    };
    let mut map = Map::new();
    map.insert("pid".into(), Value::String(pid.to_owned()));
    if let Some(offset) = offset {
        map.insert("offset".into(), Value::String(offset.to_owned()));
    }
    if let Some(Value::Object(auth)) = auth {
        map.extend(auth);
    }
    Some(Value::Object(map))
}

/// CONNECT_ERROR data: `{message, data}` (socket.io 3+) or a bare string.
fn connect_error(data: Option<&str>) -> Error {
    let value: Value = data
        .and_then(|d| serde_json::from_str(d).ok())
        .unwrap_or(Value::Null);
    match value {
        Value::String(message) => Error::ConnectRejected {
            message,
            data: None,
        },
        Value::Object(mut map) => Error::ConnectRejected {
            message: map
                .remove("message")
                .and_then(|m| m.as_str().map(str::to_owned))
                .unwrap_or_default(),
            data: map.remove("data"),
        },
        _ => Error::ConnectRejected {
            message: String::new(),
            data: None,
        },
    }
}

/// Engine.io handshake + namespace CONNECT, bounded by the timeout.
/// `auth_with` runs inside the bound too, so a hanging auth function cannot stall the loop.
async fn attempt_connect(
    config: Arc<Config>,
    url: Url,
    nsp: Arc<str>,
    pid: Option<String>,
    offset: Option<String>,
) -> Result<Conn> {
    let timeout = config.timeout;
    let attempt = async move {
        let auth = match &config.auth_fn {
            Some(auth_fn) => {
                let auth = std::panic::AssertUnwindSafe(auth_fn())
                    .catch_unwind()
                    .await
                    .map_err(|_| Error::InvalidAuth("the auth_with function panicked"))?;
                Some(auth)
            }
            None => config.auth.clone(),
        };
        // socket.io requires an object (socket.io-parser rejects anything else and the server
        // closes the connection)
        if auth.as_ref().is_some_and(|a| !a.is_object()) {
            return Err(Error::InvalidAuth("the auth payload must be a JSON object"));
        }
        let auth = connect_payload(auth, pid.as_deref(), offset.as_deref());
        attempt_connect_unbounded(config, url, nsp, auth).await
    };
    tokio::time::timeout(timeout, attempt)
        .await
        .map_err(|_| Error::ConnectTimeout(timeout))?
}

async fn attempt_connect_unbounded(
    config: Arc<Config>,
    url: Url,
    nsp: Arc<str>,
    auth: Option<Value>,
) -> Result<Conn> {
    #[allow(unused_mut)]
    let mut builder = engineio::ClientBuilder::new(url)
        .timeout(config.timeout)
        .headers(config.headers.clone());
    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    if let Some(tls) = &config.tls_config {
        builder = builder.tls_config(tls.clone());
    }
    let mut engine = match config.transport {
        TransportType::Any => builder.build_with_fallback().await?,
        TransportType::Polling => builder.build_polling().await?,
        TransportType::Websocket => builder.build_websocket().await?,
        TransportType::WebsocketUpgrade => builder.build_websocket_with_upgrade().await?,
    };
    engine.connect().await?;

    let (writer_tx, writer_rx) = mpsc::unbounded_channel();
    let (failed_tx, writer_failed) = oneshot::channel();
    let writer = tokio::spawn(writer_task(engine.clone(), writer_rx, failed_tx));
    let mut conn = Conn {
        engine: engine.clone(),
        writer_tx: Some(writer_tx),
        writer,
        writer_failed,
        sid: String::new(),
        pid: None,
    };

    let connect = Packet::new(
        PacketId::Connect,
        nsp.to_string(),
        auth.map(|a| a.to_string()),
        None,
        0,
        None,
    );
    inspect(&config, Direction::Outgoing, &connect);
    if let Some(writer) = &conn.writer_tx {
        let _ = writer.send(engineio::Packet::new(
            EnginePacketId::Message,
            Bytes::from(&connect),
        ));
    }

    let result = async {
        loop {
            let packet = engine
                .next()
                .await
                .ok_or(Error::Engine(engineio::Error::NotConnected))??;
            if packet.packet_id != EnginePacketId::Message {
                continue;
            }
            let packet = Packet::try_from(&packet.data)?;
            inspect(&config, Direction::Incoming, &packet);
            if *packet.nsp != *nsp {
                continue;
            }
            match packet.packet_type {
                PacketId::Connect => {
                    let data: Value = packet
                        .data
                        .as_deref()
                        .and_then(|d| serde_json::from_str(d).ok())
                        .unwrap_or(Value::Null);
                    let sid = data
                        .get("sid")
                        .and_then(Value::as_str)
                        .ok_or(Error::IncompatibleServer)?;
                    let pid = data.get("pid").and_then(Value::as_str).map(str::to_owned);
                    return Ok((sid.to_owned(), pid));
                }
                PacketId::ConnectError => return Err(connect_error(packet.data.as_deref())),
                _ => {}
            }
        }
    }
    .await;
    match result {
        Ok((sid, pid)) => {
            conn.sid = sid;
            conn.pid = pid;
            Ok(conn)
        }
        Err(err) => {
            let _ = tokio::time::timeout(GOODBYE_TIMEOUT, engine.disconnect()).await;
            Err(err)
        }
    }
}

/// Sends queued packets in order; reports the first failure and stops.
async fn writer_task(
    engine: engineio::Client,
    mut rx: mpsc::UnboundedReceiver<engineio::Packet>,
    failed: oneshot::Sender<engineio::Error>,
) {
    while let Some(first) = rx.recv().await {
        // whatever queued up meanwhile goes in the same request (polling)
        let mut batch = vec![first];
        while batch.len() < 512 {
            match rx.try_recv() {
                Ok(packet) => batch.push(packet),
                Err(_) => break,
            }
        }
        if let Err(err) = engine.emit_all(batch).await {
            let _ = failed.send(err);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Test builds pause the connection task right after it signalled `connect()`: whatever
    /// `connect()` promises must be true by then, not a few polls later.
    pub(super) async fn after_ready() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connect_returns_a_connected_client() {
        let client = crate::ClientBuilder::new(socketio_next_testing::socket_io_server())
            .connect()
            .await
            .unwrap();
        assert!(client.connected());
        assert!(client.id().is_some());
        assert!(matches!(client.state(), ConnectionState::Connected { .. }));
        assert!(client.link_info().sid.is_some());
        client
            .emit("echo", json!(1))
            .expect("emit right after connect");
        client.disconnect().await.unwrap();
    }

    #[test]
    fn connect_payload_merges_recovery_fields() {
        assert_eq!(connect_payload(None, None, None), None);
        assert_eq!(
            connect_payload(Some(json!({"token": "t"})), None, None),
            Some(json!({"token": "t"}))
        );
        assert_eq!(
            connect_payload(Some(json!({"token": "t"})), Some("p"), Some("o")),
            Some(json!({"pid": "p", "offset": "o", "token": "t"}))
        );
        assert_eq!(
            connect_payload(None, Some("p"), None),
            Some(json!({"pid": "p"}))
        );
    }

    #[test]
    fn connect_error_shapes() {
        let err = connect_error(Some(r#"{"message":"auth failed","data":{"code":7}}"#));
        assert!(matches!(
            err,
            Error::ConnectRejected { ref message, data: Some(ref data) } if message == "auth failed" && data["code"] == 7
        ));
        let err = connect_error(Some(r#""Invalid namespace""#));
        assert!(
            matches!(err, Error::ConnectRejected { ref message, data: None } if message == "Invalid namespace")
        );
        assert!(matches!(connect_error(None), Error::ConnectRejected { .. }));
    }
}
