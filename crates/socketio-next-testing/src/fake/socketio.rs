//! The socket.io v4 app: packet codec, namespaces, acks, binary attachments, and the public
//! types handlers work with ([`Namespace`], [`Socket`], [`Event`], [`Data`], [`Connect`]).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};

use super::engine::{Action, Ctx, Handshake, Message};

const CONNECT: u8 = 0;
const DISCONNECT: u8 = 1;
const EVENT: u8 = 2;
const ACK: u8 = 3;
const CONNECT_ERROR: u8 = 4;
const BINARY_EVENT: u8 = 5;
const BINARY_ACK: u8 = 6;

/// Event names socket.io-parser refuses (`RESERVED_EVENTS`).
const RESERVED: &[&str] = &[
    "connect",
    "connect_error",
    "disconnect",
    "disconnecting",
    "newListener",
    "removeListener",
];

fn placeholder(num: usize) -> Value {
    json!({ "_placeholder": true, "num": num })
}

fn placeholder_num(value: &Value) -> Option<usize> {
    let map = value.as_object()?;
    if map.get("_placeholder") != Some(&Value::Bool(true)) {
        return None;
    }
    map.get("num")?.as_u64().map(|n| n as usize)
}

/// Event or ack arguments: JSON values, where binary stands as socket.io-parser placeholders
/// (`{"_placeholder":true,"num":n}`) for attachment `n`.
///
/// Received data is checked like socket.io-parser's `reconstructPacket` (a placeholder must
/// point at an attachment). Sent data is renumbered depth-first, and attachments no value
/// refers to are not sent.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Data {
    values: Vec<Value>,
    binary: Vec<Bytes>,
}

impl Data {
    /// JSON arguments.
    pub fn new(values: impl IntoIterator<Item = Value>) -> Data {
        Data {
            values: values.into_iter().collect(),
            binary: Vec::new(),
        }
    }

    /// Appends a JSON argument.
    pub fn arg(mut self, value: impl Into<Value>) -> Data {
        self.values.push(value.into());
        self
    }

    /// Appends a binary argument.
    pub fn binary_arg(mut self, bytes: impl Into<Bytes>) -> Data {
        let value = self.binary(bytes);
        self.values.push(value);
        self
    }

    /// A placeholder for `bytes`, to put anywhere inside a JSON argument.
    pub fn binary(&mut self, bytes: impl Into<Bytes>) -> Value {
        self.binary.push(bytes.into());
        placeholder(self.binary.len() - 1)
    }

    /// The JSON arguments, placeholders included.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// The bytes a placeholder value stands for.
    pub fn bytes_of(&self, value: &Value) -> Option<&Bytes> {
        self.binary.get(placeholder_num(value)?)
    }

    /// All attachments.
    pub fn attachments(&self) -> &[Bytes] {
        &self.binary
    }

    /// Only the first argument (with its attachments).
    pub fn first(&self) -> Data {
        Data {
            values: self.values.iter().take(1).cloned().collect(),
            binary: self.binary.clone(),
        }
    }

    /// Checks received placeholders against `attachments`; `None` for an illegal one.
    fn received(values: Vec<Value>, attachments: Vec<Bytes>) -> Option<Data> {
        fn check(value: &Value, count: usize) -> bool {
            if value.get("_placeholder") == Some(&Value::Bool(true)) {
                return placeholder_num(value).is_some_and(|n| n < count);
            }
            match value {
                Value::Array(items) => items.iter().all(|v| check(v, count)),
                Value::Object(map) => map.values().all(|v| check(v, count)),
                _ => true,
            }
        }
        // without attachments a placeholder lookalike is plain JSON
        if !attachments.is_empty() && !values.iter().all(|v| check(v, attachments.len())) {
            return None;
        }
        Some(Data {
            values,
            binary: attachments,
        })
    }

    /// The values with placeholders numbered depth-first, and the attachments in that order.
    fn deconstruct(self) -> (Vec<Value>, Vec<Bytes>) {
        fn walk(value: Value, pool: &[Bytes], out: &mut Vec<Bytes>) -> Value {
            if let Some(bytes) = placeholder_num(&value).and_then(|n| pool.get(n)) {
                out.push(bytes.clone());
                return placeholder(out.len() - 1);
            }
            match value {
                Value::Object(map) => Value::Object(
                    map.into_iter()
                        .map(|(k, v)| (k, walk(v, pool, out)))
                        .collect(),
                ),
                Value::Array(items) => {
                    Value::Array(items.into_iter().map(|v| walk(v, pool, out)).collect())
                }
                other => other,
            }
        }
        let mut out = Vec::new();
        let values = self
            .values
            .into_iter()
            .map(|v| walk(v, &self.binary, &mut out))
            .collect();
        (values, out)
    }
}

/// An event a client emitted.
#[derive(Debug, Clone)]
pub struct Event {
    name: String,
    data: Data,
    ack: Option<u64>,
}

impl Event {
    /// The event name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The arguments.
    pub fn data(&self) -> &Data {
        &self.data
    }

    /// The arguments, owned.
    pub fn into_data(self) -> Data {
        self.data
    }

    /// The client waits for an ack ([`Socket::ack`]).
    pub fn wants_ack(&self) -> bool {
        self.ack.is_some()
    }
}

/// The answer of [`Namespace::on_connect`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Connect {
    /// CONNECT is answered with the socket id.
    Accept,
    /// CONNECT_ERROR (a failing socket.io middleware: `next(err)`).
    Reject {
        /// `err.message`
        message: String,
        /// `err.data`
        data: Option<Value>,
    },
}

type ConnectFn = dyn Fn(&mut Socket<'_>) -> Connect + Send + Sync;
type EventFn = dyn Fn(&mut Socket<'_>, Event) + Send + Sync;
type DisconnectFn = dyn Fn(&mut Socket<'_>, &str) + Send + Sync;
type Later = Box<dyn for<'a> FnOnce(&mut Socket<'a>) + Send>;
type OnReply = Box<dyn for<'a> FnOnce(&mut Socket<'a>, Data) + Send>;

/// The behaviour of one namespace of a fake server. Handlers run on the session's task, one at
/// a time per client, and act through the [`Socket`] they get. They must not block (every fake
/// server shares one small runtime): hand work to a channel or use [`Socket::after`]. A
/// panicking handler closes that client's connection and prints the panic.
#[derive(Clone, Default)]
pub struct Namespace {
    connect: Option<Arc<ConnectFn>>,
    connect_delay: Duration,
    events: HashMap<String, Arc<EventFn>>,
    any: Option<Arc<EventFn>>,
    disconnect: Option<Arc<DisconnectFn>>,
}

impl fmt::Debug for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Namespace")
            .field("connect_delay", &self.connect_delay)
            .field("events", &self.events.keys().collect::<Vec<_>>())
            .field("any", &self.any.is_some())
            .finish()
    }
}

impl Namespace {
    /// A namespace that accepts every CONNECT and ignores every event.
    pub fn new() -> Namespace {
        Namespace::default()
    }

    /// Decides each CONNECT (the auth is in [`Socket::auth`]). What the handler emits goes out
    /// after the CONNECT answer when it accepts, and is dropped when it rejects.
    pub fn on_connect(
        mut self,
        f: impl Fn(&mut Socket<'_>) -> Connect + Send + Sync + 'static,
    ) -> Namespace {
        self.connect = Some(Arc::new(f));
        self
    }

    /// Answers CONNECT only after `delay` (a slow socket.io middleware); a packet for the
    /// namespace before that closes the connection, as on socket.io.
    pub fn connect_delay(mut self, delay: Duration) -> Namespace {
        self.connect_delay = delay;
        self
    }

    /// Handles `event`.
    ///
    /// # Panics
    /// For the names socket.io reserves (`connect`, `disconnect`, ...): clients cannot emit
    /// them; see [`on_connect`](Self::on_connect) and [`on_disconnect`](Self::on_disconnect).
    pub fn on(
        mut self,
        event: impl Into<String>,
        f: impl Fn(&mut Socket<'_>, Event) + Send + Sync + 'static,
    ) -> Namespace {
        let event = event.into();
        assert!(
            !RESERVED.contains(&event.as_str()),
            "`{event}` is a reserved socket.io event name; use on_connect / on_disconnect"
        );
        self.events.insert(event, Arc::new(f));
        self
    }

    /// Runs when the socket leaves, with socket.io's reason: `client namespace disconnect`,
    /// `server namespace disconnect` or `transport close`. Nothing can be sent from it any more.
    pub fn on_disconnect(
        mut self,
        f: impl Fn(&mut Socket<'_>, &str) + Send + Sync + 'static,
    ) -> Namespace {
        self.disconnect = Some(Arc::new(f));
        self
    }

    /// Handles every event without a handler of its own.
    pub fn on_any(
        mut self,
        f: impl Fn(&mut Socket<'_>, Event) + Send + Sync + 'static,
    ) -> Namespace {
        self.any = Some(Arc::new(f));
        self
    }
}

/// What a socket handle asked for, applied after the handler returned.
enum Op {
    Emit {
        name: Option<String>,
        data: Data,
        reply: Option<OnReply>,
    },
    Ack {
        id: u64,
        data: Data,
    },
    Disconnect,
    CloseTransport,
    Raw(String),
    After(Duration, Later),
    Restart(Duration),
}

/// One socket (a client in one namespace), as a handler sees it.
pub struct Socket<'a> {
    ns: &'a str,
    sid: &'a str,
    handshake: &'a Handshake,
    auth: Option<&'a Value>,
    ops: Vec<Op>,
}

impl fmt::Debug for Socket<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Socket")
            .field("ns", &self.ns)
            .field("sid", &self.sid)
            .finish_non_exhaustive()
    }
}

impl Socket<'_> {
    /// The namespace.
    pub fn namespace(&self) -> &str {
        self.ns
    }

    /// The socket id (in `on_connect` too: the id it gets when accepted).
    pub fn id(&self) -> &str {
        self.sid
    }

    /// A query parameter of the client's first request.
    pub fn query(&self, key: &str) -> Option<&str> {
        self.handshake
            .query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// The query of the client's first request.
    pub fn query_pairs(&self) -> &[(String, String)] {
        &self.handshake.query
    }

    /// A header of the client's first request (any case).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.handshake.header(&name.to_ascii_lowercase())
    }

    /// The CONNECT payload (socket.io `auth`).
    pub fn auth(&self) -> Option<&Value> {
        self.auth
    }

    /// Emits `event` to this socket.
    pub fn emit(&mut self, event: impl Into<String>, data: Data) {
        self.ops.push(Op::Emit {
            name: Some(event.into()),
            data,
            reply: None,
        });
    }

    /// Emits an event whose arguments start at the name's place: socket.io's `emit(buffer)`,
    /// which clients receive as an event without a string name.
    pub fn emit_unnamed(&mut self, data: Data) {
        self.ops.push(Op::Emit {
            name: None,
            data,
            reply: None,
        });
    }

    /// Emits `event` with an ack; `on_reply` runs with the client's answer.
    pub fn emit_with_ack(
        &mut self,
        event: impl Into<String>,
        data: Data,
        on_reply: impl for<'b> FnOnce(&mut Socket<'b>, Data) + Send + 'static,
    ) {
        self.ops.push(Op::Emit {
            name: Some(event.into()),
            data,
            reply: Some(Box::new(on_reply)),
        });
    }

    /// Answers `event`'s ack (nothing when the client did not ask for one; only the first answer
    /// is sent).
    pub fn ack(&mut self, event: &Event, data: Data) {
        if let Some(id) = event.ack {
            self.ops.push(Op::Ack { id, data });
        }
    }

    /// Disconnects the namespace (DISCONNECT); the engine.io connection stays.
    pub fn disconnect(&mut self) {
        self.ops.push(Op::Disconnect);
    }

    /// Closes the engine.io connection (every namespace of the client).
    pub fn close_transport(&mut self) {
        self.ops.push(Op::CloseTransport);
    }

    /// Sends `text` as one engine.io message, as is (a malformed packet, say).
    pub fn send_raw(&mut self, text: impl Into<String>) {
        self.ops.push(Op::Raw(text.into()));
    }

    /// Runs `f` with this socket after `delay`, if this socket (not a later one of the same
    /// client in the namespace) is still connected then.
    pub fn after(
        &mut self,
        delay: Duration,
        f: impl for<'b> FnOnce(&mut Socket<'b>) + Send + 'static,
    ) {
        self.ops.push(Op::After(delay, Box::new(f)));
    }

    /// Takes the whole server down for `downtime` (every session closes; new connections are
    /// accepted and dropped at once), then it serves again on the same port.
    pub fn restart_server(&mut self, downtime: Duration) {
        self.ops.push(Op::Restart(downtime));
    }
}

/// The socket.io behaviour of one fake server: its namespaces.
#[derive(Debug, Clone, Default)]
pub(super) struct Spec {
    pub namespaces: HashMap<String, Namespace>,
}

/// Delayed work, fed back by the session.
pub(super) enum Timer {
    /// A delayed CONNECT decision.
    Connect { ns: String, auth: Option<Value> },
    /// [`Socket::after`] of socket `sid`.
    Later { ns: String, sid: String, f: Later },
    /// `Server::broadcast` (`sid: None`) or `Server::emit_to`.
    Emit {
        ns: String,
        sid: Option<String>,
        event: String,
        data: Data,
    },
}

impl fmt::Debug for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Timer::Connect { ns, .. } => write!(f, "Connect({ns})"),
            Timer::Later { ns, sid, .. } => write!(f, "Later({ns}, {sid})"),
            Timer::Emit { ns, event, .. } => write!(f, "Emit({ns}, {event})"),
        }
    }
}

/// One socket.io packet.
#[derive(Debug)]
struct Packet {
    kind: u8,
    ns: String,
    id: Option<u64>,
    data: Option<Value>,
    /// Binary packets: how many attachments follow, and those received so far.
    expected: usize,
    attachments: Vec<Bytes>,
}

impl Packet {
    fn parse(text: &str) -> Option<Packet> {
        let kind = text.chars().next()?.to_digit(10)? as u8;
        if kind > BINARY_ACK {
            return None;
        }
        let mut rest = &text[1..];
        let mut expected = 0;
        if kind == BINARY_EVENT || kind == BINARY_ACK {
            let (count, after) = rest.split_once('-')?;
            expected = count.parse().ok()?;
            rest = after;
        }
        let mut ns = "/".to_owned();
        if rest.starts_with('/') {
            let (name, after) = rest.split_once(',').unwrap_or((rest, ""));
            ns = name.to_owned();
            rest = after;
        }
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        let id = if digits > 0 {
            Some(rest[..digits].parse().ok()?)
        } else {
            None
        };
        rest = &rest[digits..];
        let data = if rest.is_empty() {
            None
        } else {
            Some(serde_json::from_str(rest).ok()?)
        };
        Some(Packet {
            kind,
            ns,
            id,
            data,
            expected,
            attachments: Vec::new(),
        })
    }

    /// socket.io-parser's `isPayloadValid`.
    fn valid(&self) -> bool {
        match (self.kind, &self.data) {
            (CONNECT, None | Some(Value::Object(_))) => true,
            (DISCONNECT, None) => true,
            (EVENT | BINARY_EVENT, Some(Value::Array(items))) => match items.first() {
                Some(Value::Number(_)) => true,
                Some(Value::String(name)) => !RESERVED.contains(&name.as_str()),
                _ => false,
            },
            (ACK | BINARY_ACK, Some(Value::Array(_))) => true,
            _ => false,
        }
    }

    /// The arguments with binary checked; `None` for an illegal placeholder.
    fn args(&mut self) -> Option<Data> {
        let values = match self.data.take() {
            Some(Value::Array(values)) => values,
            _ => Vec::new(),
        };
        Data::received(values, std::mem::take(&mut self.attachments))
    }

    /// The engine.io messages of a packet (attachments after the text).
    fn encode(
        kind: u8,
        ns: &str,
        id: Option<u64>,
        data: Option<Value>,
        attachments: Vec<Bytes>,
    ) -> Vec<Message> {
        let mut text = kind.to_string();
        if !attachments.is_empty() {
            text.push_str(&format!("{}-", attachments.len()));
        }
        if ns != "/" {
            text.push_str(ns);
            text.push(',');
        }
        if let Some(id) = id {
            text.push_str(&id.to_string());
        }
        if let Some(data) = data {
            text.push_str(&data.to_string());
        }
        let mut out = vec![Message::Text(text)];
        out.extend(attachments.into_iter().map(Message::Binary));
        out
    }
}

static NEXT_SID: AtomicU64 = AtomicU64::new(1);

/// A connected namespace of this session.
struct Joined {
    sid: String,
    auth: Option<Value>,
    /// Ack ids answered already (socket.io's ack function works once).
    answered: HashSet<u64>,
}

/// The socket.io state of one engine.io session.
pub(super) struct SocketIo {
    spec: Arc<Spec>,
    /// Connected namespaces.
    joined: HashMap<String, Joined>,
    /// Namespaces whose CONNECT waits for its delay.
    pending: HashSet<String>,
    /// A binary packet waiting for its attachments.
    partial: Option<Packet>,
    /// Server acks: the next id and the replies awaited (namespace, id) -> (socket id, reply).
    next_ack: u64,
    replies: HashMap<(String, u64), (String, OnReply)>,
}

impl fmt::Debug for SocketIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SocketIo")
            .field("joined", &self.joined.keys().collect::<Vec<_>>())
            .field("pending", &self.pending)
            .finish_non_exhaustive()
    }
}

impl SocketIo {
    pub fn new(spec: Arc<Spec>) -> SocketIo {
        SocketIo {
            spec,
            joined: HashMap::new(),
            pending: HashSet::new(),
            partial: None,
            next_ack: 0,
            replies: HashMap::new(),
        }
    }

    pub fn on_message(&mut self, message: Message, ctx: &mut Ctx<'_>) {
        let packet = match message {
            Message::Text(text) => {
                if self.partial.is_some() {
                    return ctx.act(Action::Close); // a text packet amid attachments
                }
                let Some(packet) = Packet::parse(&text) else {
                    return ctx.act(Action::Close);
                };
                if packet.expected > 0 {
                    self.partial = Some(packet);
                    return;
                }
                packet
            }
            Message::Binary(bytes) => {
                let Some(mut packet) = self.partial.take() else {
                    return ctx.act(Action::Close);
                };
                packet.attachments.push(bytes);
                if packet.attachments.len() < packet.expected {
                    self.partial = Some(packet);
                    return;
                }
                packet
            }
        };
        self.dispatch(packet, ctx);
    }

    pub fn on_timer(&mut self, timer: Timer, ctx: &mut Ctx<'_>) {
        match timer {
            Timer::Connect { ns, auth } => {
                if self.pending.remove(&ns) {
                    self.decide(&ns, auth, ctx);
                }
            }
            Timer::Later { ns, sid, f } => {
                if self.joined.get(&ns).is_some_and(|j| j.sid == sid) {
                    self.with_socket(&ns, ctx, f);
                }
            }
            Timer::Emit {
                ns,
                sid,
                event,
                data,
            } => {
                let target = self
                    .joined
                    .get(&ns)
                    .is_some_and(|j| sid.as_ref().is_none_or(|sid| *sid == j.sid));
                if target {
                    self.with_socket(&ns, ctx, |s| s.emit(event, data));
                }
            }
        }
    }

    /// socket.io's `Client._onpacket`: an invalid payload, or a packet for a namespace that is
    /// not connected (also one still waiting for its CONNECT answer), closes the connection.
    fn dispatch(&mut self, mut packet: Packet, ctx: &mut Ctx<'_>) {
        let ns = packet.ns.clone();
        if !packet.valid() {
            return ctx.act(Action::Close);
        }
        if packet.kind == CONNECT {
            // socket.io: a CONNECT for a namespace the client joined is an invalid state; one
            // while the namespace waits for its delay is refused here too (a simplification)
            if self.pending.contains(&ns) || self.joined.contains_key(&ns) {
                return ctx.act(Action::Close);
            }
            let auth = packet.data.take();
            return self.connect(&ns, auth, ctx);
        }
        if !self.joined.contains_key(&ns) {
            return ctx.act(Action::Close);
        }
        let id = packet.id;
        let Some(mut data) = packet.args() else {
            return ctx.act(Action::Close); // illegal attachments
        };
        match packet.kind {
            DISCONNECT => self.left(&ns, "client namespace disconnect", ctx),
            EVENT | BINARY_EVENT => {
                let name = match data.values.remove(0) {
                    Value::String(name) => name,
                    other => other.to_string(),
                };
                let event = Event {
                    name,
                    data,
                    ack: id,
                };
                let handler = self.spec.namespaces.get(&ns).and_then(|namespace| {
                    namespace
                        .events
                        .get(&event.name)
                        .or(namespace.any.as_ref())
                        .cloned()
                });
                if let Some(handler) = handler {
                    self.with_socket(&ns, ctx, |s| handler(s, event));
                }
            }
            _ => {
                // ACK | BINARY_ACK
                let Some((sid, reply)) = id.and_then(|id| self.replies.remove(&(ns.clone(), id)))
                else {
                    return;
                };
                if self.joined.get(&ns).is_some_and(|j| j.sid == sid) {
                    self.with_socket(&ns, ctx, |s| reply(s, data));
                }
            }
        }
    }

    fn connect(&mut self, ns: &str, auth: Option<Value>, ctx: &mut Ctx<'_>) {
        let Some(namespace) = self.spec.namespaces.get(ns) else {
            return send(
                ctx,
                Packet::encode(
                    CONNECT_ERROR,
                    ns,
                    None,
                    Some(json!({ "message": "Invalid namespace" })),
                    Vec::new(),
                ),
            );
        };
        let delay = namespace.connect_delay;
        if delay.is_zero() {
            self.decide(ns, auth, ctx);
        } else {
            self.pending.insert(ns.to_owned());
            ctx.act(Action::After(
                delay,
                Timer::Connect {
                    ns: ns.to_owned(),
                    auth,
                },
            ));
        }
    }

    /// Runs the namespace's `on_connect` and answers CONNECT or CONNECT_ERROR.
    fn decide(&mut self, ns: &str, auth: Option<Value>, ctx: &mut Ctx<'_>) {
        let spec = Arc::clone(&self.spec);
        let Some(namespace) = spec.namespaces.get(ns) else {
            return;
        };
        let sid = format!("fake-sio-{}", NEXT_SID.fetch_add(1, Ordering::Relaxed));
        let mut socket = Socket {
            ns,
            sid: &sid,
            handshake: ctx.handshake,
            auth: auth.as_ref(),
            ops: Vec::new(),
        };
        let decision = match &namespace.connect {
            Some(f) => match guarded(ns, || f(&mut socket)) {
                Some(decision) => decision,
                None => return ctx.act(Action::Close),
            },
            None => Connect::Accept,
        };
        let ops = socket.ops;
        match decision {
            Connect::Reject { message, data } => {
                let mut body = json!({ "message": message });
                if let Some(data) = data {
                    body["data"] = data;
                }
                send(
                    ctx,
                    Packet::encode(CONNECT_ERROR, ns, None, Some(body), Vec::new()),
                );
            }
            Connect::Accept => {
                send(
                    ctx,
                    Packet::encode(CONNECT, ns, None, Some(json!({ "sid": sid })), Vec::new()),
                );
                self.joined.insert(
                    ns.to_owned(),
                    Joined {
                        sid,
                        auth,
                        answered: HashSet::new(),
                    },
                );
                self.apply(ns, ops, ctx);
            }
        }
    }

    /// Calls `f` with the socket of `ns` (if it is connected), then applies what it asked for.
    fn with_socket(&mut self, ns: &str, ctx: &mut Ctx<'_>, f: impl FnOnce(&mut Socket<'_>)) {
        let Some(joined) = self.joined.get(ns) else {
            return;
        };
        let mut socket = Socket {
            ns,
            sid: &joined.sid,
            handshake: ctx.handshake,
            auth: joined.auth.as_ref(),
            ops: Vec::new(),
        };
        if guarded(ns, || f(&mut socket)).is_none() {
            return ctx.act(Action::Close);
        }
        let ops = socket.ops;
        self.apply(ns, ops, ctx);
    }

    /// The socket of `ns` is gone: `on_disconnect` runs (what it sends is dropped) and its
    /// pending acks are forgotten.
    fn left(&mut self, ns: &str, reason: &str, ctx: &mut Ctx<'_>) {
        let Some(joined) = self.joined.remove(ns) else {
            return;
        };
        self.replies
            .retain(|(n, _), (sid, _)| !(n == ns && *sid == joined.sid));
        let handler = self
            .spec
            .namespaces
            .get(ns)
            .and_then(|n| n.disconnect.clone());
        if let Some(handler) = handler {
            let mut socket = Socket {
                ns,
                sid: &joined.sid,
                handshake: ctx.handshake,
                auth: joined.auth.as_ref(),
                ops: Vec::new(),
            };
            guarded(ns, || handler(&mut socket, reason));
        }
    }

    /// The engine.io connection ended: every socket leaves with `transport close`.
    pub fn on_close(&mut self, ctx: &mut Ctx<'_>) {
        let namespaces: Vec<String> = self.joined.keys().cloned().collect();
        for ns in namespaces {
            self.left(&ns, "transport close", ctx);
        }
    }

    fn apply(&mut self, ns: &str, ops: Vec<Op>, ctx: &mut Ctx<'_>) {
        for op in ops {
            match op {
                Op::Emit { name, data, reply } => {
                    let Some(sid) = self.joined.get(ns).map(|j| j.sid.clone()) else {
                        continue; // left meanwhile
                    };
                    let id = reply.map(|reply| {
                        let id = self.next_ack;
                        self.next_ack += 1;
                        self.replies.insert((ns.to_owned(), id), (sid, reply));
                        id
                    });
                    let (args, attachments) = data.deconstruct();
                    let mut values: Vec<Value> = name.into_iter().map(Value::String).collect();
                    values.extend(args);
                    let kind = if attachments.is_empty() {
                        EVENT
                    } else {
                        BINARY_EVENT
                    };
                    send(
                        ctx,
                        Packet::encode(kind, ns, id, Some(Value::Array(values)), attachments),
                    );
                }
                Op::Ack { id, data } => {
                    let first = self
                        .joined
                        .get_mut(ns)
                        .is_some_and(|j| j.answered.insert(id));
                    if !first {
                        continue;
                    }
                    let (values, attachments) = data.deconstruct();
                    let kind = if attachments.is_empty() {
                        ACK
                    } else {
                        BINARY_ACK
                    };
                    send(
                        ctx,
                        Packet::encode(kind, ns, Some(id), Some(Value::Array(values)), attachments),
                    );
                }
                Op::Disconnect => {
                    if self.joined.contains_key(ns) {
                        send(ctx, Packet::encode(DISCONNECT, ns, None, None, Vec::new()));
                        self.left(ns, "server namespace disconnect", ctx);
                    }
                }
                Op::CloseTransport => ctx.act(Action::Close),
                Op::Raw(text) => ctx.send(Message::Text(text)),
                Op::After(delay, f) => {
                    if let Some(joined) = self.joined.get(ns) {
                        ctx.act(Action::After(
                            delay,
                            Timer::Later {
                                ns: ns.to_owned(),
                                sid: joined.sid.clone(),
                                f,
                            },
                        ));
                    }
                }
                Op::Restart(downtime) => ctx.act(Action::Restart(downtime)),
            }
        }
    }
}

/// Runs a user handler; a panic is printed (the test sees it on stderr) and gives `None`.
fn guarded<T>(ns: &str, f: impl FnOnce() -> T) -> Option<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(value) => Some(value),
        Err(panic) => {
            let text = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_owned())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            eprintln!("socketio-next-testing: a handler of namespace {ns} panicked: {text}");
            None
        }
    }
}

fn send(ctx: &mut Ctx<'_>, messages: Vec<Message>) {
    for message in messages {
        ctx.send(message);
    }
}
