//! The socket.io v4 app: packet codec, namespaces, acks, binary attachments, and the fixtures of
//! `ci/socket-io.js`, `ci/socket-io-auth.js` and `ci/socket-io-restart.js`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};

use super::engine::{Action, Ctx, Message};

/// Which `ci/` server the app behaves like.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fixture {
    /// `ci/socket-io.js`
    Main,
    /// `ci/socket-io-auth.js`
    Auth,
    /// `ci/socket-io-restart.js`
    Restart,
}

/// Delayed work, fed back by the session.
#[derive(Debug)]
pub(super) enum Timer {
    /// The `/slow` namespace middleware passes.
    Accept { ns: String },
    /// The `/reject` namespace middleware fails.
    Reject { ns: String },
    /// `ack_late` answers.
    AckLate { ns: String, id: u64 },
}

/// The middleware delay of `/slow` and `/reject`.
const MIDDLEWARE_DELAY: Duration = Duration::from_millis(200);
const ACK_LATE_DELAY: Duration = Duration::from_millis(500);

const CONNECT: u8 = 0;
const DISCONNECT: u8 = 1;
const EVENT: u8 = 2;
const ACK: u8 = 3;
const CONNECT_ERROR: u8 = 4;
const BINARY_EVENT: u8 = 5;
const BINARY_ACK: u8 = 6;

/// Arguments: JSON values where binary stands as a reference into `pool`.
///
/// Received placeholders are resolved into references (checked against the attachments, as
/// socket.io-parser's `reconstructPacket` does); sent packets are deconstructed again with
/// their own depth-first numbering. So the client's numbering is never echoed back as is, and
/// attachments no value refers to are not sent.
#[derive(Debug, Clone, Default)]
struct Args {
    values: Vec<Value>,
    pool: Vec<Bytes>,
}

/// The key of a binary reference (`{BIN: index into the pool}`); not valid in client JSON.
const BIN: &str = "\u{0}bin";

impl Args {
    fn text(values: Vec<Value>) -> Args {
        Args {
            values,
            pool: Vec::new(),
        }
    }

    /// A binary value to put into `values`.
    fn bin(&mut self, bytes: &'static [u8]) -> Value {
        self.pool.push(Bytes::from_static(bytes));
        json!({ BIN: self.pool.len() - 1 })
    }

    /// Resolves placeholders against `attachments`; `None` for an illegal one.
    fn received(values: Vec<Value>, attachments: Vec<Bytes>) -> Option<Args> {
        fn resolve(value: Value, count: usize) -> Option<Value> {
            Some(match value {
                Value::Object(map) if map.get("_placeholder") == Some(&Value::Bool(true)) => {
                    let num = map.get("num")?.as_u64()? as usize;
                    if num >= count {
                        return None;
                    }
                    json!({ BIN: num })
                }
                Value::Object(map) => Value::Object(
                    map.into_iter()
                        .map(|(k, v)| Some((k, resolve(v, count)?)))
                        .collect::<Option<_>>()?,
                ),
                Value::Array(items) => Value::Array(
                    items
                        .into_iter()
                        .map(|v| resolve(v, count))
                        .collect::<Option<_>>()?,
                ),
                other => other,
            })
        }
        let count = attachments.len();
        let values = values
            .into_iter()
            .map(|v| resolve(v, count))
            .collect::<Option<_>>()?;
        Some(Args {
            values,
            pool: attachments,
        })
    }

    /// The values with placeholders numbered depth-first, and the attachments in that order.
    fn deconstruct(self) -> (Vec<Value>, Vec<Bytes>) {
        fn walk(value: Value, pool: &[Bytes], out: &mut Vec<Bytes>) -> Value {
            match value {
                Value::Object(map) if map.len() == 1 && map.contains_key(BIN) => {
                    let index = map[BIN].as_u64().unwrap() as usize;
                    out.push(pool[index].clone());
                    json!({ "_placeholder": true, "num": out.len() - 1 })
                }
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
            .map(|v| walk(v, &self.pool, &mut out))
            .collect();
        (values, out)
    }
}

/// Event names socket.io-parser refuses (`RESERVED_EVENTS`).
const RESERVED: &[&str] = &[
    "connect",
    "connect_error",
    "disconnect",
    "disconnecting",
    "newListener",
    "removeListener",
];

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

    /// The engine.io messages of a packet with `data` (attachments after the text).
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

    /// The arguments with binary resolved; `None` for an illegal placeholder.
    fn args(&mut self) -> Option<Args> {
        let values = match self.data.take() {
            Some(Value::Array(values)) => values,
            _ => Vec::new(),
        };
        Args::received(values, std::mem::take(&mut self.attachments))
    }
}

static NEXT_SID: AtomicU64 = AtomicU64::new(1);

/// The socket.io state of one engine.io session.
#[derive(Debug)]
pub(super) struct SocketIo {
    fixture: Fixture,
    /// Connected namespaces and their socket ids.
    namespaces: HashMap<String, String>,
    /// Namespaces whose middleware (`/slow`, `/reject`) has not answered yet.
    pending: HashSet<String>,
    /// A binary packet waiting for its attachments.
    partial: Option<Packet>,
    /// Server acks: the next id and the ones awaiting the client's answer (`ask_me`).
    next_ack: u64,
    asked: HashSet<(String, u64)>,
}

impl SocketIo {
    pub fn new(fixture: Fixture) -> SocketIo {
        SocketIo {
            fixture,
            namespaces: HashMap::new(),
            pending: HashSet::new(),
            partial: None,
            next_ack: 0,
            asked: HashSet::new(),
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
            Timer::Accept { ns } => {
                if self.pending.remove(&ns) {
                    self.accept(&ns, None, ctx);
                }
            }
            Timer::Reject { ns } => {
                if self.pending.remove(&ns) {
                    send(
                        ctx,
                        Packet::encode(
                            CONNECT_ERROR,
                            &ns,
                            None,
                            Some(
                                json!({ "message": "not authorized", "data": { "reason": "test" } }),
                            ),
                            Vec::new(),
                        ),
                    );
                }
            }
            Timer::AckLate { ns, id } => {
                if self.namespaces.contains_key(&ns) {
                    ack(ctx, &ns, id, Args::text(vec![json!("late")]));
                }
            }
        }
    }

    /// socket.io's `Client._onpacket`: an invalid payload, or a packet for a namespace that is
    /// not connected (also one still in its middleware), closes the connection.
    fn dispatch(&mut self, mut packet: Packet, ctx: &mut Ctx<'_>) {
        let ns = packet.ns.clone();
        if !packet.valid() {
            return ctx.act(Action::Close);
        }
        if packet.kind == CONNECT {
            if self.pending.contains(&ns) {
                return ctx.act(Action::Close);
            }
            return self.connect(&ns, packet.data.take(), ctx);
        }
        if !self.namespaces.contains_key(&ns) {
            return ctx.act(Action::Close);
        }
        let id = packet.id;
        let Some(mut args) = packet.args() else {
            return ctx.act(Action::Close); // illegal attachments
        };
        match packet.kind {
            DISCONNECT => {
                self.namespaces.remove(&ns);
            }
            EVENT | BINARY_EVENT => {
                let name = args.values.remove(0);
                let name = match &name {
                    Value::String(name) => name.clone(),
                    other => other.to_string(),
                };
                self.event(&ns, &name, args, id, ctx);
            }
            _ => {
                // ACK | BINARY_ACK
                if id.is_some_and(|id| self.asked.remove(&(ns.clone(), id))) {
                    emit(ctx, &ns, "answered", args, None);
                }
            }
        }
    }

    fn connect(&mut self, ns: &str, auth: Option<Value>, ctx: &mut Ctx<'_>) {
        let known = match self.fixture {
            Fixture::Main => match ns {
                "/" | "/admin" => true,
                "/slow" => {
                    self.pending.insert(ns.into());
                    return ctx.act(Action::After(
                        MIDDLEWARE_DELAY,
                        Timer::Accept { ns: ns.into() },
                    ));
                }
                "/reject" => {
                    self.pending.insert(ns.into());
                    return ctx.act(Action::After(
                        MIDDLEWARE_DELAY,
                        Timer::Reject { ns: ns.into() },
                    ));
                }
                _ => false,
            },
            Fixture::Auth => matches!(ns, "/" | "/admin"),
            Fixture::Restart => ns == "/",
        };
        if known {
            self.accept(ns, auth, ctx);
        } else {
            send(
                ctx,
                Packet::encode(
                    CONNECT_ERROR,
                    ns,
                    None,
                    Some(json!({ "message": "Invalid namespace" })),
                    Vec::new(),
                ),
            );
        }
    }

    fn accept(&mut self, ns: &str, auth: Option<Value>, ctx: &mut Ctx<'_>) {
        let sid = format!("fake-sio-{}", NEXT_SID.fetch_add(1, Ordering::Relaxed));
        send(
            ctx,
            Packet::encode(CONNECT, ns, None, Some(json!({ "sid": sid })), Vec::new()),
        );
        self.namespaces.insert(ns.to_owned(), sid);
        match self.fixture {
            Fixture::Main => {
                emit(
                    ctx,
                    ns,
                    "Hello from the message event!",
                    Args::default(),
                    None,
                );
                emit(
                    ctx,
                    ns,
                    "test",
                    Args::text(vec![json!("Hello from the test event!")]),
                    None,
                );
                // `emit(Buffer)`: the event "name" is the attachment
                emit_values(ctx, ns, &[], binary(&[4, 5, 6]), None);
                emit(ctx, ns, "test", binary(&[1, 2, 3]), None);
                emit(
                    ctx,
                    ns,
                    "This is the first argument",
                    Args::text(vec![
                        json!("This is the second argument"),
                        json!({ "argCount": 3 }),
                    ]),
                    None,
                );
                emit(
                    ctx,
                    ns,
                    "on_abc_event",
                    Args::text(vec![json!(""), json!({ "abc": 0, "some_other": "value" })]),
                    None,
                );
            }
            Fixture::Auth => {
                let ok = auth.as_ref().and_then(|a| a.get("password")) == Some(&json!("123"));
                let result = if ok { "success" } else { "failed" };
                emit(ctx, ns, "auth", Args::text(vec![json!(result)]), None);
            }
            Fixture::Restart => {
                let message = ctx
                    .handshake
                    .header("message_back")
                    .unwrap_or("test")
                    .to_owned();
                emit(ctx, ns, "message", Args::text(vec![json!(message)]), None);
            }
        }
    }

    fn event(&mut self, ns: &str, name: &str, mut args: Args, id: Option<u64>, ctx: &mut Ctx<'_>) {
        if self.fixture == Fixture::Restart {
            if name == "restart_server" {
                ctx.act(Action::Restart);
            }
            return;
        }
        if self.fixture != Fixture::Main {
            return;
        }
        // `client.emit('x-received', data)`: the first argument only
        let first = |args: &mut Args| {
            args.values.truncate(1);
            std::mem::take(args)
        };
        match name {
            "test" => {
                emit(ctx, ns, "test-received", first(&mut args), None);
                if let Some(id) = id {
                    ack(ctx, ns, id, Args::text(vec![json!("woot")]));
                }
            }
            "message" => emit(ctx, ns, "message-received", first(&mut args), None),
            "binary" => {
                emit(ctx, ns, "binary-received", first(&mut args), None);
                if let Some(id) = id {
                    ack(ctx, ns, id, binary(&[1, 2, 3]));
                }
            }
            "close_transport" => ctx.act(Action::Close),
            "binary_delimiter" => emit(ctx, ns, "binary-delimiter", binary(&[0x1e, 1, 0x1e]), None),
            "binary_with_args" => {
                let mut args = Args::text(vec![json!("extra")]);
                let nine = args.bin(&[9]);
                args.values.push(nine);
                emit(ctx, ns, "binary-args", args, None);
            }
            "echo" => match id {
                Some(id) => ack(ctx, ns, id, args),
                None => emit(ctx, ns, "echo-back", args, None),
            },
            "ack_late" => {
                if let Some(id) = id {
                    ctx.act(Action::After(
                        ACK_LATE_DELAY,
                        Timer::AckLate { ns: ns.into(), id },
                    ));
                }
            }
            "handshake_query" => {
                if let Some(id) = id {
                    let query: serde_json::Map<String, Value> = ctx
                        .handshake
                        .query
                        .iter()
                        .map(|(k, v)| (k.clone(), json!(v)))
                        .collect();
                    ack(ctx, ns, id, Args::text(vec![Value::Object(query)]));
                }
            }
            // a packet socket.io-parser rejects
            "invalid_packet" => ctx.send(Message::Text(r#"2[true,"foo"]"#.into())),
            "ask_me" => {
                let id = self.next_ack;
                self.next_ack += 1;
                self.asked.insert((ns.to_owned(), id));
                emit(ctx, ns, "question", args, Some(id));
            }
            "binary_mixed" => {
                let mut args = Args::default();
                let (a, b) = (args.bin(&[1]), args.bin(&[2, 3]));
                args.values = vec![json!({ "a": a, "b": [b] }), json!("tail")];
                emit(ctx, ns, "binary-mixed", args, None);
            }
            // a namespace disconnect; the engine.io connection stays
            "server_disconnect" => {
                self.namespaces.remove(ns);
                send(ctx, Packet::encode(DISCONNECT, ns, None, None, Vec::new()));
            }
            _ => {}
        }
    }
}

/// One binary argument.
fn binary(bytes: &'static [u8]) -> Args {
    let mut args = Args::default();
    let value = args.bin(bytes);
    args.values.push(value);
    args
}

fn send(ctx: &mut Ctx<'_>, messages: Vec<Message>) {
    for message in messages {
        ctx.send(message);
    }
}

/// An event `name` with `args`; with `id` the server waits for the client's ack.
fn emit(ctx: &mut Ctx<'_>, ns: &str, name: &str, args: Args, id: Option<u64>) {
    emit_values(ctx, ns, &[name], args, id);
}

/// An event whose leading values are `names`: none for `emit(Buffer)`, where the attachment
/// stands in the name's place.
fn emit_values(ctx: &mut Ctx<'_>, ns: &str, names: &[&str], args: Args, id: Option<u64>) {
    let (args, attachments) = args.deconstruct();
    let mut values: Vec<Value> = names.iter().map(|n| json!(n)).collect();
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

fn ack(ctx: &mut Ctx<'_>, ns: &str, id: u64, args: Args) {
    let (values, attachments) = args.deconstruct();
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
