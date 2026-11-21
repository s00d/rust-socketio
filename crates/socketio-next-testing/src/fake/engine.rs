//! One engine.io v4 session: an actor that owns the transport, the heartbeat and the app.
//!
//! Requests and websocket readers talk to it through [`SessionHandle`] / its mailbox; nothing
//! else touches its state. It tells the server actor when it ends.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use futures_util::SinkExt;
use futures_util::stream::{SplitSink, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use super::http::{MAX_PAYLOAD, ServerHandle, header};
use super::{App, echo, socketio};

pub(super) type Ws = WebSocketStream<TcpStream>;

/// How long a closed polling session waits for the poll that carries its close packet.
const CLOSE_LINGER: Duration = Duration::from_secs(1);

/// The first request of the session (socket.io's `handshake` / `request`).
#[derive(Debug, Clone)]
pub(super) struct Handshake {
    pub query: Vec<(String, String)>,
    /// Lower-case names.
    pub headers: Vec<(String, String)>,
}

impl Handshake {
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }
}

/// An engine.io message (packet type 4) or a binary message.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Message {
    Text(String),
    Binary(Bytes),
}

/// What an app asks the session to do.
#[derive(Debug)]
pub(super) enum Action {
    Send(Message),
    /// Close the engine.io connection.
    Close,
    /// Feed the timer back to the app after the delay. Only the socket.io app has timers;
    /// ponytail: a concrete type, make it the app's own when a second app needs one.
    After(Duration, socketio::Timer),
    /// Take the whole server down for a while (`restart_server`).
    Restart(Duration),
}

/// The app's view of the session during one callback.
#[derive(Debug)]
pub(super) struct Ctx<'a> {
    pub handshake: &'a Handshake,
    actions: Vec<Action>,
}

impl Ctx<'_> {
    pub fn send(&mut self, message: Message) {
        self.actions.push(Action::Send(message));
    }

    pub fn act(&mut self, action: Action) {
        self.actions.push(action);
    }
}

/// The app a session runs.
#[derive(Debug)]
enum AppState {
    SocketIo(Box<socketio::SocketIo>),
    Echo(echo::Echo),
}

impl AppState {
    fn new(app: &App, polling_only: bool) -> AppState {
        match app {
            App::EngineEcho => AppState::Echo(echo::Echo { polling_only }),
            App::SocketIo(spec) => AppState::SocketIo(Box::new(socketio::SocketIo::new(
                std::sync::Arc::clone(spec),
            ))),
        }
    }

    fn on_open(&mut self, ctx: &mut Ctx<'_>) {
        if let AppState::Echo(app) = self {
            app.on_open(ctx);
        }
    }

    fn on_message(&mut self, message: Message, ctx: &mut Ctx<'_>) {
        match self {
            AppState::SocketIo(app) => app.on_message(message, ctx),
            AppState::Echo(app) => app.on_message(message, ctx),
        }
    }

    fn on_close(&mut self, ctx: &mut Ctx<'_>) {
        if let AppState::SocketIo(app) = self {
            app.on_close(ctx);
        }
    }

    fn on_timer(&mut self, timer: socketio::Timer, ctx: &mut Ctx<'_>) {
        if let AppState::SocketIo(app) = self {
            app.on_timer(timer, ctx);
        }
    }
}

/// The answer to a polling POST.
#[derive(Debug, PartialEq)]
pub(super) enum Posted {
    Ok,
    /// The session runs on a websocket now, or ended: a 400.
    Refused,
}

/// Talks to a session actor.
#[derive(Debug, Clone)]
pub(super) struct SessionHandle(mpsc::UnboundedSender<Cmd>);

impl SessionHandle {
    /// A long poll: the encoded payload, `None` if the session refused it or ended.
    pub async fn poll(&self) -> Option<String> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Cmd::Poll(tx)).ok()?;
        rx.await.ok()
    }

    /// A polling POST body.
    pub async fn post(&self, body: Vec<u8>) -> Posted {
        let (tx, rx) = oneshot::channel();
        if self.0.send(Cmd::Post(body, tx)).is_err() {
            return Posted::Refused;
        }
        rx.await.unwrap_or(Posted::Refused)
    }

    /// A websocket that wants to take over this polling session.
    pub fn probe(&self, ws: Ws) {
        let _ = self.0.send(Cmd::Probe(Box::new(ws)));
    }

    /// Feeds `timer` to the app (a broadcast from the server handle).
    pub fn timer(&self, timer: socketio::Timer) {
        let _ = self.0.send(Cmd::Timer(timer));
    }

    /// Server-side close.
    pub fn close(&self) {
        let _ = self.0.send(Cmd::Close);
    }
}

#[derive(Debug)]
enum Cmd {
    Poll(oneshot::Sender<String>),
    Post(Vec<u8>, oneshot::Sender<Posted>),
    Probe(Box<Ws>),
    Frame(u64, Frame),
    Timer(socketio::Timer),
    Close,
}

#[derive(Debug)]
enum Frame {
    Text(String),
    Binary(Bytes),
    Closed,
}

/// An engine.io packet on the wire: a text packet with its type digit, or a binary message.
#[derive(Debug, Clone)]
enum Packet {
    Text(String),
    Binary(Bytes),
}

impl Packet {
    fn message(message: Message) -> Packet {
        match message {
            Message::Text(text) => Packet::Text(format!("4{text}")),
            Message::Binary(bytes) => Packet::Binary(bytes),
        }
    }

    fn polling(&self) -> String {
        match self {
            Packet::Text(text) => text.clone(),
            Packet::Binary(bytes) => format!("b{}", BASE64.encode(bytes)),
        }
    }

    fn websocket(self) -> WsMessage {
        match self {
            Packet::Text(text) => WsMessage::text(text),
            Packet::Binary(bytes) => WsMessage::Binary(bytes),
        }
    }
}

/// A websocket attached to the session: the write half here, the read half in a task that
/// forwards frames to the mailbox.
#[derive(Debug)]
struct Link {
    id: u64,
    sink: SplitSink<Ws, WsMessage>,
    reader: JoinHandle<()>,
}

impl Link {
    async fn send(&mut self, packet: Packet) -> bool {
        self.sink.send(packet.websocket()).await.is_ok()
    }

    /// Closes the socket (engine.io closes a websocket without a close packet).
    async fn close(mut self) {
        let _ = tokio::time::timeout(Duration::from_millis(200), self.sink.close()).await;
        self.reader.abort();
    }
}

/// Where an incoming packet came from.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Source {
    Transport,
    Probe,
}

/// What the actor does after a command.
#[derive(Debug, PartialEq)]
enum Flow {
    Continue,
    /// The server closes: queued packets and a close packet go to the polling client.
    Close,
    /// The peer went away, closed, or broke the protocol.
    Gone,
}

static NEXT_SID: AtomicU64 = AtomicU64::new(1);

/// Spawns a session (the server actor registers the returned handle). With `ws` it runs on that
/// websocket from the start and sends the open packet over it; without, it is a polling session
/// and the open packet is returned for the handshake response.
pub(super) fn spawn(
    server: ServerHandle,
    handshake: Handshake,
    ws: Option<Ws>,
) -> (String, SessionHandle, String) {
    let sid = format!("fake-eio-{}", NEXT_SID.fetch_add(1, Ordering::Relaxed));
    let (tx, rx) = mpsc::unbounded_channel();
    let config = &server.config;
    let upgrades = if ws.is_some() || config.polling_only {
        vec![]
    } else {
        vec!["websocket"]
    };
    let open = format!(
        "0{}",
        serde_json::json!({
            "sid": sid,
            "upgrades": upgrades,
            "pingInterval": config.ping_interval.as_millis() as u64,
            "pingTimeout": config.ping_timeout.as_millis() as u64,
            "maxPayload": MAX_PAYLOAD,
        })
    );
    let mut session = Session {
        sid: sid.clone(),
        app: AppState::new(&config.app, config.polling_only),
        server,
        handshake,
        tx: tx.clone(),
        outbox: VecDeque::new(),
        poll: None,
        ws: None,
        probe: None,
        next_link: 0,
        pong_deadline: None,
    };
    let ws_open = ws.map(|ws| {
        session.ws = Some(session.attach(ws));
        open.clone()
    });
    tokio::spawn(session.run(rx, ws_open));
    (sid, SessionHandle(tx), open)
}

#[derive(Debug)]
struct Session {
    sid: String,
    app: AppState,
    server: ServerHandle,
    handshake: Handshake,
    /// The own mailbox, for timers and websocket readers.
    tx: mpsc::UnboundedSender<Cmd>,
    /// Polling: packets waiting for the next poll.
    outbox: VecDeque<Packet>,
    /// Polling: the pending long poll.
    poll: Option<oneshot::Sender<String>>,
    /// The websocket transport, once upgraded (or from the start).
    ws: Option<Link>,
    /// A websocket probing for the upgrade.
    probe: Option<Link>,
    next_link: u64,
    /// A ping is unanswered until then.
    pong_deadline: Option<Instant>,
}

impl Session {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Cmd>, ws_open: Option<String>) {
        let mut flow = match ws_open {
            Some(open) => self.send(Packet::Text(open)).await,
            None => Flow::Continue,
        };
        if flow == Flow::Continue {
            let mut ctx = Ctx {
                handshake: &self.handshake,
                actions: Vec::new(),
            };
            self.app.on_open(&mut ctx);
            let actions = ctx.actions;
            flow = self.apply(actions).await;
        }

        let interval = self.server.config.ping_interval;
        let mut ping = tokio::time::interval_at(Instant::now() + interval, interval);
        while flow == Flow::Continue {
            let deadline = self.pong_deadline;
            flow = tokio::select! {
                // the session holds its own sender: the mailbox never closes
                Some(cmd) = rx.recv() => self.handle(cmd).await,
                _ = ping.tick() => {
                    if self.pong_deadline.is_none() {
                        self.pong_deadline = Some(Instant::now() + self.server.config.ping_timeout);
                    }
                    self.send(Packet::Text("2".into())).await
                }
                _ = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {
                    Flow::Gone // ping timeout
                }
            };
        }
        self.shutdown(flow == Flow::Close, rx).await;
    }

    /// Ends the session; the registry forgets it only afterwards, so polls still reach it while
    /// it lingers for the close packet.
    async fn shutdown(mut self, graceful: bool, mut rx: mpsc::UnboundedReceiver<Cmd>) {
        // the app learns the end first (socket.io: `disconnect` with `transport close`)
        let mut ctx = Ctx {
            handshake: &self.handshake,
            actions: Vec::new(),
        };
        self.app.on_close(&mut ctx);
        if let Some(probe) = self.probe.take() {
            probe.reader.abort();
        }
        match self.ws.take() {
            Some(link) => link.close().await,
            None if graceful => {
                // engine.io drains the write buffer before the close packet
                self.outbox.push_back(Packet::Text("1".into()));
                let payload = self.payload();
                if let Some(poll) = self.poll.take() {
                    let _ = poll.send(payload);
                } else {
                    let linger = tokio::time::sleep(CLOSE_LINGER);
                    tokio::pin!(linger);
                    loop {
                        tokio::select! {
                            Some(cmd) = rx.recv() => match cmd {
                                Cmd::Poll(poll) => {
                                    let _ = poll.send(payload);
                                    break;
                                }
                                Cmd::Post(_, reply) => {
                                    let _ = reply.send(Posted::Refused);
                                }
                                _ => {}
                            },
                            _ = &mut linger => break,
                        }
                    }
                }
            }
            // the polling transport answers a pending poll with a noop when it closes
            None => {
                if let Some(poll) = self.poll.take() {
                    let _ = poll.send("6".into());
                }
            }
        }
        self.server.ended(&self.sid);
    }

    fn attach(&mut self, ws: Ws) -> Link {
        let id = self.next_link;
        self.next_link += 1;
        let (sink, mut stream) = ws.split();
        let tx = self.tx.clone();
        let reader = tokio::spawn(async move {
            while let Some(Ok(message)) = stream.next().await {
                let frame = match message {
                    WsMessage::Text(text) => Frame::Text(text.to_string()),
                    WsMessage::Binary(bytes) => Frame::Binary(bytes),
                    WsMessage::Close(_) => break,
                    _ => continue,
                };
                if tx.send(Cmd::Frame(id, frame)).is_err() {
                    return;
                }
            }
            let _ = tx.send(Cmd::Frame(id, Frame::Closed));
        });
        Link { id, sink, reader }
    }

    async fn handle(&mut self, cmd: Cmd) -> Flow {
        match cmd {
            Cmd::Poll(poll) => {
                if self.ws.is_some() {
                    return Flow::Continue; // dropped: a 400
                }
                if self.poll.is_some() {
                    return Flow::Gone; // "overlap from client": the new poll gets a 400
                }
                self.poll = Some(poll);
                self.flush_poll();
                Flow::Continue
            }
            Cmd::Post(body, reply) => {
                if self.ws.is_some() {
                    let _ = reply.send(Posted::Refused);
                    return Flow::Continue;
                }
                let _ = reply.send(Posted::Ok);
                let Ok(body) = String::from_utf8(body) else {
                    return Flow::Gone;
                };
                for packet in body.split('\x1e') {
                    let flow = match packet.strip_prefix('b') {
                        Some(b64) => match BASE64.decode(b64) {
                            Ok(bytes) => self.message(Message::Binary(bytes.into())).await,
                            Err(_) => Flow::Gone,
                        },
                        None => self.text(packet, Source::Transport).await,
                    };
                    if flow != Flow::Continue {
                        return flow;
                    }
                }
                Flow::Continue
            }
            Cmd::Probe(ws) => {
                let link = self.attach(*ws);
                if self.ws.is_some() {
                    link.reader.abort();
                } else if let Some(old) = self.probe.replace(link) {
                    old.reader.abort();
                }
                Flow::Continue
            }
            Cmd::Frame(id, frame) => {
                let source = if self.ws.as_ref().is_some_and(|l| l.id == id) {
                    Source::Transport
                } else if self.probe.as_ref().is_some_and(|l| l.id == id) {
                    Source::Probe
                } else {
                    return Flow::Continue;
                };
                match (frame, source) {
                    (Frame::Text(text), _) => self.text(&text, source).await,
                    (Frame::Binary(bytes), Source::Transport) => {
                        self.message(Message::Binary(bytes)).await
                    }
                    (Frame::Closed, Source::Transport) => Flow::Gone,
                    (_, Source::Probe) => {
                        self.drop_probe();
                        Flow::Continue
                    }
                }
            }
            Cmd::Timer(timer) => {
                let mut ctx = Ctx {
                    handshake: &self.handshake,
                    actions: Vec::new(),
                };
                self.app.on_timer(timer, &mut ctx);
                let actions = ctx.actions;
                self.apply(actions).await
            }
            Cmd::Close => Flow::Close,
        }
    }

    /// A failed probe ends only the probe; the session stays on polling.
    fn drop_probe(&mut self) {
        if let Some(probe) = self.probe.take() {
            probe.reader.abort();
        }
    }

    /// A text packet from the client.
    async fn text(&mut self, packet: &str, source: Source) -> Flow {
        let mut chars = packet.chars();
        let kind = chars.next();
        let data = chars.as_str();
        match (kind, source) {
            (Some('1'), Source::Transport) => Flow::Gone,
            // engine.io v4: pings go from the server to the client only
            (Some('2'), Source::Transport) => Flow::Gone,
            (Some('3'), Source::Transport) => {
                self.pong_deadline = None;
                Flow::Continue
            }
            (Some('4'), Source::Transport) => self.message(Message::Text(data.to_owned())).await,
            (Some('6'), Source::Transport) => Flow::Continue,
            (Some('2'), Source::Probe) if data == "probe" => {
                let probe = self.probe.as_mut().unwrap();
                if !probe.send(Packet::Text("3probe".into())).await {
                    self.drop_probe();
                    return Flow::Continue;
                }
                // release the pending poll so the client can pause polling for the upgrade
                if let Some(poll) = self.poll.take() {
                    let _ = poll.send("6".into());
                }
                Flow::Continue
            }
            (Some('5'), Source::Probe) => {
                let mut link = self.probe.take().unwrap();
                // a pending poll ends with a noop; later packets go over the websocket
                if let Some(poll) = self.poll.take() {
                    let _ = poll.send("6".into());
                }
                while let Some(packet) = self.outbox.pop_front() {
                    if !link.send(packet).await {
                        link.reader.abort();
                        return Flow::Gone;
                    }
                }
                self.ws = Some(link);
                Flow::Continue
            }
            (Some('6'), Source::Probe) => Flow::Continue,
            (_, Source::Probe) => {
                self.drop_probe();
                Flow::Continue
            }
            _ => Flow::Gone, // engine.io closes on a packet it cannot parse
        }
    }

    async fn message(&mut self, message: Message) -> Flow {
        let mut ctx = Ctx {
            handshake: &self.handshake,
            actions: Vec::new(),
        };
        self.app.on_message(message, &mut ctx);
        let actions = ctx.actions;
        self.apply(actions).await
    }

    async fn apply(&mut self, actions: Vec<Action>) -> Flow {
        for action in actions {
            let flow = match action {
                Action::Send(message) => self.send(Packet::message(message)).await,
                Action::Close => Flow::Close,
                Action::After(delay, timer) => {
                    let tx = self.tx.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let _ = tx.send(Cmd::Timer(timer));
                    });
                    Flow::Continue
                }
                Action::Restart(downtime) => {
                    self.server.restart(downtime);
                    Flow::Continue
                }
            };
            if flow != Flow::Continue {
                return flow;
            }
        }
        Flow::Continue
    }

    async fn send(&mut self, packet: Packet) -> Flow {
        match &mut self.ws {
            Some(link) => {
                if link.send(packet).await {
                    Flow::Continue
                } else {
                    Flow::Gone
                }
            }
            None => {
                self.outbox.push_back(packet);
                self.flush_poll();
                Flow::Continue
            }
        }
    }

    /// The outbox as one polling payload.
    fn payload(&mut self) -> String {
        let packets: Vec<String> = self.outbox.drain(..).map(|p| p.polling()).collect();
        packets.join("\x1e")
    }

    fn flush_poll(&mut self) {
        if self.outbox.is_empty() {
            return;
        }
        if let Some(poll) = self.poll.take() {
            let payload = self.payload();
            let _ = poll.send(payload);
        }
    }
}
