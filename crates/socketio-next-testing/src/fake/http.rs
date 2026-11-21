//! The HTTP side of a fake server: the server actor (listeners, session registry, restart),
//! HTTP/1.1 requests for polling and the websocket handshake.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;

use super::Config;
use super::engine::{self, Handshake, Posted, SessionHandle, Ws};

/// How long `restart_server` keeps the server down (`ci/socket-io-restart.js`).
const RESTART_DOWNTIME: Duration = Duration::from_secs(2);

/// engine.io's default `maxHttpBufferSize`, advertised as `maxPayload`.
pub(super) const MAX_PAYLOAD: usize = 1_000_000;

/// The first value of header `name` (lower case) in a header list.
pub(super) fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.as_str())
}

/// Talks to the server actor: connections and sessions use it.
#[derive(Debug, Clone)]
pub(super) struct ServerHandle {
    pub config: Config,
    tx: mpsc::UnboundedSender<Control>,
}

impl ServerHandle {
    /// Opens a session; `None` while the server is down. With `ws` the session runs on that
    /// websocket from the start; without, it returns the open packet for the handshake response.
    async fn open(&self, handshake: Handshake, ws: Option<Ws>) -> Option<String> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Control::Open {
                handshake,
                ws: ws.map(Box::new),
                reply,
            })
            .ok()?;
        rx.await.ok()
    }

    async fn lookup(&self, sid: &str) -> Option<SessionHandle> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Control::Lookup {
                sid: sid.to_owned(),
                reply,
            })
            .ok()?;
        rx.await.ok().flatten()
    }

    /// A session ended: it leaves the registry.
    pub fn ended(&self, sid: &str) {
        let _ = self.tx.send(Control::Ended(sid.to_owned()));
    }

    /// Takes the whole server down for [`RESTART_DOWNTIME`] (`restart_server`).
    pub fn restart(&self) {
        let _ = self.tx.send(Control::Restart);
    }
}

#[derive(Debug)]
enum Control {
    Open {
        handshake: Handshake,
        ws: Option<Box<Ws>>,
        reply: oneshot::Sender<String>,
    },
    Lookup {
        sid: String,
        reply: oneshot::Sender<Option<SessionHandle>>,
    },
    Ended(String),
    Restart,
}

/// Starts a server for `config` on a free port; returns `http://127.0.0.1:<port>`.
pub(super) async fn start(config: Config) -> String {
    let (v4, v6) = bind_pair();
    let port = v4.local_addr().unwrap().port();
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = ServerHandle { config, tx };
    let server = Server {
        handle: handle.clone(),
        sessions: HashMap::new(),
        connections: JoinSet::new(),
        state: State::Up,
    };
    tokio::spawn(server.run(v4, v6, rx));
    format!("http://127.0.0.1:{port}")
}

/// A free port on 127.0.0.1, and on [::1] with the same number where the host has IPv6 (the
/// IPv6 tests connect to `[::1]:<port>`). Tries other ports when [::1] has that one taken.
fn bind_pair() -> (TcpListener, Option<TcpListener>) {
    for _ in 0..20 {
        let v4 = bind("127.0.0.1:0".parse().unwrap()).expect("bind 127.0.0.1");
        let port = v4.local_addr().unwrap().port();
        match bind(SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], port))) {
            Ok(v6) => return (v4, Some(v6)),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
            // no IPv6 on this host
            Err(_) => return (v4, None),
        }
    }
    panic!("no port free on both 127.0.0.1 and [::1]");
}

fn bind(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(128)
}

#[derive(Debug)]
enum State {
    Up,
    /// `restart_server`: the sessions are closing; connections are refused.
    Closing,
    /// Down until then; connections are refused.
    Down(Instant),
}

/// The server actor: owns the listeners, the connection tasks and the session registry.
#[derive(Debug)]
struct Server {
    handle: ServerHandle,
    sessions: HashMap<String, SessionHandle>,
    connections: JoinSet<()>,
    state: State,
}

impl Server {
    async fn run(
        mut self,
        v4: TcpListener,
        v6: Option<TcpListener>,
        mut control: mpsc::UnboundedReceiver<Control>,
    ) {
        loop {
            let down_until = match self.state {
                State::Down(until) => Some(until),
                _ => None,
            };
            tokio::select! {
                Ok((stream, _)) = v4.accept() => self.accepted(stream),
                Ok((stream, _)) = async { v6.as_ref().unwrap().accept().await }, if v6.is_some() => {
                    self.accepted(stream)
                }
                Some(control) = control.recv() => self.control(control),
                _ = tokio::time::sleep_until(down_until.unwrap_or_else(Instant::now)), if down_until.is_some() => {
                    self.state = State::Up;
                }
            }
            while self.connections.try_join_next().is_some() {}
        }
    }

    fn accepted(&mut self, stream: TcpStream) {
        // while restarting the listeners stay bound (no other socket can take the port) but
        // every connection is dropped: a refused connection for the client
        if matches!(self.state, State::Up) {
            self.connections.spawn(serve(stream, self.handle.clone()));
        }
    }

    fn control(&mut self, control: Control) {
        match control {
            Control::Open {
                handshake,
                ws,
                reply,
            } => {
                if !matches!(self.state, State::Up) {
                    return; // reply dropped: the request fails
                }
                // registered before it runs: its end always finds it here
                let (sid, session, open) =
                    engine::spawn(self.handle.clone(), handshake, ws.map(|ws| *ws));
                self.sessions.insert(sid, session);
                let _ = reply.send(open);
            }
            Control::Lookup { sid, reply } => {
                let _ = reply.send(self.sessions.get(&sid).cloned());
            }
            Control::Ended(sid) => {
                self.sessions.remove(&sid);
                self.restart_progress();
            }
            Control::Restart => {
                self.state = State::Closing;
                // close packets reach the pending polls before the connections go
                for session in self.sessions.values() {
                    session.close();
                }
                self.restart_progress();
            }
        }
    }

    /// Once every session closed, drops the connections and stays down for a while.
    fn restart_progress(&mut self) {
        if matches!(self.state, State::Closing) && self.sessions.is_empty() {
            self.connections.abort_all();
            self.state = State::Down(Instant::now() + RESTART_DOWNTIME);
        }
    }
}

/// One parsed HTTP request (the body is not read when it is too large).
#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    /// Lower-case names.
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    too_large: bool,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }

    fn param(&self, name: &str) -> Option<&str> {
        header(&self.query, name)
    }

    fn handshake(&self) -> Handshake {
        Handshake {
            query: self.query.clone(),
            headers: self.headers.clone(),
        }
    }
}

/// What a polling request gets.
#[derive(Debug)]
enum Reply {
    Now(&'static str, String),
    /// A long poll on this session.
    Poll(SessionHandle),
}

/// Serves one TCP connection: polling requests (keep-alive) or one websocket.
async fn serve(stream: TcpStream, server: ServerHandle) {
    let mut reader = BufReader::new(stream);
    loop {
        let request = match read_request(&mut reader).await {
            Ok(Some(request)) => request,
            Ok(None) => return,
            Err(e) => {
                let _ = reader
                    .get_mut()
                    .write_all(&response("400 Bad Request", e.as_bytes(), true))
                    .await;
                return;
            }
        };
        let websocket = request
            .header("upgrade")
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
        if websocket {
            if !reader.buffer().is_empty() {
                return; // frames before the 101: not a client we test
            }
            websocket_request(reader.into_inner(), request, &server).await;
            return;
        }
        let close = request.too_large
            || request
                .header("connection")
                .is_some_and(|v| v.eq_ignore_ascii_case("close"));
        let (status, body) = match polling_request(request, &server).await {
            Reply::Now(status, body) => (status, body),
            Reply::Poll(session) => {
                // engine.io closes the session when the client drops a pending poll
                let poll = session.poll();
                tokio::pin!(poll);
                let eof = async {
                    match reader.fill_buf().await {
                        Ok([]) | Err(_) => true,
                        Ok(_) => false,
                    }
                };
                tokio::select! {
                    body = &mut poll => poll_reply(body),
                    gone = eof => {
                        if gone {
                            session.close();
                            return;
                        }
                        poll_reply(poll.await)
                    }
                }
            }
        };
        if reader
            .get_mut()
            .write_all(&response(status, body.as_bytes(), close))
            .await
            .is_err()
            || close
        {
            return;
        }
    }
}

fn poll_reply(body: Option<String>) -> (&'static str, String) {
    match body {
        Some(body) => ("200 OK", body),
        None => engine_error(3, "Bad request"),
    }
}

/// Reads one request; `None` on a clean end of stream.
async fn read_request(reader: &mut BufReader<TcpStream>) -> Result<Option<Request>, String> {
    let mut line = String::new();
    if reader
        .read_line(&mut line)
        .await
        .map_err(|e| e.to_string())?
        == 0
    {
        return Ok(None);
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().ok_or("no request target")?.to_owned();
    let mut headers = Vec::new();
    loop {
        line.clear();
        if reader
            .read_line(&mut line)
            .await
            .map_err(|e| e.to_string())?
            == 0
        {
            return Err("eof in the request head".into());
        }
        let header = line.trim_end();
        if header.is_empty() {
            break;
        }
        let (name, value) = header.split_once(':').ok_or("bad header")?;
        headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
    }
    // ponytail: no chunked request bodies, the clients under test send Content-Length
    if header(&headers, "transfer-encoding").is_some() {
        return Err("chunked bodies are not supported".into());
    }
    let length: usize = header(&headers, "content-length")
        .map(|v| v.parse().map_err(|_| "bad content-length"))
        .transpose()?
        .unwrap_or(0);
    let too_large = length > MAX_PAYLOAD;
    let mut body = Vec::new();
    if !too_large {
        body = vec![0; length];
        reader
            .read_exact(&mut body)
            .await
            .map_err(|e| e.to_string())?;
    }
    let (path, query) = target.split_once('?').unwrap_or((&target, ""));
    Ok(Some(Request {
        method,
        path: path.to_owned(),
        query: url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect(),
        headers,
        body,
        too_large,
    }))
}

fn response(status: &str, body: &[u8], close: bool) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=UTF-8\r\nContent-Length: {}\r\n{}\r\n",
        body.len(),
        if close { "Connection: close\r\n" } else { "" }
    )
    .into_bytes();
    out.extend_from_slice(body);
    out
}

/// `{"code":..,"message":..}` with status 400, like engine.io.
fn engine_error(code: u8, message: &str) -> (&'static str, String) {
    (
        "400 Bad Request",
        serde_json::json!({ "code": code, "message": message }).to_string(),
    )
}

/// The engine.io checks every request passes (path, `EIO`, transport); the error response.
fn check(
    request: &Request,
    config: &Config,
    transport: &str,
) -> Result<(), (&'static str, String)> {
    if request.path != config.path {
        return Err(("404 Not Found", String::new()));
    }
    if request.param("EIO") != Some("4") {
        return Err(engine_error(5, "Unsupported protocol version"));
    }
    if request.param("transport") != Some(transport)
        || (transport == "websocket" && config.polling_only)
    {
        return Err(engine_error(0, "Transport unknown"));
    }
    Ok(())
}

async fn polling_request(request: Request, server: &ServerHandle) -> Reply {
    if let Err((status, body)) = check(&request, &server.config, "polling") {
        return Reply::Now(status, body);
    }
    let Some(sid) = request.param("sid") else {
        if request.method != "GET" {
            let (status, body) = engine_error(2, "Bad handshake method");
            return Reply::Now(status, body);
        }
        return match server.open(request.handshake(), None).await {
            Some(open) => Reply::Now("200 OK", open),
            None => Reply::Now("503 Service Unavailable", String::new()),
        };
    };
    let Some(session) = server.lookup(sid).await else {
        let (status, body) = engine_error(1, "Session ID unknown");
        return Reply::Now(status, body);
    };
    if request.too_large {
        session.close();
        return Reply::Now("413 Payload Too Large", String::new());
    }
    match request.method.as_str() {
        "GET" => Reply::Poll(session),
        "POST" => match session.post(request.body).await {
            Posted::Ok => Reply::Now("200 OK", "ok".into()),
            Posted::Refused => {
                let (status, body) = engine_error(3, "Bad request");
                Reply::Now(status, body)
            }
        },
        _ => {
            let (status, body) = engine_error(2, "Bad request");
            Reply::Now(status, body)
        }
    }
}

async fn websocket_request(mut stream: TcpStream, request: Request, server: &ServerHandle) {
    let refuse = |(status, body): (&'static str, String)| response(status, body.as_bytes(), true);
    if let Err(e) = check(&request, &server.config, "websocket") {
        let _ = stream.write_all(&refuse(e)).await;
        return;
    }
    let session = match request.param("sid") {
        Some(sid) => match server.lookup(sid).await {
            Some(session) => Some(session),
            None => {
                let _ = stream
                    .write_all(&refuse(engine_error(1, "Session ID unknown")))
                    .await;
                return;
            }
        },
        None => None,
    };
    let Some(key) = request.header("sec-websocket-key") else {
        let _ = stream
            .write_all(&refuse(("400 Bad Request", "no Sec-WebSocket-Key".into())))
            .await;
        return;
    };
    let accept = derive_accept_key(key.as_bytes());
    let head = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    if stream.write_all(head.as_bytes()).await.is_err() {
        return;
    }
    let ws = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
    match session {
        Some(session) => session.probe(ws),
        None => {
            // dropped (the socket closes) when the server is down
            let _ = server.open(request.handshake(), Some(ws)).await;
        }
    }
}
