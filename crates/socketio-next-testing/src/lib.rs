//! Test support for socket.io clients (socketio-next and others).
//!
//! - [`fake`]: in-process Socket.IO v4 / Engine.IO v4 servers you program with handlers
//!   ([`fake::Server::builder`]), no Node needed.
//! - Ready-made servers ([`socket_io_server`], [`engine_io_server`], ...): the fixtures of
//!   socketio-next's own suite, built with [`fake`]; each starts on first use.
//! - [`Proxy`]: a TCP proxy that can cut, freeze or refuse connections; [`HttpProxy`]: an HTTP
//!   proxy that records what it forwarded.
//!
//! ```no_run
//! use socketio_next_testing::fake::{Data, Namespace, Server};
//!
//! # async fn demo() {
//! let server = Server::builder()
//!     .namespace("/", Namespace::new().on("ping", |s, e| s.ack(&e, Data::default().arg("pong"))))
//!     .start();
//! // put a proxy in between and cut the link under the client
//! let proxy = socketio_next_testing::Proxy::start(&socketio_next_testing::host_port(server.url())).await;
//! proxy.cut();
//! # }
//! ```
//!
//! The feature `repo` is for socketio-next's own repository checkout (it reads its `ci/`
//! directory): the test CA, the Node conformance servers, the differential harness, and the
//! environment variables (`SOCKET_IO_SERVER`, ... in upper case) that point the ready-made
//! servers at the Node ones.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

pub mod fake;

#[cfg(feature = "repo")]
fn env_or(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

/// The fake server's URL; with the feature `repo`, the variable `var` wins.
fn env_or_fake(var: &str, fake: impl FnOnce(&fake::Urls) -> &String) -> String {
    #[cfg(feature = "repo")]
    if let Ok(url) = std::env::var(var) {
        return url;
    }
    let _ = var;
    fake(fake::urls()).clone()
}

#[cfg(feature = "repo")]
/// The repository root (two levels above this crate).
pub fn repo_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(std::path::PathBuf::from)
        .expect("crate lives in crates/<name>")
}

/// socket.io server with the event fixtures of `ci/socket-io.js` (in-process fake; Node:
/// `http://localhost:4200`).
pub fn socket_io_server() -> String {
    env_or_fake("SOCKET_IO_SERVER", |u| &u.socket_io)
}

/// socket.io server that checks `auth.password`, `ci/socket-io-auth.js` (in-process fake;
/// Node: `http://localhost:4204`).
pub fn socket_io_auth_server() -> String {
    env_or_fake("SOCKET_IO_AUTH_SERVER", |u| &u.socket_io_auth)
}

/// socket.io server that echoes the `message_back` header and restarts on `restart_server`,
/// `ci/socket-io-restart.js` (in-process fake; Node: `http://localhost:4205`).
pub fn socket_io_restart_server() -> String {
    env_or_fake("SOCKET_IO_RESTART_SERVER", |u| &u.socket_io_restart)
}

/// A restart server of its own (no environment override): for a test that restarts it while
/// other tests use `socket_io_restart_server`.
pub fn new_socket_io_restart_server() -> String {
    fake::new_restart_server()
}

/// engine.io server (path `/engine.io/`) that greets with `hello client` and answers
/// `respond` / `close`, `ci/engine-io.js` (in-process fake; Node: `http://localhost:4201`).
pub fn engine_io_server() -> String {
    env_or_fake("ENGINE_IO_SERVER", |u| &u.engine_io)
}

/// The same, polling only, without upgrades, `ci/engine-io-polling.js` (in-process fake;
/// Node: `http://localhost:4203`).
pub fn engine_io_polling_server() -> String {
    env_or_fake("ENGINE_IO_POLLING_SERVER", |u| &u.engine_io_polling)
}

#[cfg(feature = "repo")]
/// socket.io 4.8 conformance server (`ci/conformance/server.js`).
pub fn conformance_server() -> String {
    env_or("CONFORMANCE_SERVER", "http://localhost:4210")
}

#[cfg(feature = "repo")]
/// socket.io 4.8 server with connection state recovery (`ci/conformance/server.js`).
pub fn recovery_server() -> String {
    env_or("RECOVERY_SERVER", "http://localhost:4211")
}

/// socket.io server with a 300 ms / 200 ms heartbeat and the fixtures of `ci/socket-io.js`
/// (in-process fake; Node: `http://localhost:4212`, the conformance events).
pub fn fast_ping_server() -> String {
    env_or_fake("FAST_PING_SERVER", |u| &u.fast_ping)
}

#[cfg(feature = "repo")]
/// The test CA (`ci/cert/ca.crt`, `CA_CERT_PATH` overrides).
pub fn ca_cert_pem() -> Vec<u8> {
    let path = std::env::var("CA_CERT_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| repo_root().join("ci/cert/ca.crt"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// A local TCP proxy in front of a test server: count connections, cut them, refuse new ones.
#[derive(Debug, Clone)]
pub struct Proxy {
    addr: String,
    state: Arc<ProxyState>,
    accept: Arc<tokio::task::AbortHandle>,
}

#[derive(Debug, Default)]
struct ProxyState {
    cut: Notify,
    freeze: Notify,
    refuse: AtomicBool,
    block_websocket: AtomicBool,
    accepted: AtomicUsize,
    open: AtomicUsize,
}

impl Proxy {
    /// Starts a proxy to `target` (`host:port`).
    pub async fn start(target: &str) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = Arc::new(ProxyState::default());
        let target = target.to_owned();
        let task_state = Arc::clone(&state);
        let accept = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    return;
                };
                if task_state.refuse.load(Ordering::SeqCst) {
                    drop(inbound);
                    continue;
                }
                task_state.accepted.fetch_add(1, Ordering::SeqCst);
                let state = Arc::clone(&task_state);
                let target = target.clone();
                tokio::spawn(async move {
                    let Ok(mut outbound) = TcpStream::connect(&target).await else {
                        return;
                    };
                    if state.block_websocket.load(Ordering::SeqCst) {
                        // read the request head; a websocket upgrade is swallowed (blackholed)
                        let mut head = vec![0u8; 4096];
                        let Ok(n) = inbound.read(&mut head).await else {
                            return;
                        };
                        let text = String::from_utf8_lossy(&head[..n]).to_ascii_lowercase();
                        if text.contains("upgrade: websocket") {
                            state.cut.notified().await;
                            return;
                        }
                        if outbound.write_all(&head[..n]).await.is_err() {
                            return;
                        }
                    }
                    state.open.fetch_add(1, Ordering::SeqCst);
                    let cut = state.cut.notified();
                    let freeze = state.freeze.notified();
                    tokio::pin!(cut);
                    tokio::select! {
                        _ = copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _ = &mut cut => {}
                        // a half-dead link: the sockets stay open, nothing gets through
                        _ = freeze => cut.await,
                    }
                    state.open.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });
        Proxy {
            addr,
            state,
            accept: Arc::new(accept.abort_handle()),
        }
    }

    /// Stops listening and drops every open connection: the port refuses from now on.
    pub fn stop(&self) {
        self.accept.abort();
        self.state.cut.notify_waiters();
    }

    /// `http://<proxy addr>`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Drops every open connection.
    pub fn cut(&self) {
        self.state.cut.notify_waiters();
    }

    /// Stops forwarding on every open connection but keeps the sockets open (a blackholed
    /// link), until [`cut`](Self::cut).
    pub fn freeze(&self) {
        self.state.freeze.notify_waiters();
    }

    /// Swallow websocket upgrade requests on new connections (a proxy that blackholes
    /// websockets); other HTTP requests pass.
    pub fn block_websocket(&self, block: bool) {
        self.state.block_websocket.store(block, Ordering::SeqCst);
    }

    /// Refuse (true) or accept (false) new connections.
    pub fn refuse(&self, refuse: bool) {
        self.state.refuse.store(refuse, Ordering::SeqCst);
    }

    /// Connections accepted so far.
    pub fn accepted(&self) -> usize {
        self.state.accepted.load(Ordering::SeqCst)
    }

    /// Connections open now.
    pub fn open(&self) -> usize {
        self.state.open.load(Ordering::SeqCst)
    }

    /// Waits up to `within` for `open() == want`.
    pub async fn wait_open(&self, want: usize, within: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            if self.open() == want {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        self.open() == want
    }
}

/// `host:port` of an `http://host:port` URL.
pub fn host_port(url: &str) -> String {
    url.trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_owned()
}

#[cfg(feature = "repo")]
/// Runs `ci/conformance/client.js <scenario> <tag>` (the Node socket.io-client 4.8); returns its
/// stdout lines. Panics when node or the script fails.
pub async fn run_node_client(scenario: &str, tag: &str, transports: Option<&str>) -> Vec<String> {
    let dir = repo_root().join("ci/conformance");
    let mut command = tokio::process::Command::new("node");
    if let Some(transports) = transports {
        command.env("TRANSPORTS", transports);
    }
    let out = command
        .arg("client.js")
        .arg(scenario)
        .arg(tag)
        .env("CONFORMANCE_SERVER", conformance_server())
        .current_dir(&dir)
        .output()
        .await
        .expect("node in PATH");
    assert!(
        out.status.success(),
        "node client {scenario} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::to_owned)
        .collect()
}

#[cfg(feature = "repo")]
/// The packets the conformance server recorded for `tag` (`GET /log?tag=`): one string per
/// packet, `<in|out> <engine.io type> <data>`, normalised by the server (sids, timestamps).
pub async fn recorded_packets(tag: &str) -> Vec<String> {
    let host = host_port(&conformance_server());
    // A tiny HTTP/1.0 GET: no HTTP client dependency for one request.
    let mut stream = TcpStream::connect(&host).await.unwrap();
    stream
        .write_all(format!("GET /log?tag={tag} HTTP/1.0\r\nHost: {host}\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut body = String::new();
    stream.read_to_string(&mut body).await.unwrap();
    let json = body.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("[]");
    let value: serde_json::Value = serde_json::from_str(json).unwrap();
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect()
}

#[cfg(feature = "repo")]
/// socket.io 4.8 server with engine.io cookies on (`ci/conformance/cookies.js`); `GET
/// /cookies?sid=<engine sid>` lists the `Cookie` header of every request of that session.
pub fn cookie_server() -> String {
    env_or("COOKIE_SERVER", "http://localhost:4213")
}

#[cfg(feature = "repo")]
/// The `Cookie` headers the cookie server saw for engine.io session `sid`, as
/// `<transport> <cookie or ->` per request.
pub async fn cookies_seen(sid: &str) -> Vec<String> {
    let host = host_port(&cookie_server());
    let mut stream = TcpStream::connect(&host).await.unwrap();
    stream
        .write_all(format!("GET /cookies?sid={sid} HTTP/1.0\r\nHost: {host}\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut body = String::new();
    stream.read_to_string(&mut body).await.unwrap();
    let json = body.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("[]");
    let value: serde_json::Value = serde_json::from_str(json).unwrap();
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect()
}

/// A minimal HTTP proxy: `CONNECT` tunnels (websockets) and absolute-form requests (polling,
/// forwarded one per connection in origin form). Records what it was asked.
#[derive(Debug, Clone)]
pub struct HttpProxy {
    addr: String,
    log: Arc<std::sync::Mutex<Vec<String>>>,
    accept: Arc<tokio::task::AbortHandle>,
}

impl HttpProxy {
    /// Starts the proxy on a free local port.
    pub async fn start() -> HttpProxy {
        HttpProxy::start_on("127.0.0.1:0").await
    }

    /// Starts the proxy on `addr` (`[::1]:0` for IPv6).
    pub async fn start_on(addr: &str) -> HttpProxy {
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let task_log = Arc::clone(&log);
        let accept = tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                tokio::spawn(proxy_one(inbound, Arc::clone(&task_log)));
            }
        });
        HttpProxy {
            addr,
            log,
            accept: Arc::new(accept.abort_handle()),
        }
    }

    /// Stops listening (connections already tunnelled keep going until they end).
    pub fn stop(&self) {
        self.accept.abort();
    }

    /// `http://<addr>` (`[::1]:port` for IPv6).
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// One line per request: `CONNECT host:port [auth]` or `GET|POST /path [auth]`, where
    /// `auth` is the `Proxy-Authorization` header if any.
    pub fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
}

async fn proxy_one(mut inbound: TcpStream, log: Arc<std::sync::Mutex<Vec<String>>>) {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0u8];
        if inbound.read(&mut byte).await.unwrap_or(0) == 0 || head.len() > 65536 {
            return;
        }
        head.push(byte[0]);
    }
    let text = String::from_utf8_lossy(&head).into_owned();
    let mut lines = text.split("\r\n");
    let first = lines.next().unwrap_or_default().to_owned();
    let headers: Vec<&str> = lines.filter(|l| !l.is_empty()).collect();
    let auth = headers
        .iter()
        .find_map(|h| {
            let (name, value) = h.split_once(':')?;
            name.eq_ignore_ascii_case("proxy-authorization")
                .then(|| format!(" {}", value.trim()))
        })
        .unwrap_or_default();
    let mut parts = first.split_whitespace();
    let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    if method == "CONNECT" {
        log.lock().unwrap().push(format!("CONNECT {target}{auth}"));
        let Ok(mut outbound) = TcpStream::connect(target).await else {
            let _ = inbound.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
            return;
        };
        if inbound
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await
            .is_ok()
        {
            let _ = copy_bidirectional(&mut inbound, &mut outbound).await;
        }
        return;
    }
    // absolute form: http://host:port/path?query
    let rest = target.trim_start_matches("http://");
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    let path = format!("/{path}");
    log.lock().unwrap().push(format!(
        "{method} {}{auth}",
        path.split('?').next().unwrap_or("")
    ));
    let Ok(mut outbound) = TcpStream::connect(host).await else {
        return;
    };
    let mut forwarded = format!("{method} {path} HTTP/1.1\r\n");
    for h in headers {
        let lower = h.to_ascii_lowercase();
        if lower.starts_with("proxy-") || lower.starts_with("connection:") {
            continue;
        }
        forwarded.push_str(h);
        forwarded.push_str("\r\n");
    }
    forwarded.push_str("Connection: close\r\n\r\n");
    if outbound.write_all(forwarded.as_bytes()).await.is_ok() {
        let _ = copy_bidirectional(&mut inbound, &mut outbound).await;
    }
}
