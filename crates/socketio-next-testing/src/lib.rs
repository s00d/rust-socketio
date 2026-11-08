//! Test support for socketio-next (not published): the URLs of the Node test servers in `ci/`,
//! the test CA, a TCP proxy that can cut connections, and the differential harness that runs a
//! scenario with the Node client.
//!
//! Servers: `just servers` (ci/: ports 4200-4206; ci/conformance: 4210-4212). Every URL can
//! be overridden with an environment variable of the same name in upper case.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

fn env_or(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

/// The repository root (two levels above this crate).
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("crate lives in crates/<name>")
}

/// socket.io 4.x server with the event fixtures (`ci/socket-io.js`).
pub fn socket_io_server() -> String {
    env_or("SOCKET_IO_SERVER", "http://localhost:4200")
}

/// socket.io server that checks `auth.password` (`ci/socket-io-auth.js`).
pub fn socket_io_auth_server() -> String {
    env_or("SOCKET_IO_AUTH_SERVER", "http://localhost:4204")
}

/// socket.io server that restarts on `restart_server` (`ci/socket-io-restart.js`).
pub fn socket_io_restart_server() -> String {
    env_or("SOCKET_IO_RESTART_SERVER", "http://localhost:4205")
}

/// socket.io 4.8 conformance server (`ci/conformance/server.js`).
pub fn conformance_server() -> String {
    env_or("CONFORMANCE_SERVER", "http://localhost:4210")
}

/// socket.io 4.8 server with connection state recovery (`ci/conformance/server.js`).
pub fn recovery_server() -> String {
    env_or("RECOVERY_SERVER", "http://localhost:4211")
}

/// socket.io 4.8 server with a 300 ms / 200 ms heartbeat (`ci/conformance/server.js`).
pub fn fast_ping_server() -> String {
    env_or("FAST_PING_SERVER", "http://localhost:4212")
}

/// The test CA (`ci/cert/ca.crt`, `CA_CERT_PATH` overrides).
pub fn ca_cert_pem() -> Vec<u8> {
    let path = std::env::var("CA_CERT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root().join("ci/cert/ca.crt"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// A local TCP proxy in front of a test server: count connections, cut them, refuse new ones.
#[derive(Debug, Clone)]
pub struct Proxy {
    addr: String,
    state: Arc<ProxyState>,
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
        tokio::spawn(async move {
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
        Proxy { addr, state }
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
