//! In-process Engine.IO v4 / Socket.IO v4 test servers.
//!
//! Build one with [`Server::builder`]: namespaces with handlers ([`Namespace`]) that act on the
//! client through a [`Socket`]. The ready-made servers of the crate root
//! ([`socket_io_server`](crate::socket_io_server), ...) are built the same way. Handlers must
//! not block: every fake server of the process shares one small runtime.
//!
//! ```no_run
//! use socketio_next_testing::fake::{Connect, Data, Namespace, Server};
//! use serde_json::json;
//!
//! let server = Server::builder()
//!     .namespace(
//!         "/",
//!         Namespace::new()
//!             .on_connect(|s| {
//!                 if s.auth().and_then(|a| a.get("token")) != Some(&json!("secret")) {
//!                     return Connect::Reject { message: "bad token".into(), data: None };
//!                 }
//!                 s.emit("welcome", Data::default().arg("hi"));
//!                 Connect::Accept
//!             })
//!             .on("ping", |s, e| s.ack(&e, Data::default().arg("pong"))),
//!     )
//!     .start();
//! let url = server.url(); // http://127.0.0.1:<port>
//! # let _ = url;
//! ```
//!
//! Structure (internal): `http` is the server actor (listeners, connections, session registry,
//! restart) and the HTTP / websocket handling; `engine` one actor per engine.io session
//! (transport, heartbeat, upgrade); `socketio` the socket.io codec and namespace logic. The
//! codecs are written from the protocol, independently of socketio-next's, so a client codec
//! bug is not mirrored by the server. Every server runs on one background runtime (a thread of
//! its own), started on first use: servers outlive the per-test runtimes of `#[tokio::test]`.

mod echo;
mod engine;
mod fixtures;
mod http;
mod socketio;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

pub use socketio::{Connect, Data, Event, Namespace, Socket};

/// What a fake server runs.
#[derive(Debug, Clone)]
enum App {
    SocketIo(Arc<socketio::Spec>),
    /// `ci/engine-io.js` / `ci/engine-io-polling.js`
    EngineEcho,
}

/// One fake server's settings.
#[derive(Debug, Clone)]
struct Config {
    app: App,
    /// The engine.io path.
    path: String,
    ping_interval: Duration,
    ping_timeout: Duration,
    /// Polling only, no upgrades.
    polling_only: bool,
}

/// Builds a fake socket.io server: [`Server::builder`].
#[derive(Debug, Clone)]
pub struct Builder {
    path: String,
    ping_interval: Duration,
    ping_timeout: Duration,
    polling_only: bool,
    namespaces: HashMap<String, Namespace>,
}

impl Builder {
    /// The engine.io path (default `/socket.io/`; a trailing `/` is added, as engine.io does).
    pub fn path(mut self, path: impl Into<String>) -> Builder {
        let path = path.into();
        let trimmed = path.trim_matches('/');
        self.path = if trimmed.is_empty() {
            "/".into()
        } else {
            format!("/{trimmed}/")
        };
        self
    }

    /// The heartbeat (default 25 s / 20 s, socket.io's).
    pub fn ping(mut self, interval: Duration, timeout: Duration) -> Builder {
        self.ping_interval = interval;
        self.ping_timeout = timeout;
        self
    }

    /// Polling only: no websocket, no upgrade offered.
    pub fn polling_only(mut self) -> Builder {
        self.polling_only = true;
        self
    }

    /// Serves namespace `name` (`/`, `/admin`, ...). `/` is served (accepting every CONNECT,
    /// ignoring events) unless given here; a CONNECT to another namespace that is not served is
    /// refused with `Invalid namespace`, as on socket.io.
    pub fn namespace(mut self, name: impl Into<String>, namespace: Namespace) -> Builder {
        self.namespaces.insert(name.into(), namespace);
        self
    }

    /// Starts the server on a free port (127.0.0.1, and `[::1]` with the same port where the
    /// host has IPv6). Blocks briefly; works inside and outside a tokio runtime.
    pub fn start(mut self) -> Server {
        self.namespaces.entry("/".into()).or_default();
        let config = Config {
            app: App::SocketIo(Arc::new(socketio::Spec {
                namespaces: self.namespaces,
            })),
            path: self.path,
            ping_interval: self.ping_interval,
            ping_timeout: self.ping_timeout,
            polling_only: self.polling_only,
        };
        start(config)
    }
}

/// A running fake server. Dropping the handle does not stop it: it lives as long as the process
/// unless [`shutdown`](Self::shutdown) is called.
#[derive(Clone)]
pub struct Server {
    url: String,
    handle: http::ServerHandle,
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Server").field("url", &self.url).finish()
    }
}

impl Server {
    /// A builder with socket.io's defaults and no namespace yet.
    pub fn builder() -> Builder {
        Builder {
            path: "/socket.io/".into(),
            ping_interval: Duration::from_millis(25_000),
            ping_timeout: Duration::from_millis(20_000),
            polling_only: false,
            namespaces: HashMap::new(),
        }
    }

    /// `http://127.0.0.1:<port>`.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Emits `event` to every socket connected to namespace `ns` (a server push).
    pub fn broadcast(&self, ns: &str, event: &str, data: Data) {
        self.handle.emit(ns, None, event, data);
    }

    /// Emits `event` to the socket with id `sid` ([`Socket::id`]) in namespace `ns`, if it is
    /// connected.
    pub fn emit_to(&self, ns: &str, sid: &str, event: &str, data: Data) {
        self.handle.emit(ns, Some(sid), event, data);
    }

    /// Takes the server down for `downtime` (every session closes; new connections are
    /// accepted and dropped at once), then it serves again on the same port.
    pub fn restart(&self, downtime: Duration) {
        self.handle.restart(downtime);
    }

    /// Closes every session and stops listening, for good: clients see the connection refused.
    pub fn shutdown(&self) {
        self.handle.shutdown();
    }
}

fn start(config: Config) -> Server {
    let (tx, rx) = std::sync::mpsc::channel();
    runtime().spawn(async move {
        let _ = tx.send(http::start(config).await);
    });
    let (url, handle) = rx.recv().expect("fake server started");
    Server { url, handle }
}

/// The background runtime every fake server runs on.
fn runtime() -> &'static tokio::runtime::Handle {
    static RUNTIME: OnceLock<tokio::runtime::Handle> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("socketio-fake-servers".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .enable_all()
                    .build()
                    .expect("fake server runtime");
                let _ = tx.send(runtime.handle().clone());
                runtime.block_on(std::future::pending::<()>());
            })
            .expect("spawn the fake server thread");
        rx.recv().expect("fake server runtime started")
    })
}

fn socket_io(namespaces: Vec<(&'static str, Namespace)>) -> Builder {
    namespaces
        .into_iter()
        .fold(Server::builder(), |b, (name, ns)| b.namespace(name, ns))
}

/// The URLs of the shared ready-made servers.
#[derive(Debug)]
pub(crate) struct Urls {
    pub socket_io: String,
    pub socket_io_auth: String,
    pub socket_io_restart: String,
    pub fast_ping: String,
    pub engine_io: String,
    pub engine_io_polling: String,
}

/// Starts every shared ready-made server on first use; their URLs.
pub(crate) fn urls() -> &'static Urls {
    static URLS: OnceLock<Urls> = OnceLock::new();
    URLS.get_or_init(|| {
        let echo = |polling_only| Config {
            app: App::EngineEcho,
            path: "/engine.io/".into(),
            ping_interval: Duration::from_millis(25_000),
            ping_timeout: Duration::from_millis(20_000),
            polling_only,
        };
        Urls {
            socket_io: socket_io(fixtures::main()).start().url,
            socket_io_auth: socket_io(fixtures::auth()).start().url,
            socket_io_restart: socket_io(fixtures::restart()).start().url,
            fast_ping: socket_io(fixtures::main())
                .ping(Duration::from_millis(300), Duration::from_millis(200))
                .start()
                .url,
            engine_io: start(echo(false)).url,
            engine_io_polling: start(echo(true)).url,
        }
    })
}

/// A new restart server of its own (a test that restarts it does not take down the server of
/// another test running at the same time).
pub(crate) fn new_restart_server() -> String {
    socket_io(fixtures::restart()).start().url
}
