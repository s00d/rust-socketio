//! In-process Engine.IO v4 / Socket.IO v4 test servers: the fixtures of `ci/*.js` without Node.
//!
//! Structure:
//! - [`http`]: one server per fixture. It owns its listeners (127.0.0.1 and, if possible, [::1]
//!   on the same port), the connection tasks and the session registry. HTTP/1.1 keep-alive for
//!   polling, the websocket handshake for `transport=websocket`, a restart on request.
//! - [`engine`]: one actor per engine.io session. It owns the transport (polling outbox and
//!   pending poll, websocket sink, upgrade probe), the heartbeat and the app.
//! - [`socketio`]: the socket.io app (packet codec, namespaces, acks, binary attachments) and
//!   the behaviour of `ci/socket-io.js`, `ci/socket-io-auth.js`, `ci/socket-io-restart.js`.
//! - [`echo`]: the engine.io app of `ci/engine-io.js` / `ci/engine-io-polling.js`.
//!
//! The codecs are written here from the protocol, independently of socketio-next's, so a codec
//! bug in the client is not mirrored by the server. Behaviour that only the reference server
//! settles (the differential tests, connection state recovery, cookies) stays on Node.
//!
//! Every fixture runs on one background runtime (a thread of its own), started on first use:
//! the servers outlive the per-test runtimes of `#[tokio::test]`.

mod echo;
mod engine;
mod http;
mod socketio;

use std::sync::OnceLock;
use std::time::Duration;

use socketio::Fixture;

/// What a fake server runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum App {
    /// A socket.io server behaving like one of the `ci/socket-io*.js` fixtures.
    SocketIo(socketio::Fixture),
    /// `ci/engine-io.js` / `ci/engine-io-polling.js`
    EngineEcho,
}

/// One fake server's settings.
#[derive(Debug, Clone, Copy)]
struct Config {
    app: App,
    /// The engine.io path (`/socket.io/`, `/engine.io/`).
    path: &'static str,
    ping_interval: Duration,
    ping_timeout: Duration,
    /// Polling only, no upgrades (`ci/engine-io-polling.js`).
    polling_only: bool,
}

impl Config {
    const fn socket_io(app: App) -> Config {
        Config {
            app,
            path: "/socket.io/",
            ping_interval: Duration::from_millis(25_000),
            ping_timeout: Duration::from_millis(20_000),
            polling_only: false,
        }
    }
}

/// The URLs of the running fake servers.
#[derive(Debug)]
pub(crate) struct Urls {
    pub socket_io: String,
    pub socket_io_auth: String,
    pub socket_io_restart: String,
    pub fast_ping: String,
    pub engine_io: String,
    pub engine_io_polling: String,
}

/// The shared fake servers, and the runtime they run on.
#[derive(Debug)]
struct Servers {
    urls: Urls,
    runtime: tokio::runtime::Handle,
}

/// Starts every shared fake server on first use; their URLs.
pub(crate) fn urls() -> &'static Urls {
    &servers().urls
}

/// A new restart server of its own (a test that restarts it does not take down the server of
/// another test running at the same time).
pub(crate) fn new_restart_server() -> String {
    let (tx, rx) = std::sync::mpsc::channel();
    servers().runtime.spawn(async move {
        let url = http::start(Config::socket_io(App::SocketIo(Fixture::Restart))).await;
        let _ = tx.send(url);
    });
    rx.recv().expect("restart server started")
}

fn servers() -> &'static Servers {
    static SERVERS: OnceLock<Servers> = OnceLock::new();
    SERVERS.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("socketio-fake-servers".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("fake server runtime");
                runtime.block_on(async move {
                    let urls = Urls {
                        socket_io: http::start(Config::socket_io(App::SocketIo(Fixture::Main)))
                            .await,
                        socket_io_auth: http::start(Config::socket_io(App::SocketIo(
                            Fixture::Auth,
                        )))
                        .await,
                        socket_io_restart: http::start(Config::socket_io(App::SocketIo(
                            Fixture::Restart,
                        )))
                        .await,
                        fast_ping: http::start(Config {
                            ping_interval: Duration::from_millis(300),
                            ping_timeout: Duration::from_millis(200),
                            ..Config::socket_io(App::SocketIo(Fixture::Main))
                        })
                        .await,
                        engine_io: http::start(Config {
                            path: "/engine.io/",
                            ..Config::socket_io(App::EngineEcho)
                        })
                        .await,
                        engine_io_polling: http::start(Config {
                            path: "/engine.io/",
                            polling_only: true,
                            ..Config::socket_io(App::EngineEcho)
                        })
                        .await,
                    };
                    let runtime = tokio::runtime::Handle::current();
                    tx.send(Servers { urls, runtime })
                        .expect("test thread waits for the urls");
                    std::future::pending::<()>().await
                })
            })
            .expect("spawn the fake server thread");
        rx.recv().expect("fake servers started")
    })
}
