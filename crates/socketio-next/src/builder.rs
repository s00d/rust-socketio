use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use http::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use crate::backoff::DEFAULT_JITTER;
use crate::engineio::TlsConfig;
use crate::handler::{Handler, Handlers};
use crate::protocol::socketio::Packet;
use crate::{Client, ConnectionState, Error, Event, Result, TransportType};

/// Called before every CONNECT for the auth payload (socket.io-client `auth` as a function).
pub(crate) type AuthFn = Arc<dyn Fn() -> BoxFuture<'static, Value> + Send + Sync>;

/// Which way a packet went, for [`ClientBuilder::inspect_packets`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Direction {
    /// From the server.
    Incoming,
    /// To the server.
    Outgoing,
}

pub(crate) type ParamsFn = Arc<dyn Fn() -> BoxFuture<'static, ConnectParams> + Send + Sync>;

/// Query parameters and headers computed for one connect attempt
/// ([`ClientBuilder::connect_params_with`]). A key given here replaces the builder's value of
/// the same key for that attempt.
#[derive(Clone, Default)]
pub struct ConnectParams {
    pub(crate) query: Vec<(String, String)>,
    pub(crate) headers: Vec<(String, String)>,
}

impl fmt::Debug for ConnectParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys = |v: &[(String, String)]| v.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
        f.debug_struct("ConnectParams")
            .field("query_keys", &keys(&self.query))
            .field("header_keys", &keys(&self.headers))
            .finish()
    }
}

impl ConnectParams {
    /// No parameters.
    pub fn new() -> Self {
        ConnectParams::default()
    }

    /// A query parameter for this attempt.
    pub fn query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.push((key.into(), value.into()));
        self
    }

    /// An HTTP header for this attempt.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }
}

/// The proxy setting of a builder.
#[derive(Clone, Debug)]
pub(crate) enum ProxyChoice {
    Env,
    Url(String),
    Off,
}

pub(crate) type Inspector = Arc<dyn Fn(Direction, &Packet) + Send + Sync>;

/// Everything the connection actor needs; fixed at `connect()`.
#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) address: String,
    pub(crate) path: String,
    pub(crate) namespace: Option<String>,
    pub(crate) query: Vec<(String, String)>,
    pub(crate) headers: HeaderMap,
    pub(crate) auth: Option<Value>,
    pub(crate) auth_fn: Option<AuthFn>,
    pub(crate) params_fn: Option<ParamsFn>,
    /// The environment's proxy, an explicit one, or none.
    pub(crate) proxy: ProxyChoice,
    /// Built by `connect()`: one HTTP client (pool, cookies) for every attempt.
    pub(crate) http: Option<crate::engineio::http::Http>,
    #[cfg_attr(not(any(feature = "rustls", feature = "native-tls")), allow(dead_code))]
    pub(crate) tls_config: Option<TlsConfig>,
    pub(crate) transport: TransportType,
    pub(crate) timeout: Duration,
    pub(crate) reconnection: bool,
    pub(crate) reconnection_attempts: Option<u32>,
    pub(crate) reconnection_delay: Duration,
    pub(crate) reconnection_delay_max: Duration,
    pub(crate) randomization_factor: f64,
    pub(crate) reconnect_on_disconnect: bool,
    pub(crate) buffer_while_disconnected: bool,
    pub(crate) redact_query: bool,
    pub(crate) inspector: Option<Inspector>,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("path", &self.path)
            .field("namespace", &self.namespace)
            .field(
                "query_keys",
                &self.query.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            )
            .field("auth", &self.auth.as_ref().map(|_| "***"))
            .field("transport", &self.transport)
            .field("timeout", &self.timeout)
            .field("reconnection", &self.reconnection)
            .field("reconnection_attempts", &self.reconnection_attempts)
            .field("reconnection_delay", &self.reconnection_delay)
            .field("reconnection_delay_max", &self.reconnection_delay_max)
            .field("randomization_factor", &self.randomization_factor)
            .field("reconnect_on_disconnect", &self.reconnect_on_disconnect)
            .field("buffer_while_disconnected", &self.buffer_while_disconnected)
            .field("proxy", &!matches!(self.proxy, ProxyChoice::Env))
            .finish_non_exhaustive()
    }
}

/// Configures a [`Client`]. Defaults follow socket.io-client 4.8.
///
/// ```no_run
/// use std::time::Duration;
/// use socketio_next::{ClientBuilder, Event, Client};
/// use serde_json::json;
///
/// # async fn run() -> socketio_next::Result<()> {
/// let client = ClientBuilder::new("http://localhost:4200")
///     .namespace("/admin")
///     .query("token", "s3cr3t")
///     .auth(json!({ "user": "me" }))
///     .on("chat", |event: Event, _client: Client| async move {
///         println!("chat: {:?}", event.payload());
///     })
///     .connect()
///     .await?;
/// client.emit("chat", "hello")?;
/// let reply = client.emit_with_ack("sum", vec![json!(1), json!(2)], Duration::from_secs(5)).await?;
/// client.disconnect().await?;
/// # Ok(()) }
/// ```
pub struct ClientBuilder {
    pub(crate) config: Config,
    pub(crate) handlers: Handlers,
    raw_headers: Vec<(String, String)>,
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("config", &self.config)
            .field("handlers", &self.handlers)
            .field(
                "headers",
                &self.raw_headers.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl ClientBuilder {
    /// A builder for the server at `address` (`http`, `https`, `ws` or `wss`).
    ///
    /// Like socket.io-client, the URL path is the **namespace** (`http://host/admin` joins
    /// `/admin`); the engine.io path is [`path`](Self::path) (default `/socket.io/`).
    pub fn new(address: impl Into<String>) -> Self {
        ClientBuilder {
            config: Config {
                address: address.into(),
                path: "/socket.io/".to_owned(),
                namespace: None,
                query: Vec::new(),
                headers: HeaderMap::new(),
                auth: None,
                auth_fn: None,
                params_fn: None,
                proxy: ProxyChoice::Env,
                http: None,
                tls_config: None,
                transport: TransportType::Any,
                timeout: Duration::from_secs(20),
                reconnection: true,
                reconnection_attempts: None,
                reconnection_delay: Duration::from_millis(1000),
                reconnection_delay_max: Duration::from_millis(5000),
                randomization_factor: DEFAULT_JITTER,
                reconnect_on_disconnect: false,
                buffer_while_disconnected: false,
                redact_query: true,
                inspector: None,
            },
            handlers: Handlers::default(),
            raw_headers: Vec::new(),
        }
    }

    /// The namespace to join (overrides the URL path). A missing leading `/` is added.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        let mut nsp = namespace.into();
        if !nsp.starts_with('/') {
            nsp.insert(0, '/');
        }
        self.config.namespace = Some(nsp);
        self
    }

    /// The engine.io path on the server (socket.io-client `path`, default `/socket.io/`).
    pub fn path(mut self, path: impl Into<String>) -> Self {
        let mut path = path.into();
        if !path.starts_with('/') {
            path.insert(0, '/');
        }
        if !path.ends_with('/') {
            path.push('/');
        }
        self.config.path = path;
        self
    }

    /// Adds a query parameter to every request (socket.io-client `query`). Values are
    /// percent-encoded and, by default, shown as `***` in logs (see
    /// [`redact_query`](Self::redact_query)).
    pub fn query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.query.push((key.into(), value.into()));
        self
    }

    /// Adds an HTTP header to every polling request and the websocket handshake
    /// (socket.io-client `extraHeaders`). Fails at `connect()` when it is not valid HTTP.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.raw_headers.push((key.into(), value.into()));
        self
    }

    /// The CONNECT payload (socket.io-client `auth` object), read by the server as
    /// `socket.handshake.auth`. Must be a JSON object (`connect()` fails with
    /// [`Error::InvalidAuth`] otherwise).
    pub fn auth(mut self, auth: impl Into<Value>) -> Self {
        self.config.auth = Some(auth.into());
        self
    }

    /// Computes the CONNECT payload before every connect and reconnect (socket.io-client
    /// `auth` as a function), e.g. to refresh a token. Overrides [`auth`](Self::auth).
    pub fn auth_with<F, Fut>(mut self, auth: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Value> + Send + 'static,
    {
        self.config.auth_fn = Some(Arc::new(move || auth().boxed()));
        self
    }

    /// Computes query parameters and headers before every connect and reconnect, e.g. a
    /// signed timestamp or a fresh bearer token in `Authorization`. Runs inside the attempt's
    /// timeout; a panic fails the attempt. Values replace the builder's ones of the same key.
    pub fn connect_params_with<F, Fut>(mut self, params: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ConnectParams> + Send + 'static,
    {
        self.config.params_fn = Some(Arc::new(move || params().boxed()));
        self
    }

    /// TLS for `https`/`wss`: a [`TlsConfig`], a `rustls::ClientConfig` or a
    /// `native_tls::TlsConnector`. Default: the backend's default roots.
    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "rustls", feature = "native-tls"))))]
    pub fn tls_config(mut self, tls_config: impl Into<TlsConfig>) -> Self {
        self.config.tls_config = Some(tls_config.into());
        self
    }

    /// Sends polling requests and the websocket through the HTTP proxy `proxy`
    /// (`http://[user:password@]host:port`; the websocket goes through a `CONNECT` tunnel with
    /// basic auth from the user info). Without it the environment is used: `HTTPS_PROXY` /
    /// `HTTP_PROXY` / `ALL_PROXY` and `NO_PROXY`, for polling and websocket alike.
    pub fn proxy(mut self, proxy: impl Into<String>) -> Self {
        self.config.proxy = ProxyChoice::Url(proxy.into());
        self
    }

    /// No proxy for any transport, whatever `HTTPS_PROXY` / `HTTP_PROXY` say (environment
    /// proxies apply to `ws://` too; a proxy that refuses `CONNECT` to other ports than 443
    /// would otherwise break websockets; with `TransportType::Any` the client then stays on
    /// polling after the failed upgrade).
    pub fn no_proxy(mut self) -> Self {
        self.config.proxy = ProxyChoice::Off;
        self
    }

    /// Which engine.io transports to use (default: polling, upgraded to websocket, polling when
    /// the websocket fails).
    pub fn transport_type(mut self, transport: TransportType) -> Self {
        self.config.transport = transport;
        self
    }

    /// Bound for one connect attempt: engine.io handshake, upgrade and namespace CONNECT
    /// (socket.io-client `timeout`, default 20 s).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    /// Reconnect after a lost connection (socket.io-client `reconnection`, default on).
    pub fn reconnection(mut self, reconnection: bool) -> Self {
        self.config.reconnection = reconnection;
        self
    }

    /// At most this many reconnect attempts per outage (socket.io-client
    /// `reconnectionAttempts`, default unlimited). Then the client closes with
    /// [`DisconnectReason::ReconnectFailed`](crate::DisconnectReason::ReconnectFailed).
    pub fn reconnection_attempts(mut self, attempts: u32) -> Self {
        self.config.reconnection_attempts = Some(attempts);
        self
    }

    /// First reconnect delay (socket.io-client `reconnectionDelay`, default 1 s). Attempt `n`
    /// waits `delay * 2^(n-1)`, randomised, capped by
    /// [`reconnection_delay_max`](Self::reconnection_delay_max). The first attempt waits too.
    pub fn reconnection_delay(mut self, delay: Duration) -> Self {
        self.config.reconnection_delay = delay;
        self
    }

    /// Longest reconnect delay (socket.io-client `reconnectionDelayMax`, default 5 s).
    pub fn reconnection_delay_max(mut self, delay: Duration) -> Self {
        self.config.reconnection_delay_max = delay;
        self
    }

    /// Randomises each delay by up to ± this fraction (socket.io-client `randomizationFactor`,
    /// default 0.5; 0 disables).
    pub fn randomization_factor(mut self, factor: f64) -> Self {
        self.config.randomization_factor = factor;
        self
    }

    /// Also reconnect after the server disconnected the socket (socket.io-client does not;
    /// default off).
    pub fn reconnect_on_disconnect(mut self, reconnect: bool) -> Self {
        self.config.reconnect_on_disconnect = reconnect;
        self
    }

    /// Queue emits while disconnected and send them after the next connect, like
    /// socket.io-client's `sendBuffer` (default off: `emit` fails fast with
    /// [`Error::NotConnected`]). Pending acks keep their timeout.
    pub fn buffer_while_disconnected(mut self, buffer: bool) -> Self {
        self.config.buffer_while_disconnected = buffer;
        self
    }

    /// Show query values as `***` in logs and `Debug` output (default on; the engine.io
    /// parameters stay visible). Errors never contain the URL either way.
    pub fn redact_query(mut self, redact: bool) -> Self {
        self.config.redact_query = redact;
        self
    }

    /// Sees every socket.io packet in and out, after decoding / before encoding. For debugging;
    /// runs on the connection task, so keep it quick.
    pub fn inspect_packets<F>(mut self, inspector: F) -> Self
    where
        F: Fn(Direction, &Packet) + Send + Sync + 'static,
    {
        self.config.inspector = Some(Arc::new(inspector));
        self
    }

    /// Handles `event` (see [`Handler`] for the closure forms).
    pub fn on<M>(mut self, event: impl Into<String>, handler: impl Handler<Event, M>) -> Self {
        self.handlers.on(event.into(), handler.into_boxed(), false);
        self
    }

    /// Handles the next `event` only.
    pub fn once<M>(mut self, event: impl Into<String>, handler: impl Handler<Event, M>) -> Self {
        self.handlers.on(event.into(), handler.into_boxed(), true);
        self
    }

    /// Handles every event (socket.io-client `onAny`), before the event's own handlers.
    pub fn on_any<M>(mut self, handler: impl Handler<Event, M>) -> Self {
        self.handlers.on_any(handler.into_boxed());
        self
    }

    /// Sees every [`ConnectionState`] transition, in order with the events.
    pub fn on_state_change<M>(mut self, handler: impl Handler<ConnectionState, M>) -> Self {
        self.handlers.on_state(handler.into_boxed());
        self
    }

    /// The engine.io URL `connect()` will use (before `connect_params_with`): the address with
    /// its path replaced by [`path`](Self::path) (the URL's path is the namespace) and the
    /// query added, normalized the way the connection normalizes it (`/v1/../x/` is `/x/`).
    /// For checking where a connection goes, e.g. against an allow-list.
    pub fn engine_url(&self) -> Result<url::Url> {
        Ok(crate::client::resolve(&self.config)?.0)
    }

    /// Connects and joins the namespace. Resolves once the server answered CONNECT; fails with
    /// the error of this first attempt (no retry: reconnects start after a connection existed).
    pub async fn connect(mut self) -> Result<Client> {
        if self.config.auth.as_ref().is_some_and(|a| !a.is_object()) {
            return Err(Error::InvalidAuth("the auth payload must be a JSON object"));
        }
        for (key, value) in std::mem::take(&mut self.raw_headers) {
            let name = HeaderName::try_from(key.as_str())
                .map_err(|_| Error::InvalidHeader(format!("bad header name {key:?}")))?;
            let value = HeaderValue::try_from(value.as_str())
                .map_err(|_| Error::InvalidHeader(format!("bad value for header {key:?}")))?;
            self.config.headers.append(name, value);
        }
        use crate::engineio::http::ProxyMode;
        let proxy = match &self.config.proxy {
            ProxyChoice::Url(proxy) => {
                ProxyMode::Explicit(url::Url::parse(proxy).map_err(|_| {
                    crate::engineio::Error::ProxyFailed("the proxy is not a valid URL".into())
                })?)
            }
            ProxyChoice::Env => ProxyMode::Env,
            ProxyChoice::Off => ProxyMode::Off,
        };
        self.config.http = Some(crate::engineio::http::Http::new(
            self.config.tls_config.as_ref(),
            proxy,
        )?);
        crate::client::connect(self.config, self.handlers).await
    }
}
