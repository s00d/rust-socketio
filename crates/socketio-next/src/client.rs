use std::cell::Cell;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot, watch};

use crate::actor::{Actor, Command, Cut, Dispatch, Link};
use crate::builder::Config;
use crate::handler::{Handler, Handlers, ListenerId};
use crate::protocol::socketio::{Packet, is_reserved_event};
use crate::state::LinkInfo;
use crate::{ConnectionState, Error, Event, Payload, Result};

tokio::task_local! {
    /// The client (its [`Client::key`]) whose handler the event task is running.
    static IN_HANDLER_TASK: usize;
}

thread_local! {
    /// The client whose sync handler this blocking-pool thread is running (0: none).
    pub(crate) static IN_HANDLER_THREAD: Cell<usize> = const { Cell::new(0) };
}

/// How long `disconnect()` waits for the handlers of what was received before it returns.
const HANDLER_DRAIN: Duration = Duration::from_secs(5);

/// What the handles of one client share. The connection task owns the client's life (its
/// sessions, the state, the link snapshot); the handles reach it through `cmd`.
pub(crate) struct Inner {
    /// The connection task's commands, for the client's whole life.
    cmd: mpsc::UnboundedSender<Command>,
    /// Dropped with the last handle: tells the connection task to stop.
    _shutdown: oneshot::Sender<()>,
    state: watch::Receiver<ConnectionState>,
    link: Link,
    handlers: Mutex<Handlers>,
    nsp: Arc<str>,
    buffer_while_disconnected: bool,
}

/// A connected socket.io client (one namespace). Cheap to clone; all clones are one client.
///
/// Dropping the last clone closes the connection. Handlers get a clone for the duration of
/// the call. A handler that keeps a `Client` of its own (captured in the closure) keeps the
/// client alive for good, since the client owns its handlers: capture a [`WeakClient`]
/// ([`downgrade`](Self::downgrade)) instead.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

/// A [`Client`] that does not keep the connection open: for handlers and other places that
/// the client itself (directly or not) owns. [`upgrade`](Self::upgrade) gives the client back
/// while a `Client` exists elsewhere.
#[derive(Clone)]
pub struct WeakClient {
    inner: Weak<Inner>,
}

impl WeakClient {
    /// The client, unless every `Client` was dropped (the connection is closed then).
    pub fn upgrade(&self) -> Option<Client> {
        self.inner.upgrade().map(|inner| Client { inner })
    }
}

impl fmt::Debug for WeakClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakClient")
            .field("alive", &(self.inner.strong_count() > 0))
            .finish()
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("nsp", &self.inner.nsp)
            .field("state", &*self.inner.state.borrow())
            .finish_non_exhaustive()
    }
}

/// Starts the connection task and the event task; resolves with the first connect attempt.
pub(crate) async fn connect(config: Config, handlers: Handlers) -> Result<Client> {
    let (url, nsp) = resolve(&config)?;
    let nsp: Arc<str> = nsp.into();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (dispatch_tx, dispatch_rx) = mpsc::unbounded_channel();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (state_tx, state_rx) = watch::channel(ConnectionState::Connecting { attempt: 0 });
    let link = Link::default();
    let cut: Cut = Arc::new(AtomicU64::new(0));
    let inner = Arc::new(Inner {
        cmd: cmd_tx.clone(),
        _shutdown: shutdown_tx,
        state: state_rx,
        link: link.clone(),
        handlers: Mutex::new(handlers),
        nsp: Arc::clone(&nsp),
        buffer_while_disconnected: config.buffer_while_disconnected,
    });
    tokio::spawn(dispatch(
        dispatch_rx,
        Arc::downgrade(&inner),
        Arc::clone(&cut),
    ));
    let panic_state = state_tx.clone();
    let panic_dispatch = dispatch_tx.clone();
    let actor = Actor::new(
        config,
        url,
        nsp,
        cmd_rx,
        cmd_tx,
        shutdown_rx,
        state_tx,
        dispatch_tx,
        link,
        cut,
    );
    // A bug in the connection task must not leave the state at `Connected` forever.
    tokio::spawn(async move {
        if AssertUnwindSafe(actor.run(ready_tx))
            .catch_unwind()
            .await
            .is_err()
        {
            tracing::error!("the socket.io connection task panicked; the client is closed");
            let closed = ConnectionState::Closed {
                reason: crate::DisconnectReason::TransportError,
                error: None,
            };
            panic_state.send_replace(closed.clone());
            let _ = panic_dispatch.send(Dispatch::State(closed));
        }
    });
    match ready_rx.await {
        Ok(Ok(())) => Ok(Client { inner }),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(Error::Closed),
    }
}

/// The engine.io URL and the namespace: like socket.io-client, the URL path is the namespace
/// unless one is set explicitly; the engine.io path comes from the builder.
pub(crate) fn resolve(config: &Config) -> Result<(url::Url, String)> {
    let mut url = url::Url::parse(&config.address)?;
    let url_nsp = match url.path() {
        "" | "/" => None,
        path => Some(percent_decode(path)),
    };
    let nsp = config
        .namespace
        .clone()
        .or(url_nsp)
        .unwrap_or_else(|| "/".to_owned());
    url.set_path(&config.path);
    if !config.query.is_empty() {
        url.query_pairs_mut().extend_pairs(&config.query);
    }
    Ok((url, nsp))
}

/// `%XX` escapes of a URL path back to UTF-8 (the namespace is compared as text).
fn percent_decode(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Runs handlers one at a time, in order, for the client's whole life (one task across
/// reconnects). Holds only a weak reference, so it never keeps the client alive.
///
/// ponytail: the queue is unbounded (like socket.io-client's event emitter); a server that
/// floods events faster than slow handlers finish grows memory. Add a bound if that matters.
async fn dispatch(mut rx: mpsc::UnboundedReceiver<Dispatch>, inner: Weak<Inner>, cut: Cut) {
    while let Some(item) = rx.recv().await {
        let Some(strong) = inner.upgrade() else {
            break;
        };
        let client = Client { inner: strong };
        let key = client.key();
        match item {
            Dispatch::Event { session, event } => {
                let handlers = client.handlers().take_for_event(event.name());
                for handler in handlers {
                    // a disconnect() since the event arrived: no more handlers for it
                    if session < cut.load(Ordering::Acquire) {
                        break;
                    }
                    run(key, || handler(event.clone(), client.clone())).await;
                }
            }
            Dispatch::State(state) => {
                let handlers = client.handlers().state_handlers();
                for handler in handlers {
                    run(key, || handler(state.clone(), client.clone())).await;
                }
            }
            Dispatch::Fence(reached) => {
                let _ = reached.send(());
            }
        }
    }
}

/// One handler call of client `key`; a panic is logged and does not stop the event task. The
/// call itself is inside the guard too: an async handler's closure body runs while its future
/// is built.
async fn run(key: usize, call: impl FnOnce() -> futures_util::future::BoxFuture<'static, ()>) {
    let guarded = async move {
        let fut = std::panic::catch_unwind(AssertUnwindSafe(call)).map_err(|_| ())?;
        AssertUnwindSafe(fut).catch_unwind().await.map_err(|_| ())
    };
    if IN_HANDLER_TASK.scope(key, guarded).await.is_err() {
        tracing::error!("an event handler panicked");
    }
}

impl Client {
    /// A handle that does not keep the client alive.
    pub fn downgrade(&self) -> WeakClient {
        WeakClient {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Identifies this client among others (handlers of client A may drive client B).
    pub(crate) fn key(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }

    /// Called from one of this client's own handlers.
    fn in_own_handler(&self) -> bool {
        let key = self.key();
        IN_HANDLER_TASK.try_with(|k| *k == key).unwrap_or(false)
            || IN_HANDLER_THREAD.with(Cell::get) == key
    }

    /// Connects again after the client closed (a `ConnectRejected`, `ReconnectFailed`, a
    /// server disconnect without `reconnect_on_disconnect`, or `disconnect()`), with the same
    /// handlers and options; `auth_with` and `connect_params_with` run again, so a new token
    /// can be handed in first. Resolves like `connect()`: once the server accepted the
    /// namespace, or with the error of that attempt (the client then stays closed). A no-op
    /// while the client is not closed. `reconnect()` and `disconnect()` take effect in the
    /// order they are called.
    pub async fn reconnect(&self) -> Result<()> {
        let (ready, rx) = oneshot::channel();
        self.inner
            .cmd
            .send(Command::Reconnect { ready })
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    fn handlers(&self) -> std::sync::MutexGuard<'_, Handlers> {
        self.inner
            .handlers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn event_packet(&self, event: &str, data: Payload) -> Result<Packet> {
        if is_reserved_event(event) {
            return Err(Error::ReservedEvent(event.to_owned()));
        }
        Ok(Packet::new_event(data, event, &self.inner.nsp, None)?)
    }

    fn check_open(&self) -> Result<()> {
        match &*self.inner.state.borrow() {
            ConnectionState::Closed { .. } => Err(Error::Closed),
            ConnectionState::Connected { .. } => Ok(()),
            _ if self.inner.buffer_while_disconnected => Ok(()),
            _ => Err(Error::NotConnected),
        }
    }

    /// Sends `event` with `data` ([`Payload`], `serde_json::Value`, a string, bytes, `()` for
    /// no arguments). Never blocks: the packet is queued for the connection task.
    ///
    /// Fails with [`Error::NotConnected`] while disconnected (unless
    /// `buffer_while_disconnected`), [`Error::Closed`] after the client closed, and
    /// [`Error::ReservedEvent`] for `connect`, `disconnect`, ...
    pub fn emit(&self, event: impl AsRef<str>, data: impl Into<Payload>) -> Result<()> {
        let packet = self.event_packet(event.as_ref(), data.into())?;
        self.check_open()?;
        self.inner
            .cmd
            .send(Command::Emit { packet, ack: None })
            .map_err(|_| Error::Closed)
    }

    /// Sends `event` and waits for the server's ack (socket.io-client
    /// `socket.timeout(ms).emitWithAck(...)`). Resolves with the ack's arguments, or fails with
    /// [`Error::AckTimeout`] after `timeout`, [`Error::Disconnected`] when the connection drops
    /// first. Dropping the future forgets the ack.
    pub async fn emit_with_ack(
        &self,
        event: impl AsRef<str>,
        data: impl Into<Payload>,
        timeout: Duration,
    ) -> Result<Payload> {
        let packet = self.event_packet(event.as_ref(), data.into())?;
        self.check_open()?;
        let (tx, rx) = oneshot::channel();
        self.inner
            .cmd
            .send(Command::Emit {
                packet,
                ack: Some((tx, timeout)),
            })
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Adds a handler for `event`; returns its id for [`off_listener`](Self::off_listener).
    pub fn on<M>(&self, event: impl Into<String>, handler: impl Handler<Event, M>) -> ListenerId {
        self.handlers()
            .on(event.into(), handler.into_boxed(), false)
    }

    /// Adds a handler for the next `event` only.
    pub fn once<M>(&self, event: impl Into<String>, handler: impl Handler<Event, M>) -> ListenerId {
        self.handlers().on(event.into(), handler.into_boxed(), true)
    }

    /// Adds a handler for every event (socket.io-client `onAny`).
    pub fn on_any<M>(&self, handler: impl Handler<Event, M>) -> ListenerId {
        self.handlers().on_any(handler.into_boxed())
    }

    /// Adds a handler for every [`ConnectionState`] transition.
    pub fn on_state_change<M>(&self, handler: impl Handler<ConnectionState, M>) -> ListenerId {
        self.handlers().on_state(handler.into_boxed())
    }

    /// Removes every handler of `event`; returns how many.
    pub fn off(&self, event: &str) -> usize {
        self.handlers().off(event)
    }

    /// Removes every catch-all handler; returns how many.
    pub fn off_any(&self) -> usize {
        self.handlers().off_any()
    }

    /// Removes one handler; `false` when it was gone already (a `once` handler that ran).
    pub fn off_listener(&self, id: ListenerId) -> bool {
        self.handlers().off_listener(id)
    }

    /// Number of handlers of `event` (socket.io-client `listeners(event).length`).
    pub fn listener_count(&self, event: &str) -> usize {
        self.handlers().listener_count(event)
    }

    /// The current state.
    pub fn state(&self) -> ConnectionState {
        self.inner.state.borrow().clone()
    }

    /// A watch of the state: `changed().await` wakes on transitions (a slow reader sees the
    /// latest; use [`on_state_change`](Self::on_state_change) to see every one).
    pub fn state_watch(&self) -> watch::Receiver<ConnectionState> {
        self.inner.state.clone()
    }

    /// Waits until the state matches `predicate` (or the client closed).
    pub async fn wait_for(&self, predicate: impl Fn(&ConnectionState) -> bool) -> ConnectionState {
        let mut watch = self.inner.state.clone();
        loop {
            let state = watch.borrow_and_update().clone();
            if predicate(&state) || state.is_closed() {
                return state;
            }
            if watch.changed().await.is_err() {
                return watch.borrow().clone();
            }
        }
    }

    /// The namespace is joined right now.
    pub fn connected(&self) -> bool {
        self.inner.state.borrow().is_connected()
    }

    /// The socket id while connected (`socket.id`).
    pub fn id(&self) -> Option<String> {
        match &*self.inner.state.borrow() {
            ConnectionState::Connected { sid, .. } => Some(sid.clone()),
            _ => None,
        }
    }

    /// The namespace of this client.
    pub fn namespace(&self) -> &str {
        &self.inner.nsp
    }

    /// The last connect restored the session (connection state recovery, `socket.recovered`).
    pub fn recovered(&self) -> bool {
        matches!(
            self.state(),
            ConnectionState::Connected {
                recovered: true,
                ..
            }
        )
    }

    /// A snapshot of the link: transport, sids, heartbeat, last packet, attempts.
    pub fn link_info(&self) -> LinkInfo {
        self.inner.link.get().clone()
    }

    /// Leaves the namespace (DISCONNECT packet), closes the connection and stops reconnecting.
    ///
    /// Bounded: the goodbye (queued packets, DISCONNECT, engine.io close) gets at most 2 s on a
    /// dead link. No event handler starts for events received from now on (like
    /// socket.io-client, which stops emitting events once disconnected); the `Closed` state
    /// still reaches `on_state_change`. It waits up to 5 s for the handlers already running or
    /// queued before it. Safe to call from inside one of the client's handlers: it then does not
    /// wait for them.
    pub async fn disconnect(&self) -> Result<()> {
        let (done, rx) = oneshot::channel();
        if self.inner.cmd.send(Command::Disconnect { done }).is_err() {
            return Ok(()); // the connection task is gone: closed already
        }
        let Ok(fence) = rx.await else {
            return Ok(());
        };
        if !self.in_own_handler() && tokio::time::timeout(HANDLER_DRAIN, fence).await.is_err() {
            tracing::warn!("handlers still running {HANDLER_DRAIN:?} after disconnect()");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn percent_decode_paths() {
        assert_eq!(super::percent_decode("/%C3%A4dmin"), "/ädmin");
        assert_eq!(super::percent_decode("/a%2"), "/a%2");
        assert_eq!(super::percent_decode("/plain"), "/plain");
    }
}
