use std::cell::Cell;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::actor::{Actor, Command, Dispatch};
use crate::builder::Config;
use crate::handler::{Handler, Handlers, ListenerId};
use crate::protocol::socketio::{Packet, is_reserved_event};
use crate::state::LinkInfo;
use crate::{ConnectionState, Error, Event, Payload, Result};

tokio::task_local! {
    /// Set while the event task runs a handler.
    static IN_HANDLER_TASK: ();
}

thread_local! {
    /// Set while a blocking-pool thread runs a sync handler.
    pub(crate) static IN_HANDLER_THREAD: Cell<bool> = const { Cell::new(false) };
}

fn in_handler() -> bool {
    IN_HANDLER_TASK.try_with(|_| ()).is_ok() || IN_HANDLER_THREAD.with(Cell::get)
}

/// How long `disconnect()` waits for running handlers before it mutes the rest.
const HANDLER_DRAIN: Duration = Duration::from_secs(5);

pub(crate) struct Inner {
    /// The running actor's command queue (replaced by `reconnect()`).
    cmd: Mutex<mpsc::UnboundedSender<Command>>,
    /// Dropped with the last handle (or replaced): tells the actor to stop.
    _shutdown: Mutex<oneshot::Sender<()>>,
    state: watch::Receiver<ConnectionState>,
    state_tx: watch::Sender<ConnectionState>,
    config: Config,
    url: url::Url,
    /// One `reconnect()` at a time.
    restart: tokio::sync::Mutex<()>,
    handlers: Mutex<Handlers>,
    shared: Arc<Shared>,
    nsp: Arc<str>,
    buffer_while_disconnected: bool,
    actor: Mutex<Option<JoinHandle<()>>>,
    dispatcher: Mutex<Option<JoinHandle<()>>>,
}

/// State the actor writes and the handles read.
#[derive(Debug, Default)]
pub(crate) struct Shared {
    pub(crate) connected: AtomicBool,
    pub(crate) closed: AtomicBool,
    /// No handler runs once set (after `disconnect()` returned).
    pub(crate) muted: AtomicBool,
    pub(crate) link: Mutex<LinkInfo>,
}

impl Shared {
    pub(crate) fn link(&self) -> std::sync::MutexGuard<'_, LinkInfo> {
        self.link.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A connected socket.io client (one namespace). Cheap to clone; all clones are one client.
///
/// Dropping the last clone closes the connection. Handlers get a clone for the duration of
/// the call.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("nsp", &self.inner.nsp)
            .field("state", &*self.inner.state.borrow())
            .finish_non_exhaustive()
    }
}

pub(crate) async fn connect(config: Config, handlers: Handlers) -> Result<Client> {
    let (cmd_tx, _) = mpsc::unbounded_channel();
    let (shutdown_tx, _) = oneshot::channel();
    let (state_tx, state_rx) = watch::channel(ConnectionState::Connecting { attempt: 0 });
    let (url, nsp) = resolve(&config)?;
    let inner = Arc::new(Inner {
        cmd: Mutex::new(cmd_tx),
        _shutdown: Mutex::new(shutdown_tx),
        state: state_rx,
        state_tx,
        handlers: Mutex::new(handlers),
        shared: Arc::new(Shared::default()),
        nsp: nsp.into(),
        buffer_while_disconnected: config.buffer_while_disconnected,
        config,
        url,
        restart: tokio::sync::Mutex::new(()),
        actor: Mutex::default(),
        dispatcher: Mutex::default(),
    });
    let ready = start(&inner);
    match ready.await {
        Ok(Ok(())) => Ok(Client { inner }),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(Error::Closed),
    }
}

/// Spawns the event task and the connection task for `inner`; the receiver resolves with the
/// first connect attempt.
fn start(inner: &Arc<Inner>) -> oneshot::Receiver<Result<()>> {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (dispatch_tx, dispatch_rx) = mpsc::unbounded_channel();
    let (ready_tx, ready_rx) = oneshot::channel();
    inner
        .state_tx
        .send_replace(ConnectionState::Connecting { attempt: 0 });
    let shared = Arc::clone(&inner.shared);
    shared.closed.store(false, Ordering::Release);
    shared.muted.store(false, Ordering::Release);
    *inner.cmd.lock().unwrap_or_else(PoisonError::into_inner) = cmd_tx.clone();
    *inner
        ._shutdown
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = shutdown_tx;

    let dispatcher = tokio::spawn(dispatch(dispatch_rx, Arc::downgrade(inner)));
    let panic_state = inner.state_tx.clone();
    let actor = Actor::new(
        inner.config.clone(),
        inner.url.clone(),
        Arc::clone(&inner.nsp),
        cmd_rx,
        cmd_tx,
        shutdown_rx,
        inner.state_tx.clone(),
        dispatch_tx,
        Arc::clone(&shared),
    );
    // A bug in the connection task must not leave the state at `Connected` forever.
    let actor = tokio::spawn(async move {
        if AssertUnwindSafe(actor.run(ready_tx))
            .catch_unwind()
            .await
            .is_err()
        {
            tracing::error!("the socket.io connection task panicked; the client is closed");
            shared.connected.store(false, Ordering::Release);
            shared.closed.store(true, Ordering::Release);
            panic_state.send_replace(ConnectionState::Closed {
                reason: crate::DisconnectReason::TransportError,
                error: None,
            });
        }
    });
    *inner.actor.lock().unwrap_or_else(PoisonError::into_inner) = Some(actor);
    *inner
        .dispatcher
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(dispatcher);
    ready_rx
}

/// The engine.io URL and the namespace: like socket.io-client, the URL path is the namespace
/// unless one is set explicitly; the engine.io path comes from the builder.
fn resolve(config: &Config) -> Result<(url::Url, String)> {
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

/// Runs handlers one at a time, in order. Holds only a weak reference, so it never keeps the
/// client alive.
///
/// ponytail: the queue is unbounded (like socket.io-client's event emitter); a server that
/// floods events faster than slow handlers finish grows memory. Add a bound if that matters.
async fn dispatch(mut rx: mpsc::UnboundedReceiver<Dispatch>, inner: Weak<Inner>) {
    while let Some(item) = rx.recv().await {
        let Some(strong) = inner.upgrade() else {
            break;
        };
        if strong.shared.muted.load(Ordering::Acquire) {
            continue;
        }
        let client = Client {
            inner: Arc::clone(&strong),
        };
        drop(strong);
        match item {
            Dispatch::Event(event) => {
                let handlers = client.handlers().take_for_event(event.name());
                for handler in handlers {
                    if client.inner.shared.muted.load(Ordering::Acquire) {
                        break;
                    }
                    run(handler(event.clone(), client.clone())).await;
                }
            }
            Dispatch::State(state) => {
                let handlers = client.handlers().state_handlers();
                for handler in handlers {
                    if client.inner.shared.muted.load(Ordering::Acquire) {
                        break;
                    }
                    run(handler(state.clone(), client.clone())).await;
                }
            }
        }
    }
}

/// One handler call; a panic is logged and does not stop the event task.
async fn run(fut: futures_util::future::BoxFuture<'static, ()>) {
    if IN_HANDLER_TASK
        .scope((), AssertUnwindSafe(fut).catch_unwind())
        .await
        .is_err()
    {
        tracing::error!("an event handler panicked");
    }
}

impl Client {
    fn cmd(&self) -> mpsc::UnboundedSender<Command> {
        self.inner
            .cmd
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Connects again after the client closed (a `ConnectRejected`, `ReconnectFailed`, a
    /// server disconnect without `reconnect_on_disconnect`, or `disconnect()`), with the same
    /// handlers and options; `auth_with` and `connect_params_with` run again, so a new token
    /// can be handed in first. Resolves like `connect()`: once the server accepted the
    /// namespace, or with the error of that attempt (the client then stays closed). A no-op
    /// while the client is not closed. Must not be called from one of its handlers.
    pub async fn reconnect(&self) -> Result<()> {
        let _guard = self.inner.restart.lock().await;
        if !self.state().is_closed() {
            return Ok(());
        }
        // the previous tasks end on their own once closed; wait so none outlives the restart
        let actor = self
            .inner
            .actor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(actor) = actor {
            let _ = actor.await;
        }
        let dispatcher = self
            .inner
            .dispatcher
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(dispatcher) = dispatcher {
            if tokio::time::timeout(HANDLER_DRAIN, &mut Box::pin(dispatcher))
                .await
                .is_err()
            {
                tracing::warn!(
                    "handlers still running after {HANDLER_DRAIN:?}; reconnecting anyway"
                );
            }
        }
        match start(&self.inner).await {
            Ok(result) => result,
            Err(_) => Err(Error::Closed),
        }
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
        let shared = &self.inner.shared;
        if shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if !self.inner.buffer_while_disconnected && !shared.connected.load(Ordering::Acquire) {
            return Err(Error::NotConnected);
        }
        Ok(())
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
        self.cmd()
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
        self.cmd()
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
        self.inner.shared.connected.load(Ordering::Acquire)
    }

    /// The socket id while connected (`socket.id`).
    pub fn id(&self) -> Option<String> {
        self.inner.shared.link().sid.clone()
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
        self.inner.shared.link().clone()
    }

    /// Leaves the namespace (DISCONNECT packet), closes the connection and stops reconnecting.
    ///
    /// Bounded: the goodbye (queued packets, DISCONNECT, engine.io close) gets at most 2 s on a
    /// dead link. Events already received are still handled; handlers get up to 5 s in total,
    /// then the event task is stopped. When it returns no handler starts any more. Safe to call
    /// from inside a handler (it then does not wait for the handlers; the event task ends after
    /// the running handler).
    pub async fn disconnect(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        if self.cmd().send(Command::Disconnect { done: tx }).is_ok() {
            let _ = rx.await;
        }
        let actor = self
            .inner
            .actor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(actor) = actor {
            let _ = actor.await;
        }
        if !in_handler() {
            let dispatcher = self
                .inner
                .dispatcher
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(mut dispatcher) = dispatcher {
                if tokio::time::timeout(HANDLER_DRAIN, &mut dispatcher)
                    .await
                    .is_err()
                {
                    // a handler is stuck: stop the event task so nothing else starts
                    tracing::warn!(
                        "handlers still running after {HANDLER_DRAIN:?}; event task stopped"
                    );
                    dispatcher.abort();
                    let _ = dispatcher.await;
                }
            }
        }
        self.inner.shared.muted.store(true, Ordering::Release);
        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // The actor stops when `_shutdown` drops right after this; nothing may run any more.
        self.shared.muted.store(true, Ordering::Release);
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
