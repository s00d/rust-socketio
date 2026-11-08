//! The client without async: the same [`crate::Client`] driven by a private tokio runtime
//! (one worker thread), with blocking methods and plain-closure handlers.
//!
//! ```no_run
//! use std::time::Duration;
//! use socketio_next::blocking::{Client, ClientBuilder};
//! use socketio_next::Event;
//!
//! let client = ClientBuilder::new("http://localhost:3000")
//!     .on("news", |event: Event, client: Client| {
//!         println!("news: {:?}", event.payload());
//!         client.emit("thanks", ()).ok();
//!     })
//!     .connect()?;
//! let reply = client.emit_with_ack("ping", (), Duration::from_secs(5))?;
//! client.disconnect()?;
//! # Ok::<(), socketio_next::Error>(())
//! ```
//!
//! The blocking methods must not be called from inside an async runtime (tokio panics on a
//! nested `block_on`); handlers run on blocking threads, where they are fine.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::runtime::{Handle, Runtime};

#[cfg(any(feature = "rustls", feature = "native-tls"))]
use crate::engineio::TlsConfig;
use crate::handler::ListenerId;
use crate::{
    ConnectionState, Direction, Event, LinkInfo, Payload, Result, TransportType, protocol,
};

/// Owns the runtime; shuts it down without blocking (safe from any thread).
struct RuntimeOwner(Option<Runtime>);

impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// Builder of the blocking [`Client`]; the options of [`crate::ClientBuilder`].
pub struct ClientBuilder {
    inner: crate::ClientBuilder,
    runtime: Arc<RuntimeOwner>,
    handle: Handle,
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

macro_rules! delegate {
    ($($(#[$meta:meta])* fn $name:ident($($arg:ident: $ty:ty),*);)*) => {$(
        $(#[$meta])*
        pub fn $name(self, $($arg: $ty),*) -> Self {
            ClientBuilder { inner: self.inner.$name($($arg),*), ..self }
        }
    )*};
}

impl ClientBuilder {
    /// A builder for `address` (see [`crate::ClientBuilder::new`]). Starts the runtime thread.
    ///
    /// # Panics
    /// When the OS refuses to start a thread.
    pub fn new(address: impl Into<String>) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("socketio-next")
            .enable_all()
            .build()
            .expect("start the socketio-next runtime");
        let handle = runtime.handle().clone();
        ClientBuilder {
            inner: crate::ClientBuilder::new(address),
            runtime: Arc::new(RuntimeOwner(Some(runtime))),
            handle,
        }
    }

    delegate! {
        /// See [`crate::ClientBuilder::namespace`].
        fn namespace(namespace: impl Into<String>);
        /// See [`crate::ClientBuilder::path`].
        fn path(path: impl Into<String>);
        /// See [`crate::ClientBuilder::query`].
        fn query(key: impl Into<String>, value: impl Into<String>);
        /// See [`crate::ClientBuilder::header`].
        fn header(key: impl Into<String>, value: impl Into<String>);
        /// See [`crate::ClientBuilder::auth`].
        fn auth(auth: impl Into<Value>);
        /// See [`crate::ClientBuilder::transport_type`].
        fn transport_type(transport: TransportType);
        /// See [`crate::ClientBuilder::timeout`].
        fn timeout(timeout: Duration);
        /// See [`crate::ClientBuilder::reconnection`].
        fn reconnection(reconnection: bool);
        /// See [`crate::ClientBuilder::reconnection_attempts`].
        fn reconnection_attempts(attempts: u32);
        /// See [`crate::ClientBuilder::reconnection_delay`].
        fn reconnection_delay(delay: Duration);
        /// See [`crate::ClientBuilder::reconnection_delay_max`].
        fn reconnection_delay_max(delay: Duration);
        /// See [`crate::ClientBuilder::randomization_factor`].
        fn randomization_factor(factor: f64);
        /// See [`crate::ClientBuilder::reconnect_on_disconnect`].
        fn reconnect_on_disconnect(reconnect: bool);
        /// See [`crate::ClientBuilder::buffer_while_disconnected`].
        fn buffer_while_disconnected(buffer: bool);
        /// See [`crate::ClientBuilder::redact_query`].
        fn redact_query(redact: bool);
    }

    /// See [`crate::ClientBuilder::tls_config`].
    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    pub fn tls_config(self, tls_config: impl Into<TlsConfig>) -> Self {
        ClientBuilder {
            inner: self.inner.tls_config(tls_config),
            ..self
        }
    }

    /// See [`crate::ClientBuilder::auth_with`]; `auth` runs on a blocking thread.
    pub fn auth_with<F>(self, auth: F) -> Self
    where
        F: Fn() -> Value + Send + Sync + 'static,
    {
        let auth = Arc::new(auth);
        let inner = self.inner.auth_with(move || {
            let auth = Arc::clone(&auth);
            async move {
                tokio::task::spawn_blocking(move || auth())
                    .await
                    .unwrap_or(Value::Null)
            }
        });
        ClientBuilder { inner, ..self }
    }

    /// See [`crate::ClientBuilder::inspect_packets`].
    pub fn inspect_packets<F>(self, inspector: F) -> Self
    where
        F: Fn(Direction, &protocol::socketio::Packet) + Send + Sync + 'static,
    {
        ClientBuilder {
            inner: self.inner.inspect_packets(inspector),
            ..self
        }
    }

    /// Handles `event` (on a blocking thread, in arrival order).
    pub fn on<F>(self, event: impl Into<String>, handler: F) -> Self
    where
        F: Fn(Event, Client) + Send + Sync + 'static,
    {
        let handle = self.handle.clone();
        ClientBuilder {
            inner: self.inner.on(event, wrap(handle, handler)),
            ..self
        }
    }

    /// Handles the next `event` only.
    pub fn once<F>(self, event: impl Into<String>, handler: F) -> Self
    where
        F: Fn(Event, Client) + Send + Sync + 'static,
    {
        let handle = self.handle.clone();
        ClientBuilder {
            inner: self.inner.once(event, wrap(handle, handler)),
            ..self
        }
    }

    /// Handles every event.
    pub fn on_any<F>(self, handler: F) -> Self
    where
        F: Fn(Event, Client) + Send + Sync + 'static,
    {
        let handle = self.handle.clone();
        ClientBuilder {
            inner: self.inner.on_any(wrap(handle, handler)),
            ..self
        }
    }

    /// Sees every state transition.
    pub fn on_state_change<F>(self, handler: F) -> Self
    where
        F: Fn(ConnectionState, Client) + Send + Sync + 'static,
    {
        let handle = self.handle.clone();
        ClientBuilder {
            inner: self.inner.on_state_change(wrap(handle, handler)),
            ..self
        }
    }

    /// Connects (see [`crate::ClientBuilder::connect`]), blocking until CONNECT or failure.
    pub fn connect(self) -> Result<Client> {
        let inner = self.handle.block_on(self.inner.connect())?;
        Ok(Client {
            inner,
            handle: self.handle,
            _runtime: Some(self.runtime),
        })
    }
}

fn wrap<A, F>(handle: Handle, handler: F) -> impl Fn(A, crate::Client) + Send + Sync + 'static
where
    A: Send + 'static,
    F: Fn(A, Client) + Send + Sync + 'static,
{
    move |arg, inner| {
        handler(
            arg,
            Client {
                inner,
                handle: handle.clone(),
                _runtime: None,
            },
        )
    }
}

/// The blocking client. Cheap to clone; dropping the last clone the user holds closes the
/// connection and stops the runtime.
#[derive(Clone)]
pub struct Client {
    inner: crate::Client,
    handle: Handle,
    _runtime: Option<Arc<RuntimeOwner>>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Client").field(&self.inner).finish()
    }
}

impl Client {
    /// The async client behind this one.
    pub fn as_async(&self) -> &crate::Client {
        &self.inner
    }

    /// See [`crate::Client::emit`] (never blocks).
    pub fn emit(&self, event: impl AsRef<str>, data: impl Into<Payload>) -> Result<()> {
        self.inner.emit(event, data)
    }

    /// See [`crate::Client::emit_with_ack`]; blocks until the ack, the timeout or the loss.
    pub fn emit_with_ack(
        &self,
        event: impl AsRef<str>,
        data: impl Into<Payload>,
        timeout: Duration,
    ) -> Result<Payload> {
        self.handle
            .block_on(self.inner.emit_with_ack(event, data, timeout))
    }

    /// See [`crate::Client::disconnect`]; blocks (bounded).
    pub fn disconnect(&self) -> Result<()> {
        self.handle.block_on(self.inner.disconnect())
    }

    /// Adds a handler for `event`.
    pub fn on<F>(&self, event: impl Into<String>, handler: F) -> ListenerId
    where
        F: Fn(Event, Client) + Send + Sync + 'static,
    {
        self.inner.on(event, wrap(self.handle.clone(), handler))
    }

    /// Adds a handler for the next `event`.
    pub fn once<F>(&self, event: impl Into<String>, handler: F) -> ListenerId
    where
        F: Fn(Event, Client) + Send + Sync + 'static,
    {
        self.inner.once(event, wrap(self.handle.clone(), handler))
    }

    /// Removes every handler of `event`.
    pub fn off(&self, event: &str) -> usize {
        self.inner.off(event)
    }

    /// Removes one handler.
    pub fn off_listener(&self, id: ListenerId) -> bool {
        self.inner.off_listener(id)
    }

    /// Number of handlers of `event`.
    pub fn listener_count(&self, event: &str) -> usize {
        self.inner.listener_count(event)
    }

    /// The current state.
    pub fn state(&self) -> ConnectionState {
        self.inner.state()
    }

    /// Blocks until the state matches `predicate`, the client closed, or `timeout` passed;
    /// returns the state then.
    pub fn wait_for(
        &self,
        predicate: impl Fn(&ConnectionState) -> bool,
        timeout: Duration,
    ) -> ConnectionState {
        self.handle.block_on(async {
            tokio::time::timeout(timeout, self.inner.wait_for(predicate))
                .await
                .unwrap_or_else(|_| self.inner.state())
        })
    }

    /// The namespace is joined right now.
    pub fn connected(&self) -> bool {
        self.inner.connected()
    }

    /// The socket id while connected.
    pub fn id(&self) -> Option<String> {
        self.inner.id()
    }

    /// The namespace.
    pub fn namespace(&self) -> &str {
        self.inner.namespace()
    }

    /// A snapshot of the link.
    pub fn link_info(&self) -> LinkInfo {
        self.inner.link_info()
    }
}
