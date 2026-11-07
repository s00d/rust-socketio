use std::{
    ops::DerefMut,
    pin::Pin,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc, Mutex,
    },
};

use backoff::{backoff::Backoff, ExponentialBackoffBuilder};
use futures_util::{future::BoxFuture, stream, Stream, StreamExt};
use log::{error, trace};
use rust_engineio::header::{HeaderMap, HeaderValue};
use serde_json::Value;
use tokio::{
    sync::RwLock,
    task::JoinHandle,
    time::{sleep, Duration, Instant},
};

use super::{
    ack::Ack,
    builder::ClientBuilder,
    callback::{Callback, DynAsyncCallback},
};
use crate::{
    asynchronous::socket::Socket as InnerSocket,
    error::{Error, Result},
    packet::{Packet, PacketId},
    CloseReason, Event, Payload,
};

#[derive(Default)]
enum DisconnectReason {
    /// There is no known reason for the disconnect; likely a network error
    #[default]
    Unknown,
    /// The user disconnected manually
    Manual,
    /// The server disconnected
    Server,
    /// The server refused the namespace (CONNECT_ERROR): socket.io-client does not reconnect
    Rejected,
}

/// Settings that can be updated before reconnecting to a server
#[derive(Default)]
pub struct ReconnectSettings {
    address: Option<String>,
    auth: Option<serde_json::Value>,
    headers: Option<HeaderMap>,
}

impl ReconnectSettings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the URL that will be used when reconnecting to the server
    pub fn address<T>(&mut self, address: T) -> &mut Self
    where
        T: Into<String>,
    {
        self.address = Some(address.into());
        self
    }

    /// Sets the authentication data that will be send in the opening request
    pub fn auth(&mut self, auth: serde_json::Value) {
        self.auth = Some(auth);
    }

    /// Adds an http header to a container that is going to completely replace opening headers on reconnect.
    /// If there are no headers set in `ReconnectSettings`, client will use headers initially set via the builder.
    pub fn opening_header<T: Into<HeaderValue>, K: Into<String>>(
        &mut self,
        key: K,
        val: T,
    ) -> &mut Self {
        self.headers
            .get_or_insert_with(|| HeaderMap::default())
            .insert(key.into(), val.into());
        self
    }
}

/// The background task that drives the packet stream and the reconnect loop.
type PollTask = Arc<Mutex<Option<JoinHandle<()>>>>;

tokio::task_local! {
    /// Set inside a poll task to its client's [`PollTask`] address.
    static POLL_TASK: usize;
}

/// Held only by the handles returned to the user (and their clones), never by the poll task or
/// the clones it passes to callbacks: when the last of them is dropped, the poll task is aborted
/// and drops the engine with it, so a dropped client neither reconnects nor keeps a socket open.
struct PollTaskOwner(PollTask);

impl Drop for PollTaskOwner {
    fn drop(&mut self) {
        if let Some(task) = self.0.lock().unwrap_or_else(|e| e.into_inner()).take() {
            task.abort();
        }
    }
}

/// A socket which handles communication with the server. It's initialized with
/// a specific address as well as an optional namespace to connect to. If `None`
/// is given the client will connect to the default namespace `"/"`.
///
/// The client owns its background poll task (packet stream and reconnect loop): `disconnect()`
/// aborts and joins it, and dropping the last handle returned by `connect()` (or a clone of it)
/// aborts it, which closes the connection. Clones passed to callbacks do not keep it alive.
#[derive(Clone)]
pub struct Client {
    /// The inner socket client to delegate the methods to.
    socket: Arc<RwLock<InnerSocket>>,
    outstanding_acks: Arc<RwLock<Vec<Ack>>>,
    // next ack id; unique per client like socket.io-client's counter
    ack_id: Arc<AtomicI32>,
    // namespace, for multiplexing messages
    nsp: String,
    // Data send in the opening packet (commonly used as for auth)
    auth: Option<serde_json::Value>,
    builder: Arc<RwLock<ClientBuilder>>,
    disconnect_reason: Arc<RwLock<DisconnectReason>>,
    poll_task: PollTask,
    _owner: Option<Arc<PollTaskOwner>>,
}

impl Client {
    /// Creates a socket with a certain address to connect to as well as a
    /// namespace. If `None` is passed in as namespace, the default namespace
    /// `"/"` is taken.
    /// ```
    pub(crate) fn new(socket: InnerSocket, builder: ClientBuilder) -> Result<Self> {
        Ok(Client {
            socket: Arc::new(RwLock::new(socket)),
            nsp: builder.namespace.to_owned(),
            outstanding_acks: Arc::new(RwLock::new(Vec::new())),
            ack_id: Arc::new(AtomicI32::new(0)),
            auth: builder.auth.clone(),
            builder: Arc::new(RwLock::new(builder)),
            disconnect_reason: Arc::new(RwLock::new(DisconnectReason::default())),
            poll_task: Arc::default(),
            _owner: None,
        })
    }

    /// Connects the client to a server. Afterwards the `emit_*` methods can be
    /// called to interact with the server.
    pub(crate) async fn connect(&self) -> Result<()> {
        // Connect the underlying socket
        self.socket.read().await.connect().await?;

        // construct the opening packet
        let auth = self.auth.as_ref().map(|data| data.to_string());
        let open_packet = Packet::new(PacketId::Connect, self.nsp.clone(), auth, None, 0, None);

        self.socket.read().await.send(open_packet).await?;

        Ok(())
    }

    pub(crate) async fn reconnect(&mut self) -> Result<()> {
        let mut builder = self.builder.write().await;

        if let Some(config) = builder.on_reconnect.as_mut() {
            let reconnect_settings = config().await;

            if let Some(address) = reconnect_settings.address {
                builder.address = address;
            }

            if let Some(auth) = reconnect_settings.auth {
                self.auth = Some(auth);
            }

            if reconnect_settings.headers.is_some() {
                builder.opening_headers = reconnect_settings.headers;
            }
        }

        let socket = builder.inner_create().await?;

        // New inner socket that can be connected
        let mut client_socket = self.socket.write().await;
        *client_socket = socket;

        // Now that we have replaced `self.socket`, we drop the write lock
        // because the `connect` method we call below will need to use it
        drop(client_socket);

        self.connect().await?;

        Ok(())
    }

    /// Drives the stream using a thread so messages are processed
    pub(crate) async fn poll_stream(&mut self) -> Result<()> {
        let builder = self.builder.read().await;
        let reconnect_delay_min = builder.reconnect_delay_min;
        let reconnect_delay_max = builder.reconnect_delay_max;
        let max_reconnect_attempts = builder.max_reconnect_attempts;
        let reconnect = builder.reconnect;
        let reconnect_on_disconnect = builder.reconnect_on_disconnect;
        drop(builder);

        // The task's own handle must not own the task, or it would keep itself alive.
        let mut client_clone = Client {
            _owner: None,
            ..self.clone()
        };

        let task_key = Arc::as_ptr(&self.poll_task) as usize;
        let task = tokio::runtime::Handle::current().spawn(POLL_TASK.scope(task_key, async move {
            loop {
                let mut stream = client_clone.as_stream().await;
                // Consume the stream until it returns None and the stream is closed.
                while let Some(item) = stream.next().await {
                    if let Err(e) = item {
                        trace!("Network error occurred: {}", e);
                    }
                }

                // Drop the stream so we can once again use `socket_clone` as mutable
                drop(stream);

                // The stream also ends on an error (a packet that cannot be decoded, a transport
                // error) with the engine still open: close it, as socket.io-client does, so the
                // old connection does not linger until the server's ping timeout. Best effort and
                // bounded: on a dead link the Close packet would wait for the request timeout and
                // delay the reconnect.
                let close = async { client_clone.socket.read().await.disconnect().await };
                match tokio::time::timeout(Duration::from_secs(1), close).await {
                    Ok(Err(err)) => {
                        trace!("Closing the engine after the stream ended failed: {err}")
                    }
                    Err(_) => trace!("Closing the engine after the stream ended timed out"),
                    Ok(Ok(())) => {}
                }

                let should_reconnect = match *(client_clone.disconnect_reason.read().await) {
                    DisconnectReason::Unknown => {
                        // If we disconnected for an unknown reason, the client might not have noticed
                        // the closure yet. Hence, fire a transport close event to notify it.
                        // We don't need to do that in the other cases, since proper server close
                        // and manual client close are handled explicitly.
                        if let Some(err) = client_clone
                            .callback(&Event::Close, CloseReason::TransportClose.as_str())
                            .await
                            .err()
                        {
                            error!("Error while notifying client of transport close: {err}")
                        }

                        reconnect
                    }
                    DisconnectReason::Manual | DisconnectReason::Rejected => false,
                    DisconnectReason::Server => reconnect_on_disconnect,
                };

                if should_reconnect {
                    let mut reconnect_attempts = 0;
                    let mut backoff = ExponentialBackoffBuilder::new()
                        .with_initial_interval(Duration::from_millis(reconnect_delay_min))
                        .with_max_interval(Duration::from_millis(reconnect_delay_max))
                        .build();

                    let mut reconnected = false;
                    loop {
                        // A disconnect() during the outage ends the reconnect loop.
                        if client_clone.disconnected_manually().await {
                            break;
                        }
                        if let Some(max_reconnect_attempts) = max_reconnect_attempts {
                            reconnect_attempts += 1;
                            if reconnect_attempts > max_reconnect_attempts {
                                trace!("Max reconnect attempts reached without success");
                                break;
                            }
                        }
                        match client_clone.reconnect().await {
                            Ok(_) => {
                                trace!("Reconnected after {reconnect_attempts} attempts");
                                // disconnect() raced with this attempt and closed the old socket:
                                // close the new one too.
                                if client_clone.disconnected_manually().await {
                                    let _ = client_clone.disconnect().await;
                                }
                                reconnected = true;
                                break;
                            }
                            Err(e) => {
                                trace!("Failed to reconnect: {e:?}");
                                if let Some(delay) = backoff.next_backoff() {
                                    let delay_ms = delay.as_millis();
                                    trace!("Waiting for {delay_ms}ms before reconnecting");
                                    sleep(delay).await;
                                }
                            }
                        }
                    }
                    // Out of attempts (or disconnected manually): give up for good, like
                    // socket.io-client's reconnect_failed. Looping again would poll the dead
                    // socket, fire Close and start a fresh round of max_reconnect_attempts.
                    if !reconnected {
                        break;
                    }
                } else {
                    break;
                }
            }
        }));
        *self.poll_task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
        self._owner = Some(Arc::new(PollTaskOwner(Arc::clone(&self.poll_task))));

        Ok(())
    }

    /// Aborts the poll task and waits until it is gone (with the reconnect attempt and the engine
    /// it may hold). Called from inside the task (a callback), it only lets the loop end on its own.
    async fn stop_poll_task(&self) {
        let key = Arc::as_ptr(&self.poll_task) as usize;
        if POLL_TASK
            .try_with(|current| *current == key)
            .unwrap_or(false)
        {
            return;
        }
        let task = self
            .poll_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }

    /// Drops the emits still waiting for CONNECT and the acks they registered.
    async fn drop_buffered(&self, socket: &InnerSocket) {
        let dropped = socket.take_buffered().await;
        let ids: Vec<i32> = dropped.iter().filter_map(|packet| packet.id).collect();
        if !ids.is_empty() {
            self.outstanding_acks
                .write()
                .await
                .retain(|ack| !ids.contains(&ack.id));
        }
    }

    async fn disconnected_manually(&self) -> bool {
        matches!(
            *self.disconnect_reason.read().await,
            DisconnectReason::Manual
        )
    }

    /// Sends a message to the server using the underlying `engine.io` protocol.
    /// This message takes an event, which could either be one of the common
    /// events like "message" or "error" or a custom event like "foo". But be
    /// careful, the data string needs to be valid JSON. It's recommended to use
    /// a library like `serde_json` to serialize the data properly.
    ///
    /// # Example
    /// ```
    /// use rust_socketio::{asynchronous::{ClientBuilder, Client}, Payload};
    /// use serde_json::json;
    /// use futures_util::FutureExt;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut socket = ClientBuilder::new("http://localhost:4200/")
    ///         .on("test", |payload: Payload, socket: Client| {
    ///             async move {
    ///                 println!("Received: {:#?}", payload);
    ///                 socket.emit("test", json!({"hello": true})).await.expect("Server unreachable");
    ///             }.boxed()
    ///         })
    ///         .connect()
    ///         .await
    ///         .expect("connection failed");
    ///
    ///     let json_payload = json!({"token": 123});
    ///
    ///     let result = socket.emit("foo", json_payload).await;
    ///
    ///     assert!(result.is_ok());
    /// }
    /// ```
    #[inline]
    pub async fn emit<E, D>(&self, event: E, data: D) -> Result<()>
    where
        E: Into<Event>,
        D: Into<Payload>,
    {
        self.socket
            .read()
            .await
            .emit(&self.nsp, event.into(), data.into())
            .await
    }

    /// Disconnects this client from the server by sending a `socket.io` closing
    /// packet.
    /// # Example
    /// ```rust
    /// use rust_socketio::{asynchronous::{ClientBuilder, Client}, Payload};
    /// use serde_json::json;
    /// use futures_util::{FutureExt, future::BoxFuture};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     // apparently the syntax for functions is a bit verbose as rust currently doesn't
    ///     // support an `AsyncFnMut` type that conform with async functions
    ///     fn handle_test(payload: Payload, socket: Client) -> BoxFuture<'static, ()> {
    ///         async move {
    ///             println!("Received: {:#?}", payload);
    ///             socket.emit("test", json!({"hello": true})).await.expect("Server unreachable");
    ///         }.boxed()
    ///     }
    ///
    ///     let mut socket = ClientBuilder::new("http://localhost:4200/")
    ///         .on("test", handle_test)
    ///         .connect()
    ///         .await
    ///         .expect("connection failed");
    ///
    ///     let json_payload = json!({"token": 123});
    ///
    ///     socket.emit("foo", json_payload).await;
    ///
    ///     // disconnect from the server
    ///     socket.disconnect().await;
    /// }
    /// ```
    pub async fn disconnect(&self) -> Result<()> {
        *(self.disconnect_reason.write().await) = DisconnectReason::Manual;
        // A reconnect attempt in flight holds a new engine of its own: stop it before closing.
        self.stop_poll_task().await;

        let disconnect_packet =
            Packet::new(PacketId::Disconnect, self.nsp.clone(), None, None, 0, None);

        let socket = self.socket.read().await;
        self.drop_buffered(&socket).await;
        // The namespace may already be gone (server disconnect, CONNECT_ERROR, a reconnect in
        // progress): there is nothing to leave then, but the engine.io connection must still close.
        let leave = if socket.is_connected() {
            socket.send(disconnect_packet).await
        } else {
            Ok(())
        };
        let close = socket.disconnect().await;

        leave.and(close)
    }

    /// Sends a message to the server but `alloc`s an `ack` to check whether the
    /// server responded in a given time span. This message takes an event, which
    /// could either be one of the common events like "message" or "error" or a
    /// custom event like "foo", as well as a data parameter. But be careful,
    /// in case you send a [`Payload::String`], the string needs to be valid JSON.
    /// It's even recommended to use a library like serde_json to serialize the data properly.
    /// It also requires a timeout `Duration` in which the client needs to answer.
    /// If the ack is acked in the correct time span, the specified callback is
    /// called. The callback consumes a [`Payload`] which represents the data send
    /// by the server.
    ///
    /// Please note that the requirements on the provided callbacks are similar to the ones
    /// for [`crate::asynchronous::ClientBuilder::on`].
    /// # Example
    /// ```
    /// use rust_socketio::{asynchronous::{ClientBuilder, Client}, Payload};
    /// use serde_json::json;
    /// use std::time::Duration;
    /// use std::thread::sleep;
    /// use futures_util::FutureExt;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut socket = ClientBuilder::new("http://localhost:4200/")
    ///         .on("foo", |payload: Payload, _| async move { println!("Received: {:#?}", payload) }.boxed())
    ///         .connect()
    ///         .await
    ///         .expect("connection failed");
    ///
    ///     let ack_callback = |message: Payload, socket: Client| {
    ///         async move {
    ///             match message {
    ///                 Payload::Text(values) => println!("{:#?}", values),
    ///                 Payload::Binary(bytes) => println!("Received bytes: {:#?}", bytes),
    ///                 // This is deprecated use Payload::Text instead
    ///                 Payload::String(str) => println!("{}", str),
    ///             }
    ///         }.boxed()
    ///     };
    ///
    ///
    ///     let payload = json!({"token": 123});
    ///     socket.emit_with_ack("foo", payload, Duration::from_secs(2), ack_callback).await.unwrap();
    ///
    ///     sleep(Duration::from_secs(2));
    /// }
    /// ```
    #[inline]
    pub async fn emit_with_ack<F, E, D>(
        &self,
        event: E,
        data: D,
        timeout: Duration,
        callback: F,
    ) -> Result<()>
    where
        F: for<'a> std::ops::FnMut(Payload, Client) -> BoxFuture<'static, ()>
            + 'static
            + Send
            + Sync,
        E: Into<Event>,
        D: Into<Payload>,
    {
        // A random id from 0..999 collided with pending acks: a reply went to the wrong
        // callback, and duplicate ids broke the removal in handle_ack.
        let id = self.ack_id.fetch_add(1, Ordering::Relaxed) & i32::MAX;
        let socket_packet =
            Packet::new_from_payload(data.into(), event.into(), &self.nsp, Some(id))?;

        let ack = Ack {
            id,
            time_started: Instant::now(),
            timeout,
            callback: Callback::<DynAsyncCallback>::new(callback),
        };

        let mut outstanding_acks = self.outstanding_acks.write().await;
        // Acks the server never answered would otherwise stay here forever.
        outstanding_acks.retain(|ack| ack.time_started.elapsed() < ack.timeout);
        outstanding_acks.push(ack);
        drop(outstanding_acks);

        self.socket.read().await.send(socket_packet).await
    }

    async fn callback<P: Into<Payload>>(&self, event: &Event, payload: P) -> Result<()> {
        let mut builder = self.builder.write().await;
        let payload = payload.into();

        if let Some(callback) = builder.on.get_mut(event) {
            callback(payload.clone(), self.clone()).await;
        }

        // Call on_any for all common and custom events.
        match event {
            Event::Message | Event::Custom(_) => {
                if let Some(callback) = builder.on_any.as_mut() {
                    callback(event.clone(), payload, self.clone()).await;
                }
            }
            _ => (),
        }

        Ok(())
    }

    /// Handles the incoming acks and classifies what callbacks to call and how.
    #[inline]
    async fn handle_ack(&self, socket_packet: &Packet) -> Result<()> {
        let Some(id) = socket_packet.id else {
            return Ok(());
        };
        // Take the ack out first: its callback may emit (with an ack) and must not run under the
        // registry lock.
        let ack = {
            let mut outstanding_acks = self.outstanding_acks.write().await;
            outstanding_acks
                .iter()
                .position(|ack| ack.id == id)
                .map(|index| outstanding_acks.remove(index))
        };
        let Some(mut ack) = ack else {
            return Ok(());
        };

        if ack.time_started.elapsed() < ack.timeout {
            if let Some(ref payload) = socket_packet.data {
                ack.callback.deref_mut()(Payload::from(payload.to_owned()), self.clone()).await;
            }
            if let Some(ref attachments) = socket_packet.attachments {
                if let Some(payload) = attachments.get(0) {
                    ack.callback.deref_mut()(Payload::Binary(payload.to_owned()), self.clone())
                        .await;
                }
            }
        } else {
            trace!("Received an Ack that is now timed out (elapsed time was longer than specified duration)");
        }
        Ok(())
    }

    /// Handles a binary event.
    #[inline]
    async fn handle_binary_event(&self, packet: &Packet) -> Result<()> {
        // Stripping every quote from `"name","extra"` gave the event `name,extra`.
        let event = packet.binary_event_name();

        if let Some(attachments) = &packet.attachments {
            if let Some(binary_payload) = attachments.get(0) {
                self.callback(&event, Payload::Binary(binary_payload.to_owned()))
                    .await?;
            }
        }
        Ok(())
    }

    /// A method that parses a packet and eventually calls the corresponding
    /// callback with the supplied data.
    async fn handle_event(&self, packet: &Packet) -> Result<()> {
        let Some(ref data) = packet.data else {
            return Ok(());
        };

        // a socketio message always comes in one of the following two flavors (both JSON):
        // 1: `["event", "msg", ...]`
        // 2: `["msg"]`
        // in case 2, the message is ment for the default message event, in case 1 the event
        // is specified
        if let Ok(Value::Array(contents)) = serde_json::from_str::<Value>(data) {
            let (event, payloads) = match contents.len() {
                0 => return Err(Error::IncompletePacket()),
                // Incorrect packet, ignore it
                1 => (Event::Message, contents.as_slice()),
                // it's a message event
                _ => match contents.first() {
                    Some(Value::String(ev)) => (Event::from(ev.as_str()), &contents[1..]),
                    // get rest(1..) of them as data, not just take the 2nd element
                    _ => (Event::Message, contents.as_slice()),
                    // take them all as data
                },
            };

            // call the correct callback
            self.callback(&event, payloads.to_vec()).await?;
        }

        Ok(())
    }

    /// Handles the incoming messages and classifies what callbacks to call and how.
    /// This method is later registered as the callback for the `on_data` event of the
    /// engineio client.
    #[inline]
    async fn handle_socketio_packet(&self, packet: &Packet) -> Result<()> {
        if packet.nsp == self.nsp {
            match packet.packet_type {
                PacketId::Ack | PacketId::BinaryAck => {
                    if let Err(err) = self.handle_ack(packet).await {
                        self.callback(&Event::Error, err.to_string()).await?;
                        return Err(err);
                    }
                }
                PacketId::BinaryEvent => {
                    if let Err(err) = self.handle_binary_event(packet).await {
                        self.callback(&Event::Error, err.to_string()).await?;
                    }
                }
                PacketId::Connect => {
                    // A manual disconnect is final: a CONNECT that raced with it must not undo it.
                    let mut reason = self.disconnect_reason.write().await;
                    if !matches!(*reason, DisconnectReason::Manual) {
                        *reason = DisconnectReason::default();
                    }
                    drop(reason);
                    self.callback(&Event::Connect, "").await?;
                }
                PacketId::Disconnect => {
                    let mut reason = self.disconnect_reason.write().await;
                    if !matches!(*reason, DisconnectReason::Manual) {
                        *reason = DisconnectReason::Server;
                    }
                    drop(reason);
                    self.callback(&Event::Close, CloseReason::IOServerDisconnect.as_str())
                        .await?;
                    // A namespace-only server disconnect leaves the engine.io connection open, so
                    // the stream would never end and reconnect_on_disconnect would never apply.
                    // Each Client owns its engine: close it (socket.io-client does the same).
                    if let Err(err) = self.socket.read().await.disconnect().await {
                        trace!("Closing the engine after a server disconnect failed: {err}");
                    }
                }
                PacketId::ConnectError => {
                    let mut reason = self.disconnect_reason.write().await;
                    if !matches!(*reason, DisconnectReason::Manual) {
                        *reason = DisconnectReason::Rejected;
                    }
                    drop(reason);
                    self.callback(
                        &Event::Error,
                        String::from("Received an ConnectError frame: ")
                            + packet
                                .data
                                .as_ref()
                                .unwrap_or(&String::from("\"No error message provided\"")),
                    )
                    .await?;
                    self.drop_buffered(&*self.socket.read().await).await;
                    // The namespace was refused: close the engine.io connection too (it would
                    // otherwise stay open, answering pings, forever) and do not reconnect.
                    if let Err(err) = self.socket.read().await.disconnect().await {
                        trace!("Closing the engine after CONNECT_ERROR failed: {err}");
                    }
                }
                PacketId::Event => {
                    if let Err(err) = self.handle_event(packet).await {
                        self.callback(&Event::Error, err.to_string()).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Returns the packet stream for the client.
    pub(crate) async fn as_stream<'a>(
        &'a self,
    ) -> Pin<Box<dyn Stream<Item = Result<Packet>> + Send + 'a>> {
        let socket_clone = (*self.socket.read().await).clone();

        stream::unfold(socket_clone, |mut socket| async {
            // wait for the next payload
            let packet: Option<std::result::Result<Packet, Error>> = socket.next().await;
            match packet {
                // end the stream if the underlying one is closed
                None => None,
                Some(Err(err)) => {
                    // call the error callback
                    match self.callback(&Event::Error, err.to_string()).await {
                        Err(callback_err) => Some((Err(callback_err), socket)),
                        Ok(_) => Some((Err(err), socket)),
                    }
                }
                Some(Ok(packet)) => match self.handle_socketio_packet(&packet).await {
                    Err(callback_err) => Some((Err(callback_err), socket)),
                    Ok(_) => Some((Ok(packet), socket)),
                },
            }
        })
        .boxed()
    }
}

#[cfg(test)]
mod test {

    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use bytes::Bytes;
    use futures_util::{FutureExt, StreamExt};
    use native_tls::TlsConnector;
    use serde_json::json;
    use serial_test::serial;
    use tokio::{
        sync::{mpsc, Mutex},
        time::{sleep, timeout},
    };

    use crate::{
        asynchronous::{
            client::{builder::ClientBuilder, client::Client},
            ReconnectSettings,
        },
        error::Result,
        packet::{Packet, PacketId},
        CloseReason, Event, Payload, TransportType,
    };

    #[tokio::test]
    async fn socket_io_integration() -> Result<()> {
        let url = crate::test::socket_io_server();

        let socket = ClientBuilder::new(url)
            .on("test", |msg, _| {
                async {
                    match msg {
                        Payload::Text(values) => println!("Received json: {:#?}", values),
                        #[allow(deprecated)]
                        Payload::String(str) => println!("Received string: {}", str),
                        Payload::Binary(bin) => println!("Received binary data: {:#?}", bin),
                    }
                }
                .boxed()
            })
            .connect()
            .await?;

        let payload = json!({"token": 123_i32});
        let result = socket.emit("test", Payload::from(payload.clone())).await;

        assert!(result.is_ok());

        let ack = socket
            .emit_with_ack(
                "test",
                Payload::from(payload),
                Duration::from_secs(1),
                |message: Payload, socket: Client| {
                    async move {
                        let result = socket
                            .emit("test", Payload::from(json!({"got ack": true})))
                            .await;
                        assert!(result.is_ok());

                        println!("Yehaa! My ack got acked?");
                        if let Payload::Text(json) = message {
                            println!("Received json Ack");
                            println!("Ack data: {:#?}", json);
                        }
                    }
                    .boxed()
                },
            )
            .await;
        assert!(ack.is_ok());

        sleep(Duration::from_secs(2)).await;

        assert!(socket.disconnect().await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn socket_io_async_callback() -> Result<()> {
        // Test whether asynchronous callbacks are fully executed.
        let url = crate::test::socket_io_server();

        // This synchronization mechanism is used to let the test know that the end of the
        // async callback was reached.
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = notify.clone();

        let socket = ClientBuilder::new(url)
            .on("test", move |_, _| {
                let cl = notify_clone.clone();
                async move {
                    sleep(Duration::from_secs(1)).await;
                    // The async callback should be awaited and not aborted.
                    // Thus, the notification should be called.
                    cl.notify_one();
                }
                .boxed()
            })
            .connect()
            .await?;

        let payload = json!({"token": 123_i32});
        let result = socket.emit("test", Payload::from(payload)).await;

        assert!(result.is_ok());
        // If the timeout did not trigger, the async callback was fully executed.
        let timeout = timeout(Duration::from_secs(5), notify.notified()).await;
        assert!(timeout.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn socket_io_builder_integration() -> Result<()> {
        let url = crate::test::socket_io_server();

        // test socket build logic
        let socket_builder = ClientBuilder::new(url);

        let tls_connector = TlsConnector::builder()
            .use_sni(true)
            .build()
            .expect("Found illegal configuration");

        let socket = socket_builder
            .namespace("/admin")
            .tls_config(tls_connector)
            .opening_header("accept-encoding", "application/json")
            .on("test", |str, _| {
                async move { println!("Received: {:#?}", str) }.boxed()
            })
            .on("message", |payload, _| {
                async move { println!("{:#?}", payload) }.boxed()
            })
            .connect()
            .await?;

        assert!(socket.emit("message", json!("Hello World")).await.is_ok());

        assert!(socket
            .emit("binary", Bytes::from_static(&[46, 88]))
            .await
            .is_ok());

        assert!(socket
            .emit_with_ack(
                "binary",
                json!("pls ack"),
                Duration::from_secs(1),
                |payload, _| async move {
                    println!("Yehaa the ack got acked");
                    println!("With data: {:#?}", payload);
                }
                .boxed()
            )
            .await
            .is_ok());

        sleep(Duration::from_secs(2)).await;

        Ok(())
    }

    #[tokio::test]
    #[serial(reconnect)]
    async fn socket_io_reconnect_integration() -> Result<()> {
        static CONNECT_NUM: AtomicUsize = AtomicUsize::new(0);
        static MESSAGE_NUM: AtomicUsize = AtomicUsize::new(0);
        static ON_RECONNECT_CALLED: AtomicUsize = AtomicUsize::new(0);
        let latest_message = Arc::new(Mutex::new(String::new()));
        let handler_latest_message = latest_message.clone();

        let url = crate::test::socket_io_restart_server();

        let socket = ClientBuilder::new(url.clone())
            .reconnect(true)
            .max_reconnect_attempts(100)
            .reconnect_delay(100, 100)
            .on_reconnect(move || {
                let url = url.clone();
                async move {
                    ON_RECONNECT_CALLED.fetch_add(1, Ordering::Release);

                    let mut settings = ReconnectSettings::new();

                    // Try setting the address to what we already have, just
                    // to test. This is not strictly necessary in real usage.
                    settings.address(url.to_string());
                    settings.opening_header("MESSAGE_BACK", "updated");
                    settings
                }
                .boxed()
            })
            .on("open", |_, socket| {
                async move {
                    CONNECT_NUM.fetch_add(1, Ordering::Release);
                    let r = socket.emit_with_ack(
                        "message",
                        json!(""),
                        Duration::from_millis(100),
                        |_, _| async move {}.boxed(),
                    );
                    assert!(r.await.is_ok(), "should emit message success");
                }
                .boxed()
            })
            .on("message", move |payload, _socket| {
                let latest_message = handler_latest_message.clone();
                async move {
                    // test the iterator implementation and make sure there is a constant
                    // stream of packets, even when reconnecting
                    MESSAGE_NUM.fetch_add(1, Ordering::Release);

                    let msg = match payload {
                        Payload::Text(msg) => msg
                            .into_iter()
                            .next()
                            .expect("there should be one text payload"),
                        _ => panic!(),
                    };

                    let msg = serde_json::from_value(msg).expect("payload should be json string");

                    *latest_message.lock().await = msg;
                }
                .boxed()
            })
            .connect()
            .await;

        assert!(socket.is_ok(), "should connect success");
        let socket = socket.unwrap();

        // waiting for server to emit message
        sleep(Duration::from_millis(500)).await;

        assert_eq!(load(&CONNECT_NUM), 1, "should connect once");
        assert_eq!(load(&MESSAGE_NUM), 1, "should receive one");
        assert_eq!(
            *latest_message.lock().await,
            "test",
            "should receive test message"
        );

        let r = socket.emit("restart_server", json!("")).await;
        assert!(r.is_ok(), "should emit restart success");

        // waiting for server to restart
        for _ in 0..10 {
            sleep(Duration::from_millis(400)).await;
            if load(&CONNECT_NUM) == 2 && load(&MESSAGE_NUM) == 2 {
                break;
            }
        }

        assert_eq!(load(&CONNECT_NUM), 2, "should connect twice");
        assert_eq!(load(&MESSAGE_NUM), 2, "should receive two messages");
        assert!(
            load(&ON_RECONNECT_CALLED) > 1,
            "should call on_reconnect at least once"
        );
        assert_eq!(
            *latest_message.lock().await,
            "updated",
            "should receive updated message"
        );

        socket.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn socket_io_builder_integration_iterator() -> Result<()> {
        let url = crate::test::socket_io_server();

        // test socket build logic
        let socket_builder = ClientBuilder::new(url);

        let tls_connector = TlsConnector::builder()
            .use_sni(true)
            .build()
            .expect("Found illegal configuration");

        let socket = socket_builder
            .namespace("/admin")
            .tls_config(tls_connector)
            .opening_header("accept-encoding", "application/json")
            .on("test", |str, _| {
                async move { println!("Received: {:#?}", str) }.boxed()
            })
            .on("message", |payload, _| {
                async move { println!("{:#?}", payload) }.boxed()
            })
            .connect_manual()
            .await?;

        assert!(socket.emit("message", json!("Hello World")).await.is_ok());

        assert!(socket
            .emit("binary", Bytes::from_static(&[46, 88]))
            .await
            .is_ok());

        assert!(socket
            .emit_with_ack(
                "binary",
                json!("pls ack"),
                Duration::from_secs(1),
                |payload, _| async move {
                    println!("Yehaa the ack got acked");
                    println!("With data: {:#?}", payload);
                }
                .boxed()
            )
            .await
            .is_ok());

        test_socketio_socket(socket, "/admin".to_owned()).await
    }

    #[tokio::test]
    async fn socket_io_on_any_integration() -> Result<()> {
        let url = crate::test::socket_io_server();

        let (tx, mut rx) = mpsc::channel(2);

        let mut _socket = ClientBuilder::new(url)
            .namespace("/")
            .auth(json!({ "password": "123" }))
            .on_any(move |event, payload, _| {
                let clone_tx = tx.clone();
                async move {
                    if let Payload::Text(values) = payload {
                        println!("{event}: {values:#?}");
                    }
                    clone_tx.send(String::from(event)).await.unwrap();
                }
                .boxed()
            })
            .connect()
            .await?;

        let event = rx.recv().await.unwrap();
        assert_eq!(event, "message");

        let event = rx.recv().await.unwrap();
        assert_eq!(event, "test");

        Ok(())
    }

    #[tokio::test]
    async fn socket_io_auth_builder_integration() -> Result<()> {
        let url = crate::test::socket_io_auth_server();
        let nsp = String::from("/admin");
        let socket = ClientBuilder::new(url)
            .namespace(nsp.clone())
            .auth(json!({ "password": "123" }))
            .connect_manual()
            .await?;

        // open packet
        let mut socket_stream = socket.as_stream().await;
        let _ = socket_stream.next().await.unwrap()?;

        let packet = socket_stream.next().await.unwrap()?;
        assert_eq!(
            packet,
            Packet::new(
                PacketId::Event,
                nsp,
                Some("[\"auth\",\"success\"]".to_owned()),
                None,
                0,
                None
            )
        );

        Ok(())
    }

    /// disconnect() after the server left the namespace must still close the engine.io
    /// connection, which the server keeps open after a namespace-only disconnect.
    #[tokio::test]
    async fn socket_io_disconnect_after_server_namespace_disconnect() {
        let (tx, mut rx) = mpsc::channel(4);
        let close_tx = tx.clone();
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .on(Event::Connect, move |_, _| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(Event::Connect).await;
                }
                .boxed()
            })
            .on(Event::Close, move |_, _| {
                let tx = close_tx.clone();
                async move {
                    let _ = tx.send(Event::Close).await;
                }
                .boxed()
            })
            .connect()
            .await
            .unwrap();
        let wait = Duration::from_secs(5);
        assert_eq!(
            timeout(wait, rx.recv()).await.ok().flatten(),
            Some(Event::Connect)
        );

        socket.emit("server_disconnect", json!(null)).await.unwrap();
        assert_eq!(
            timeout(wait, rx.recv()).await.ok().flatten(),
            Some(Event::Close)
        );

        socket.disconnect().await.unwrap();
        assert!(!socket.socket.read().await.is_engineio_connected());
    }

    /// A namespace-only server disconnect must end the session so reconnect_on_disconnect applies.
    #[tokio::test]
    async fn socket_io_reconnect_after_server_namespace_disconnect() {
        let (tx, mut rx) = mpsc::channel(4);
        let close_tx = tx.clone();
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .reconnect_on_disconnect(true)
            .reconnect_delay(100, 100)
            .on(Event::Connect, move |_, _| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(Event::Connect).await;
                }
                .boxed()
            })
            .on(Event::Close, move |_, _| {
                let tx = close_tx.clone();
                async move {
                    let _ = tx.send(Event::Close).await;
                }
                .boxed()
            })
            .connect()
            .await
            .unwrap();
        let wait = Duration::from_secs(5);
        assert_eq!(
            timeout(wait, rx.recv()).await.ok().flatten(),
            Some(Event::Connect)
        );

        socket.emit("server_disconnect", json!(null)).await.unwrap();
        assert_eq!(
            timeout(wait, rx.recv()).await.ok().flatten(),
            Some(Event::Close)
        );
        assert_eq!(
            timeout(wait, rx.recv()).await.ok().flatten(),
            Some(Event::Connect),
            "no reconnect after the server disconnect"
        );

        socket.disconnect().await.unwrap();
    }

    /// disconnect() while the client is retrying after an outage stops the reconnect loop.
    #[tokio::test]
    #[serial(reconnect)]
    async fn socket_io_disconnect_stops_reconnect_loop() {
        let connects = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let (on_connect, on_attempt) = (connects.clone(), attempts.clone());
        let socket = ClientBuilder::new(crate::test::socket_io_restart_server())
            .reconnect(true)
            .reconnect_delay(100, 100)
            .on(Event::Connect, move |_, _| {
                on_connect.fetch_add(1, Ordering::SeqCst);
                async {}.boxed()
            })
            .on_reconnect(move || {
                on_attempt.fetch_add(1, Ordering::SeqCst);
                async { ReconnectSettings::new() }.boxed()
            })
            .connect()
            .await
            .unwrap();
        let until = |counter: Arc<AtomicUsize>| async move {
            for _ in 0..50 {
                if counter.load(Ordering::SeqCst) > 0 {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
            panic!("timed out waiting for the counter");
        };
        until(connects.clone()).await;

        // The server goes away for 2 s; the client starts retrying.
        socket.emit("restart_server", json!("")).await.unwrap();
        until(attempts.clone()).await;
        let _ = socket.disconnect().await;

        // The server is back after 2 s: a live reconnect loop would have connected by now.
        sleep(Duration::from_secs(4)).await;
        assert_eq!(
            connects.load(Ordering::SeqCst),
            1,
            "reconnected after disconnect()"
        );
        let stopped_at = attempts.load(Ordering::SeqCst);
        sleep(Duration::from_millis(500)).await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            stopped_at,
            "still retrying"
        );
    }

    /// A TCP proxy to the ci server that counts open connections and accepts. In stall mode it
    /// holds new connections without forwarding (a hung handshake); `cut` closes every connection.
    struct CountingProxy {
        url: String,
        open: Arc<AtomicUsize>,
        accepted: Arc<AtomicUsize>,
        stall: Arc<std::sync::atomic::AtomicBool>,
        cut: tokio::sync::watch::Sender<u32>,
    }

    impl CountingProxy {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/socket.io/", listener.local_addr().unwrap());
            let upstream = crate::test::socket_io_server();
            let target = format!(
                "{}:{}",
                upstream.host_str().unwrap(),
                upstream.port().unwrap()
            );
            let open = Arc::new(AtomicUsize::new(0));
            let accepted = Arc::new(AtomicUsize::new(0));
            let stall = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (cut, cut_rx) = tokio::sync::watch::channel(0u32);
            let (o, a, st) = (open.clone(), accepted.clone(), stall.clone());
            tokio::spawn(async move {
                while let Ok((mut inbound, _)) = listener.accept().await {
                    a.fetch_add(1, Ordering::SeqCst);
                    o.fetch_add(1, Ordering::SeqCst);
                    let (o, target, stalled) =
                        (o.clone(), target.clone(), st.load(Ordering::SeqCst));
                    let mut cut_rx = cut_rx.clone();
                    cut_rx.mark_unchanged();
                    tokio::spawn(async move {
                        let pipe = async {
                            if stalled {
                                // Until the client closes its end.
                                use tokio::io::AsyncReadExt;
                                let mut buf = [0u8; 1024];
                                while matches!(inbound.read(&mut buf).await, Ok(n) if n > 0) {}
                            } else if let Ok(mut outbound) =
                                tokio::net::TcpStream::connect(target).await
                            {
                                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound)
                                    .await;
                            }
                        };
                        tokio::select! {
                            _ = pipe => {}
                            _ = cut_rx.changed() => {}
                        }
                        o.fetch_sub(1, Ordering::SeqCst);
                    });
                }
            });
            Self {
                url,
                open,
                accepted,
                stall,
                cut,
            }
        }

        fn cut(&self) {
            self.cut.send_modify(|n| *n += 1);
        }

        async fn wait_open(&self, want: usize) -> bool {
            for _ in 0..30 {
                if self.open.load(Ordering::SeqCst) == want {
                    return true;
                }
                sleep(Duration::from_millis(100)).await;
            }
            false
        }
    }

    fn counting_builder(url: &str, attempts: Arc<AtomicUsize>) -> ClientBuilder {
        ClientBuilder::new(url)
            .transport_type(TransportType::Websocket)
            .reconnect(true)
            .reconnect_delay(50, 100)
            .on_reconnect(move || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { ReconnectSettings::new() }.boxed()
            })
    }

    fn on_connect(builder: ClientBuilder) -> (ClientBuilder, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::channel(4);
        let builder = builder.on(Event::Connect, move |_, _| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(()).await;
            }
            .boxed()
        });
        (builder, rx)
    }

    async fn wait_connected(rx: &mut mpsc::Receiver<()>) {
        timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
    }

    /// Dropping the last handle aborts the poll task: the socket closes and a later outage
    /// starts no reconnect attempts.
    #[tokio::test]
    async fn socket_io_drop_stops_poll_task_and_closes_socket() {
        let proxy = CountingProxy::start().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let (builder, mut rx) = on_connect(counting_builder(&proxy.url, attempts.clone()));
        let socket = builder.connect().await.unwrap();
        wait_connected(&mut rx).await;
        let clone = socket.clone();
        drop(socket);
        sleep(Duration::from_millis(300)).await;
        assert_eq!(
            proxy.open.load(Ordering::SeqCst),
            1,
            "closed while a clone is alive"
        );
        drop(clone);
        assert!(proxy.wait_open(0).await, "socket still open after drop");

        proxy.cut();
        sleep(Duration::from_secs(1)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 0, "reconnected after drop");
        assert_eq!(proxy.accepted.load(Ordering::SeqCst), 1);
    }

    /// disconnect() during a hung reconnect attempt stops it: its socket closes at once instead of
    /// at the handshake timeout, and no further attempt follows.
    #[tokio::test]
    async fn socket_io_disconnect_stops_hung_reconnect_attempt() {
        let proxy = CountingProxy::start().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let (builder, mut rx) = on_connect(counting_builder(&proxy.url, attempts.clone()));
        let socket = builder.connect().await.unwrap();
        wait_connected(&mut rx).await;

        proxy.stall.store(true, Ordering::SeqCst);
        proxy.cut();
        for _ in 0..50 {
            if proxy.accepted.load(Ordering::SeqCst) == 2 && proxy.open.load(Ordering::SeqCst) == 1
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            proxy.accepted.load(Ordering::SeqCst),
            2,
            "no reconnect attempt"
        );

        let _ = timeout(Duration::from_secs(1), socket.disconnect()).await;
        assert!(
            proxy.wait_open(0).await,
            "hung attempt still holds its socket"
        );
        let (tried, accepted) = (
            attempts.load(Ordering::SeqCst),
            proxy.accepted.load(Ordering::SeqCst),
        );
        sleep(Duration::from_secs(1)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), tried, "still retrying");
        assert_eq!(proxy.accepted.load(Ordering::SeqCst), accepted);
    }

    /// disconnect() from a callback (inside the poll task) must not wait for itself, and the task
    /// still ends once the callback returns.
    #[tokio::test]
    async fn socket_io_disconnect_from_callback_does_not_deadlock() {
        let proxy = CountingProxy::start().await;
        let (tx, mut rx) = mpsc::channel(4);
        let socket = ClientBuilder::new(proxy.url.as_str())
            .transport_type(TransportType::Websocket)
            .on(Event::Connect, move |_, socket: Client| {
                let tx = tx.clone();
                async move {
                    let _ = socket.disconnect().await;
                    let _ = tx.send(()).await;
                }
                .boxed()
            })
            .connect()
            .await
            .unwrap();
        wait_connected(&mut rx).await;
        let mut finished = false;
        for _ in 0..30 {
            finished = socket
                .poll_task
                .lock()
                .unwrap()
                .as_ref()
                .is_none_or(|task| task.is_finished());
            if finished {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        assert!(finished, "poll task still running after disconnect()");
        drop(socket);
        assert!(proxy.wait_open(0).await, "socket still open after drop");
    }

    /// An ack callback that emits with an ack again must not deadlock the receive task.
    #[tokio::test]
    async fn socket_io_ack_callback_can_emit_with_ack() {
        let (connected_tx, mut connected_rx) = mpsc::channel(1);
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .on(Event::Connect, move |_, _| {
                let tx = connected_tx.clone();
                async move {
                    let _ = tx.send(()).await;
                }
                .boxed()
            })
            .connect()
            .await
            .unwrap();
        let wait = Duration::from_secs(5);
        timeout(wait, connected_rx.recv()).await.unwrap();

        let (tx, mut rx) = mpsc::channel(4);
        socket
            .emit_with_ack("test", json!("outer"), wait, move |_, socket: Client| {
                let tx = tx.clone();
                async move {
                    let inner_tx = tx.clone();
                    socket
                        .emit_with_ack("test", json!("inner"), wait, move |_, _| {
                            let tx = inner_tx.clone();
                            async move {
                                let _ = tx.send("inner").await;
                            }
                            .boxed()
                        })
                        .await
                        .unwrap();
                    let _ = tx.send("outer").await;
                }
                .boxed()
            })
            .await
            .unwrap();

        assert_eq!(timeout(wait, rx.recv()).await.ok().flatten(), Some("outer"));
        assert_eq!(timeout(wait, rx.recv()).await.ok().flatten(), Some("inner"));
        socket.disconnect().await.unwrap();
    }

    /// Duplicate ids must not panic the receive task; expired acks are dropped on the next emit.
    #[tokio::test]
    async fn socket_io_ack_registry_duplicates_and_expiry() {
        use super::{Ack, Callback, DynAsyncCallback, Instant};
        let ack = |id, timeout| Ack {
            id,
            timeout,
            time_started: Instant::now(),
            callback: Callback::<DynAsyncCallback>::new(|_, _| async {}.boxed()),
        };
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .connect_manual()
            .await
            .unwrap();
        let long = Duration::from_secs(60);

        socket
            .outstanding_acks
            .write()
            .await
            .extend([ack(7, long), ack(8, long), ack(7, long)]);
        let reply = Packet::new(PacketId::Ack, "/".into(), None, Some(7), 0, None);
        socket.handle_ack(&reply).await.unwrap();
        let ids: Vec<i32> = socket
            .outstanding_acks
            .read()
            .await
            .iter()
            .map(|a| a.id)
            .collect();
        assert_eq!(ids, vec![8, 7]);

        socket.outstanding_acks.write().await.clear();
        socket
            .outstanding_acks
            .write()
            .await
            .push(ack(1000, Duration::ZERO));
        socket
            .emit_with_ack("test", json!(1), long, |_, _| async {}.boxed())
            .await
            .unwrap();
        let ids: Vec<i32> = socket
            .outstanding_acks
            .read()
            .await
            .iter()
            .map(|a| a.id)
            .collect();
        assert_eq!(ids.len(), 1);
        assert_ne!(ids[0], 1000);
    }

    /// emit_with_ack with binary data must reach the server as an event and get its ack.
    #[tokio::test]
    async fn socket_io_binary_emit_with_ack() {
        let (tx, mut rx) = mpsc::channel(4);
        let connected_tx = tx.clone();
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .on(Event::Connect, move |_, _| {
                let tx = connected_tx.clone();
                async move {
                    let _ = tx.send(None).await;
                }
                .boxed()
            })
            .connect()
            .await
            .unwrap();
        let wait = Duration::from_secs(5);
        assert_eq!(timeout(wait, rx.recv()).await.ok().flatten(), Some(None));

        socket
            .emit_with_ack(
                "binary",
                Bytes::from_static(&[7, 8]),
                wait,
                move |payload, _| {
                    let tx = tx.clone();
                    async move {
                        let _ = tx.send(Some(payload)).await;
                    }
                    .boxed()
                },
            )
            .await
            .unwrap();
        // ci/socket-io.js acks `binary` with Buffer [1, 2, 3]
        assert_eq!(
            timeout(wait, rx.recv()).await.ok().flatten().flatten(),
            Some(Payload::Binary(Bytes::from_static(&[1, 2, 3])))
        );
        socket.disconnect().await.unwrap();
    }

    /// A binary event with extra arguments reaches the handler registered for its name.
    #[tokio::test]
    async fn socket_io_binary_event_with_extra_arguments() {
        let (tx, mut rx) = mpsc::channel(4);
        let connected_tx = tx.clone();
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .on(Event::Connect, move |_, _| {
                let tx = connected_tx.clone();
                async move {
                    let _ = tx.send(None).await;
                }
                .boxed()
            })
            .on("binary-args", move |payload, _| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(Some(payload)).await;
                }
                .boxed()
            })
            .connect()
            .await
            .unwrap();
        let wait = Duration::from_secs(5);
        assert_eq!(timeout(wait, rx.recv()).await.ok().flatten(), Some(None));

        socket.emit("binary_with_args", json!(null)).await.unwrap();
        assert_eq!(
            timeout(wait, rx.recv()).await.ok().flatten().flatten(),
            Some(Payload::Binary(Bytes::from_static(&[9])))
        );
        socket.disconnect().await.unwrap();
    }

    /// A websocket frame is one packet: binary data containing 0x1e must not be split.
    #[tokio::test]
    async fn socket_io_websocket_binary_with_separator_byte() {
        let (tx, mut rx) = mpsc::channel(4);
        let connected_tx = tx.clone();
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .transport_type(TransportType::Websocket)
            .on(Event::Connect, move |_, _| {
                let tx = connected_tx.clone();
                async move {
                    let _ = tx.send(None).await;
                }
                .boxed()
            })
            .on("binary-delimiter", move |payload, _| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(Some(payload)).await;
                }
                .boxed()
            })
            .connect()
            .await
            .unwrap();
        let wait = Duration::from_secs(5);
        assert_eq!(timeout(wait, rx.recv()).await.ok().flatten(), Some(None));

        socket.emit("binary_delimiter", json!(null)).await.unwrap();
        let payload = timeout(wait, rx.recv()).await.ok().flatten().flatten();
        assert_eq!(
            payload,
            Some(Payload::Binary(Bytes::from_static(&[0x1e, 1, 0x1e])))
        );
        socket.disconnect().await.unwrap();
    }

    /// socket.ts "should not try to reconnect after a middleware failure": after CONNECT_ERROR
    /// socket.io-client closes the engine and does not reconnect.
    #[tokio::test]
    async fn socket_io_connect_error_closes_engine_without_reconnect() {
        let (tx, mut rx) = mpsc::channel(4);
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .namespace("/reject")
            .reconnect(true)
            .reconnect_delay(100, 100)
            .on(Event::Error, move |_, _| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(()).await;
                }
                .boxed()
            })
            .on_reconnect(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                async { ReconnectSettings::new() }.boxed()
            })
            .connect()
            .await
            .unwrap();
        timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("no connect_error");

        sleep(Duration::from_secs(1)).await;
        assert!(
            !socket.socket.read().await.is_engineio_connected(),
            "engine left open after CONNECT_ERROR"
        );
        sleep(Duration::from_secs(1)).await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            0,
            "reconnected after CONNECT_ERROR"
        );
    }

    /// connection.ts "should close the engine upon decoding exception"
    #[tokio::test]
    async fn socket_io_decoding_exception_closes_engine() {
        let (tx, mut rx) = mpsc::channel(1);
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .reconnect(false)
            .on(Event::Connect, move |_, _| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(()).await;
                }
                .boxed()
            })
            .connect()
            .await
            .unwrap();
        timeout(Duration::from_secs(5), rx.recv()).await.unwrap();

        socket.emit("invalid_packet", json!(null)).await.unwrap();
        sleep(Duration::from_secs(1)).await;
        assert!(
            !socket.socket.read().await.is_engineio_connected(),
            "engine left open after a packet that cannot be decoded"
        );
    }

    /// Collects the payloads of `event` (upstream #502 tests below).
    fn collect(
        builder: ClientBuilder,
        event: &str,
    ) -> (ClientBuilder, mpsc::Receiver<Option<Payload>>) {
        let (tx, rx) = mpsc::channel(16);
        let close_tx = tx.clone();
        let builder = builder
            .on(event, move |payload, _| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(Some(payload)).await;
                }
                .boxed()
            })
            .on(Event::Close, move |_, _| {
                let tx = close_tx.clone();
                async move {
                    let _ = tx.send(None).await;
                }
                .boxed()
            });
        (builder, rx)
    }

    /// #502: an emit sent before the server answered CONNECT is delivered (socket.io-client
    /// buffers it) and the connection stays open.
    #[tokio::test]
    async fn socket_io_emit_right_after_connect_is_delivered() {
        let builder = ClientBuilder::new(crate::test::socket_io_server()).namespace("/slow");
        let (builder, mut rx) = collect(builder, "test-received");
        let socket = builder.connect().await.unwrap();
        socket.emit("test", json!(1)).await.unwrap();

        let wait = Duration::from_secs(3);
        assert_eq!(
            timeout(wait, rx.recv()).await.ok().flatten(),
            Some(Some(Payload::Text(vec![json!(1)]))),
            "early emit lost or connection closed"
        );
        sleep(Duration::from_millis(300)).await;
        assert!(
            rx.try_recv().is_err(),
            "connection closed after the early emit"
        );
        socket.disconnect().await.unwrap();
    }

    /// #502: several buffered emits keep their order.
    #[tokio::test]
    async fn socket_io_buffered_emits_keep_their_order() {
        let builder = ClientBuilder::new(crate::test::socket_io_server()).namespace("/slow");
        let (builder, mut rx) = collect(builder, "echo-back");
        let socket = builder.connect().await.unwrap();
        for i in 0..5 {
            socket.emit("echo", json!(i)).await.unwrap();
        }
        for i in 0..5 {
            assert_eq!(
                timeout(Duration::from_secs(3), rx.recv())
                    .await
                    .ok()
                    .flatten(),
                Some(Some(Payload::Text(vec![json!(i)]))),
                "emit {i}"
            );
        }
        socket.disconnect().await.unwrap();
    }

    /// #502: the ack of a buffered emit resolves.
    #[tokio::test]
    async fn socket_io_ack_on_buffered_emit_resolves() {
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .namespace("/slow")
            .connect()
            .await
            .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        socket
            .emit_with_ack(
                "test",
                json!(1),
                Duration::from_secs(5),
                move |payload, _| {
                    let tx = tx.clone();
                    async move {
                        let _ = tx.send(payload).await;
                    }
                    .boxed()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(3), rx.recv())
                .await
                .ok()
                .flatten(),
            Some(Payload::Text(vec![json!(["woot"])]))
        );
        socket.disconnect().await.unwrap();
    }

    /// #502: a buffered emit (and its pending ack) is dropped on CONNECT_ERROR.
    #[tokio::test]
    async fn socket_io_buffered_emit_dropped_on_connect_error() {
        let (tx, mut rx) = mpsc::channel(1);
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .namespace("/reject")
            .on(Event::Error, move |_, _| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(()).await;
                }
                .boxed()
            })
            .connect()
            .await
            .unwrap();
        socket
            .emit_with_ack("test", json!(1), Duration::from_secs(60), |_, _| {
                async {}.boxed()
            })
            .await
            .unwrap();
        timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("no CONNECT_ERROR (connection closed by the early emit?)");
        sleep(Duration::from_millis(200)).await;
        assert!(
            socket.outstanding_acks.read().await.is_empty(),
            "ack of the dropped emit leaked"
        );
    }

    #[tokio::test]
    async fn socket_io_transport_close() -> Result<()> {
        let url = crate::test::socket_io_server();

        let (tx, mut rx) = mpsc::channel(1);

        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = notify.clone();

        let socket = ClientBuilder::new(url)
            .on(Event::Connect, move |_, _| {
                let cl = notify_clone.clone();
                async move {
                    cl.notify_one();
                }
                .boxed()
            })
            .on(Event::Close, move |payload, _| {
                let clone_tx = tx.clone();
                async move { clone_tx.send(payload).await.unwrap() }.boxed()
            })
            .connect()
            .await?;

        // Wait until socket is connected
        let connect_timeout = timeout(Duration::from_secs(1), notify.notified()).await;
        assert!(connect_timeout.is_ok());

        // Instruct server to close transport
        let result = socket.emit("close_transport", Payload::from("")).await;
        assert!(result.is_ok());

        // Wait for Event::Close
        let rx_timeout = timeout(Duration::from_secs(1), rx.recv()).await;
        assert!(rx_timeout.is_ok());

        assert_eq!(
            rx_timeout.unwrap(),
            Some(Payload::from(CloseReason::TransportClose.as_str()))
        );

        Ok(())
    }

    #[tokio::test]
    async fn socketio_polling_integration() -> Result<()> {
        let url = crate::test::socket_io_server();
        let socket = ClientBuilder::new(url.clone())
            .transport_type(TransportType::Polling)
            .connect_manual()
            .await?;
        test_socketio_socket(socket, "/".to_owned()).await
    }

    #[tokio::test]
    async fn socket_io_websocket_integration() -> Result<()> {
        let url = crate::test::socket_io_server();
        let socket = ClientBuilder::new(url.clone())
            .transport_type(TransportType::Websocket)
            .connect_manual()
            .await?;
        test_socketio_socket(socket, "/".to_owned()).await
    }

    #[tokio::test]
    async fn socket_io_websocket_upgrade_integration() -> Result<()> {
        let url = crate::test::socket_io_server();
        let socket = ClientBuilder::new(url)
            .transport_type(TransportType::WebsocketUpgrade)
            .connect_manual()
            .await?;
        test_socketio_socket(socket, "/".to_owned()).await
    }

    #[tokio::test]
    async fn socket_io_any_integration() -> Result<()> {
        let url = crate::test::socket_io_server();
        let socket = ClientBuilder::new(url)
            .transport_type(TransportType::Any)
            .connect_manual()
            .await?;
        test_socketio_socket(socket, "/".to_owned()).await
    }

    async fn test_socketio_socket(socket: Client, nsp: String) -> Result<()> {
        // open packet
        let mut socket_stream = socket.as_stream().await;
        let _: Option<Packet> = Some(socket_stream.next().await.unwrap()?);

        let packet: Option<Packet> = Some(socket_stream.next().await.unwrap()?);

        assert!(packet.is_some());

        let packet = packet.unwrap();

        assert_eq!(
            packet,
            Packet::new(
                PacketId::Event,
                nsp.clone(),
                Some("[\"Hello from the message event!\"]".to_owned()),
                None,
                0,
                None,
            )
        );

        let packet: Option<Packet> = Some(socket_stream.next().await.unwrap()?);

        assert!(packet.is_some());

        let packet = packet.unwrap();

        assert_eq!(
            packet,
            Packet::new(
                PacketId::Event,
                nsp.clone(),
                Some("[\"test\",\"Hello from the test event!\"]".to_owned()),
                None,
                0,
                None
            )
        );
        let packet: Option<Packet> = Some(socket_stream.next().await.unwrap()?);

        assert!(packet.is_some());

        let packet = packet.unwrap();
        assert_eq!(
            packet,
            Packet::new(
                PacketId::BinaryEvent,
                nsp.clone(),
                None,
                None,
                1,
                Some(vec![Bytes::from_static(&[4, 5, 6])]),
            )
        );

        let packet: Option<Packet> = Some(socket_stream.next().await.unwrap()?);

        assert!(packet.is_some());

        let packet = packet.unwrap();
        assert_eq!(
            packet,
            Packet::new(
                PacketId::BinaryEvent,
                nsp.clone(),
                Some("\"test\"".to_owned()),
                None,
                1,
                Some(vec![Bytes::from_static(&[1, 2, 3])]),
            )
        );

        let packet: Option<Packet> = Some(socket_stream.next().await.unwrap()?);

        assert!(packet.is_some());

        let packet = packet.unwrap();
        assert_eq!(
            packet,
            Packet::new(
                PacketId::Event,
                nsp.clone(),
                Some(
                    serde_json::Value::Array(vec![
                        serde_json::Value::from("This is the first argument"),
                        serde_json::Value::from("This is the second argument"),
                        serde_json::json!({"argCount":3})
                    ])
                    .to_string()
                ),
                None,
                0,
                None,
            )
        );

        let packet: Option<Packet> = Some(socket_stream.next().await.unwrap()?);

        assert!(packet.is_some());

        let packet = packet.unwrap();
        assert_eq!(
            packet,
            Packet::new(
                PacketId::Event,
                nsp.clone(),
                Some(
                    serde_json::json!([
                        "on_abc_event",
                        "",
                        {
                        "abc": 0,
                        "some_other": "value",
                        }
                    ])
                    .to_string()
                ),
                None,
                0,
                None,
            )
        );

        let cb = |message: Payload, _| {
            async {
                println!("Yehaa! My ack got acked?");
                if let Payload::Text(values) = message {
                    println!("Received json ack");
                    println!("Ack data: {:#?}", values);
                }
            }
            .boxed()
        };

        assert!(socket
            .emit_with_ack(
                "test",
                Payload::from("123".to_owned()),
                Duration::from_secs(10),
                cb
            )
            .await
            .is_ok());

        // Replies to the emits the caller sent before reading the stream (`message-received`,
        // `binary-received` and its binary ack) may arrive before or after the ones above,
        // depending on server timing: skip them.
        let packet = loop {
            let packet = socket_stream.next().await.unwrap()?;
            let early_reply = packet.packet_type == PacketId::BinaryAck
                || packet.data.as_deref().is_some_and(|data| {
                    data.contains("\"message-received\"") || data.contains("\"binary-received\"")
                });
            if !early_reply {
                break packet;
            }
        };
        assert_eq!(
            packet,
            Packet::new(
                PacketId::Event,
                nsp.clone(),
                Some("[\"test-received\",123]".to_owned()),
                None,
                0,
                None,
            )
        );

        let packet: Option<Packet> = Some(socket_stream.next().await.unwrap()?);

        assert!(packet.is_some());
        let packet = packet.unwrap();
        assert!(matches!(
            packet,
            Packet {
                packet_type: PacketId::Ack,
                nsp: _,
                data: Some(_),
                id: Some(_),
                attachment_count: 0,
                attachments: None,
            }
        ));

        Ok(())
    }

    fn load(num: &AtomicUsize) -> usize {
        num.load(Ordering::Acquire)
    }
}
