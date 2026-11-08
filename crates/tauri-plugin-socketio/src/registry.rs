//! The registry of named clients, the subscriptions that forward them to webviews, and the
//! server acks waiting for an answer from a webview.
//!
//! Each client has one [`Hub`]: a single pair of listeners on the client that fans events and
//! states out to the client's subscriptions. Subscriptions are plain records in the hub, so
//! ending one drops its channel at once; the hub's lock orders every message of a client.
//!
//! Lock order: `clients` → a hub's `state`. Hub callbacks (the client's event task) take only
//! their hub's lock, and no lock is held across an await.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use socketio_next::{
    AckSender, Client, ClientBuilder, ConnectionState, Event, ListenerId, Payload,
};
use tauri::ipc::Channel;
use tokio::sync::watch;

use crate::models::{Access, ClientInfo, Dynamic, Message, State};
use crate::{Error, Result};

/// Server acks wait this long for the webview's answer, then are forgotten.
pub const ACK_TTL: Duration = Duration::from_secs(120);
/// At most this many server acks of one client wait for one webview; past it the webview's own
/// oldest is forgotten (never another webview's).
pub const MAX_PENDING_ACKS_PER_WEBVIEW: usize = 256;
/// At most this many subscriptions per webview (over all clients).
pub const MAX_SUBSCRIPTIONS_PER_WEBVIEW: usize = 64;
/// How long a JS `connect` waits for the client's event task to forward the connected state.
const SETTLE: Duration = Duration::from_secs(2);

/// Identifies one subscription (unique for the life of the app).
pub type SubscriptionId = u64;

/// Named socket.io clients shared by Rust and the webviews. Get it with
/// [`SocketIoExt::socketio`](crate::SocketIoExt::socketio); cheap to clone.
///
/// Ids are unique: [`connect`](Self::connect) and [`insert`](Self::insert) fail with
/// [`Error::AlreadyExists`] for an id in use, so a client is never replaced behind the back of
/// whoever holds it. [`remove`](Self::remove) it first. [`reserve`](Self::reserve) an id in
/// `setup` when Rust registers it later: webviews then cannot take it.
#[derive(Clone, Default)]
pub struct SocketIo {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    clients: Mutex<HashMap<String, Entry>>,
    reserved: Mutex<HashSet<String>>,
    /// Connects in flight: their ids and subscription slots.
    pending: Mutex<Pending>,
    /// Bumped when a webview loads a new page: a JS connect that started before is stale.
    generations: Mutex<HashMap<String, u64>>,
    /// URL prefixes a webview may connect to; empty: any.
    allowed: Vec<url::Url>,
    next_sub: AtomicU64,
}

struct Entry {
    client: Client,
    access: Access,
    hub: Arc<Hub>,
    /// The hub's listeners when they were added after the client was built (`insert`).
    listeners: Vec<ListenerId>,
    /// Auth, headers and query a webview may replace (clients a webview connected).
    dynamic: Option<Arc<Mutex<Dynamic>>>,
}

/// Fans one client out to its subscriptions; keeps the acks offered to webviews.
struct Hub {
    state: Mutex<HubState>,
    /// Counts forwarded states, so `connect` can wait for the connected one.
    forwarded: watch::Sender<u64>,
}

struct HubState {
    subs: HashMap<SubscriptionId, Sub>,
    /// The last state forwarded: the snapshot new subscriptions start from.
    last: Option<State>,
    acks: HashMap<u64, PendingAck>,
    next_ack: u64,
    /// Server acks are offered to webviews: Full clients, until they disconnect.
    offer_acks: bool,
    /// Removed from the registry: forwards nothing more.
    closed: bool,
}

struct Sub {
    channel: Channel<Message>,
    events: Option<HashSet<String>>,
    /// The webview it belongs to; `None` for subscriptions made from Rust (no acks offered).
    webview: Option<String>,
}

struct PendingAck {
    sender: AckSender,
    at: Instant,
    /// The webviews it was offered to; the first answer from one of them wins.
    webviews: Vec<String>,
}

/// A subscription before it is attached: what the webview asked for.
pub(crate) struct SubscribeArgs {
    pub events: Option<Vec<String>>,
    pub channel: Channel<Message>,
    pub webview: Option<String>,
}

impl SubscribeArgs {
    fn into_sub(self) -> Sub {
        Sub {
            channel: self.channel,
            events: self.events.map(|e| e.into_iter().collect()),
            webview: self.webview,
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl fmt::Debug for SocketIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SocketIo")
            .field("clients", &self.ids())
            .field("reserved", &lock(&self.inner.reserved).len())
            .field("allowed_urls", &self.inner.allowed.len())
            .finish()
    }
}

impl SocketIo {
    pub(crate) fn with_allowed(allowed: Vec<url::Url>) -> Self {
        SocketIo {
            inner: Arc::new(Inner {
                allowed,
                ..Inner::default()
            }),
        }
    }

    /// Keeps `id` for Rust: a webview's `connect` with it fails with [`Error::Reserved`], while
    /// [`connect`](Self::connect) and [`insert`](Self::insert) from Rust work. Call it in
    /// `setup` (it is synchronous) for every id Rust registers later, so no page script can
    /// take the id first. Reservations last for the life of the app.
    pub fn reserve(&self, id: impl Into<String>) {
        lock(&self.inner.reserved).insert(id.into());
    }

    /// Builds `builder`, connects and registers the client under `id` with [`Access::Full`].
    /// Fails with the connect error, or [`Error::AlreadyExists`] (then the new client is
    /// dropped, which closes it).
    pub async fn connect(&self, id: impl Into<String>, builder: ClientBuilder) -> Result<Client> {
        Ok(self.connect_with(id.into(), builder, None, None).await?.0)
    }

    /// The webview's connect: reservations, the URL allow-list and the subscription cap apply;
    /// the subscription is attached before connecting (no event after CONNECT is missed) and
    /// starts with a snapshot of the connected state.
    pub(crate) async fn connect_from_webview(
        &self,
        id: String,
        url: &str,
        engine_path: Option<&str>,
        builder: ClientBuilder,
        dynamic: Arc<Mutex<Dynamic>>,
        sub: SubscribeArgs,
    ) -> Result<(SubscriptionId, State)> {
        if lock(&self.inner.reserved).contains(&id) {
            return Err(Error::Reserved(id));
        }
        if !self.url_allowed(url, engine_path) {
            return Err(Error::UrlNotAllowed);
        }
        let (_, sub) = self
            .connect_with(id, builder, Some(sub), Some(dynamic))
            .await?;
        Ok(sub.expect("a subscription was given"))
    }

    async fn connect_with(
        &self,
        id: String,
        builder: ClientBuilder,
        sub: Option<SubscribeArgs>,
        dynamic: Option<Arc<Mutex<Dynamic>>>,
    ) -> Result<(Client, Option<(SubscriptionId, State)>)> {
        let webview = sub.as_ref().and_then(|s| s.webview.clone());
        // The id and (for a webview) one subscription slot are held while connecting, so
        // concurrent connects can neither share an id nor exceed the cap.
        let pending = {
            let clients = lock(&self.inner.clients);
            let mut pending = lock(&self.inner.pending);
            if clients.contains_key(&id) || pending.ids.contains(&id) {
                return Err(Error::AlreadyExists(id));
            }
            if let Some(label) = &webview {
                let held = pending.slots.get(label).copied().unwrap_or_default();
                check_cap(&clients, label, held)?;
                *pending.slots.entry(label.clone()).or_default() += 1;
            }
            pending.ids.insert(id.clone());
            PendingGuard {
                inner: &self.inner,
                id: id.clone(),
                webview: webview.clone(),
            }
        };
        let generation = webview.as_deref().map(|l| self.generation(l));
        let hub = Hub::new(true);
        let sub_id = sub.map(|args| {
            let sub_id = self.next_sub();
            lock(&hub.state).subs.insert(sub_id, args.into_sub());
            sub_id
        });
        let (on_event, on_state) = (Arc::clone(&hub), Arc::clone(&hub));
        let client = builder
            .on_any(move |event: Event, _: Client| {
                on_event.event(event);
                std::future::ready(())
            })
            .on_state_change(move |state: ConnectionState, _: Client| {
                on_state.state(&state);
                std::future::ready(())
            })
            .connect()
            .await?;
        #[cfg(test)]
        let id_for_test = id.clone();
        {
            let mut clients = lock(&self.inner.clients);
            if clients.contains_key(&id) {
                return Err(Error::AlreadyExists(id));
            }
            clients.insert(
                id,
                Entry {
                    client: client.clone(),
                    access: Access::Full,
                    hub: Arc::clone(&hub),
                    listeners: Vec::new(),
                    dynamic,
                },
            );
            // registered: the client and its subscription count from here, the pending id
            // and slot do not any more
            drop(pending);
        }
        #[cfg(test)]
        tests::after_insert(&id_for_test).await;
        let Some(sub_id) = sub_id else {
            return Ok((client, None));
        };
        // The page that asked reloaded meanwhile: its subscription would talk to the new page.
        if let (Some(label), Some(before)) = (&webview, generation) {
            if self.generation(label) != before {
                hub.remove_sub(sub_id, None);
            }
        }
        hub.settle(&client).await;
        let state = hub.snapshot_to(sub_id, &client);
        Ok((client, Some((sub_id, state))))
    }

    /// Registers a client built elsewhere, e.g. by Rust code that must stay free of Tauri.
    /// With [`Access::ReadOnly`] the webview can observe it but never emit, ack or disconnect
    /// it, whatever its capabilities allow. The registry keeps a clone; the client lives until
    /// it is removed and every other clone is dropped.
    pub fn insert(&self, id: impl Into<String>, client: Client, access: Access) -> Result<()> {
        let id = id.into();
        let mut clients = lock(&self.inner.clients);
        if clients.contains_key(&id) {
            return Err(Error::AlreadyExists(id));
        }
        let hub = Hub::new(access == Access::Full);
        let (on_event, on_state) = (Arc::clone(&hub), Arc::clone(&hub));
        let listeners = vec![
            client.on_any(move |event: Event, _: Client| {
                on_event.event(event);
                std::future::ready(())
            }),
            client.on_state_change(move |state: ConnectionState, _: Client| {
                on_state.state(&state);
                std::future::ready(())
            }),
        ];
        clients.insert(
            id,
            Entry {
                client,
                access,
                hub,
                listeners,
                dynamic: None,
            },
        );
        Ok(())
    }

    /// A clone of the client registered under `id`.
    pub fn get(&self, id: &str) -> Option<Client> {
        lock(&self.inner.clients).get(id).map(|e| e.client.clone())
    }

    /// The access the webview has to `id`.
    pub fn access(&self, id: &str) -> Option<Access> {
        lock(&self.inner.clients).get(id).map(|e| e.access)
    }

    /// The registered ids, sorted.
    pub fn ids(&self) -> Vec<String> {
        let mut ids: Vec<_> = lock(&self.inner.clients).keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Unregisters `id`: its subscriptions end (their channels are dropped) and its pending
    /// server acks are forgotten. Does **not** disconnect: the client is handed back (call
    /// [`Client::disconnect`] on it, or drop it; dropping the last clone closes it).
    pub fn remove(&self, id: &str) -> Option<Client> {
        let entry = lock(&self.inner.clients).remove(id)?;
        Some(entry.release())
    }

    /// Forwards the client's state transitions and events (all, or only `events`) to
    /// `channel` as [`Message`]s, starting with a snapshot of the current state. Ends with
    /// [`unsubscribe`](Self::unsubscribe), [`remove`](Self::remove) or when a send fails.
    /// Server acks are not offered to subscriptions made from Rust (answer them in a handler).
    pub fn subscribe(
        &self,
        id: &str,
        events: Option<Vec<String>>,
        channel: Channel<Message>,
    ) -> Result<SubscriptionId> {
        Ok(self
            .subscribe_with(
                id,
                SubscribeArgs {
                    events,
                    channel,
                    webview: None,
                },
            )?
            .0)
    }

    pub(crate) fn subscribe_with(
        &self,
        id: &str,
        args: SubscribeArgs,
    ) -> Result<(SubscriptionId, State)> {
        let clients = lock(&self.inner.clients);
        if let Some(label) = &args.webview {
            let held = lock(&self.inner.pending)
                .slots
                .get(label)
                .copied()
                .unwrap_or_default();
            check_cap(&clients, label, held)?;
        }
        let entry = clients
            .get(id)
            .ok_or_else(|| Error::UnknownClient(id.to_owned()))?;
        let sub_id = self.next_sub();
        let client = &entry.client;
        let state = entry
            .hub
            .attach(sub_id, args.into_sub(), || State::from(&client.state()));
        Ok((sub_id, state))
    }

    /// Stops a subscription; `false` when it was gone already.
    pub fn unsubscribe(&self, id: &str, subscription: SubscriptionId) -> bool {
        self.unsubscribe_from(id, subscription, None)
    }

    /// `webview`: only that webview's subscription (the webview's `unsubscribe`).
    pub(crate) fn unsubscribe_from(
        &self,
        id: &str,
        subscription: SubscriptionId,
        webview: Option<&str>,
    ) -> bool {
        lock(&self.inner.clients)
            .get(id)
            .is_some_and(|e| e.hub.remove_sub(subscription, webview))
    }

    /// A webview starts a new page or is gone: its subscriptions end, its acks are forgotten,
    /// and a JS connect it started meanwhile does not attach.
    pub(crate) fn drop_webview(&self, label: &str) {
        *lock(&self.inner.generations)
            .entry(label.to_owned())
            .or_default() += 1;
        for entry in lock(&self.inner.clients).values() {
            entry.hub.drop_webview(label);
        }
    }

    fn generation(&self, label: &str) -> u64 {
        lock(&self.inner.generations)
            .get(label)
            .copied()
            .unwrap_or_default()
    }

    fn next_sub(&self) -> SubscriptionId {
        self.inner.next_sub.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The connection goes where an allowed prefix says: same scheme, host and port, and the
    /// **engine.io path** (the `path` option, default `/socket.io/`; the URL's own path is the
    /// namespace and replaced by it) under the prefix's path on a `/` boundary. The namespace
    /// is not part of the check. No allow-list: anything.
    pub(crate) fn url_allowed(&self, url: &str, engine_path: Option<&str>) -> bool {
        if self.inner.allowed.is_empty() {
            return true;
        }
        // dot segments and backslashes are refused outright: they mean something else to the
        // URL parser (and to some servers) than to a prefix comparison
        if let Some(path) = engine_path {
            let lower = path.to_ascii_lowercase();
            if path.contains("..")
                || lower.contains("%2e")
                || lower.contains("%25")
                || lower.contains("%u")
                || path.contains('\\')
                || path.chars().any(char::is_control)
            {
                return false;
            }
        }
        // the URL the connection really uses, built by socketio-next itself
        let mut builder = ClientBuilder::new(url);
        if let Some(path) = engine_path {
            builder = builder.path(path);
        }
        let Ok(engine) = builder.engine_url() else {
            return false;
        };
        let path = engine.path();
        self.inner.allowed.iter().any(|p| {
            let base = p.path().trim_end_matches('/');
            p.scheme() == engine.scheme()
                && p.host_str() == engine.host_str()
                && p.port_or_known_default() == engine.port_or_known_default()
                && (base.is_empty() || path.strip_prefix(base).is_some_and(|r| r.starts_with('/')))
        })
    }

    /// The client for a webview command that drives it: [`Access::Full`] only.
    pub(crate) fn full(&self, id: &str) -> Result<Client> {
        match lock(&self.inner.clients).get(id) {
            None => Err(Error::UnknownClient(id.to_owned())),
            Some(e) if e.access == Access::ReadOnly => Err(Error::AccessDenied(id.to_owned())),
            Some(e) => Ok(e.client.clone()),
        }
    }

    /// `client` for observing commands (any access).
    pub(crate) fn any(&self, id: &str) -> Result<Client> {
        self.get(id)
            .ok_or_else(|| Error::UnknownClient(id.to_owned()))
    }

    /// Disconnects a [`Access::Full`] client and unregisters it. Its subscriptions see the
    /// final `closed` state; no server ack is offered any more.
    pub(crate) async fn disconnect(&self, id: &str) -> Result<()> {
        let entry = {
            let mut clients = lock(&self.inner.clients);
            match clients.get(id).map(|e| e.access) {
                None => return Err(Error::UnknownClient(id.to_owned())),
                Some(Access::ReadOnly) => return Err(Error::AccessDenied(id.to_owned())),
                Some(Access::Full) => clients.remove(id).expect("checked under the lock"),
            }
        };
        entry.hub.stop_acks();
        let result = entry.client.disconnect().await;
        entry.release();
        Ok(result?)
    }

    /// Changes what the next connect attempt of `id` sends (a client a webview connected,
    /// [`Access::Full`]): `f` edits its auth, headers and query.
    pub(crate) fn configure(&self, id: &str, f: impl FnOnce(&mut Dynamic)) -> Result<()> {
        let clients = lock(&self.inner.clients);
        let entry = clients
            .get(id)
            .ok_or_else(|| Error::UnknownClient(id.to_owned()))?;
        if entry.access == Access::ReadOnly {
            return Err(Error::AccessDenied(id.to_owned()));
        }
        let dynamic = entry
            .dynamic
            .as_ref()
            .ok_or_else(|| Error::NotConfigurable(id.to_owned()))?;
        f(&mut crate::models::lock(dynamic));
        Ok(())
    }

    /// A client a webview connected ([`Access::Full`], not registered from Rust): what
    /// `reconnect` may restart.
    pub(crate) fn webview_client(&self, id: &str) -> Result<Client> {
        let clients = lock(&self.inner.clients);
        let entry = clients
            .get(id)
            .ok_or_else(|| Error::UnknownClient(id.to_owned()))?;
        if entry.access == Access::ReadOnly {
            return Err(Error::AccessDenied(id.to_owned()));
        }
        if entry.dynamic.is_none() {
            return Err(Error::NotConfigurable(id.to_owned()));
        }
        Ok(entry.client.clone())
    }

    /// Answers server ack `ack_id` of client `id`, offered to `webview`.
    pub(crate) fn ack(&self, id: &str, ack_id: u64, webview: &str, payload: Payload) -> Result<()> {
        let hub = {
            let clients = lock(&self.inner.clients);
            match clients.get(id) {
                None => return Err(Error::UnknownClient(id.to_owned())),
                Some(e) if e.access == Access::ReadOnly => {
                    return Err(Error::AccessDenied(id.to_owned()));
                }
                Some(e) => Arc::clone(&e.hub),
            }
        };
        let sender = hub
            .take_ack(ack_id, webview)
            .ok_or(Error::UnknownAck(ack_id))?;
        Ok(sender.send(payload)?)
    }

    pub(crate) fn list(&self) -> Vec<ClientInfo> {
        let clients = lock(&self.inner.clients);
        let mut out: Vec<_> = clients
            .iter()
            .map(|(id, e)| ClientInfo {
                id: id.clone(),
                access: e.access,
                namespace: e.client.namespace().to_owned(),
                state: State::from(&e.client.state()),
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    #[cfg(test)]
    fn hub(&self, id: &str) -> Arc<Hub> {
        Arc::clone(&lock(&self.inner.clients)[id].hub)
    }
}

/// Ids and subscription slots held by connects in flight.
#[derive(Default)]
struct Pending {
    ids: HashSet<String>,
    slots: HashMap<String, usize>,
}

/// Gives the pending id and slot back when the connect ends (success or failure; on success
/// the client and its subscription are registered by then).
struct PendingGuard<'a> {
    inner: &'a Inner,
    id: String,
    webview: Option<String>,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        let mut pending = lock(&self.inner.pending);
        pending.ids.remove(&self.id);
        if let Some(label) = &self.webview {
            if let Some(n) = pending.slots.get_mut(label) {
                *n = n.saturating_sub(1);
            }
        }
    }
}

fn check_cap(clients: &HashMap<String, Entry>, label: &str, held: usize) -> Result<()> {
    let count: usize = clients.values().map(|e| e.hub.count(label)).sum::<usize>() + held;
    if count >= MAX_SUBSCRIPTIONS_PER_WEBVIEW {
        return Err(Error::TooManySubscriptions(MAX_SUBSCRIPTIONS_PER_WEBVIEW));
    }
    Ok(())
}

impl Entry {
    /// Ends the hub and takes its listeners off the client; hands the client back.
    fn release(self) -> Client {
        self.hub.close();
        for id in self.listeners {
            self.client.off_listener(id);
        }
        self.client
    }
}

impl Hub {
    fn new(offer_acks: bool) -> Arc<Hub> {
        Arc::new(Hub {
            state: Mutex::new(HubState {
                subs: HashMap::new(),
                last: None,
                acks: HashMap::new(),
                next_ack: 0,
                offer_acks,
                closed: false,
            }),
            forwarded: watch::Sender::new(0),
        })
    }

    fn event(&self, event: Event) {
        let mut st = lock(&self.state);
        if st.closed || st.subs.is_empty() {
            return;
        }
        let name = event.name().to_owned();
        let wanted = |s: &Sub| s.events.as_ref().is_none_or(|e| e.contains(&name));
        // One ack id per server ack, offered to every webview that gets the event.
        let mut ack_id = None;
        if let Some(sender) = event.ack_sender().filter(|_| st.offer_acks) {
            let mut webviews: Vec<String> = st
                .subs
                .values()
                .filter(|s| wanted(s))
                .filter_map(|s| s.webview.clone())
                .collect();
            webviews.sort();
            webviews.dedup();
            if !webviews.is_empty() {
                st.acks.retain(|_, a| a.at.elapsed() < ACK_TTL);
                for label in &webviews {
                    st.make_room(label);
                }
                st.next_ack += 1;
                let id = st.next_ack;
                st.acks.insert(
                    id,
                    PendingAck {
                        sender,
                        at: Instant::now(),
                        webviews,
                    },
                );
                ack_id = Some(id);
            }
        }
        let args: crate::Args = event.into_payload().into();
        st.send_all(|s| {
            wanted(s).then(|| Message::Event {
                event: name.clone(),
                args: args.clone(),
                ack_id: ack_id.filter(|_| s.webview.is_some()),
            })
        });
    }

    fn state(&self, state: &ConnectionState) {
        self.forward_state(State::from(state));
    }

    fn forward_state(&self, state: State) {
        let mut st = lock(&self.state);
        if st.closed {
            return;
        }
        st.last = Some(state.clone());
        st.send_all(|_| {
            Some(Message::State {
                state: state.clone(),
                snapshot: false,
            })
        });
        drop(st);
        self.forwarded.send_modify(|n| *n += 1);
    }

    /// Waits (bounded) until the event task forwarded a state past the initial connect, so a
    /// snapshot taken now is the connected state and nothing older follows it.
    async fn settle(&self, client: &Client) {
        let mut rx = self.forwarded.subscribe();
        let settled = |hub: &Hub| {
            !matches!(
                lock(&hub.state).last,
                None | Some(State::Connecting { attempt: 0 })
            )
        };
        let _ = tokio::time::timeout(SETTLE, async {
            while !settled(self) && !client.state().is_closed() {
                if rx.changed().await.is_err() {
                    return;
                }
            }
        })
        .await;
    }

    /// Sends the snapshot (the last forwarded state, else the client's) as the first state
    /// message of `sub`, under the lock that orders every message of this client.
    fn snapshot_to(&self, sub: SubscriptionId, client: &Client) -> State {
        self.snapshot_or(sub, || State::from(&client.state()))
    }

    fn snapshot_or(&self, sub: SubscriptionId, fallback: impl FnOnce() -> State) -> State {
        Self::snapshot_locked(&mut lock(&self.state), sub, fallback)
    }

    /// Adds `sub` and sends its snapshot in one critical section: the snapshot is its very
    /// first message.
    fn attach(&self, id: SubscriptionId, sub: Sub, fallback: impl FnOnce() -> State) -> State {
        let mut st = lock(&self.state);
        st.subs.insert(id, sub);
        Self::snapshot_locked(&mut st, id, fallback)
    }

    fn snapshot_locked(
        st: &mut HubState,
        sub: SubscriptionId,
        fallback: impl FnOnce() -> State,
    ) -> State {
        let state = st.last.clone().unwrap_or_else(fallback);
        if let Some(s) = st.subs.get(&sub) {
            let message = Message::State {
                state: state.clone(),
                snapshot: true,
            };
            if s.channel.send(message).is_err() {
                st.subs.remove(&sub);
            }
        }
        state
    }

    fn remove_sub(&self, sub: SubscriptionId, webview: Option<&str>) -> bool {
        let mut st = lock(&self.state);
        match st.subs.get(&sub) {
            Some(s) if webview.is_none_or(|w| s.webview.as_deref() == Some(w)) => {
                st.subs.remove(&sub);
                true
            }
            _ => false,
        }
    }

    fn drop_webview(&self, label: &str) {
        let mut st = lock(&self.state);
        st.subs.retain(|_, s| s.webview.as_deref() != Some(label));
        st.acks.retain(|_, a| {
            a.webviews.retain(|w| w != label);
            !a.webviews.is_empty()
        });
    }

    fn count(&self, label: &str) -> usize {
        lock(&self.state)
            .subs
            .values()
            .filter(|s| s.webview.as_deref() == Some(label))
            .count()
    }

    fn take_ack(&self, ack_id: u64, webview: &str) -> Option<AckSender> {
        let mut st = lock(&self.state);
        let ok = st
            .acks
            .get(&ack_id)
            .is_some_and(|a| a.at.elapsed() < ACK_TTL && a.webviews.iter().any(|w| w == webview));
        ok.then(|| st.acks.remove(&ack_id).expect("checked").sender)
    }

    fn stop_acks(&self) {
        let mut st = lock(&self.state);
        st.offer_acks = false;
        st.acks.clear();
    }

    fn close(&self) {
        let mut st = lock(&self.state);
        st.closed = true;
        st.subs.clear();
        st.acks.clear();
    }
}

impl HubState {
    /// Sends `make(sub)` to every subscription it returns a message for; a failed send (the
    /// webview is gone) ends that subscription.
    fn send_all(&mut self, make: impl Fn(&Sub) -> Option<Message>) {
        self.subs.retain(|_, s| match make(s) {
            Some(message) => s.channel.send(message).is_ok(),
            None => true,
        });
    }

    /// Keeps `label` under its ack budget by forgetting its own oldest offer.
    ///
    /// ponytail: O(pending acks) per server ack, bounded by the per-webview budget times the
    /// webviews; an ordered queue per webview if that shows up in a profile.
    fn make_room(&mut self, label: &str) {
        let mine = || {
            self.acks
                .iter()
                .filter(|(_, a)| a.webviews.iter().any(|w| w == label))
        };
        if mine().count() < MAX_PENDING_ACKS_PER_WEBVIEW {
            return;
        }
        let Some(oldest) = mine().min_by_key(|(_, a)| a.at).map(|(id, _)| *id) else {
            return;
        };
        let ack = self.acks.get_mut(&oldest).expect("found above");
        ack.webviews.retain(|w| w != label);
        if ack.webviews.is_empty() {
            self.acks.remove(&oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;
    use tokio::sync::Notify;

    /// (client id, reached, go): the connect of that id (only: other tests run in parallel)
    /// waits right after it registered its client.
    type Pause = (String, Arc<Notify>, Arc<Notify>);
    static PAUSE: OnceLock<Mutex<Option<Pause>>> = OnceLock::new();

    pub(super) async fn after_insert(id: &str) {
        let pause = lock(PAUSE.get_or_init(Mutex::default)).clone();
        if let Some((target, reached, go)) = pause {
            if target == id {
                reached.notify_one();
                go.notified().await;
            }
        }
    }

    /// Once the client is registered, its pending id and slot are given back (they counted
    /// twice against the cap while the connect settled).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_registered_connect_holds_no_pending_slot() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let io = SocketIo::default();
            let (reached, go) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
            *lock(PAUSE.get_or_init(Mutex::default)) =
                Some(("p".into(), Arc::clone(&reached), Arc::clone(&go)));
            let (ch, _rx) = channel();
            let connecting = {
                let io = io.clone();
                tokio::spawn(async move {
                    let url = conformance_server();
                    let builder = ClientBuilder::new(url.clone());
                    io.connect_from_webview(
                        "p".into(),
                        &url,
                        None,
                        builder,
                        Default::default(),
                        sub(ch, "w"),
                    )
                    .await
                })
            };
            reached.notified().await;
            *lock(PAUSE.get().unwrap()) = None;
            {
                let pending = lock(&io.inner.pending);
                assert!(pending.ids.is_empty(), "the id is still pending");
                assert_eq!(pending.slots.get("w").copied().unwrap_or_default(), 0);
            }
            go.notify_one();
            connecting.await.unwrap().unwrap();
            io.remove("p").unwrap().disconnect().await.unwrap();
        })
        .await
        .expect("test `a_registered_connect_holds_no_pending_slot` timed out")
    }
    use socketio_next_testing::{conformance_server, socket_io_server};
    use tauri::ipc::InvokeResponseBody;

    fn channel() -> (
        Channel<Message>,
        std::sync::mpsc::Receiver<serde_json::Value>,
    ) {
        let (tx, rx) = std::sync::mpsc::channel();
        let channel = Channel::new(move |body| {
            if let InvokeResponseBody::Json(json) = body {
                let _ = tx.send(serde_json::from_str(&json).unwrap());
            }
            Ok(())
        });
        (channel, rx)
    }

    fn sub(channel: Channel<Message>, webview: &str) -> SubscribeArgs {
        SubscribeArgs {
            events: None,
            channel,
            webview: Some(webview.to_owned()),
        }
    }

    /// A real server event with an ack (they are only made by the client): `question`.
    async fn question(client_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>) -> Event {
        loop {
            let e = client_events.recv().await.unwrap();
            if e.name() == "question" {
                return e;
            }
        }
    }

    async fn asking_client() -> (Client, tokio::sync::mpsc::UnboundedReceiver<Event>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let client = ClientBuilder::new(conformance_server())
            .on("question", move |event: Event, _: Client| {
                let _ = tx.send(event);
                std::future::ready(())
            })
            .connect()
            .await
            .unwrap();
        (client, rx)
    }

    /// Finding 7: once a disconnect started, server acks are no longer offered or kept.
    #[tokio::test(flavor = "multi_thread")]
    async fn no_acks_are_minted_after_stop_acks() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let (client, mut events) = asking_client().await;
            let hub = Hub::new(true);
            let (ch, rx) = channel();
            lock(&hub.state).subs.insert(1, sub(ch, "w").into_sub());
            client.emit("ask", ()).unwrap();
            let q = question(&mut events).await;
            hub.stop_acks();
            hub.event(q);
            let m = rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(m["event"], "question");
            assert!(m.get("ackId").is_none(), "{m}");
            assert!(lock(&hub.state).acks.is_empty());
            client.disconnect().await.unwrap();
        })
        .await
        .expect("test `no_acks_are_minted_after_stop_acks` timed out")
    }

    /// Finding 3: a webview's flood of acks forgets only its own oldest offers.
    #[tokio::test(flavor = "multi_thread")]
    async fn ack_budget_is_per_webview() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let (client, mut events) = asking_client().await;
            let hub = Hub::new(true);
            let (greedy, _g) = channel();
            let (polite, polite_rx) = channel();
            lock(&hub.state)
                .subs
                .insert(1, sub(greedy, "greedy").into_sub());
            // the polite webview gets the first question only
            lock(&hub.state)
                .subs
                .insert(2, sub(polite, "polite").into_sub());
            client.emit("ask", ()).unwrap();
            hub.event(question(&mut events).await);
            let first = polite_rx.recv_timeout(Duration::from_secs(5)).unwrap()["ackId"]
                .as_u64()
                .unwrap();
            hub.remove_sub(2, None);
            let q = question_with_ack(&client, &mut events).await;
            for _ in 0..MAX_PENDING_ACKS_PER_WEBVIEW + 10 {
                hub.event(q.clone());
            }
            let greedy_count = lock(&hub.state)
                .acks
                .values()
                .filter(|a| a.webviews.iter().any(|w| w == "greedy"))
                .count();
            assert_eq!(greedy_count, MAX_PENDING_ACKS_PER_WEBVIEW);
            assert!(
                hub.take_ack(first, "greedy").is_none(),
                "greedy's own oldest went"
            );
            assert!(
                hub.take_ack(first, "polite").is_some(),
                "the polite webview kept its ack"
            );
            client.disconnect().await.unwrap();
        })
        .await
        .expect("test `ack_budget_is_per_webview` timed out")
    }

    async fn question_with_ack(
        client: &Client,
        events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    ) -> Event {
        client.emit("ask", ()).unwrap();
        question(events).await
    }

    /// Finding 8: one server ack, one ack id for every subscription, answered once.
    #[tokio::test(flavor = "multi_thread")]
    async fn one_ack_id_per_server_ack() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let (client, mut events) = asking_client().await;
            let hub = Hub::new(true);
            let (a, a_rx) = channel();
            let (b, b_rx) = channel();
            lock(&hub.state).subs.insert(1, sub(a, "w1").into_sub());
            lock(&hub.state).subs.insert(2, sub(b, "w2").into_sub());
            hub.event(question_with_ack(&client, &mut events).await);
            let ida = a_rx.recv_timeout(Duration::from_secs(5)).unwrap()["ackId"].clone();
            let idb = b_rx.recv_timeout(Duration::from_secs(5)).unwrap()["ackId"].clone();
            assert_eq!(ida, idb);
            let id = ida.as_u64().unwrap();
            assert!(hub.take_ack(id, "w3").is_none(), "not offered to w3");
            assert!(hub.take_ack(id, "w2").is_some());
            assert!(hub.take_ack(id, "w1").is_none(), "answered once");
            client.disconnect().await.unwrap();
        })
        .await
        .expect("test `one_ack_id_per_server_ack` timed out")
    }

    /// Finding 5: the snapshot is the first state message and nothing older follows it, even
    /// while states are being forwarded.
    #[test]
    fn snapshot_is_ordered_with_transitions() {
        let hub = Hub::new(true);
        let (ch0, _rx0) = channel();
        lock(&hub.state).subs.insert(1, sub(ch0, "w").into_sub());
        let writer = {
            let hub = Arc::clone(&hub);
            std::thread::spawn(move || {
                for attempt in 1..=2000 {
                    hub.forward_state(State::Connecting { attempt });
                }
            })
        };
        // subscribe while the writer runs
        let mut seqs = Vec::new();
        for n in 0..20u64 {
            let (ch, rx) = channel();
            let snapshot = hub.attach(100 + n, sub(ch, "w").into_sub(), || State::Connecting {
                attempt: 0,
            });
            seqs.push((snapshot, rx));
        }
        writer.join().unwrap();
        for (snapshot, rx) in seqs {
            let attempts: Vec<u64> = rx
                .try_iter()
                .map(|m| m["state"]["attempt"].as_u64().unwrap())
                .collect();
            let State::Connecting { attempt } = snapshot else {
                unreachable!()
            };
            assert_eq!(attempts[0], u64::from(attempt), "the snapshot comes first");
            assert!(
                attempts.windows(2).all(|w| w[0] < w[1]),
                "ordered: {attempts:?}"
            );
        }
    }

    /// The real `snapshot_to` with the client fallback and the last forwarded state.
    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_uses_the_last_forwarded_state() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let client = ClientBuilder::new(socket_io_server())
                .connect()
                .await
                .unwrap();
            let hub = Hub::new(true);
            let (ch, rx) = channel();
            lock(&hub.state).subs.insert(1, sub(ch, "w").into_sub());
            assert!(
                matches!(hub.snapshot_to(1, &client), State::Connected { .. }),
                "fallback"
            );
            hub.forward_state(State::Disconnected {
                reason: "transport close".into(),
                error: None,
            });
            let (ch2, rx2) = channel();
            lock(&hub.state).subs.insert(2, sub(ch2, "w").into_sub());
            assert!(matches!(
                hub.snapshot_to(2, &client),
                State::Disconnected { .. }
            ));
            assert_eq!(rx2.recv().unwrap()["snapshot"], true);
            assert_eq!(rx.recv().unwrap()["state"]["status"], "connected");
            client.disconnect().await.unwrap();
        })
        .await
        .expect("test `snapshot_uses_the_last_forwarded_state` timed out")
    }

    /// Finding 4: a page reload while its connect is in flight leaves no subscription.
    #[tokio::test(flavor = "multi_thread")]
    async fn reload_during_connect_attaches_nothing() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let io = SocketIo::default();
            let (ch, _rx) = channel();
            let connecting = {
                let io = io.clone();
                tokio::spawn(async move {
                    // /slow answers CONNECT after 200 ms
                    let url = format!("{}/slow", socket_io_server());
                    let builder = ClientBuilder::new(url.clone());
                    io.connect_from_webview(
                        "slow".into(),
                        &url,
                        None,
                        builder,
                        Default::default(),
                        sub(ch, "w"),
                    )
                    .await
                })
            };
            tokio::time::sleep(Duration::from_millis(50)).await;
            io.drop_webview("w");
            connecting.await.unwrap().unwrap();
            assert_eq!(io.hub("slow").count("w"), 0);
            io.remove("slow").unwrap().disconnect().await.unwrap();
        })
        .await
        .expect("test `reload_during_connect_attaches_nothing` timed out")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_reloaded_webview_loses_its_subscriptions_and_acks() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let io = SocketIo::default();
            io.connect("c", ClientBuilder::new(conformance_server()))
                .await
                .unwrap();
            let (a, _) = channel();
            let (b, _) = channel();
            let (sa, _) = io.subscribe_with("c", sub(a, "a")).unwrap();
            let (sb, _) = io.subscribe_with("c", sub(b, "b")).unwrap();
            io.drop_webview("a");
            assert!(!io.unsubscribe("c", sa));
            assert!(io.unsubscribe("c", sb));
            io.remove("c").unwrap().disconnect().await.unwrap();
        })
        .await
        .expect("test `a_reloaded_webview_loses_its_subscriptions_and_acks` timed out")
    }

    #[test]
    fn url_allow_list() {
        let io = SocketIo::with_allowed(vec![
            url::Url::parse("https://api.example.com").unwrap(),
            url::Url::parse("wss://rt.example.com/v1/").unwrap(),
        ]);
        let ok = |url: &str, path: Option<&str>| io.url_allowed(url, path);
        assert!(ok("https://api.example.com", None));
        assert!(ok(
            "https://api.example.com:443/admin?x=1",
            Some("/anything/")
        ));
        assert!(!ok("https://api.example.com.evil.com", None));
        assert!(!ok("https://api.example.com:8443", None));
        assert!(!ok("http://api.example.com", None));
        // the engine.io path is what is checked: the URL's path is only the namespace
        assert!(ok("wss://rt.example.com", Some("/v1/socket.io")));
        assert!(ok("wss://rt.example.com/v1", Some("v1/")));
        assert!(
            !ok("wss://rt.example.com/v1", None),
            "default /socket.io/ is outside /v1/"
        );
        assert!(!ok("wss://rt.example.com/v1/chat", Some("/v10/")));
        assert!(!ok("not a url", None));
        assert!(SocketIo::default().url_allowed("http://anything", Some("/x/")));
        // dot segments and backslashes cannot climb out of the prefix
        for path in [
            "/v1/../admin/",
            "/v1/%2e%2e/admin/",
            "/v1/%2E%2E/admin/",
            "/v1/..\\admin/",
            "/v1/.%2e/admin/",
            // the URL parser drops tabs and newlines: these are `..` once parsed
            "/v1/.\n./admin/",
            "/v1/\t../admin/",
            "/v1/%252e%252e/admin/",
            "/v1/%u002e%u002e/admin/",
        ] {
            assert!(!ok("wss://rt.example.com", Some(path)), "{path:?}");
        }
        // a plain path inside the prefix passes the textual checks and the resolution
        assert!(ok("wss://rt.example.com", Some("/v1/./socket.io/")));
        // only the resolved URL gets this right: `/v1/./socket.io/` is `/v1/socket.io/`,
        // inside a prefix of exactly that path (the raw text is not)
        let deep = SocketIo::with_allowed(vec![
            url::Url::parse("wss://rt.example.com/v1/socket.io/").unwrap(),
        ]);
        assert!(deep.url_allowed("wss://rt.example.com", Some("/v1/./socket.io/")));
        assert!(!deep.url_allowed("wss://rt.example.com", Some("/v1/./other/")));
    }

    #[test]
    fn unknown_ids() {
        let io = SocketIo::default();
        let channel = Channel::new(|_: InvokeResponseBody| Ok(()));
        assert!(matches!(
            io.subscribe("x", None, channel),
            Err(Error::UnknownClient(_))
        ));
        assert!(matches!(io.full("x"), Err(Error::UnknownClient(_))));
        assert!(matches!(
            io.ack("x", 1, "w", Payload::empty()),
            Err(Error::UnknownClient(_))
        ));
        assert!(!io.unsubscribe("x", 1));
        assert!(io.remove("x").is_none());
        assert!(io.ids().is_empty());
    }
}
