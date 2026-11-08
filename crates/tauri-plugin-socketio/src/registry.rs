//! The registry of named clients, the subscriptions that forward them to webviews, and the
//! server acks waiting for an answer from JS.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use socketio_next::{
    AckSender, Client, ClientBuilder, ConnectionState, Event, ListenerId, Payload,
};
use tauri::ipc::Channel;

use crate::models::{Access, ClientInfo, Message, State};
use crate::{Error, Result};

/// Server acks wait this long for the webview's answer, then are forgotten.
pub const ACK_TTL: Duration = Duration::from_secs(120);
/// At most this many server acks wait for the webview (the oldest is forgotten first).
pub const MAX_PENDING_ACKS: usize = 1024;

/// Identifies one subscription (unique for the life of the app).
pub type SubscriptionId = u64;

/// Named socket.io clients shared by Rust and the webviews. Get it with
/// [`SocketIoExt::socketio`](crate::SocketIoExt::socketio); cheap to clone.
///
/// Ids are unique: [`connect`](Self::connect) and [`insert`](Self::insert) fail with
/// [`Error::AlreadyExists`] for an id in use, so a client is never replaced behind the back of
/// whoever holds it. [`remove`](Self::remove) it first.
#[derive(Clone, Default)]
pub struct SocketIo {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    clients: Mutex<HashMap<String, Entry>>,
    acks: Mutex<HashMap<u64, PendingAck>>,
    next_id: AtomicU64,
}

struct Entry {
    client: Client,
    access: Access,
    subs: HashMap<SubscriptionId, Sub>,
}

/// A subscription's listeners on the client. `active` mutes them at once (listeners added
/// through the builder at connect have no id to remove).
struct Sub {
    active: Arc<AtomicBool>,
    listeners: Vec<ListenerId>,
    webview: Option<String>,
}

impl Sub {
    fn stop(self, client: &Client) {
        self.active.store(false, Ordering::Release);
        for id in self.listeners {
            client.off_listener(id);
        }
    }
}

struct PendingAck {
    client: String,
    sender: AckSender,
    at: Instant,
}

/// A subscription before it is attached: what the webview asked for.
pub(crate) struct SubscribeArgs {
    pub events: Option<Vec<String>>,
    pub channel: Channel<Message>,
    pub webview: Option<String>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl fmt::Debug for SocketIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SocketIo")
            .field("clients", &self.ids())
            .field("pending_acks", &lock(&self.inner.acks).len())
            .finish()
    }
}

impl SocketIo {
    /// Builds `builder`, connects and registers the client under `id` with [`Access::Full`].
    /// Fails with the connect error, or [`Error::AlreadyExists`] (then the new client is
    /// dropped, which closes it).
    pub async fn connect(&self, id: impl Into<String>, builder: ClientBuilder) -> Result<Client> {
        Ok(self.connect_with(id.into(), builder, None).await?.0)
    }

    pub(crate) async fn connect_with(
        &self,
        id: String,
        mut builder: ClientBuilder,
        sub: Option<SubscribeArgs>,
    ) -> Result<(Client, Option<SubscriptionId>)> {
        if lock(&self.inner.clients).contains_key(&id) {
            return Err(Error::AlreadyExists(id));
        }
        // Listeners go on the builder so the events right after CONNECT are not missed.
        let mut pre = None;
        if let Some(args) = sub {
            let (sub_id, fwd) = self.forwarder(&id, &args, Access::Full);
            let active = Arc::clone(&fwd.active);
            let a = Arc::clone(&fwd);
            builder = builder
                .on_any(move |event: Event, _: Client| {
                    a.event(event);
                    std::future::ready(())
                })
                .on_state_change(move |state: ConnectionState, _: Client| {
                    fwd.state(&state);
                    std::future::ready(())
                });
            pre = Some((
                sub_id,
                Sub {
                    active,
                    listeners: Vec::new(),
                    webview: args.webview,
                },
            ));
        }
        let client = builder.connect().await?;
        let mut clients = lock(&self.inner.clients);
        if clients.contains_key(&id) {
            return Err(Error::AlreadyExists(id));
        }
        let mut subs = HashMap::new();
        let sub_id = pre.map(|(sub_id, sub)| {
            subs.insert(sub_id, sub);
            sub_id
        });
        clients.insert(
            id,
            Entry {
                client: client.clone(),
                access: Access::Full,
                subs,
            },
        );
        Ok((client, sub_id))
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
        clients.insert(
            id,
            Entry {
                client,
                access,
                subs: HashMap::new(),
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

    /// Unregisters `id`: its subscriptions stop and its pending server acks are forgotten.
    /// Does **not** disconnect: the client is handed back (call
    /// [`Client::disconnect`] on it, or drop it; dropping the last clone closes it).
    pub fn remove(&self, id: &str) -> Option<Client> {
        self.take(id).map(|e| e.client)
    }

    fn take(&self, id: &str) -> Option<Entry> {
        let mut entry = lock(&self.inner.clients).remove(id)?;
        for (_, sub) in entry.subs.drain() {
            sub.stop(&entry.client);
        }
        lock(&self.inner.acks).retain(|_, ack| ack.client != id);
        Some(entry)
    }

    /// Forwards the client's state transitions and events (all, or only `events`) to
    /// `channel` as [`Message`]s, starting with the current state. Ends with
    /// [`unsubscribe`](Self::unsubscribe), [`remove`](Self::remove) or when a send fails
    /// (webview gone).
    pub fn subscribe(
        &self,
        id: &str,
        events: Option<Vec<String>>,
        channel: Channel<Message>,
    ) -> Result<SubscriptionId> {
        self.subscribe_with(
            id,
            SubscribeArgs {
                events,
                channel,
                webview: None,
            },
        )
    }

    pub(crate) fn subscribe_with(&self, id: &str, args: SubscribeArgs) -> Result<SubscriptionId> {
        let mut clients = lock(&self.inner.clients);
        let entry = clients
            .get_mut(id)
            .ok_or_else(|| Error::UnknownClient(id.to_owned()))?;
        let (sub_id, fwd) = self.forwarder(id, &args, entry.access);
        let (a, b) = (Arc::clone(&fwd), Arc::clone(&fwd));
        let listeners = vec![
            entry.client.on_any(move |event: Event, _: Client| {
                a.event(event);
                std::future::ready(())
            }),
            entry
                .client
                .on_state_change(move |state: ConnectionState, _: Client| {
                    b.state(&state);
                    std::future::ready(())
                }),
        ];
        entry.subs.insert(
            sub_id,
            Sub {
                active: Arc::clone(&fwd.active),
                listeners,
                webview: args.webview,
            },
        );
        let client = entry.client.clone();
        drop(clients);
        // After the listeners: a transition in between is seen twice, never lost.
        fwd.state(&client.state());
        Ok(sub_id)
    }

    /// Stops a subscription; `false` when it was gone already.
    pub fn unsubscribe(&self, id: &str, subscription: SubscriptionId) -> bool {
        self.inner.unsubscribe(id, subscription)
    }

    /// Stops every subscription of a webview (it reloads or closed).
    pub(crate) fn drop_webview(&self, label: &str) {
        let mut clients = lock(&self.inner.clients);
        for entry in clients.values_mut() {
            let gone: Vec<_> = entry
                .subs
                .iter()
                .filter(|(_, s)| s.webview.as_deref() == Some(label))
                .map(|(id, _)| *id)
                .collect();
            for id in gone {
                if let Some(sub) = entry.subs.remove(&id) {
                    sub.stop(&entry.client);
                }
            }
        }
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
    /// final `closed` state; the id is free from the start (nothing else can reach the client).
    pub(crate) async fn disconnect(&self, id: &str) -> Result<()> {
        let mut entry = {
            let mut clients = lock(&self.inner.clients);
            match clients.get(id).map(|e| e.access) {
                None => return Err(Error::UnknownClient(id.to_owned())),
                Some(Access::ReadOnly) => return Err(Error::AccessDenied(id.to_owned())),
                Some(Access::Full) => clients.remove(id).expect("checked under the lock"),
            }
        };
        lock(&self.inner.acks).retain(|_, ack| ack.client != id);
        let result = entry.client.disconnect().await;
        for (_, sub) in entry.subs.drain() {
            sub.stop(&entry.client);
        }
        Ok(result?)
    }

    /// Answers server ack `ack_id` of client `id`.
    pub(crate) fn ack(&self, id: &str, ack_id: u64, payload: Payload) -> Result<()> {
        self.full(id)?;
        let pending = {
            let mut acks = lock(&self.inner.acks);
            match acks.get(&ack_id) {
                Some(a) if a.client == id && a.at.elapsed() < ACK_TTL => acks.remove(&ack_id),
                _ => None,
            }
        };
        let pending = pending.ok_or(Error::UnknownAck(ack_id))?;
        Ok(pending.sender.send(payload)?)
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

    fn forwarder(
        &self,
        id: &str,
        args: &SubscribeArgs,
        access: Access,
    ) -> (SubscriptionId, Arc<Forwarder>) {
        let sub = self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let fwd = Arc::new(Forwarder {
            id: id.to_owned(),
            sub,
            events: args.events.clone().map(|e| e.into_iter().collect()),
            channel: args.channel.clone(),
            acks: access == Access::Full,
            active: Arc::new(AtomicBool::new(true)),
            registry: Arc::downgrade(&self.inner),
        });
        (sub, fwd)
    }
}

impl Inner {
    fn unsubscribe(&self, id: &str, subscription: SubscriptionId) -> bool {
        let mut clients = lock(&self.clients);
        let Some(entry) = clients.get_mut(id) else {
            return false;
        };
        match entry.subs.remove(&subscription) {
            Some(sub) => {
                sub.stop(&entry.client);
                true
            }
            None => false,
        }
    }

    /// Keeps `sender` for the webview's answer; returns the ack id it sees.
    ///
    /// ponytail: expiry and the size cap are O(n) scans over at most MAX_PENDING_ACKS entries
    /// per server ack; an ordered queue if that ever shows up in a profile.
    fn keep_ack(&self, client: &str, sender: AckSender) -> u64 {
        let ack_id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut acks = lock(&self.acks);
        acks.retain(|_, a| a.at.elapsed() < ACK_TTL);
        if acks.len() >= MAX_PENDING_ACKS {
            if let Some(oldest) = acks.iter().min_by_key(|(_, a)| a.at).map(|(id, _)| *id) {
                acks.remove(&oldest);
            }
        }
        acks.insert(
            ack_id,
            PendingAck {
                client: client.to_owned(),
                sender,
                at: Instant::now(),
            },
        );
        ack_id
    }
}

/// The listeners' side of one subscription.
struct Forwarder {
    id: String,
    sub: SubscriptionId,
    events: Option<HashSet<String>>,
    channel: Channel<Message>,
    /// Server acks are offered to the webview (not for read-only clients: Rust answers them).
    acks: bool,
    active: Arc<AtomicBool>,
    registry: Weak<Inner>,
}

impl Forwarder {
    fn event(&self, event: Event) {
        if !self.active.load(Ordering::Acquire)
            || self
                .events
                .as_ref()
                .is_some_and(|e| !e.contains(event.name()))
        {
            return;
        }
        let name = event.name().to_owned();
        let ack_id = event
            .ack_sender()
            .filter(|_| self.acks)
            .and_then(|ack| Some(self.registry.upgrade()?.keep_ack(&self.id, ack)));
        self.send(Message::Event {
            event: name,
            args: event.into_payload().into(),
            ack_id,
        });
    }

    fn state(&self, state: &ConnectionState) {
        if self.active.load(Ordering::Acquire) {
            self.send(Message::State {
                state: state.into(),
            });
        }
    }

    /// A failed send means the webview is gone: the subscription ends.
    fn send(&self, message: Message) {
        if self.channel.send(message).is_err() {
            self.active.store(false, Ordering::Release);
            if let Some(registry) = self.registry.upgrade() {
                registry.unsubscribe(&self.id, self.sub);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use socketio_next_testing::conformance_server;
    use tauri::ipc::InvokeResponseBody;

    /// A real `AckSender` (they are only made by the client): the server's `question`.
    #[tokio::test(flavor = "multi_thread")]
    async fn pending_acks_are_capped() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let client = ClientBuilder::new(conformance_server())
            .on("question", move |event: Event, _: Client| {
                let _ = tx.send(event.ack_sender());
                std::future::ready(())
            })
            .connect()
            .await
            .unwrap();
        client.emit("ask", ()).unwrap();
        let sender = rx
            .recv()
            .await
            .unwrap()
            .expect("the server asks with a callback");
        let inner = Inner::default();
        for _ in 0..MAX_PENDING_ACKS + 10 {
            inner.keep_ack("c", sender.clone());
        }
        assert_eq!(lock(&inner.acks).len(), MAX_PENDING_ACKS);
        let first = lock(&inner.acks).keys().min().copied().unwrap();
        assert!(first > 10, "the oldest went first");
        client.disconnect().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_reloaded_webview_loses_its_subscriptions() {
        let io = SocketIo::default();
        io.connect("c", ClientBuilder::new(conformance_server()))
            .await
            .unwrap();
        let sub = |webview: &str| SubscribeArgs {
            events: None,
            channel: Channel::new(|_: InvokeResponseBody| Ok(())),
            webview: Some(webview.to_owned()),
        };
        let a = io.subscribe_with("c", sub("a")).unwrap();
        let b = io.subscribe_with("c", sub("b")).unwrap();
        io.drop_webview("a");
        assert!(!io.unsubscribe("c", a));
        assert!(io.unsubscribe("c", b));
        io.remove("c").unwrap().disconnect().await.unwrap();
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
            io.ack("x", 1, Payload::empty()),
            Err(Error::UnknownClient(_))
        ));
        assert!(!io.unsubscribe("x", 1));
        assert!(io.remove("x").is_none());
        assert!(io.ids().is_empty());
    }
}
