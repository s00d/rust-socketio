//! Event handlers: plain closures or closures returning a future, no manual boxing.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;

use crate::Client;

/// A handler, type-erased.
pub(crate) type BoxHandler<A> = Arc<dyn Fn(A, Client) -> BoxFuture<'static, ()> + Send + Sync>;

/// Something that can handle `A` (an [`Event`](crate::Event) or a
/// [`ConnectionState`](crate::ConnectionState)) together with the [`Client`].
///
/// Implemented for
/// - `Fn(A, Client) -> impl Future<Output = ()>`: `|event, client| async move { .. }`, run on
///   the client's event task;
/// - `Fn(A, Client)`: a plain closure, run on tokio's blocking pool so it may block.
///
/// Handlers run one at a time, in arrival order. The marker `M` only tells the two apart.
pub trait Handler<A, M>: Send + Sync + 'static {
    #[doc(hidden)]
    fn into_boxed(self) -> BoxHandler<A>;
}

#[doc(hidden)]
#[derive(Debug)]
pub enum AsyncHandler {}
#[doc(hidden)]
#[derive(Debug)]
pub enum SyncHandler {}

impl<A, F, Fut> Handler<A, (AsyncHandler, Fut)> for F
where
    A: Send + 'static,
    F: Fn(A, Client) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn into_boxed(self) -> BoxHandler<A> {
        Arc::new(move |arg, client| self(arg, client).boxed())
    }
}

impl<A, F> Handler<A, SyncHandler> for F
where
    A: Send + 'static,
    F: Fn(A, Client) + Send + Sync + 'static,
{
    fn into_boxed(self) -> BoxHandler<A> {
        let f = Arc::new(self);
        Arc::new(move |arg, client| {
            let f = Arc::clone(&f);
            async move {
                let result = tokio::task::spawn_blocking(move || {
                    crate::client::IN_HANDLER_THREAD.with(|flag| flag.set(true));
                    f(arg, client);
                    crate::client::IN_HANDLER_THREAD.with(|flag| flag.set(false));
                })
                .await;
                if let Err(err) = result {
                    if err.is_panic() {
                        tracing::error!("an event handler panicked");
                    }
                }
            }
            .boxed()
        })
    }
}

/// Identifies one registered listener (see [`Client::off_listener`](crate::Client::off_listener)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ListenerId(u64);

struct Entry<A> {
    id: ListenerId,
    handler: BoxHandler<A>,
    once: bool,
}

impl<A> Clone for Entry<A> {
    fn clone(&self) -> Self {
        Entry {
            id: self.id,
            handler: Arc::clone(&self.handler),
            once: self.once,
        }
    }
}

/// All listeners of one client.
pub(crate) struct Handlers {
    next_id: u64,
    events: HashMap<String, Vec<Entry<crate::Event>>>,
    any: Vec<Entry<crate::Event>>,
    state: Vec<Entry<crate::ConnectionState>>,
}

impl fmt::Debug for Handlers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handlers")
            .field("events", &self.events.keys().collect::<Vec<_>>())
            .field("any", &self.any.len())
            .field("state", &self.state.len())
            .finish()
    }
}

impl Default for Handlers {
    fn default() -> Self {
        Handlers {
            next_id: 1,
            events: HashMap::new(),
            any: Vec::new(),
            state: Vec::new(),
        }
    }
}

impl Handlers {
    fn id(&mut self) -> ListenerId {
        self.next_id += 1;
        ListenerId(self.next_id)
    }

    pub(crate) fn on(
        &mut self,
        event: String,
        handler: BoxHandler<crate::Event>,
        once: bool,
    ) -> ListenerId {
        let id = self.id();
        self.events
            .entry(event)
            .or_default()
            .push(Entry { id, handler, once });
        id
    }

    pub(crate) fn on_any(&mut self, handler: BoxHandler<crate::Event>) -> ListenerId {
        let id = self.id();
        self.any.push(Entry {
            id,
            handler,
            once: false,
        });
        id
    }

    pub(crate) fn on_state(&mut self, handler: BoxHandler<crate::ConnectionState>) -> ListenerId {
        let id = self.id();
        self.state.push(Entry {
            id,
            handler,
            once: false,
        });
        id
    }

    /// Removes every listener of `event`.
    pub(crate) fn off(&mut self, event: &str) -> usize {
        self.events.remove(event).map_or(0, |v| v.len())
    }

    pub(crate) fn off_any(&mut self) -> usize {
        std::mem::take(&mut self.any).len()
    }

    pub(crate) fn off_listener(&mut self, id: ListenerId) -> bool {
        let mut removed = false;
        for list in self.events.values_mut() {
            let before = list.len();
            list.retain(|e| e.id != id);
            removed |= list.len() != before;
        }
        self.events.retain(|_, list| !list.is_empty());
        for list in [&mut self.any] {
            let before = list.len();
            list.retain(|e| e.id != id);
            removed |= list.len() != before;
        }
        let before = self.state.len();
        self.state.retain(|e| e.id != id);
        removed | (self.state.len() != before)
    }

    pub(crate) fn listener_count(&self, event: &str) -> usize {
        self.events.get(event).map_or(0, Vec::len)
    }

    /// The handlers for `event`: catch-all ones first (socket.io-client `onAny` order), then
    /// the event's own. `once` listeners are removed here.
    pub(crate) fn take_for_event(&mut self, event: &str) -> Vec<BoxHandler<crate::Event>> {
        let mut out: Vec<_> = self.any.iter().map(|e| Arc::clone(&e.handler)).collect();
        if let Some(list) = self.events.get_mut(event) {
            out.extend(list.iter().map(|e| Arc::clone(&e.handler)));
            list.retain(|e| !e.once);
            if list.is_empty() {
                self.events.remove(event);
            }
        }
        out
    }

    pub(crate) fn state_handlers(&self) -> Vec<BoxHandler<crate::ConnectionState>> {
        self.state.iter().map(|e| Arc::clone(&e.handler)).collect()
    }
}
