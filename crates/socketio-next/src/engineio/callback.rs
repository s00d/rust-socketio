use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use tokio::sync::mpsc;

use crate::engineio::Packet;

pub(crate) type Callback<I> = Option<Arc<dyn Fn(I) -> BoxFuture<'static, ()> + Send + Sync>>;

pub(crate) fn boxed<I, F, Fut>(f: F) -> Arc<dyn Fn(I) -> BoxFuture<'static, ()> + Send + Sync>
where
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Arc::new(move |input| Box::pin(f(input)))
}

/// The optional engine.io event callbacks.
#[derive(Clone, Default)]
pub(crate) struct Callbacks {
    pub(crate) on_open: Callback<()>,
    pub(crate) on_close: Callback<()>,
    pub(crate) on_data: Callback<Bytes>,
    pub(crate) on_error: Callback<String>,
    pub(crate) on_packet: Callback<Packet>,
}

/// One callback invocation.
#[derive(Debug)]
pub(crate) enum Event {
    Open,
    Close,
    Data(Bytes),
    Error(String),
    Packet(Packet),
}

impl Callbacks {
    fn is_empty(&self) -> bool {
        self.on_open.is_none()
            && self.on_close.is_none()
            && self.on_data.is_none()
            && self.on_error.is_none()
            && self.on_packet.is_none()
    }

    /// The call for `event`, if that callback is set.
    fn call(&self, event: Event) -> Option<BoxFuture<'static, ()>> {
        match event {
            Event::Open => self.on_open.as_ref().map(|f| f(())),
            Event::Close => self.on_close.as_ref().map(|f| f(())),
            Event::Data(data) => self.on_data.as_ref().map(|f| f(data)),
            Event::Error(error) => self.on_error.as_ref().map(|f| f(error)),
            Event::Packet(packet) => self.on_packet.as_ref().map(|f| f(packet)),
        }
    }
}

/// Runs the callbacks of one session one at a time, in the order the events happened (like
/// engine.io-client's synchronous emitter). The task ends when the session is dropped.
#[derive(Clone)]
pub(crate) struct Dispatcher(Option<mpsc::UnboundedSender<Event>>);

impl Dispatcher {
    /// Starts the dispatch task; none when no callback is set.
    pub(crate) fn start(callbacks: Callbacks) -> Self {
        if callbacks.is_empty() {
            return Dispatcher(None);
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                // a panicking callback is logged and does not stop the later ones
                let call = std::panic::catch_unwind(AssertUnwindSafe(|| callbacks.call(event)));
                let done = match call {
                    Ok(Some(fut)) => AssertUnwindSafe(fut).catch_unwind().await.is_ok(),
                    Ok(None) => true,
                    Err(_) => false,
                };
                if !done {
                    tracing::error!("an engine.io callback panicked");
                }
            }
        });
        Dispatcher(Some(tx))
    }

    pub(crate) fn fire(&self, event: Event) {
        if let Some(tx) = &self.0 {
            let _ = tx.send(event);
        }
    }
}

impl fmt::Debug for Callbacks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Callbacks")
            .field("on_open", &self.on_open.is_some())
            .field("on_close", &self.on_close.is_some())
            .field("on_data", &self.on_data.is_some())
            .field("on_error", &self.on_error.is_some())
            .field("on_packet", &self.on_packet.is_some())
            .finish()
    }
}

impl fmt::Debug for Dispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Dispatcher")
            .field(&self.0.is_some())
            .finish()
    }
}
