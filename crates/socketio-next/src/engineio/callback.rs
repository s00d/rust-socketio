use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;

use crate::engineio::Packet;

pub(crate) type Callback<I> = Option<Arc<dyn Fn(I) -> BoxFuture<'static, ()> + Send + Sync>>;

pub(crate) fn boxed<I, F, Fut>(f: F) -> Arc<dyn Fn(I) -> BoxFuture<'static, ()> + Send + Sync>
where
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Arc::new(move |input| Box::pin(f(input)))
}

/// The optional engine.io event callbacks. Each one is spawned as its own task.
#[derive(Clone, Default)]
pub(crate) struct Callbacks {
    pub(crate) on_open: Callback<()>,
    pub(crate) on_close: Callback<()>,
    pub(crate) on_data: Callback<Bytes>,
    pub(crate) on_error: Callback<String>,
    pub(crate) on_packet: Callback<Packet>,
}

impl Callbacks {
    pub(crate) fn fire<I: Send + 'static>(callback: &Callback<I>, input: I) {
        if let Some(callback) = callback {
            tokio::spawn(callback(input));
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
