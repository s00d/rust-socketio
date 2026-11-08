use std::{pin::Pin, sync::Arc};

use crate::error::Result;
use futures_util::{ready, FutureExt, Stream, StreamExt};
use tokio::sync::Mutex;

/// A handy type alias for a pinned + boxed Stream trait object that iterates
/// over object of a certain type `T`.
pub(crate) type Generator<T> = Pin<Box<dyn Stream<Item = T> + 'static + Send>>;

/// An internal type that implements stream by repeatedly calling [`Stream::poll_next`] on an
/// underlying stream. Note that the generic parameter will be wrapped in a [`Result`].
#[derive(Clone)]
pub(crate) struct StreamGenerator<T> {
    inner: Arc<Mutex<Generator<Result<T>>>>,
}

impl<T> Stream for StreamGenerator<T> {
    type Item = Result<T>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut lock = ready!(Box::pin(self.inner.lock()).poll_unpin(cx));
        lock.poll_next_unpin(cx)
    }
}

impl<T> StreamGenerator<T> {
    pub(crate) fn new(generator_stream: Generator<Result<T>>) -> Self {
        StreamGenerator {
            inner: Arc::new(Mutex::new(generator_stream)),
        }
    }
}

/// Ends `stream` right after its first error, like a `try_stream!` block that hit `?`.
pub(crate) fn end_after_error<T, S>(stream: S) -> impl Stream<Item = Result<T>> + Send
where
    S: Stream<Item = Result<T>> + Send + Unpin,
    T: Send,
{
    futures_util::stream::unfold((stream, false), |(mut stream, failed)| async move {
        if failed {
            return None;
        }
        let item = stream.next().await?;
        let failed = item.is_err();
        Some((item, (stream, failed)))
    })
}
