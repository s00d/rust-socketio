//! socketio-next behaviour tests: models, stories and the corpus, one test binary.
//! See `TESTING.md` at the repository root for the layout and the rules.

mod common;

mod model_engineio_codec;
mod model_socketio_codec;

#[cfg(feature = "blocking")]
mod story_blocking;
mod story_connect;
#[cfg(feature = "cookies")]
mod story_cookies;
mod story_engineio;
mod story_events_acks;
mod story_fake_server;
mod story_handlers;
mod story_reconnect;
#[cfg(any(feature = "rustls", feature = "native-tls"))]
mod story_tls;
mod story_transports;

mod corpus_conformance;
