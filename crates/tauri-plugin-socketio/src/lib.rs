//! Tauri 2 plugin for [socketio-next](https://docs.rs/socketio-next): one registry of named
//! Socket.IO clients that Rust and the webviews share.
//!
//! - Rust registers clients with [`SocketIo::connect`] (built and connected here) or
//!   [`SocketIo::insert`] (built elsewhere, e.g. by Tauri-free core code), optionally
//!   [`Access::ReadOnly`] so the webview may only observe them.
//! - JS (`tauri-plugin-socketio-api`) connects its own clients or attaches to registered ones;
//!   events and state transitions reach it over a per-subscription IPC channel, in order.
//!
//! ```no_run
//! use socketio_next::ClientBuilder;
//! use tauri_plugin_socketio::{Access, SocketIoExt};
//!
//! // `tauri::Builder::default()` in an app
//! fn app<R: tauri::Runtime>(builder: tauri::Builder<R>) -> tauri::Builder<R> {
//!     builder
//!         .plugin(tauri_plugin_socketio::init())
//!         .setup(|app| {
//!             // before any await: no page script can take the id first
//!             app.socketio().reserve("core");
//!             let handle = app.handle().clone();
//!             tauri::async_runtime::spawn(async move {
//!                 // e.g. built by a crate that knows nothing about Tauri
//!                 let client = ClientBuilder::new("http://localhost:3000").connect().await?;
//!                 handle.socketio().insert("core", client, Access::ReadOnly)?;
//!                 Ok::<_, tauri_plugin_socketio::Error>(())
//!             });
//!             Ok(())
//!         })
//! }
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]

use tauri::plugin::TauriPlugin;
use tauri::webview::PageLoadEvent;
use tauri::{AppHandle, Manager, RunEvent, Runtime, Webview, WindowEvent};

mod commands;
mod error;
mod models;
mod registry;

pub use error::{Error, ErrorPayload, Result};
pub use models::{Access, Args, ClientInfo, Link, Message, State};
pub use registry::{
    ACK_TTL, MAX_PENDING_ACKS_PER_WEBVIEW, MAX_SUBSCRIPTIONS_PER_WEBVIEW, SocketIo, SubscriptionId,
};
/// The client crate, re-exported.
pub use socketio_next;

/// `app.socketio()` on anything that is a [`Manager`] (`App`, `AppHandle`, `Window`,
/// `Webview`, ...). Panics when the plugin is not registered ([`init`]).
pub trait SocketIoExt<R: Runtime> {
    /// The client registry.
    fn socketio(&self) -> &SocketIo;
}

impl<R: Runtime, T: Manager<R>> SocketIoExt<R> for T {
    fn socketio(&self) -> &SocketIo {
        self.state::<SocketIo>().inner()
    }
}

/// The plugin with the default configuration: [`Builder::new().build()`](Builder).
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new().build()
}

/// Configures the plugin (id `socketio`).
///
/// Subscriptions of a webview end when it starts loading a new page or its window is
/// destroyed, so a reload never leaves listeners behind.
#[derive(Debug, Default)]
pub struct Builder {
    allowed: Vec<url::Url>,
}

impl Builder {
    /// No URL restriction.
    pub fn new() -> Self {
        Builder::default()
    }

    /// Lets the webview `connect` only to URLs under `prefix` (same scheme, host and port;
    /// the path must start with the prefix's path on a `/` boundary). Call it once per
    /// allowed server; without any call every URL is allowed. Rust's
    /// [`SocketIo::connect`] and [`SocketIo::insert`] are never restricted.
    ///
    /// The socket runs in Rust, so the app's CSP `connect-src` does **not** apply to it: with
    /// `allow-connect`, a page script could otherwise open connections anywhere with any
    /// headers.
    ///
    /// # Panics
    ///
    /// When `prefix` is not an absolute URL (a configuration bug, found at startup).
    pub fn allow_url_prefix(mut self, prefix: impl AsRef<str>) -> Self {
        let prefix = prefix.as_ref();
        let url = url::Url::parse(prefix)
            .unwrap_or_else(|e| panic!("tauri-plugin-socketio: bad URL prefix: {e}"));
        self.allowed.push(url);
        self
    }

    /// The plugin.
    pub fn build<R: Runtime>(self) -> TauriPlugin<R> {
        let allowed = self.allowed;
        tauri::plugin::Builder::new("socketio")
            .invoke_handler(tauri::generate_handler![
                commands::connect,
                commands::disconnect,
                commands::emit,
                commands::emit_with_ack,
                commands::ack,
                commands::subscribe,
                commands::unsubscribe,
                commands::list,
                commands::state,
                commands::link_info,
            ])
            .setup(move |app, _api| {
                app.manage(SocketIo::with_allowed(allowed));
                Ok(())
            })
            .on_page_load(|webview, payload| page_load(webview, payload.event()))
            .on_event(run_event)
            .build()
    }
}

/// A new page starts loading: the old page's subscriptions and acks go.
fn page_load<R: Runtime>(webview: &Webview<R>, event: PageLoadEvent) {
    if event == PageLoadEvent::Started {
        if let Some(io) = webview.try_state::<SocketIo>() {
            io.drop_webview(webview.label());
        }
    }
}

/// A window is destroyed: its webview's subscriptions and acks go.
fn run_event<R: Runtime>(app: &AppHandle<R>, event: &RunEvent) {
    if let RunEvent::WindowEvent {
        label,
        event: WindowEvent::Destroyed,
        ..
    } = event
    {
        window_destroyed(app, label);
    }
}

fn window_destroyed<R: Runtime>(app: &AppHandle<R>, label: &str) {
    if let Some(io) = app.try_state::<SocketIo>() {
        io.drop_webview(label);
    }
}

/// The hooks with the events tauri passes them. tauri's mock runtime emits neither page loads
/// nor `Destroyed`, so that tauri calls them is not covered here (nor `run_event`'s one-line
/// match); what they do is.
#[cfg(test)]
mod tests {
    use super::*;
    use tauri::ipc::{Channel, InvokeResponseBody};
    use tauri::test::{mock_builder, mock_context, noop_assets};

    #[test]
    fn the_hooks_end_the_webview_subscriptions() {
        let app = mock_builder()
            .plugin(init())
            .build(mock_context(noop_assets()))
            .unwrap();
        let webview = tauri::WebviewWindowBuilder::new(&app, "w", Default::default())
            .build()
            .unwrap();
        let io = app.socketio().clone();
        let client = tauri::async_runtime::block_on(io.connect(
            "c",
            socketio_next::ClientBuilder::new(socketio_next_testing::conformance_server()),
        ))
        .unwrap();
        let sub = || {
            io.subscribe_with(
                "c",
                registry::SubscribeArgs {
                    events: None,
                    channel: Channel::new(|_: InvokeResponseBody| Ok(())),
                    webview: Some("w".into()),
                },
            )
            .unwrap()
            .0
        };
        let s = sub();
        page_load(webview.as_ref(), PageLoadEvent::Finished);
        assert!(io.unsubscribe("c", s), "a finished load keeps it");
        let s = sub();
        page_load(webview.as_ref(), PageLoadEvent::Started);
        assert!(!io.unsubscribe("c", s), "a new page ends it");
        let s = sub();
        // RunEvent is non_exhaustive (not constructible here); run_event only matches it
        window_destroyed(app.handle(), "w");
        assert!(!io.unsubscribe("c", s), "a destroyed window ends it");
        tauri::async_runtime::block_on(client.disconnect()).unwrap();
    }
}
