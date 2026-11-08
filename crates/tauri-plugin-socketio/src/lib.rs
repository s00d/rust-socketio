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

use tauri::plugin::{Builder, TauriPlugin};
use tauri::webview::PageLoadEvent;
use tauri::{Manager, RunEvent, Runtime, WindowEvent};

mod commands;
mod error;
mod models;
mod registry;

pub use error::{Error, ErrorPayload, Result};
pub use models::{Access, Args, ClientInfo, Link, Message, State};
pub use registry::{ACK_TTL, MAX_PENDING_ACKS, SocketIo, SubscriptionId};
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

/// The plugin (id `socketio`).
///
/// Subscriptions of a webview end when it starts loading a new page or its window is
/// destroyed, so a reload never leaves listeners behind.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new("socketio")
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
        .setup(|app, _api| {
            app.manage(SocketIo::default());
            Ok(())
        })
        .on_page_load(|webview, payload| {
            if payload.event() == PageLoadEvent::Started {
                if let Some(io) = webview.try_state::<SocketIo>() {
                    io.drop_webview(webview.label());
                }
            }
        })
        .on_event(|app, event| {
            if let RunEvent::WindowEvent {
                label,
                event: WindowEvent::Destroyed,
                ..
            } = event
            {
                if let Some(io) = app.try_state::<SocketIo>() {
                    io.drop_webview(label);
                }
            }
        })
        .build()
}
