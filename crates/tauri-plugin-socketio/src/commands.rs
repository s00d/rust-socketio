//! The webview's commands (`plugin:socketio|<name>`). Clients registered as
//! [`Access::ReadOnly`](crate::Access::ReadOnly) refuse `emit`, `emit_with_ack`, `ack` and
//! `disconnect` whatever the capabilities allow. `unsubscribe` and `ack` act only on what was
//! given to the calling webview.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::Value;
use tauri::ipc::Channel;
use tauri::{Runtime, State, Webview};

use crate::models::{
    Args, ClientInfo, ConnectOptions, Link, Message, State as ConnState, Subscribed,
};
use crate::registry::{SocketIo, SubscribeArgs, SubscriptionId};
use crate::{Error, Result};

fn sub_args<R: Runtime>(
    webview: &Webview<R>,
    events: Option<Vec<String>>,
    channel: Channel<Message>,
) -> SubscribeArgs {
    SubscribeArgs {
        events,
        channel,
        webview: Some(webview.label().to_owned()),
    }
}

/// A duration argument in ms: a finite number ≥ 0 (JSON turns `Infinity` into `null`).
fn millis(value: &Value, what: &str) -> Result<Duration> {
    value
        .as_f64()
        .filter(|v| v.is_finite() && *v >= 0.0)
        .map(|v| Duration::from_millis(v as u64))
        .ok_or_else(|| {
            Error::InvalidOptions(format!("{what} must be a finite number of milliseconds"))
        })
}

/// Connects a new client under `id` ([`Access::Full`](crate::Access::Full)).
#[tauri::command]
pub(crate) async fn connect<R: Runtime>(
    webview: Webview<R>,
    io: State<'_, SocketIo>,
    id: String,
    options: Value,
    events: Option<Vec<String>>,
    channel: Channel<Message>,
) -> Result<Subscribed> {
    let options = ConnectOptions::parse(options)?;
    let (url, path) = (options.url.clone(), options.path.clone());
    let (builder, dynamic) = options.into_builder()?;
    let sub = sub_args(&webview, events, channel);
    let (subscription, state) = io
        .connect_from_webview(id, &url, path.as_deref(), builder, dynamic, sub)
        .await?;
    Ok(Subscribed {
        subscription,
        state,
    })
}

/// Leaves the namespace, closes the connection and unregisters the client.
#[tauri::command]
pub(crate) async fn disconnect(io: State<'_, SocketIo>, id: String) -> Result<()> {
    io.disconnect(&id).await
}

#[tauri::command]
pub(crate) fn emit(io: State<'_, SocketIo>, id: String, event: String, args: Args) -> Result<()> {
    Ok(io.full(&id)?.emit(event, args)?)
}

#[tauri::command]
pub(crate) async fn emit_with_ack(
    io: State<'_, SocketIo>,
    id: String,
    event: String,
    args: Args,
    timeout: Value,
) -> Result<Args> {
    let timeout = millis(&timeout, "timeout")?;
    let client = io.full(&id)?;
    Ok(client.emit_with_ack(event, args, timeout).await?.into())
}

/// Answers a server ack forwarded to this webview with `ackId`.
#[tauri::command]
pub(crate) fn ack<R: Runtime>(
    webview: Webview<R>,
    io: State<'_, SocketIo>,
    id: String,
    ack_id: u64,
    args: Args,
) -> Result<()> {
    io.ack(&id, ack_id, webview.label(), args.into())
}

/// Forwards an existing client (any access) to the channel.
#[tauri::command]
pub(crate) fn subscribe<R: Runtime>(
    webview: Webview<R>,
    io: State<'_, SocketIo>,
    id: String,
    events: Option<Vec<String>>,
    channel: Channel<Message>,
) -> Result<Subscribed> {
    let (subscription, state) = io.subscribe_with(&id, sub_args(&webview, events, channel))?;
    Ok(Subscribed {
        subscription,
        state,
    })
}

/// Ends one of this webview's subscriptions.
#[tauri::command]
pub(crate) fn unsubscribe<R: Runtime>(
    webview: Webview<R>,
    io: State<'_, SocketIo>,
    id: String,
    subscription: SubscriptionId,
) -> bool {
    io.unsubscribe_from(&id, subscription, Some(webview.label()))
}

#[tauri::command]
pub(crate) fn list(io: State<'_, SocketIo>) -> Vec<ClientInfo> {
    io.list()
}

#[tauri::command]
pub(crate) fn state(io: State<'_, SocketIo>, id: String) -> Result<ConnState> {
    Ok(ConnState::from(&io.any(&id)?.state()))
}

#[tauri::command]
pub(crate) fn link_info(io: State<'_, SocketIo>, id: String) -> Result<Link> {
    Ok(Link::from(&io.any(&id)?.link_info()))
}

/// Replaces the auth payload sent from the next connect attempt (e.g. a refreshed token).
#[tauri::command]
pub(crate) fn set_auth(io: State<'_, SocketIo>, id: String, auth: Value) -> Result<()> {
    if !auth.is_object() {
        return Err(Error::InvalidOptions("auth must be an object".into()));
    }
    io.configure(&id, |d| d.auth = Some(auth))
}

/// Replaces the extra headers sent from the next connect attempt.
#[tauri::command]
pub(crate) fn set_headers(
    io: State<'_, SocketIo>,
    id: String,
    headers: BTreeMap<String, String>,
) -> Result<()> {
    crate::models::check_headers(&headers)?;
    io.configure(&id, |d| d.headers = headers)
}

/// Replaces the query parameters sent from the next connect attempt.
#[tauri::command]
pub(crate) fn set_query(
    io: State<'_, SocketIo>,
    id: String,
    query: BTreeMap<String, String>,
) -> Result<()> {
    io.configure(&id, |d| d.query = query)
}

/// Connects a closed client the webview connected again (rejected, reconnect attempts used
/// up, server disconnect); the target is the one checked at connect (query and headers do
/// not change it). Rust's clients: `NotConfigurable`.
#[tauri::command]
pub(crate) async fn reconnect(io: State<'_, SocketIo>, id: String) -> Result<ConnState> {
    let client = io.webview_client(&id)?;
    client.reconnect().await?;
    Ok(ConnState::from(&client.state()))
}
