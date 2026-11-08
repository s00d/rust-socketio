//! The webview's commands (`plugin:socketio|<name>`). Clients registered as
//! [`Access::ReadOnly`](crate::Access::ReadOnly) refuse `emit`, `emit_with_ack`, `ack` and
//! `disconnect` whatever the capabilities allow.

use std::time::Duration;

use serde::Serialize;
use tauri::ipc::Channel;
use tauri::{Runtime, State, Webview};

use crate::Result;
use crate::models::{Args, ClientInfo, ConnectOptions, Link, Message, State as ConnState};
use crate::registry::{SocketIo, SubscribeArgs, SubscriptionId};

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

/// What `connect` resolves with.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Connected {
    subscription: SubscriptionId,
}

/// Connects a new client under `id` ([`Access::Full`](crate::Access::Full)); the channel
/// subscription is attached before the connect, so no event after CONNECT is missed.
#[tauri::command]
pub(crate) async fn connect<R: Runtime>(
    webview: Webview<R>,
    io: State<'_, SocketIo>,
    id: String,
    options: ConnectOptions,
    events: Option<Vec<String>>,
    channel: Channel<Message>,
) -> Result<Connected> {
    let sub = sub_args(&webview, events, channel);
    let (_, subscription) = io
        .connect_with(id, options.into_builder(), Some(sub))
        .await?;
    Ok(Connected {
        subscription: subscription.unwrap_or_default(),
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
    timeout: u64,
) -> Result<Args> {
    let client = io.full(&id)?;
    let reply = client
        .emit_with_ack(event, args, Duration::from_millis(timeout))
        .await?;
    Ok(reply.into())
}

/// Answers a server ack forwarded with `ackId`.
#[tauri::command]
pub(crate) fn ack(io: State<'_, SocketIo>, id: String, ack_id: u64, args: Args) -> Result<()> {
    io.ack(&id, ack_id, args.into())
}

/// Forwards an existing client (any access) to the channel.
#[tauri::command]
pub(crate) fn subscribe<R: Runtime>(
    webview: Webview<R>,
    io: State<'_, SocketIo>,
    id: String,
    events: Option<Vec<String>>,
    channel: Channel<Message>,
) -> Result<SubscriptionId> {
    io.subscribe_with(&id, sub_args(&webview, events, channel))
}

#[tauri::command]
pub(crate) fn unsubscribe(
    io: State<'_, SocketIo>,
    id: String,
    subscription: SubscriptionId,
) -> bool {
    io.unsubscribe(&id, subscription)
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
