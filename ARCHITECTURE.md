# Architecture

How the crates are split and who owns which state. Read this before changing the client's
tasks or adding a crate.

## Crates

| crate | what it holds | published |
|---|---|---|
| `socketio-next` | the Socket.IO v4 client: `Client` / `ClientBuilder`, the `engineio` transport layer, the `protocol` codecs (no I/O), the `blocking` wrapper | yes |
| `tauri-plugin-socketio` | named clients shared by Rust and the webviews of a Tauri 2 app; the npm package `tauri-plugin-socketio-api` is its webview side | yes (crate + npm) |
| `socketio-next-testing` | test support: fake servers, Node server URLs, proxies, the differential harness | no |
| `examples` | examples and the load run | no |

## The client's tasks (socketio-next)

Every piece of state has one owner; the others hold a handle and send it messages.

- **Connection task** (`actor.rs`), one per `Client`, for the client's whole life. It owns the
  engine.io connection, pending acks, the send buffer, the reconnect loop, connection state
  recovery, the published `ConnectionState` (a `watch`) and the `LinkInfo` snapshot. When the
  client closes it waits for `reconnect()`. Handles reach it through one command channel
  (`Command`); `emit`, `reconnect` and `disconnect` take effect in the order they are sent.
- **Event task** (`client.rs`, `dispatch`), one per `Client`: runs the handlers one at a time in
  arrival order. It holds a weak reference, so it never keeps the client alive. `disconnect()`
  raises a cut (sessions below it dispatch no more events) and waits for a fence queued after
  the `Closed` state.
- **Writer task** (`actor.rs`, `writer_task`), one per connection: batches packets into the
  engine.io socket so a slow write never blocks the connection task.
- **engine.io session** (`engineio/socket.rs`): phase and heartbeat in one `SessionState`
  behind one lock, transitions only through `Socket` methods; writes serialised by the writer
  lock; callbacks run in order on one dispatcher task (`engineio/callback.rs`).
- **Handles**: `Client` (an `Arc`; the last one dropped stops the connection task) and
  `WeakClient` for places the client owns, such as handlers.

## Test servers (socketio-next-testing)

`fake/` serves the `ci/*.js` fixtures in process: a server actor owns the listeners, the
connections and the session registry; each engine.io session is an actor owning its transport,
heartbeat and app (socket.io fixtures or the engine.io echo). Its codecs are written from the
protocol, not shared with the client. Behaviour only the reference implementation settles
(differential tests, recovery, cookies, TLS) stays on the Node servers in `ci/`.

## The plugin (tauri-plugin-socketio)

`registry.rs` keeps the named clients, each with a hub that fans events and states out to the
webview subscriptions (Tauri channels) and keeps the server acks offered to webviews.
`commands.rs` is the IPC surface, `models.rs` the IPC shapes and option checks, `guest-js/` the
webview API.
