# tauri-plugin-socketio

[![crates.io](https://img.shields.io/crates/v/tauri-plugin-socketio.svg)](https://crates.io/crates/tauri-plugin-socketio)
[![npm](https://img.shields.io/npm/v/tauri-plugin-socketio-api.svg)](https://www.npmjs.com/package/tauri-plugin-socketio-api)
[![docs.rs](https://docs.rs/tauri-plugin-socketio/badge.svg)](https://docs.rs/tauri-plugin-socketio)

A Tauri 2 plugin for Socket.IO v4 on top of [socketio-next](https://crates.io/crates/socketio-next):
one registry of **named clients** that Rust and the webviews share.

- Rust connects clients through the plugin, or registers clients it built itself — for example
  in core code that must not depend on Tauri — optionally **read-only** for the webview.
- JS connects its own clients (`connect`) or attaches to registered ones (`attach`) with an API
  close to socket.io-client: `emit`, `emitWithAck`, `on` / `once` / `off` / `onAny`, acks
  for server callbacks, binary anywhere in the arguments.
- Events and state transitions go to each webview over its own IPC channel, in order, filtered
  by event name if asked. Subscriptions end when the webview reloads or closes.

The socket runs in Rust: no CORS, no browser websocket limits, TLS through rustls (or the
platform library), and the connection survives page reloads.

## Install

```toml
# src-tauri/Cargo.toml
[dependencies]
tauri-plugin-socketio = "0.1"
```

```sh
pnpm add tauri-plugin-socketio-api   # or npm / yarn
```

```rust
// src-tauri/src/lib.rs
tauri::Builder::default()
    .plugin(tauri_plugin_socketio::init())
```

```json
// src-tauri/capabilities/default.json
{ "identifier": "default", "windows": ["main"], "permissions": ["core:default", "socketio:default"] }
```

## JS

```ts
import { connect, attach, list, SocketIoError } from 'tauri-plugin-socketio-api'

const socket = await connect('chat', {
  url: 'https://chat.example.com',      // the path is the namespace, like socket.io-client
  auth: { token },
  transports: ['websocket'],            // default: polling upgraded to websocket
  reconnectionDelay: 500,
  events: ['message', 'typing'],        // optional: forward only these events to this webview
})

socket.on('message', (text, from) => console.log(from, text))
socket.on('question', (q, ack) => ack('answer'))       // the server emitted with a callback
socket.onAny((event, ...args) => console.debug(event, args))
const off = socket.onState((s) => console.log(s.status)) // connecting / connected / disconnected / reconnecting / closed

await socket.emit('message', 'hi', new Uint8Array([1, 2, 3]))
const [sum] = await socket.emitWithAck('add', [1, 2], { timeout: 5000 })

socket.connected; socket.id; socket.lastState
await socket.state(); await socket.linkInfo()
await socket.close()       // stop receiving here; the client stays connected
await socket.disconnect()  // leave, close, unregister (every webview sees `closed`)

const core = await attach('core')   // a client Rust registered
await list()                        // [{ id, access: 'full' | 'readOnly', namespace, state }]
```

Options (milliseconds for durations): `url`, `namespace`, `path`, `query`, `headers`
(`extraHeaders`), `auth`, `transports` (`['polling', 'websocket']`, `['websocket']`,
`['polling']` or `'websocket-upgrade'`), `timeout`, `reconnection`, `reconnectionAttempts`,
`reconnectionDelay`, `reconnectionDelayMax`, `randomizationFactor`, `reconnectOnDisconnect`,
`bufferWhileDisconnected`, plus `events` (the forwarding filter).

**Timing.** `connect` attaches the subscription before connecting, so the events the server
sends right after the connect are not lost; messages that arrive before `connect` / `attach`
resolve are held until the next task. Add handlers right after the `await`.

**Ids are unique.** `connect` with an id in use rejects with `AlreadyExists` — typically after a
page reload, since the client lives in Rust. `attach(id)` to it, or `disconnect` it first.

**Binary.** `Uint8Array`, `ArrayBuffer`, typed arrays and `DataView`s anywhere in the arguments
are sent as socket.io attachments; incoming attachments arrive as `Uint8Array`s. On the IPC
they are `{ args, binary }`, where `{"_placeholder":true,"num":n}` in `args` stands for
`binary[n]` (socket.io-parser's encoding; `encodeArgs` / `decodeArgs` are exported). Without
attachments nothing is replaced: a JSON object that looks like a placeholder stays JSON. Bytes
cross the IPC as arrays of numbers, so prefer the websocket for large blobs.

**Errors** are `SocketIoError` with `kind`, `message` and `data`: `ConnectRejected` (`data` is
the server middleware's `err.data`), `ConnectTimeout`, `NotConnected`, `AckTimeout`,
`Disconnected`, `Closed`, `AccessDenied`, `UnknownClient`, `AlreadyExists`, `UnknownAck`, engine
failures by name (`HttpStatus` with `data.status`, `Websocket`, `Http`, `TlsProviderMissing`,
...), and `Ipc` for a rejection that is not the plugin's (a command the capability does not
allow). Messages never contain URLs or query values.

## Rust

```rust
use socketio_next::ClientBuilder;
use tauri_plugin_socketio::{Access, SocketIoExt};

// in setup or a command; `app` is anything that implements `tauri::Manager`
let io = app.socketio();
let client = io.connect("chat", ClientBuilder::new("https://chat.example.com")).await?; // Access::Full
io.insert("core", my_client, Access::ReadOnly)?; // built elsewhere
io.get("core");          // Option<Client>
io.access("core");       // Option<Access>
io.ids();                // Vec<String>
io.remove("core");       // Option<Client>: unregisters, does not disconnect
io.subscribe("core", Some(vec!["tock".into()]), channel)?; // forward to a tauri::ipc::Channel
```

| Method | Behaviour |
|---|---|
| `connect(id, builder) -> Result<Client>` | builds, connects, registers as `Access::Full`; `AlreadyExists` when the id is taken (the new client is dropped) |
| `insert(id, client, access) -> Result<()>` | registers a client built elsewhere; `AlreadyExists` when the id is taken — never replaces |
| `get(id) -> Option<Client>` / `access(id)` / `ids()` | lookups |
| `remove(id) -> Option<Client>` | unregisters; its subscriptions stop and its pending server acks are dropped; **does not disconnect** (call `disconnect().await` on the returned client, or drop it: the last clone closes) |
| `subscribe(id, events, channel)` / `unsubscribe(id, sub)` | what `attach` uses, for Rust-side channels |

The JS `disconnect()` (`Access::Full` only) unregisters first, then disconnects, so its
subscriptions still see the final `closed` state.

`tauri_plugin_socketio::socketio_next` re-exports the client crate.

## Read-only clients (Rust owns the socket)

When the socket belongs to Rust code that must stay free of Tauri, let that code build the
client and hand it over:

```rust
// a Tauri-free crate
pub async fn start(url: &str) -> socketio_next::Result<socketio_next::Client> {
    socketio_next::ClientBuilder::new(url).connect().await
}

// the app
.setup(|app| {
    let handle = app.handle().clone();
    tauri::async_runtime::spawn(async move {
        let client = core::start("https://example.com").await?;
        handle.socketio().insert("core", client.clone(), Access::ReadOnly)?;
        // keep `client` in your own state to emit from Rust
        Ok::<_, tauri_plugin_socketio::Error>(())
    });
    Ok(())
})
```

```json
{ "identifier": "default", "windows": ["main"], "permissions": ["core:default", "socketio:allow-read"] }
```

```ts
const core = await attach('core')
core.on('news', render)
await core.emit('x') // rejects: AccessDenied
```

`Access::ReadOnly` clients refuse `emit`, `emitWithAck`, `ack` and `disconnect` from every
webview **whatever the capabilities allow**; the permission sets only decide which commands a
window may call at all. Server acks of a read-only client are not forwarded with an `ackId`
answerable from JS (`ack` is refused); answer them in Rust.

## Permissions

| Identifier | Commands |
|---|---|
| `socketio:default` | everything below |
| `socketio:allow-read` | `list`, `subscribe`, `unsubscribe`, `state`, `link_info` |
| `socketio:allow-connect` / `allow-disconnect` | `connect` / `disconnect` |
| `socketio:allow-emit` / `allow-emit-with-ack` / `allow-ack` | `emit` / `emit_with_ack` / `ack` |
| `socketio:allow-subscribe`, `allow-unsubscribe`, `allow-list`, `allow-state`, `allow-link-info` | one each |

Every `allow-*` has a `deny-*`. The full table is in
[permissions/autogenerated/reference.md](permissions/autogenerated/reference.md).

## Messages and limits

A subscription's channel carries `{ type: 'state', state }` (the first one is the state at
subscribe time) and `{ type: 'event', event, args, binary?, ackId? }`. State objects are
`{ status: 'connecting', attempt }`, `{ status: 'connected', sid, recovered }`,
`{ status: 'disconnected', reason }`, `{ status: 'reconnecting', attempt, delay, error? }`,
`{ status: 'closed', reason, error? }` with socket.io-client's reason strings (`io server
disconnect`, `io client disconnect`, `ping timeout`, `transport close`, `transport error`,
`parse error`) plus `connect rejected`, `reconnect failed`, `client dropped`.

Server acks wait for the webview's answer for `ACK_TTL` (2 min), at most `MAX_PENDING_ACKS`
(1024) at a time, the oldest dropped first. A failed first connect sends no state message: the
rejected promise is the answer.

## TLS, mobile

Features: `rustls` (default) and `native-tls`, forwarded to socketio-next. If any crate in your
build enables reqwest 0.13's `rustls-no-provider` — `tauri-plugin-updater` does — keep `rustls`
on, or connects fail with `TlsProviderMissing` (see socketio-next's README). On Android and iOS
rustls with the bundled webpki roots works without setup.

## Troubleshooting

| Symptom | Check |
|---|---|
| `Ipc: ... not allowed` | The window's capability lacks the command (`socketio:default` or the `allow-*` you need). |
| `AlreadyExists` after a reload | The client survived the reload in Rust: `attach(id)`, or `disconnect()` it before `connect`. |
| `AccessDenied` | The client was registered `Access::ReadOnly`; drive it from Rust. |
| `UnknownClient` right at startup | Rust registers its client after its first connect; retry `attach` or wait for `list()`. |
| `UnknownAck` | The ack was answered already, expired (`ACK_TTL`) or belongs to another client. |
| `TlsProviderMissing` | See TLS above. |
| Missed events | Add handlers right after `await connect(...)`; check the `events` filter. |
| Anything else about the connection | socketio-next's troubleshooting table; `RUST_LOG=socketio_next=debug`. |

## Example

[`examples/tauri-app`](examples/tauri-app): Rust registers a read-only `core` client and drives
it, the webview attaches to it and connects its own `chat` client. Run `just servers` at the
repository root, then `pnpm install && pnpm tauri dev` in the example.

## License

MIT
