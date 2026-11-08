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
await socket.state(); await socket.linkInfo()   // ... packetsIn/Out, bytesIn/Out, sessions
await socket.setAuth({ token: fresh })           // used from the next connect attempt
await socket.setHeaders({ authorization: `Bearer ${fresh}` }); await socket.setQuery({ v: '2' })
await socket.reconnect()   // after `closed` (rejected, attempts used up, server disconnect)
await socket.close()       // stop receiving here; the client stays connected
await socket.disconnect()  // leave, close, unregister (every webview sees `closed`)

const core = await attach('core')   // a client Rust registered
await list()                        // [{ id, access: 'full' | 'readOnly', namespace, state }]
```

Options (milliseconds for durations): `url`, `namespace`, `path`, `query`, `headers`
(`extraHeaders`), `auth`, `transports` (`['polling', 'websocket']`, `['websocket']`,
`['polling']` or `'websocket-upgrade'`), `timeout`, `reconnection`, `reconnectionAttempts`,
`reconnectionDelay`, `reconnectionDelayMax`, `randomizationFactor`, `reconnectOnDisconnect`,
`bufferWhileDisconnected`, `ca` (PEM certificates of a private CA; checks cannot be turned
off), plus `events` (the forwarding filter). Auth, headers and query are read again before every
connect attempt, so `setAuth` / `setHeaders` / `setQuery` take effect on the next reconnect;
they and `reconnect` work on clients a webview connected (`NotConfigurable` for Rust's,
`AccessDenied` for read-only ones).

**Timing.** `connect` attaches the subscription before connecting, so the events the server
sends right after the connect are not lost. When `connect` / `attach` resolve, the socket
already has the current state (`socket.connected`, `socket.id`); messages that arrived before
are held until the next task (states older than that snapshot are dropped). Add handlers right
after the `await`.

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
`Disconnected`, `Closed`, `AccessDenied`, `UnknownClient`, `AlreadyExists`, `UnknownAck`,
`Reserved`, `UrlNotAllowed`, `TooManySubscriptions`, `InvalidOptions` (a wrong option type or
value; the value is not echoed), `NotConfigurable`, `WebsocketClosed` (`data: { code, reason }`),
`ProxyFailed`, engine
failures by name (`HttpStatus` with `data.status` and `data.message`, `Websocket`, `Http`, `TlsProviderMissing`,
...), and `Ipc` for a rejection that is not the plugin's (a command the capability does not
allow). Messages never contain URLs or query values.

## Rust

```rust
use socketio_next::ClientBuilder;
use tauri_plugin_socketio::{Access, SocketIoExt};

// in setup or a command; `app` is anything that implements `tauri::Manager`
let io = app.socketio();
io.reserve("core");      // in setup, before any await: webviews cannot take the id
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
| `reserve(id)` | synchronous; a webview's `connect` with this id fails with `Reserved`, Rust's `connect` / `insert` work. Lasts for the app's life |
| `get(id) -> Option<Client>` / `access(id)` / `ids()` | lookups |
| `remove(id) -> Option<Client>` | unregisters; its subscriptions stop and its pending server acks are dropped; **does not disconnect** (call `disconnect().await` on the returned client, or drop it: the last clone closes) |
| `subscribe(id, events, channel)` / `unsubscribe(id, sub)` | what `attach` uses, for Rust-side channels (no server acks are offered to them; any subscription can be ended from Rust) |

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
    app.socketio().reserve("core"); // synchronous: before any page script can connect("core")
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

## Security

- **Reserve Rust's ids.** Rust usually registers its clients after an `await` (the connect).
  Until then any page script with `allow-connect` could `connect` under the same id, and Rust's
  `insert` would fail with `AlreadyExists` while `get(id)` returned the page's client. Call
  `reserve(id)` in `setup`.
- **The CSP does not cover these sockets.** They run in Rust, so `connect-src` does not limit
  them: with `allow-connect` a page can reach any host with any headers. Restrict it:

  ```rust
  .plugin(
      tauri_plugin_socketio::Builder::new()
          .allow_url_prefix("https://api.example.com")       // scheme + host + port
          .allow_url_prefix("wss://rt.example.com/v1/")      // ... and a path prefix
          .build(),
  )
  ```

  What is enforced: the scheme, the host, the port, and the **engine.io path** (the `path`
  option, default `/socket.io/`) under the prefix's path on a `/` boundary. The URL's own
  path is the namespace and is not part of the check, nor is the `namespace` option. Query and
  headers changed later (`setQuery`, `setHeaders`) and `reconnect` cannot move the target.
  Without `allow_url_prefix` every URL is allowed (`init()`). Rust's `connect` / `insert` are
  never restricted.
- **Headers from the webview are limited.** `Host`, `Connection`, `Upgrade`,
  `Content-Length`, `Transfer-Encoding`, `Cookie` (cookies come from the jar),
  `Sec-WebSocket-*` and `Proxy-*` are refused with `InvalidOptions` naming the header, in the
  connect options and in `setHeaders`. Rust's clients are not limited. Grant `allow-read` instead of `default` to windows that only observe.
- **Webviews are isolated.** `unsubscribe` and `ack` act only on the calling webview's
  subscriptions and on acks offered to it (`UnknownAck` otherwise). A webview has at most
  `MAX_SUBSCRIPTIONS_PER_WEBVIEW` (64) subscriptions and, per client, at most
  `MAX_PENDING_ACKS_PER_WEBVIEW` (256) unanswered server acks; past that its own oldest ack is
  forgotten, never another webview's.

## Permissions

| Identifier | Commands |
|---|---|
| `socketio:default` | everything below |
| `socketio:allow-read` | `list`, `subscribe`, `unsubscribe`, `state`, `link_info` |
| `socketio:allow-connect` / `allow-disconnect` | `connect` / `disconnect` |
| `socketio:allow-emit` / `allow-emit-with-ack` / `allow-ack` | `emit` / `emit_with_ack` / `ack` |
| `socketio:allow-set-auth` / `allow-set-headers` / `allow-set-query` / `allow-reconnect` | `set_auth` / `set_headers` / `set_query` / `reconnect` |
| `socketio:allow-subscribe`, `allow-unsubscribe`, `allow-list`, `allow-state`, `allow-link-info` | one each |

Every `allow-*` has a `deny-*`. The full table is in
[permissions/autogenerated/reference.md](permissions/autogenerated/reference.md).

## Messages and limits

A subscription's channel carries `{ type: 'state', state }` (the first one is the state at
subscribe time) and `{ type: 'event', event, args, binary?, ackId? }`. State objects are
`{ status: 'connecting', attempt }`, `{ status: 'connected', sid, recovered }`,
`{ status: 'disconnected', reason, error? }`, `{ status: 'reconnecting', attempt, delay, error? }`,
`{ status: 'closed', reason, error? }` with socket.io-client's reason strings (`io server
disconnect`, `io client disconnect`, `ping timeout`, `transport close`, `transport error`,
`parse error`) plus `connect rejected`, `reconnect failed`, `client dropped`.

A server ack gets one `ackId`, offered to every webview subscribed to the event; the first
answer wins, later ones fail with `UnknownAck`. Acks wait for `ACK_TTL` (2 min); see Security
for the budgets. Once a disconnect starts, no ack is offered any more.

A failed first connect can send the attempt's states (`connecting`, `closed`) on the channel
before the promise rejects; guest-js drops that socket, the rejection is the answer.

The first state message of a subscription carries `snapshot: true`: the state at subscribe
time, also returned by `connect` / `attach`; nothing older follows it on that channel.

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
| `Reserved` | Rust reserved the id; `attach` to it instead. |
| `UrlNotAllowed` | Add the server to `Builder::allow_url_prefix`. |
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
