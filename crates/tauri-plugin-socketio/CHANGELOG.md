# Changelog

All notable changes to `tauri-plugin-socketio` (crate) and `tauri-plugin-socketio-api` (npm)
are documented in this file. Both are released together with the same version.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- A webview may not set the `Origin`, `Forwarded`, `X-Forwarded-*` and `X-Real-IP` headers
  (a page could pose as another origin or client address to the server).
- A webview's query can no longer override engine.io's own parameters (`sid`, `EIO`,
  `transport`, `t`, `b64`): with a read-only client's engine sid from `linkInfo`, a page could
  disturb that client's polling session (socketio-next drops those keys now).

### Fixed

- The clients a webview connected are disconnected when its window is destroyed (a reload keeps
  them), and one webview may have at most `MAX_CLIENTS_PER_WEBVIEW` (64) at once
  (`TooManyClients`); before, a loop of connect + close left every client connected.
- A `reconnect` racing a `disconnect` revived a client that was already unregistered; the
  revived connection is closed again and the id reported unknown.

### Fixed (guest-js)

- `connect()` / `attach()`: the messages that came before the call resolved are held until the
  subscription's snapshot arrived, not for one fixed tick; a large event ahead of the snapshot
  (Tauri sends it by `fetch`) made stale `connecting` / `connected` states fire again.
- A throwing listener no longer stops the other listeners or the later messages; the error is
  rethrown on its own.
- `off(event, fn)` removes a listener added with `once(event, fn)`.
- `Blob` / `File` arguments are read and sent as binary (they went out as `{}`).
- After `disconnect()` the socket reports `closed` (`io client disconnect`) right away.
- A failed ack answer (expired, or answered by another socket of the webview) is logged instead
  of an unhandled promise rejection.

### Added (guest-js)

- `connect`, `disconnect` (with the reason) and `connect_error` events from the state
  transitions, as socket.io-client emits them.

### Changed

- Documentation at <https://s00d.github.io/rust-socketio/tauri/setup>; the README is short
  and links to it.
- guest-js exports the `WireArgs` type (the return type of `encodeArgs`).
- guest-js needs `@tauri-apps/api` 2.12.3 or newer (earlier versions stop a channel after a
  throwing callback and leak channel callbacks).
- Connect options in milliseconds (`timeout`, `reconnectionDelay`, `reconnectionDelayMax`) are
  read like `emitWithAck`'s timeout: a fraction is cut (it was refused), a negative is refused.

## [0.1.0] - 2026-11-08

### Added

- First release: a registry of named socketio-next clients shared by Rust and the webviews.
- Rust: `SocketIoExt::socketio()` → `SocketIo` with `connect`, `insert` (`Access::Full` /
  `Access::ReadOnly`), `get`, `access`, `ids`, `remove` (never disconnects), `subscribe` /
  `unsubscribe`. Ids are unique (`AlreadyExists`, no silent replacement).
- JS: `connect`, `attach`, `list`; `Socket` with `emit`, `emitWithAck`, `on` / `once` / `off` /
  `onAny` / `offAny`, `onState`, `state`, `linkInfo`, `connected`, `id`, `disconnect`, `close`;
  typed `SocketIoError { kind, message, data }`.
- Per-subscription IPC channels (ordered, per webview) with an optional event filter; ended on
  unsubscribe, on a failed send, when the webview loads a new page or its window is destroyed,
  and when the client is removed.
- Server acks answered from JS: one `ackId` per server ack for every subscribed webview,
  answered once; 2 min TTL and a budget per webview; not offered for read-only clients or once a
  disconnect started.
- `SocketIo::reserve(id)`: ids Rust registers later cannot be taken by a page script.
- `Builder::allow_url_prefix`: URL allow-list for webview connects (the CSP does not cover
  these sockets); `init()` is `Builder::new().build()`.
- Webview isolation: `unsubscribe` and `ack` act only on the caller's subscriptions and acks;
  at most 64 subscriptions per webview.
- `connect` / `attach` resolve with the current state (the socket is `connected` right away);
  the channel starts with that snapshot (`snapshot: true`) and nothing older follows it.
- One forwarding hub per client: ending a subscription drops its channel; a reload during a
  JS `connect` leaves no subscription behind.
- `setAuth` / `setHeaders` / `setQuery` (commands `set_auth`, `set_headers`, `set_query`) for
  clients a webview connected, read before every connect attempt; `reconnect` after `closed`;
  a `ca` option (PEM) for private CAs.
- `linkInfo` has `packetsIn` / `packetsOut`, `bytesIn` / `bytesOut`, `sessions`; `disconnected`
  states carry the error (`WebsocketClosed` with `data: { code, reason }`); `HttpStatus`
  errors have `data.message`.
- The URL allow-list checks the engine.io path the connection uses (not the URL path, which
  is the namespace); concurrent connects hold their id and subscription slot, so they cannot
  pass the per-webview cap; `reconnect` only for clients a webview connected; headers the
  webview may not set (`Host`, `Cookie`, `Sec-WebSocket-*`, `Proxy-*`, ...).
- The URL allow-list compares the URL socketio-next really builds (dot segments resolved) and
  refuses `..`, `%2e` and backslashes in `path`.
- `State` and `Message` and their struct-like variants are `#[non_exhaustive]`.
- Typed `InvalidOptions` for wrong option types/values (values are not echoed); connect
  options' `Debug` hides query, header and auth values.
- Binary arguments both ways (`Uint8Array` ↔ socket.io attachments).
- Permissions per command, `socketio:allow-read` (observe only) and `socketio:default`.
- Features `rustls` (default) and `native-tls`, forwarded to socketio-next.
