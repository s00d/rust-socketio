# Changelog

All notable changes to `tauri-plugin-socketio` (crate) and `tauri-plugin-socketio-api` (npm)
are documented in this file. Both are released together with the same version.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- Server acks answered from JS (`ackId`, bounded: 2 min, 1024 pending); not offered for
  read-only clients.
- Binary arguments both ways (`Uint8Array` ↔ socket.io attachments).
- Permissions per command, `socketio:allow-read` (observe only) and `socketio:default`.
- Features `rustls` (default) and `native-tls`, forwarded to socketio-next.
