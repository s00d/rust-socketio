# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `tauri-plugin-socketio` (crate) and `tauri-plugin-socketio-api` (npm): a Tauri 2 plugin with
  named clients shared by Rust and the webviews; its own changelog is
  [crates/tauri-plugin-socketio/CHANGELOG.md](crates/tauri-plugin-socketio/CHANGELOG.md).

- `ClientBuilder::connect_params_with`: query parameters and headers computed before every
  connect attempt (inside its timeout, panics contained), like `auth_with`.
- `Client::reconnect()` (and blocking): connect a closed client again with the same handlers.
- `ClientBuilder::proxy(..)` and `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY` / `NO_PROXY` for the
  websocket too (a `CONNECT` tunnel with basic auth); `engineio::Error::ProxyFailed`.
- `cookies` feature: one cookie jar per client for polling and the websocket handshake.
- `platform-verifier` feature: rustls with the OS trust store (not on Android).
- `engineio::Error::WebsocketClosed { code, reason }` from close frames (also during the
  upgrade probe), a `TransportClose`; `ConnectionState::Disconnected` carries the error.
- `HttpStatus` has the body's message (`message`, at most 4 KiB read, URLs removed).
- `LinkInfo`: `packets_in` / `packets_out`, `bytes_in` / `bytes_out`, `sessions`.

### Changed

- Breaking (pre-release): `ConnectionState::Disconnected` has an `error` field and
  `engineio::Error::HttpStatus` a `message` field; the struct-like variants of
  `ConnectionState`, `Error::ConnectRejected`, `engineio::Error::HttpStatus` and
  `WebsocketClosed` are `#[non_exhaustive]` (match them with `..`), so fields can be added
  later without a break. Code ported from rust_socketio matches states with `..` anyway.
- Environment proxies (`HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY`, `NO_PROXY`) now also apply to
  the websocket, `ws://` included, with the same matcher reqwest uses for polling; a proxy that
  refuses `CONNECT` to non-443 ports refuses the websocket (with `TransportType::Any` the client
  stays on polling). `ClientBuilder::no_proxy()` turns proxies off for both transports.
  `https://` and SOCKS proxies are a `ProxyFailed` error for the websocket.
- Redirects are not followed (a 3xx is an `HttpStatus` error without the target).
- The wall-clock check of the heartbeat probes the link and waits pingTimeout instead of
  closing at once, so an NTP step on a live link does not disconnect.
- `HttpStatus.message` is at most 200 characters and drops words that look like URLs, paths,
  queries or credentials, for JSON messages too.
- A refused websocket handshake (3xx, 4xx) is `HttpStatus { status }`; the response (with a
  `Location` that may carry the query) is not kept in the error.
- The scrubbing of `HttpStatus.message` keeps ordinary words ("Unauthorized") and removes
  credential values in `name=value`, `name: value`, `name value` and `Bearer value` forms.
- Environment proxies are read once, when the client is built; both transports use that
  decision for the client's life.
- `ClientBuilder::engine_url()`: the engine.io URL a connect will use (path normalized).
- One reqwest client per socket.io client, shared by every transport and reconnect (it was
  two per connect attempt); only long-poll GETs are bounded by pingInterval + pingTimeout, so
  slow POST uploads are no longer cut.

### Fixed

- The heartbeat also checks the wall clock: after a system sleep a dead link is detected on
  wake-up instead of after the remaining monotonic time.
- The websocket reader could miss a wakeup when another task held the receiver (a lock
  future polled once and dropped); the receiver is now polled under a short std lock.
- The wall-clock liveness probe over polling goes through the normal write lock (a POST
  next to another one made engine.io close the session with "data request overlap").
- `reconnect()` racing `disconnect()`: a disconnect always wins (no revived client, no hang).
- The caller's `Cookie` header and the cookie jar are merged (`user; jar`) on polling and on
  the websocket handshake (`cookies` feature).
- IPv6 servers and proxies through the `CONNECT` tunnel (`[::1]`).
- `connect().await` could return before the client was connected: `connected()` false,
  `state()` `Connecting`, `id()` None and `emit()` failing with `NotConnected` on a
  multi-thread runtime or with the blocking client. The connected state is now set before
  `connect()` returns and after every reconnect.

## [0.1.0] - 2026-11-08

First release of `socketio-next`, the successor of the s00d/rust-socketio fork of
[rust_socketio](https://github.com/1c3t3a/rust-socketio) 0.6.0 (its history is in that
repository). See "Differences from rust_socketio" in the README for the migration table.

### Added

- One crate: the socket.io client, `engineio` (transport layer) and `protocol` (codecs, no I/O).
- Socket.IO client with socket.io-client 4.8 behaviour: connection task + ordered event task,
  `ConnectionState` / `DisconnectReason` / `LinkInfo`, awaitable acks with timeout, server acks
  answered (`Event::ack`), runtime `on` / `once` / `on_any` / `off`, sync or async handlers
  without boxing, `buffer_while_disconnected` (opt-in send buffer), connection state recovery,
  `auth_with`, query and headers, packet inspector, typed cloneable errors.
- socket.io-parser binary encoding: several attachments, nested placeholders (`Payload::Mixed`);
  maxAttachments 10; numeric and argument-less event names.
- socket.io-client reconnection backoff (backo2, randomizationFactor 0.5, delay before every
  attempt); `reconnection_attempts` is final (`ReconnectFailed`).
- TLS as a feature: `rustls` (default, ring + webpki roots), `native-tls`, or none; `TlsConfig`
  with `with_root_certificates_pem`.
- `blocking` feature: the async client on a private runtime.
- `TlsProviderMissing` instead of a reqwest panic when another crate enabled reqwest's
  `rustls-no-provider` (found integrating with a Tauri app using `tauri-plugin-updater`).
- tracing with spans per client and redacted query values; errors never carry URLs.
- Differential tests against socket.io-client 4.8 (`ci/conformance`), Node test servers without
  Docker (`ci/servers.sh`), `justfile`, CI on Linux, macOS and Windows.

### Fixed (carried over from the fork, each with a regression test)

- Whole long-polling bodies; bounded handshakes; non-200 polling answers; websocket frames as
  single packets; unique ack ids; `disconnect()` stops reconnects and hung attempts; the engine
  closes after namespace disconnect, CONNECT_ERROR and decode errors; emits before CONNECT are
  buffered; BINARY_EVENT for binary emits with ack; socket.io-parser validation rules; no
  unsolicited PONG.

### Removed

- The sync engine.io client and transports; `Payload::String`; the `backoff`, `adler32`,
  `async-stream`, `async-trait` and direct `native-tls` dependencies.
