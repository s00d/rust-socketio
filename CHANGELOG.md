# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
