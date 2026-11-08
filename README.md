# socketio-next

[![crates.io](https://img.shields.io/crates/v/socketio-next.svg)](https://crates.io/crates/socketio-next)
[![docs.rs](https://docs.rs/socketio-next/badge.svg)](https://docs.rs/socketio-next)
[![CI](https://github.com/s00d/rust-socketio/actions/workflows/ci.yml/badge.svg)](https://github.com/s00d/rust-socketio/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](MSRV.md)
[![license](https://img.shields.io/crates/l/socketio-next.svg)](LICENSE)

A [Socket.IO](https://socket.io) v4 client for Rust that behaves like the reference JavaScript
client (socket.io-client 4.8 / engine.io-client 6.6): namespaces, auth, acks in both
directions, binary data anywhere in the arguments, the same reconnection backoff, connection
state recovery, polling and websocket transports, TLS through rustls or native-tls. Async
(tokio) with an optional blocking API.

It started as a fork of [rust_socketio](https://github.com/1c3t3a/rust-socketio) and is a
rewrite of its client on top of the same protocol work; see
[Differences from rust_socketio](#differences-from-rust_socketio).

```toml
[dependencies]
socketio-next = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use std::time::Duration;
use serde_json::json;
use socketio_next::{Client, ClientBuilder, ConnectionState, Event};

#[tokio::main]
async fn main() -> socketio_next::Result<()> {
    let client = ClientBuilder::new("http://localhost:3000")
        .auth(json!({ "token": "abc" }))
        .on("chat", |event: Event, _client: Client| async move {
            println!("chat: {:?}", event.payload());
        })
        .on_state_change(|state: ConnectionState, _client: Client| async move {
            println!("{state:?}");
        })
        .connect()
        .await?;

    client.emit("chat", "hello")?;
    let reply = client
        .emit_with_ack("add", vec![json!(1), json!(2)], Duration::from_secs(5))
        .await?;
    println!("1 + 2 = {}", reply.arg::<i64>(0)?);

    client.disconnect().await
}
```

## Features

| Cargo feature | Default | What |
|---|---|---|
| `rustls` | yes | TLS with rustls (ring provider, Mozilla roots from `webpki-roots`). Pure Rust: no OpenSSL, builds for Android/iOS. |
| `native-tls` | no | TLS with the platform library: OpenSSL on Linux, Security.framework on macOS, SChannel on Windows. |
| `blocking` | no | `socketio_next::blocking`: the same client with blocking methods and plain-closure handlers. |

Without `rustls` and `native-tls` (`default-features = false`) the crate builds without any TLS
code; `https`/`wss` URLs then fail with `TlsNotEnabled`. If another crate in your build turns on
reqwest 0.13's `rustls-no-provider` (for example `tauri-plugin-updater`), reqwest needs a rustls
provider even for plain `http`: connecting then fails with `TlsProviderMissing`; enable `rustls`
here (it hands reqwest a ring-based config, no global provider needed). With both, rustls is the default and a
`native_tls::TlsConnector` passed to `tls_config` selects native-tls.

## The API in short

**Connect.** `ClientBuilder::new(url)` → options → `.connect().await` resolves once the server
answered the namespace CONNECT, or fails with the error of that first attempt.

| Option | socket.io-client | Default |
|---|---|---|
| `namespace("/admin")` (or the URL path: `http://host/admin`) | `io(url + nsp)` | `/` |
| `path("/socket.io/")` | `path` | `/socket.io/` |
| `query(k, v)` | `query` | — |
| `header(k, v)` | `extraHeaders` | — |
| `auth(json)`, `auth_with(\|\| async { json })` | `auth` (object / function) | — |
| `transport_type(Any \| Polling \| Websocket \| WebsocketUpgrade)` | `transports` | polling + upgrade, polling fallback |
| `tls_config(TlsConfig \| rustls::ClientConfig \| native_tls::TlsConnector)` | `ca`, `rejectUnauthorized` | default roots |
| `timeout(d)` | `timeout` | 20 s for handshake + upgrade + CONNECT |
| `reconnection(bool)`, `reconnection_attempts(n)` | same | on, unlimited |
| `reconnection_delay(d)`, `reconnection_delay_max(d)`, `randomization_factor(f)` | same | 1 s, 5 s, 0.5 |
| `reconnect_on_disconnect(bool)` | — (never) | off |
| `buffer_while_disconnected(bool)` | `sendBuffer` (always on) | off: fail fast |
| `redact_query(bool)` | — | on |
| `inspect_packets(\|dir, packet\| ..)` | — | — |

**Emit.** `client.emit(event, data)` never blocks: it queues the packet and returns. Offline it
fails with `Error::NotConnected` (or queues, with `buffer_while_disconnected`).
`client.emit_with_ack(event, data, timeout).await` returns the ack's arguments, or
`Error::AckTimeout` / `Error::Disconnected`. `data` is anything `Into<Payload>`: a
`serde_json::Value`, a `Vec<Value>` (several arguments), a string, a number, `()` (no
arguments), `Bytes` / `Vec<u8>`, or `Payload::serialize(&your_struct)?`.

**Receive.** Handlers get an [`Event`] (name, [`Payload`], ack) and the `Client`. Write them as
a plain closure (runs on tokio's blocking pool, may block) or a closure returning a future (runs
on the client's event task); no boxing. They run one at a time in arrival order. A handler may
emit, await acks, add handlers and call `disconnect()` without deadlocks.

```rust,ignore
client.on("question", |event: Event, _: Client| {
    let (text, n): (String, u32) = event.payload().deserialize().unwrap();
    event.ack(json!({ "answer": n + 1 })).unwrap(); // the server's callback
});
client.once("ready", |_: Event, _: Client| println!("ready"));
let id = client.on_any(|event: Event, _: Client| println!("{}", event.name()));
client.off_listener(id);
```

**Binary.** A lone buffer arrives as `Payload::Binary`; buffers anywhere else (several, next to
JSON, inside objects) as `Payload::Mixed { args, attachments }`, where
`{"_placeholder":true,"num":n}` in `args` stands for `attachments[n]` — the socket.io wire
format. Send the same shapes.

**State.** `ConnectionState`: `Connecting { attempt }`, `Connected { sid, recovered }`,
`Disconnected { reason }`, `Reconnecting { attempt, delay, error }`, `Closed { reason, error }`.
Read it with `client.state()`, wait with `client.wait_for(|s| ..)`, watch it with
`client.state_watch()`, or get every transition in order with `on_state_change`.
`DisconnectReason` has socket.io-client's reasons (`io server disconnect`, `io client
disconnect`, `ping timeout`, `transport close`, `transport error`, `parse error`) plus
`ConnectRejected`, `ReconnectFailed` and `Dropped`. `client.link_info()` gives the transport,
sids, ping interval/timeout, last ping, last packet, attempts, pending acks.

**Shutdown.** `client.disconnect().await` leaves the namespace and closes the connection; it is
bounded (1 s for the goodbye packets on a dead link) and no handler starts after it returns.
Dropping the last `Client` closes the connection too.

**Errors.** One typed, `non_exhaustive`, cloneable `Error`: `ConnectRejected { message, data }`
(the server's `connect_error`), `ConnectTimeout`, `IncompatibleServer`, `Engine(engineio::Error)`
(`HttpStatus { status }`, `Timeout`, `PingTimeout`, `UpgradeUnavailable`, `InvalidHandshake`,
`TlsNotEnabled`, ...), `NotConnected`, `Closed`, `AckTimeout`, `Disconnected`, `ReservedEvent`.
Messages never contain URLs or query values.

Examples for every topic are in [`examples/`](examples/src/bin): `just example basic`, `auth`,
`reconnect`, `binary`, `transports`, `blocking`.

## Debugging

The crate logs through [`tracing`](https://docs.rs/tracing) under `socketio_next`:

| Level | What |
|---|---|
| `info` | connected (sid, transport, recovered), reconnecting (attempt, delay), closed (reason) |
| `warn` | disconnected (reason, error), connect / reconnect attempt failed, ping timeout |
| `debug` | engine.io handshake (sid, upgrades, ping interval/timeout, maxPayload), transport chosen / upgraded, every state |
| `trace` | every packet in and out (type, namespace, ack id, size) |

Every event carries a `socketio{nsp, url}` span. Query values are shown as `***` (only `EIO`,
`transport`, `sid`, `t` stay visible); `redact_query(false)` turns that off. For the full
packets use `inspect_packets`.

```sh
RUST_LOG=socketio_next=debug cargo run
```

Only `socketio_next` targets are redacted. tungstenite logs the full websocket handshake
request (URL with query, headers) with the `log` crate at trace level; if you bridge `log` into
`tracing`, keep `tungstenite=info` or higher when the query carries secrets.

### Troubleshooting

| Symptom | Check |
|---|---|
| `Engine(InvalidHandshake("... Engine.IO v3 ..."))` or `IncompatibleServer` | The server runs socket.io 2.x (Engine.IO v3). This client needs socket.io 3 or newer. |
| `ConnectTimeout` | Wrong host/port/path, a proxy that swallows the request, or a server that is not socket.io. Try `transport_type(TransportType::Polling)` and `RUST_LOG=socketio_next=debug`. |
| Websocket blocked by a proxy | `TransportType::Any` (default) falls back to polling; `Polling` never tries websocket. |
| `HttpStatus { status: 400 }` after a server restart | The old session id is gone; the client reconnects with a new one. |
| `HttpStatus { status: 403 }` / CORS | CORS does not apply to non-browser clients; a 403 comes from a proxy or the server's `allowRequest`. |
| `ConnectRejected { message, data }` | The namespace middleware refused the auth (`next(new Error(message))`), or the namespace does not exist (`Invalid namespace`). The client does not retry. |
| TLS: `invalid peer certificate` | A private CA: `tls_config(TlsConfig::with_root_certificates_pem(&pem)?)`; or build with `native-tls` to use the OS trust store. |
| `TlsNotEnabled` | Built with `default-features = false`; enable `rustls` or `native-tls`. |
| `TlsProviderMissing` | Another crate enabled reqwest's `rustls-no-provider`; enable `rustls` (see Cargo features). |
| Large messages fail | The server's `maxHttpBufferSize` (engine.io `maxPayload`, see `link_info().max_payload`). |
| Events with no handler | Handlers are matched by exact name; `on_any` sees everything. |

## Differences from rust_socketio

socketio-next started as [s00d/rust-socketio](https://github.com/s00d/rust-socketio), a fork of
rust_socketio 0.6.0 (Bastian Kersting and contributors). The protocol code was kept and
corrected; the client was rewritten. It is a different crate (`socketio-next`); code that uses
rust_socketio needs the migration below.

**Fixes** (each with a regression test):
- Long-polling responses read whole (a message larger than one HTTP chunk broke, upstream #516/#517).
- Polling and websocket handshakes bounded by a timeout; a blackholed websocket falls back to polling.
- A non-200 polling answer is an error, not a payload.
- A websocket frame is one packet (binary data containing `0x1e` broke, #490).
- Unique ack ids (collisions, a registry panic and a deadlock).
- `disconnect()` stops the reconnect loop, a hung reconnect attempt and the poll task (#374, #508).
- The engine closes after a server namespace disconnect, after CONNECT_ERROR and after a decode error.
- `max_reconnect_attempts` is final; the reconnect loop never spins after the `backoff` crate gave up.
- Emits before the CONNECT answer are buffered (#502).
- BINARY_EVENT for binary emits with an ack (#422); binary event names with extra arguments (#426).
- socket.io-parser validation rules: reserved event names, payload shapes, attachment counts.
- No unsolicited PONG after the handshake.

**New:**
- socket.io-parser binary encoding everywhere: several attachments, nested placeholders (`Payload::Mixed`).
- Acks from the server answered (`Event::ack`, ACK and BINARY_ACK); awaitable acks with timeout.
- The socket.io-client backoff (jitter 0.5, delay before every attempt), `randomization_factor`.
- Connection state recovery (`pid`/`offset`, `recovered`).
- Typed `ConnectionState`, `DisconnectReason`, `LinkInfo`; tracing with redaction; packet inspector.
- Runtime `on`/`once`/`off`/`on_any`/`listener_count`; handlers without boxing; panics contained.
- TLS backend as a feature (rustls default, native-tls, none).
- `blocking` API on top of the async client (one implementation).
- Differential tests against socket.io-client 4.8 on the wire.

**Breaking changes and migration from `rust_socketio` 0.6:**

| rust_socketio | socketio-next |
|---|---|
| `rust_socketio::asynchronous::{ClientBuilder, Client}` | `socketio_next::{ClientBuilder, Client}` |
| `rust_socketio::{ClientBuilder, RawClient}` (sync) | `socketio_next::blocking::{ClientBuilder, Client}` (feature `blocking`) |
| `.on(Event::Custom("x"), \|payload, client\| async {}.boxed())` | `.on("x", \|event: Event, client: Client\| async move {})` — `event.payload()` |
| `.on(Event::Connect / Close / Error, ..)` | `.on_state_change(\|state, client\| ..)` or `client.state()`; errors are typed |
| `"Received an ConnectError frame: ..."` in `Event::Error` | `Error::ConnectRejected { message, data }`, `DisconnectReason::ConnectRejected` |
| `.on_reconnect(\|\| async { ReconnectSettings }.boxed())` | `auth_with(..)` for fresh auth; `ConnectionState::Reconnecting` to observe |
| `.reconnect(b)`, `.reconnect_delay(min_ms, max_ms)`, `.max_reconnect_attempts(n)` | `.reconnection(b)`, `.reconnection_delay(Duration)` + `.reconnection_delay_max(Duration)`, `.reconnection_attempts(n)` |
| `.opening_header(k, v)` | `.header(k, v)` |
| `.tls_config(native_tls::TlsConnector)` | the same with feature `native-tls`, or `TlsConfig` / `rustls::ClientConfig` |
| `emit(..).await` | `emit(..)` (not async) |
| `emit_with_ack(ev, data, timeout, callback).await` | `emit_with_ack(ev, data, timeout).await -> Result<Payload>` |
| ack payload `Text(vec![Array(args)])` | `Text(args)` |
| `Payload::String` (deprecated), `From<String>` parsed JSON | removed; strings are strings, use `Value` for JSON |
| server `emit("NAME")` arrived as `message` with data `"NAME"` | arrives as event `NAME` without arguments (socket.io-client) |
| a binary event with other arguments lost them | `Payload::Mixed` keeps them |
| URL path `/socket.io/` = engine path | URL path = namespace (socket.io-client); engine path is `.path(..)` |
| `rust_engineio` crate | `socketio_next::engineio` module (async only) |

## Parity with socket.io-client 4.8

Ported and tested: the socket.io-parser and engine.io-parser vectors, connection and socket
cases of socket.io-client and engine.io-client against Node servers, plus differential runs
where the Node client and this client play the same scenario against one recording server
(`crates/socketio-next/tests/conformance.rs`).

Not implemented, on purpose: one `Manager` multiplexing several namespaces over one connection
(each `Client` has its own; connect several), `volatile` (offline emits already fail fast),
`retries`/`ackTimeout` queues, `onAnyOutgoing`, cookies (`withCredentials`), `forceBase64`,
WebTransport, browser-only options. `connect()` returns the first attempt's error instead of
retrying silently.

## Minimum Rust version

1.85 (edition 2024), checked with a lockfile resolved for it; see [MSRV.md](MSRV.md).

## Development

`just` lists the tasks. `just servers-setup && just servers` starts the Node test servers
(ports 4200-4206 and 4210-4211, no Docker), `just ci` runs what CI runs: fmt, cargo-shear,
clippy on every feature set, all tests per feature set, docs, cargo-deny, package.

## License

MIT, see [LICENSE](LICENSE). Based on rust_socketio by Bastian Kersting and the
[rust-socketio contributors](https://github.com/1c3t3a/rust-socketio/graphs/contributors).

[`Event`]: https://docs.rs/socketio-next/latest/socketio_next/struct.Event.html
[`Payload`]: https://docs.rs/socketio-next/latest/socketio_next/enum.Payload.html
