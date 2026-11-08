# socketio-next

[![crates.io](https://img.shields.io/crates/v/socketio-next.svg)](https://crates.io/crates/socketio-next)
[![docs.rs](https://docs.rs/socketio-next/badge.svg)](https://docs.rs/socketio-next)
[![docs](https://img.shields.io/badge/docs-s00d.github.io%2Frust--socketio-0ea5e9)](https://s00d.github.io/rust-socketio/)
[![CI](https://github.com/s00d/rust-socketio/actions/workflows/ci.yml/badge.svg)](https://github.com/s00d/rust-socketio/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://s00d.github.io/rust-socketio/getting-started/msrv)
[![license](https://img.shields.io/crates/l/socketio-next.svg)](LICENSE)

A [Socket.IO](https://socket.io) v4 client for Rust that behaves like the reference JavaScript
client (socket.io-client 4.8): namespaces, auth, acks in both directions, binary data anywhere
in the arguments, the same reconnection backoff, connection state recovery, polling and
websocket transports, proxies, TLS through rustls or native-tls. Async (tokio) with an optional
blocking API, and a Tauri 2 plugin.

**Documentation: [s00d.github.io/rust-socketio](https://s00d.github.io/rust-socketio/)** ·
API reference: [docs.rs/socketio-next](https://docs.rs/socketio-next)

## Install

```toml
[dependencies]
socketio-next = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Cargo features: `rustls` (default), `native-tls`, `platform-verifier`, `cookies`, `blocking`
([details](https://s00d.github.io/rust-socketio/getting-started/installation)).

```rust,no_run
use std::time::Duration;
use serde_json::json;
use socketio_next::{Client, ClientBuilder, Event};

#[tokio::main]
async fn main() -> socketio_next::Result<()> {
    let client = ClientBuilder::new("http://localhost:3000")
        .auth(json!({ "token": "abc" }))
        .on("chat", |event: Event, _client: Client| async move {
            println!("chat: {:?}", event.payload());
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

## Tauri

[`tauri-plugin-socketio`](crates/tauri-plugin-socketio) (npm
[`tauri-plugin-socketio-api`](https://www.npmjs.com/package/tauri-plugin-socketio-api)) shares
named clients between Rust and the webviews of a Tauri 2 app.

```toml
# src-tauri/Cargo.toml
tauri-plugin-socketio = "0.1"
```

```sh
pnpm add tauri-plugin-socketio-api
```

See the [plugin guide](https://s00d.github.io/rust-socketio/tauri/setup).

## Coming from rust_socketio

socketio-next started as a fork of [rust_socketio](https://github.com/1c3t3a/rust-socketio)
0.6 and rewrote its client. See the
[migration guide](https://s00d.github.io/rust-socketio/guides/migration).

## License

MIT, see [LICENSE](LICENSE). Based on rust_socketio by Bastian Kersting and the
[rust-socketio contributors](https://github.com/1c3t3a/rust-socketio/graphs/contributors).
