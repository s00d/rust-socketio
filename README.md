[![Crates.io](https://img.shields.io/crates/v/socketio-next?style=for-the-badge)](https://crates.io/crates/socketio-next)
[![Downloads](https://img.shields.io/crates/d/socketio-next?style=for-the-badge)](https://crates.io/crates/socketio-next)
[![docs.rs](https://img.shields.io/badge/docs-docs.rs-blue?style=for-the-badge)](https://docs.rs/socketio-next)
[![Guide](https://img.shields.io/badge/guide-s00d.github.io-blue?style=for-the-badge)](https://s00d.github.io/rust-socketio/)
[![CI](https://img.shields.io/github/actions/workflow/status/s00d/rust-socketio/ci.yml?branch=main&label=CI&style=for-the-badge)](https://github.com/s00d/rust-socketio/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-orange?style=for-the-badge)](https://s00d.github.io/rust-socketio/getting-started/msrv)
[![License](https://img.shields.io/badge/license-MIT-green?style=for-the-badge)](https://github.com/s00d/rust-socketio/blob/main/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/s00d/rust-socketio?style=for-the-badge)](https://github.com/s00d/rust-socketio/issues)
[![GitHub stars](https://img.shields.io/github/stars/s00d/rust-socketio?style=for-the-badge)](https://github.com/s00d/rust-socketio/stargazers)
[![Donate](https://img.shields.io/badge/Donate-Donationalerts-ff4081?style=for-the-badge)](https://www.donationalerts.com/r/s00d88)

# socketio-next

**A [Socket.IO](https://socket.io) v4 client for Rust that behaves like socket.io-client 4.8.**

Namespaces, auth, acks in both directions, binary data anywhere in the arguments, the same
reconnection backoff, connection state recovery, polling and websocket transports, proxies, TLS
through rustls or native-tls. Async (tokio) with an optional blocking API, and a Tauri 2 plugin.

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

## Support

If socketio-next helps you, you can support it via [DonationAlerts](https://www.donationalerts.com/r/s00d88).
Bugs and ideas: [issues](https://github.com/s00d/rust-socketio/issues).

## License

MIT, see [LICENSE](LICENSE). Based on rust_socketio by Bastian Kersting and the
[rust-socketio contributors](https://github.com/1c3t3a/rust-socketio/graphs/contributors).
