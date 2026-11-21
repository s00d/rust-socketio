[![Crates.io](https://img.shields.io/crates/v/tauri-plugin-socketio?style=for-the-badge)](https://crates.io/crates/tauri-plugin-socketio)
[![npm](https://img.shields.io/npm/v/tauri-plugin-socketio-api?style=for-the-badge)](https://www.npmjs.com/package/tauri-plugin-socketio-api)
[![docs.rs](https://img.shields.io/badge/docs-docs.rs-blue?style=for-the-badge)](https://docs.rs/tauri-plugin-socketio)
[![Guide](https://img.shields.io/badge/guide-s00d.github.io-blue?style=for-the-badge)](https://s00d.github.io/rust-socketio/tauri/setup)
[![CI](https://img.shields.io/github/actions/workflow/status/s00d/rust-socketio/ci.yml?branch=main&label=CI&style=for-the-badge)](https://github.com/s00d/rust-socketio/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-green?style=for-the-badge)](https://github.com/s00d/rust-socketio/blob/main/LICENSE)
[![Donate](https://img.shields.io/badge/Donate-Donationalerts-ff4081?style=for-the-badge)](https://www.donationalerts.com/r/s00d88)

# tauri-plugin-socketio

A Tauri 2 plugin for Socket.IO v4 on top of [socketio-next](https://crates.io/crates/socketio-next):
one registry of **named clients** that Rust and the webviews share. Rust connects clients or
registers its own, optionally **read-only** for the webview; JS connects or attaches with an API
close to socket.io-client. The socket runs in Rust: no CORS, no browser websocket limits, and
the connection survives page reloads.

**Documentation: [s00d.github.io/rust-socketio/tauri/setup](https://s00d.github.io/rust-socketio/tauri/setup)**

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
    // or restrict the webview's targets:
    // .plugin(tauri_plugin_socketio::Builder::new().allow_url_prefix("https://api.example.com").build())
```

```json
// src-tauri/capabilities/default.json
{ "identifier": "default", "windows": ["main"], "permissions": ["core:default", "socketio:default"] }
```

`socketio:default` allows every command, `socketio:allow-read` only observes
([permissions](https://s00d.github.io/rust-socketio/tauri/permissions)).

## JS

```ts
import { connect, attach } from 'tauri-plugin-socketio-api'

const socket = await connect('chat', { url: 'https://chat.example.com', auth: { token } })
socket.on('message', (text, from) => console.log(from, text))
socket.on('question', (q, ack) => ack('answer'))
await socket.emit('message', 'hi')
const [sum] = await socket.emitWithAck('add', [1, 2], { timeout: 5000 })

const core = await attach('core') // a client Rust registered
```

## Rust

```rust
use tauri_plugin_socketio::socketio_next::ClientBuilder;
use tauri_plugin_socketio::{Access, SocketIoExt};

let io = app.socketio();
io.reserve("core"); // in setup, before any await: webviews cannot take the id
let chat = io.connect("chat", ClientBuilder::new("https://chat.example.com")).await?;
io.insert("core", my_client, Access::ReadOnly)?; // the webview may watch, not emit
```

Before shipping, read the [security notes](https://s00d.github.io/rust-socketio/tauri/security):
reserved ids, read-only clients, the URL allow-list (the CSP does not cover these sockets) and
refused headers.

## Support

If the plugin helps you, you can support it via [DonationAlerts](https://www.donationalerts.com/r/s00d88).
Bugs and ideas: [issues](https://github.com/s00d/rust-socketio/issues).

## License

MIT
