---
title: socketio-next
description: A Socket.IO v4 client for Rust with the behaviour of socket.io-client 4.8, and a Tauri 2 plugin.
navigation: false
---

```toml [Cargo.toml]
[dependencies]
socketio-next = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust [main.rs]
use std::time::Duration;
use serde_json::json;
use socketio_next::{Client, ClientBuilder, Event};

let client = ClientBuilder::new("http://localhost:3000")
    .auth(json!({ "token": "abc" }))
    .on("chat", |event: Event, _: Client| async move {
        println!("chat: {:?}", event.payload());
    })
    .connect()
    .await?;

client.emit("chat", "hello")?;
let reply = client
    .emit_with_ack("add", vec![json!(1), json!(2)], Duration::from_secs(5))
    .await?;
assert_eq!(reply.arg::<i64>(0)?, 3);
```
