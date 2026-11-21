[![Crates.io](https://img.shields.io/crates/v/socketio-next-testing?style=for-the-badge)](https://crates.io/crates/socketio-next-testing)
[![docs.rs](https://img.shields.io/badge/docs-docs.rs-blue?style=for-the-badge)](https://docs.rs/socketio-next-testing)
[![License](https://img.shields.io/badge/license-MIT-green?style=for-the-badge)](https://github.com/s00d/rust-socketio/blob/main/LICENSE)
[![Donate](https://img.shields.io/badge/Donate-Donationalerts-ff4081?style=for-the-badge)](https://www.donationalerts.com/r/s00d88)

# socketio-next-testing

Test support for Socket.IO clients, used by [socketio-next](https://crates.io/crates/socketio-next)'s
own tests: in-process Socket.IO v4 / Engine.IO v4 servers you program in Rust (no Node), the
ready-made servers of socketio-next's suite, and proxies that break the link on demand.

```toml
[dev-dependencies]
socketio-next-testing = "0.2"
```

## Your own server

```rust,no_run
use std::time::Duration;
use serde_json::json;
use socketio_next_testing::fake::{Connect, Data, Namespace, Server};

let server = Server::builder()
    .namespace(
        "/chat",
        Namespace::new()
            // decide each CONNECT; emits here go out right after the CONNECT answer
            .on_connect(|s| {
                if s.auth().and_then(|a| a.get("token")) != Some(&json!("t1")) {
                    return Connect::Reject { message: "bad token".into(), data: None };
                }
                s.emit("welcome", Data::default().arg(s.query("room").unwrap_or("lobby")));
                Connect::Accept
            })
            // answer through the ack
            .on("sum", |s, e| {
                let total: i64 = e.data().values().iter().filter_map(|v| v.as_i64()).sum();
                s.ack(&e, Data::default().arg(total));
            })
            // binary in, binary (nested in JSON) out
            .on("upload", |s, e| {
                let first = e.data().values().first();
                let bytes = first.and_then(|v| e.data().bytes_of(v)).cloned().unwrap_or_default();
                let mut reply = Data::default();
                let file = reply.binary(bytes);
                s.ack(&e, reply.arg(json!({ "file": file })));
            })
            // ask the client and react to its answer; later work; breaking things
            .on("ask", |s, _| {
                s.emit_with_ack("question", Data::default().arg("name?"), |s, reply| {
                    s.emit("thanks", reply);
                });
            })
            .on("slow", |s, e| s.after(Duration::from_millis(500), move |s| s.ack(&e, Data::default())))
            .on("kick", |s, _| s.disconnect())          // namespace DISCONNECT
            .on("drop", |s, _| s.close_transport())     // engine.io connection closed
            .on("garbage", |s, _| s.send_raw("2[true]")) // a packet clients must reject
            .on_any(|_, e| eprintln!("unhandled {}", e.name()))
            .on_disconnect(|s, reason| eprintln!("{} left: {reason}", s.id())),
    )
    .start(); // also .path("/x"), .ping(interval, timeout), .polling_only()

let url = server.url();                               // http://127.0.0.1:<port>
server.broadcast("/chat", "news", Data::default().arg("flash"));
server.emit_to("/chat", "fake-sio-1", "dm", Data::default().arg("just you"));
server.restart(Duration::from_secs(1));               // down for 1 s, same port after
server.shutdown();                                    // gone for good: connections refused
```

Handlers run on the session's task, one at a time per client, and act through the `Socket`
they get (it also has `id()`, known in `on_connect` already, `namespace()`, `header(name)`).
They must not block: every fake server of the process shares one small runtime, so send to a
channel (`tokio::sync::mpsc::UnboundedSender::send` is fine) instead of waiting on one. A
handler that panics closes that client's connection; the panic is printed. `on_disconnect`
gets socket.io's reason: `client namespace disconnect`, `server namespace disconnect` or
`transport close`. An ack is answered once; `after` and `emit_with_ack` replies run only for
the socket that asked, not for a later one of the same client. `Data` holds the arguments;
binary stands as socket.io-parser placeholders (`{"_placeholder":true,"num":n}`), made with
`Data::binary` / `Data::binary_arg` and read with `Data::bytes_of`. A CONNECT to a namespace
the server does not serve is refused with `Invalid namespace` (`/` is always served);
`Namespace::connect_delay` answers CONNECT late, like a slow middleware. A restart keeps the
port bound and drops each new connection right after accepting it; `shutdown` closes it.

Not checked, unlike the reference servers: overlapping polling POSTs, and socket.io's
`connectTimeout` (an engine.io session that never sends CONNECT stays open).

## Proxies

```rust,no_run
use socketio_next_testing::{Proxy, host_port, socket_io_server};

async fn demo() {
    let url = socket_io_server();
    let proxy = Proxy::start(&host_port(&url)).await;   // point the client at proxy.url()
    proxy.refuse(true);                                 // no new connections
    proxy.cut();                                        // drop the open ones
    proxy.stop();                                       // close the port
}
```

## Ready-made servers

Each function starts its server on first use (one background runtime for the process) and
returns `http://127.0.0.1:<port>`, also listening on `[::1]` with the same port where the host
has IPv6.

| function | path | what it does |
|---|---|---|
| `socket_io_server()` | `/socket.io/` | namespaces `/` and `/admin` (greeting events on connect), `/slow` (CONNECT answered after 200 ms), `/reject` (`connect_error` `not authorized` with `data: {reason: "test"}`); events below |
| `socket_io_auth_server()` | `/socket.io/` | emits `auth` with `success` when `auth.password == "123"`, else `failed` |
| `socket_io_restart_server()` | `/socket.io/` | emits `message` with the `message_back` header (or `test`); `restart_server` takes it down for 2 s |
| `new_socket_io_restart_server()` | `/socket.io/` | the same, a new instance (restart without disturbing other tests) |
| `fast_ping_server()` | `/socket.io/` | like `socket_io_server`, pingInterval 300 ms / pingTimeout 200 ms |
| `engine_io_server()` | `/engine.io/` | engine.io only: greets `hello client`, `respond` -> `Roger Roger`, `close` closes |
| `engine_io_polling_server()` | `/engine.io/` | the same, polling only (no upgrade), plus `respond_large`, `utf8`, `emoji`, `binary` |

Events of `socket_io_server` (from socketio-next's `ci/socket-io.js`):

| client emits | server does |
|---|---|
| `echo`, ...args | acks with the same args, or emits `echo-back` with them without an ack |
| `test` / `message` / `binary` | emits `test-received` / `message-received` / `binary-received` with the first arg; `test` acks `woot`, `binary` acks a buffer `[1, 2, 3]` |
| `ack_late` | acks `late` after 500 ms |
| `ask_me`, q | emits `question` with q and an ack; the client's answer comes back as `answered` |
| `handshake_query` | acks the handshake query as an object |
| `binary_delimiter`, `binary_with_args`, `binary_mixed` | binary in tricky places (0x1e in a buffer, binary after a string, nested buffers) |
| `server_disconnect` | namespace DISCONNECT (the engine.io connection stays) |
| `close_transport` | closes the engine.io connection |
| `invalid_packet` | sends a packet socket.io-parser rejects |

The protocol side follows the reference servers: polling (long poll, payload batching, `b`
base64 binary), websocket, the upgrade probe, heartbeat and ping timeout, `maxPayload` (1 MB,
413), socket.io packet validation (an invalid packet or a packet for a namespace that is not
connected closes the connection), binary attachments numbered depth-first. The codecs are
written from the protocol description, not shared with socketio-next.

These are built with the same `fake::Server` API (`src/fake/fixtures.rs`).

## Proxy details

- `Proxy::start(target)`: a TCP proxy in front of a server. `cut()` drops the open connections,
  `freeze()` keeps them open but forwards nothing (a blackholed link), `refuse(true)` refuses new
  ones, `block_websocket(true)` swallows websocket upgrades, `stop()` closes the port;
  `accepted()`, `open()`, `wait_open(n, within)`.
- `HttpProxy::start()` / `start_on("[::1]:0")`: an HTTP proxy (`CONNECT` tunnels and
  absolute-form requests); `log()` lists what it was asked, with the `Proxy-Authorization` value;
  `stop()` closes the port.

## Feature `repo`

Only for socketio-next's own repository checkout (it reads its `ci/` directory): the test CA
(`ca_cert_pem`), the Node conformance servers (`conformance_server`, `recovery_server`,
`cookie_server`, `cookies_seen`), the differential harness (`run_node_client`,
`recorded_packets`), and the environment variables (`SOCKET_IO_SERVER`, ... the function name
in upper case) that point the ready-made servers at the Node ones.

## License

MIT
