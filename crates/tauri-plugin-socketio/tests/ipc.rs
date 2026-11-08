//! The plugin through the IPC, as the webview uses it: a mock Tauri app with the plugin, its
//! commands invoked by name (`plugin:socketio|<cmd>`), channel messages captured by a channel
//! interceptor. Against the Node test servers in `ci/` (`just servers`).

use std::collections::VecDeque;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use socketio_next::ClientBuilder;
use socketio_next_testing::{conformance_server, socket_io_server};
use tauri::ipc::{CallbackFn, InvokeBody, InvokeResponseBody};
use tauri::test::{
    INVOKE_KEY, MockRuntime, get_ipc_response, mock_builder, mock_context, noop_assets,
};
use tauri::utils::acl::ExecutionContext;
use tauri::webview::InvokeRequest;
use tauri::{App, WebviewWindow, WebviewWindowBuilder};
use tauri_plugin_socketio::{Access, SocketIoExt};

const ALL: &[&str] = &[
    "connect",
    "disconnect",
    "emit",
    "emit_with_ack",
    "ack",
    "subscribe",
    "unsubscribe",
    "list",
    "state",
    "link_info",
];
/// The commands of the `allow-read` set.
const READ: &[&str] = &["list", "subscribe", "unsubscribe", "state", "link_info"];

const WAIT: Duration = Duration::from_secs(10);

struct Harness {
    app: App<MockRuntime>,
    webview: WebviewWindow<MockRuntime>,
    messages: mpsc::Receiver<(u32, Value)>,
    /// Messages read while looking for another one.
    seen: VecDeque<(u32, Value)>,
}

impl Harness {
    /// A mock app with the plugin, where the webview may call `allowed` (an ACL as a
    /// capability would give).
    fn new(allowed: &[&str]) -> Harness {
        let (tx, messages) = mpsc::channel();
        let mut context = mock_context(noop_assets());
        for cmd in allowed {
            context
                .runtime_authority_mut()
                .__allow_command(format!("plugin:socketio|{cmd}"), ExecutionContext::Local);
        }
        let app = mock_builder()
            .plugin(tauri_plugin_socketio::init())
            .channel_interceptor(move |_, callback, _, body| {
                if let InvokeResponseBody::Json(json) = body {
                    let _ = tx.send((callback.0, serde_json::from_str(json).unwrap()));
                }
                true
            })
            .build(context)
            .expect("mock app");
        let webview = WebviewWindowBuilder::new(&app, "main", Default::default())
            .build()
            .expect("webview");
        Harness {
            app,
            webview,
            messages,
            seen: VecDeque::new(),
        }
    }

    fn invoke(&self, cmd: &str, body: Value) -> Result<Value, Value> {
        let request = InvokeRequest {
            cmd: format!("plugin:socketio|{cmd}"),
            callback: CallbackFn(0),
            error: CallbackFn(1),
            url: if cfg!(any(windows, target_os = "android")) {
                "http://tauri.localhost"
            } else {
                "tauri://localhost"
            }
            .parse()
            .unwrap(),
            body: InvokeBody::Json(body),
            headers: Default::default(),
            invoke_key: INVOKE_KEY.to_string(),
        };
        get_ipc_response(&self.webview, request).map(|b| b.deserialize::<Value>().unwrap())
    }

    fn ok(&self, cmd: &str, body: Value) -> Value {
        self.invoke(cmd, body)
            .unwrap_or_else(|e| panic!("{cmd} failed: {e}"))
    }

    /// The `kind` of the error `cmd` fails with.
    fn err_kind(&self, cmd: &str, body: Value) -> String {
        let err = self.invoke(cmd, body).expect_err(cmd);
        err["kind"]
            .as_str()
            .unwrap_or_else(|| panic!("{cmd}: not a plugin error: {err}"))
            .to_owned()
    }

    /// The next message on `channel` that matches `pred`; earlier ones are skipped.
    fn wait(&mut self, channel: u32, pred: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + WAIT;
        if let Some(pos) = self.seen.iter().position(|(c, m)| *c == channel && pred(m)) {
            let found = self.seen.remove(pos).unwrap().1;
            self.seen.retain(|(c, _)| *c != channel);
            return found;
        }
        self.seen.retain(|(c, _)| *c != channel);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let (c, m) = self
                .messages
                .recv_timeout(left)
                .unwrap_or_else(|_| panic!("no matching message on channel {channel}"));
            if c == channel && pred(&m) {
                return m;
            }
            if c != channel {
                self.seen.push_back((c, m));
            }
        }
    }

    fn event(&mut self, channel: u32, name: &str) -> Value {
        self.wait(channel, |m| m["type"] == "event" && m["event"] == name)
    }

    fn state(&mut self, channel: u32, status: &str) -> Value {
        self.wait(channel, |m| {
            m["type"] == "state" && m["state"]["status"] == status
        })["state"]
            .clone()
    }

    /// Every message of `channel` that arrives within `quiet`.
    fn drain(&mut self, channel: u32, quiet: Duration) -> Vec<Value> {
        let mut out: Vec<Value> = self
            .seen
            .iter()
            .filter(|(c, _)| *c == channel)
            .map(|(_, m)| m.clone())
            .collect();
        self.seen.retain(|(c, _)| *c != channel);
        while let Ok((c, m)) = self.messages.recv_timeout(quiet) {
            if c == channel {
                out.push(m);
            } else {
                self.seen.push_back((c, m));
            }
        }
        out
    }

    /// `connect` from the webview with channel `channel`; returns the subscription id.
    fn connect(&mut self, id: &str, options: Value, channel: u32) -> u64 {
        let res = self.ok(
            "connect",
            json!({ "id": id, "options": options, "channel": format!("__CHANNEL__:{channel}") }),
        );
        self.state(channel, "connected");
        res["subscription"].as_u64().unwrap()
    }

    fn subscribe(&mut self, id: &str, events: Option<&[&str]>, channel: u32) -> u64 {
        let res = self.ok(
            "subscribe",
            json!({ "id": id, "events": events, "channel": format!("__CHANNEL__:{channel}") }),
        );
        res.as_u64().unwrap()
    }
}

fn conformance() -> Value {
    json!({ "url": conformance_server() })
}

#[test]
fn every_command_is_refused_without_its_permission() {
    let h = Harness::new(READ);
    assert_eq!(h.ok("list", json!({})), json!([]));
    for cmd in ALL.iter().filter(|c| !READ.contains(c)) {
        let err = h.invoke(cmd, json!({})).expect_err(cmd);
        assert!(
            err.as_str().is_some_and(|e| e.contains("not allowed")),
            "{cmd}: {err}"
        );
    }
}

#[test]
fn unknown_ids_are_typed_errors() {
    let h = Harness::new(ALL);
    let id = json!({ "id": "nope" });
    assert_eq!(h.err_kind("state", id.clone()), "UnknownClient");
    assert_eq!(h.err_kind("link_info", id.clone()), "UnknownClient");
    assert_eq!(h.err_kind("disconnect", id), "UnknownClient");
    assert_eq!(
        h.err_kind(
            "emit",
            json!({ "id": "nope", "event": "e", "args": { "args": [] } })
        ),
        "UnknownClient"
    );
    assert_eq!(
        h.err_kind(
            "subscribe",
            json!({ "id": "nope", "channel": "__CHANNEL__:1" })
        ),
        "UnknownClient"
    );
    assert_eq!(
        h.ok("unsubscribe", json!({ "id": "nope", "subscription": 1 })),
        json!(false)
    );
}

#[test]
fn bad_options_are_refused_before_connecting() {
    let h = Harness::new(ALL);
    let err = h
        .invoke(
            "connect",
            json!({ "id": "x", "options": { "url": "http://x", "reconection": true }, "channel": "__CHANNEL__:1" }),
        )
        .expect_err("unknown option");
    assert!(err.to_string().contains("reconection"), "{err}");
    assert_eq!(h.ok("list", json!({})), json!([]));
}

#[test]
fn connect_emit_ack_state_link_disconnect() {
    let mut h = Harness::new(ALL);
    h.connect("main", conformance(), 1);

    h.ok(
        "emit",
        json!({ "id": "main", "event": "echo", "args": { "args": [1, "a"] } }),
    );
    let echo = h.event(1, "echo-back");
    assert_eq!(echo["args"], json!([1, "a"]));
    assert!(echo.get("ackId").is_none() && echo.get("binary").is_none());

    let reply = h.ok(
        "emit_with_ack",
        json!({ "id": "main", "event": "echo", "args": { "args": [{ "x": 1 }] }, "timeout": 5000 }),
    );
    assert_eq!(reply, json!({ "args": [{ "x": 1 }] }));

    let list = h.ok("list", json!({}));
    assert_eq!(list[0]["id"], "main");
    assert_eq!(list[0]["access"], "full");
    assert_eq!(list[0]["namespace"], "/");
    assert_eq!(list[0]["state"]["status"], "connected");
    assert_eq!(
        h.ok("state", json!({ "id": "main" }))["status"],
        "connected"
    );

    let link = h.ok("link_info", json!({ "id": "main" }));
    assert!(
        link["sid"].is_string() && link["engineSid"].is_string(),
        "{link}"
    );
    assert!(link["transport"] == "polling" || link["transport"] == "websocket");
    assert!(link["connectedAt"].as_u64().unwrap() > 1_700_000_000_000);
    assert!(link["pingInterval"].as_u64().unwrap() > 0);

    h.ok("disconnect", json!({ "id": "main" }));
    let closed = h.state(1, "closed");
    assert_eq!(closed["reason"], "io client disconnect");
    assert_eq!(h.ok("list", json!({})), json!([]));
    assert_eq!(
        h.err_kind("state", json!({ "id": "main" })),
        "UnknownClient"
    );
}

#[test]
fn server_ack_round_trip() {
    let mut h = Harness::new(ALL);
    h.connect("asker", conformance(), 1);
    h.ok(
        "emit",
        json!({ "id": "asker", "event": "ask", "args": { "args": [] } }),
    );
    let question = h.event(1, "question");
    assert_eq!(question["args"], json!(["why?"]));
    let ack_id = question["ackId"].as_u64().expect("the server wants an ack");

    let answer = json!({ "id": "asker", "ackId": ack_id, "args": { "args": ["because", 42] } });
    h.ok("ack", answer.clone());
    assert_eq!(h.event(1, "answered")["args"], json!(["because", 42]));
    assert_eq!(
        h.err_kind("ack", answer),
        "UnknownAck",
        "an ack is answered once"
    );
    h.ok("disconnect", json!({ "id": "asker" }));
}

#[test]
fn binary_both_ways() {
    let mut h = Harness::new(ALL);
    h.connect("bin", conformance(), 1);
    let placeholder = json!({ "_placeholder": true, "num": 0 });

    let args = json!({ "args": [placeholder, "tail"], "binary": [[1, 2, 3]] });
    h.ok(
        "emit",
        json!({ "id": "bin", "event": "echo", "args": args }),
    );
    let echo = h.event(1, "echo-back");
    assert_eq!(echo["args"], json!([placeholder, "tail"]));
    assert_eq!(echo["binary"], json!([[1, 2, 3]]));

    let reply = h.ok(
        "emit_with_ack",
        json!({ "id": "bin", "event": "echo", "args": { "args": [placeholder], "binary": [[0, 255]] }, "timeout": 5000 }),
    );
    assert_eq!(reply["binary"], json!([[0, 255]]));

    h.ok(
        "emit",
        json!({ "id": "bin", "event": "bin", "args": { "args": [] } }),
    );
    let mixed = h.event(1, "binary-mixed");
    assert_eq!(mixed["binary"], json!([[1], [2, 3]]));
    assert_eq!(mixed["args"][1], "tail");
    assert_eq!(
        mixed["args"][0]["b"][0],
        json!({ "_placeholder": true, "num": 1 })
    );
    h.ok("disconnect", json!({ "id": "bin" }));
}

#[test]
fn read_only_client_can_be_observed_not_driven() {
    let mut h = Harness::new(ALL);
    let client = tauri::async_runtime::block_on(ClientBuilder::new(conformance_server()).connect())
        .expect("connect from Rust");
    h.app
        .socketio()
        .insert("core", client.clone(), Access::ReadOnly)
        .unwrap();

    let sub = h.subscribe("core", None, 1);
    assert!(sub > 0);
    assert!(
        h.state(1, "connected")["sid"].is_string(),
        "first message: the current state"
    );
    assert_eq!(h.ok("list", json!({}))[0]["access"], "readOnly");
    assert_eq!(
        h.ok("state", json!({ "id": "core" }))["status"],
        "connected"
    );

    let args = json!({ "args": [] });
    for (cmd, body) in [
        (
            "emit",
            json!({ "id": "core", "event": "echo", "args": args }),
        ),
        (
            "emit_with_ack",
            json!({ "id": "core", "event": "echo", "args": args, "timeout": 100 }),
        ),
        ("ack", json!({ "id": "core", "ackId": 1, "args": args })),
        ("disconnect", json!({ "id": "core" })),
    ] {
        assert_eq!(h.err_kind(cmd, body), "AccessDenied", "{cmd}");
    }

    // Rust drives it; the webview sees the traffic
    client.emit("echo", json!("from rust")).unwrap();
    assert_eq!(h.event(1, "echo-back")["args"], json!(["from rust"]));
    assert!(client.connected(), "the webview did not disconnect it");

    let back = h.app.socketio().remove("core").expect("registered");
    assert!(back.connected(), "remove does not disconnect");
    tauri::async_runtime::block_on(back.disconnect()).unwrap();
}

#[test]
fn event_filter_and_unsubscribe() {
    let mut h = Harness::new(ALL);
    h.connect("f", conformance(), 1);
    h.subscribe("f", Some(&["tock"]), 2);
    let sub3 = h.subscribe("f", None, 3);
    h.state(2, "connected");
    h.state(3, "connected");

    // the server answers in order: echo-back, then tock
    h.ok(
        "emit",
        json!({ "id": "f", "event": "echo", "args": { "args": [1] } }),
    );
    h.ok(
        "emit",
        json!({ "id": "f", "event": "tick", "args": { "args": [7] } }),
    );
    assert_eq!(h.event(2, "tock")["args"], json!([7, "offset-7"]));
    h.event(3, "tock");
    let filtered = h.drain(2, Duration::from_millis(200));
    assert!(
        filtered.iter().all(|m| m["event"] != "echo-back"),
        "{filtered:?}"
    );

    assert_eq!(
        h.ok("unsubscribe", json!({ "id": "f", "subscription": sub3 })),
        json!(true)
    );
    assert_eq!(
        h.ok("unsubscribe", json!({ "id": "f", "subscription": sub3 })),
        json!(false)
    );
    h.ok(
        "emit",
        json!({ "id": "f", "event": "tick", "args": { "args": [8] } }),
    );
    h.event(2, "tock");
    // channel 1 (the connect subscription) is a sync point: it sees tock 8 after channel 3 would
    h.wait(1, |m| m["event"] == "tock" && m["args"][0] == 8);
    let after = h.drain(3, Duration::from_millis(200));
    assert!(after.is_empty(), "unsubscribed channel got {after:?}");
    h.ok("disconnect", json!({ "id": "f" }));
}

#[test]
fn ids_are_unique() {
    let mut h = Harness::new(ALL);
    h.connect("dup", conformance(), 1);
    let err = h
        .invoke(
            "connect",
            json!({ "id": "dup", "options": conformance(), "channel": "__CHANNEL__:2" }),
        )
        .expect_err("id in use");
    assert_eq!(err["kind"], "AlreadyExists");
    let other = h.app.socketio().get("dup").unwrap();
    assert!(matches!(
        h.app.socketio().insert("dup", other, Access::Full),
        Err(tauri_plugin_socketio::Error::AlreadyExists(_))
    ));
    h.ok("disconnect", json!({ "id": "dup" }));
}

#[test]
fn connect_rejected_carries_the_server_data() {
    let mut h = Harness::new(ALL);
    let err = h
        .invoke(
            "connect",
            json!({
                "id": "rej",
                "options": { "url": socket_io_server(), "namespace": "/reject", "query": { "token": "s3cr3t" } },
                "channel": "__CHANNEL__:1",
            }),
        )
        .expect_err("rejected");
    assert_eq!(err["kind"], "ConnectRejected");
    assert_eq!(err["data"], json!({ "reason": "test" }));
    let text = err.to_string();
    assert!(
        !text.contains("s3cr3t") && !text.contains("localhost"),
        "{text}"
    );
    // nothing was registered; the error is the answer (a failed first connect sends no state)
    assert_eq!(h.ok("list", json!({})), json!([]));
    let _ = &mut h;
}

#[test]
fn reconnect_state_sequence() {
    let mut h = Harness::new(ALL);
    let options = json!({
        "url": conformance_server(),
        "reconnectionDelay": 50,
        "reconnectionDelayMax": 50,
        "randomizationFactor": 0,
    });
    h.connect("re", options, 1);
    h.ok(
        "emit",
        json!({ "id": "re", "event": "drop", "args": { "args": [] } }),
    );
    assert_eq!(h.state(1, "disconnected")["reason"], "transport close");
    let reconnecting = h.state(1, "reconnecting");
    assert_eq!(reconnecting["attempt"], 1);
    assert_eq!(reconnecting["delay"], 50);
    assert_eq!(h.state(1, "connecting")["attempt"], 1);
    h.state(1, "connected");
    h.ok("disconnect", json!({ "id": "re" }));
}

#[test]
fn rust_connect_and_remove() {
    let mut h = Harness::new(ALL);
    let io = h.app.socketio().clone();
    let client =
        tauri::async_runtime::block_on(io.connect("rs", ClientBuilder::new(conformance_server())))
            .unwrap();
    assert_eq!(io.access("rs"), Some(Access::Full));
    assert_eq!(io.ids(), vec!["rs".to_owned()]);
    h.subscribe("rs", None, 1);
    h.state(1, "connected");
    // the webview may drive a Full client registered from Rust
    h.ok(
        "emit",
        json!({ "id": "rs", "event": "echo", "args": { "args": ["x"] } }),
    );
    h.event(1, "echo-back");

    let back = io.remove("rs").unwrap();
    client.emit("echo", "after remove").unwrap();
    let after = h.drain(1, Duration::from_millis(300));
    assert!(after.is_empty(), "removed client still forwarded {after:?}");
    tauri::async_runtime::block_on(back.disconnect()).unwrap();
}

#[test]
fn a_failed_send_ends_the_subscription() {
    let h = Harness::new(ALL);
    let io = h.app.socketio().clone();
    let client = tauri::async_runtime::block_on(
        io.connect("gone", ClientBuilder::new(conformance_server())),
    )
    .unwrap();
    // a webview that is gone: every send fails (the first one is the current state)
    let dead = tauri::ipc::Channel::new(|_| Err(tauri::Error::WebviewNotFound));
    let sub = io.subscribe("gone", None, dead).unwrap();
    assert!(
        !io.unsubscribe("gone", sub),
        "the subscription ended by itself"
    );
    tauri::async_runtime::block_on(client.disconnect()).unwrap();
}

/// The permission sets name every command (`default`) and exactly the observing ones
/// (`allow-read`), so the README's promise holds.
#[test]
fn permission_sets_match_the_commands() {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/permissions/default.toml"
    ))
    .unwrap();
    let (default, read) = text.split_once("[[set]]").unwrap();
    let id = |cmd: &str| format!("\"allow-{}\"", cmd.replace('_', "-"));
    for cmd in ALL {
        assert!(default.contains(&id(cmd)), "default lacks {cmd}");
        assert_eq!(
            read.contains(&id(cmd)),
            READ.contains(cmd),
            "allow-read and {cmd}"
        );
    }
}
