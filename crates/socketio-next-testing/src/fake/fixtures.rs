//! The servers of socketio-next's `ci/socket-io.js`, `ci/socket-io-auth.js` and
//! `ci/socket-io-restart.js`, built with the public [`Namespace`] API.

use std::time::Duration;

use serde_json::json;

use super::socketio::{Connect, Data, Namespace, Socket};

/// `ci/socket-io.js`: `/` and `/admin` greet and answer the test events; `/slow` answers
/// CONNECT after 200 ms; `/reject` refuses after 200 ms.
pub(super) fn main() -> Vec<(&'static str, Namespace)> {
    let delay = Duration::from_millis(200);
    vec![
        ("/", main_namespace()),
        ("/admin", main_namespace()),
        ("/slow", main_namespace().connect_delay(delay)),
        (
            "/reject",
            Namespace::new()
                .connect_delay(delay)
                .on_connect(|_| Connect::Reject {
                    message: "not authorized".into(),
                    data: Some(json!({ "reason": "test" })),
                }),
        ),
    ]
}

fn greet(s: &mut Socket<'_>) -> Connect {
    s.emit("Hello from the message event!", Data::default());
    s.emit("test", Data::default().arg("Hello from the test event!"));
    s.emit_unnamed(Data::default().binary_arg(vec![4u8, 5, 6]));
    s.emit("test", Data::default().binary_arg(vec![1u8, 2, 3]));
    s.emit(
        "This is the first argument",
        Data::default()
            .arg("This is the second argument")
            .arg(json!({ "argCount": 3 })),
    );
    s.emit(
        "on_abc_event",
        Data::default()
            .arg("")
            .arg(json!({ "abc": 0, "some_other": "value" })),
    );
    Connect::Accept
}

/// `client.emit("x", data)` with the handler's first argument (`null` when there was none),
/// as `ci/socket-io.js` does.
fn first(e: &super::socketio::Event) -> Data {
    if e.data().values().is_empty() {
        Data::default().arg(serde_json::Value::Null)
    } else {
        e.data().first()
    }
}

fn main_namespace() -> Namespace {
    Namespace::new()
        .on_connect(greet)
        .on("test", |s, e| {
            s.emit("test-received", first(&e));
            s.ack(&e, Data::default().arg("woot"));
        })
        .on("message", |s, e| s.emit("message-received", first(&e)))
        .on("binary", |s, e| {
            s.emit("binary-received", first(&e));
            s.ack(&e, Data::default().binary_arg(vec![1u8, 2, 3]));
        })
        .on("close_transport", |s, _| s.close_transport())
        .on("binary_delimiter", |s, _| {
            s.emit(
                "binary-delimiter",
                Data::default().binary_arg(vec![0x1eu8, 1, 0x1e]),
            );
        })
        .on("binary_with_args", |s, _| {
            s.emit(
                "binary-args",
                Data::default().arg("extra").binary_arg(vec![9u8]),
            );
        })
        // with an ack: the same args back through it; without: `echo-back`
        .on("echo", |s, e| {
            if e.wants_ack() {
                let data = e.data().clone();
                s.ack(&e, data);
            } else {
                s.emit("echo-back", e.into_data());
            }
        })
        .on("ack_late", |s, e| {
            s.after(Duration::from_millis(500), move |s| {
                s.ack(&e, Data::default().arg("late"));
            });
        })
        .on("handshake_query", |s, e| {
            let query: serde_json::Map<_, _> = s
                .query_pairs()
                .iter()
                .map(|(k, v)| (k.clone(), json!(v)))
                .collect();
            s.ack(&e, Data::default().arg(query));
        })
        // a packet socket.io-parser rejects
        .on("invalid_packet", |s, _| s.send_raw(r#"2[true,"foo"]"#))
        // the server asks with a callback; the client's answer comes back as `answered`
        .on("ask_me", |s, e| {
            s.emit_with_ack("question", first(&e), |s, reply| {
                s.emit("answered", reply);
            });
        })
        .on("binary_mixed", |s, _| {
            let mut data = Data::default();
            let (a, b) = (data.binary(vec![1u8]), data.binary(vec![2u8, 3]));
            s.emit(
                "binary-mixed",
                data.arg(json!({ "a": a, "b": [b] })).arg("tail"),
            );
        })
        .on("server_disconnect", |s, _| s.disconnect())
}

/// `ci/socket-io-auth.js`: emits `auth` with `success` when `auth.password == "123"`.
pub(super) fn auth() -> Vec<(&'static str, Namespace)> {
    let ns = || {
        Namespace::new().on_connect(|s| {
            let ok = s.auth().and_then(|a| a.get("password")) == Some(&json!("123"));
            s.emit(
                "auth",
                Data::default().arg(if ok { "success" } else { "failed" }),
            );
            Connect::Accept
        })
    };
    vec![("/", ns()), ("/admin", ns())]
}

/// `ci/socket-io-restart.js`: emits `message` with the `message_back` header (or `test`);
/// `restart_server` takes the server down for 2 s.
pub(super) fn restart() -> Vec<(&'static str, Namespace)> {
    vec![(
        "/",
        Namespace::new()
            .on_connect(|s| {
                let message = s.header("message_back").unwrap_or("test").to_owned();
                s.emit("message", Data::default().arg(message));
                Connect::Accept
            })
            .on("restart_server", |s, _| {
                s.restart_server(Duration::from_secs(2));
            }),
    )]
}
