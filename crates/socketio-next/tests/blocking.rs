//! The blocking client against the ci/ servers (feature `blocking`).
#![cfg(feature = "blocking")]

mod common;

use std::sync::mpsc;
use std::time::Duration;

use common::*;
use serde_json::json;
use socketio_next::blocking::{Client, ClientBuilder};
use socketio_next::{ConnectionState, DisconnectReason, Error, Event, Payload};

#[test]
fn connect_emit_ack_disconnect() {
    let (tx, rx) = mpsc::channel();
    let client = ClientBuilder::new(socket_io_server())
        .namespace("/admin")
        .on("echo-back", move |event: Event, _: Client| {
            tx.send(event.into_payload()).unwrap();
        })
        .connect()
        .unwrap();
    assert!(client.connected());
    client.emit("echo", json!({"a": 1})).unwrap();
    assert_eq!(
        rx.recv_timeout(WAIT).unwrap(),
        Payload::Text(vec![json!({"a": 1})])
    );
    let reply = client.emit_with_ack("echo", "x", WAIT).unwrap();
    assert_eq!(reply.arg::<String>(0).unwrap(), "x");
    let err = client
        .emit_with_ack("ack_late", (), Duration::from_millis(50))
        .unwrap_err();
    assert!(matches!(err, Error::AckTimeout(_)));
    client.disconnect().unwrap();
    assert!(matches!(
        client.state(),
        ConnectionState::Closed {
            reason: DisconnectReason::IoClientDisconnect,
            ..
        }
    ));
}

/// A sync handler may use the blocking methods (it runs on a blocking thread).
#[test]
fn blocking_calls_inside_a_handler() {
    let (tx, rx) = mpsc::channel();
    let client = ClientBuilder::new(socket_io_server())
        .on("test", move |event: Event, client: Client| {
            if event.arg::<String>(0).is_ok() {
                let reply = client.emit_with_ack("echo", "from handler", WAIT).unwrap();
                tx.send(reply.arg::<String>(0).unwrap()).unwrap();
                client.disconnect().unwrap();
            }
        })
        .connect()
        .unwrap();
    assert_eq!(rx.recv_timeout(WAIT).unwrap(), "from handler");
    let state = client.wait_for(ConnectionState::is_closed, WAIT);
    assert!(state.is_closed());
}

#[test]
fn connect_errors_are_typed() {
    let err = ClientBuilder::new(socket_io_server())
        .namespace("/reject")
        .connect()
        .unwrap_err();
    assert!(matches!(err, Error::ConnectRejected { .. }), "{err:?}");
}
