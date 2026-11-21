//! Load run: many clients, many events, every reply checked.
//!
//! `cargo run -p examples --bin load -- <url|fake> [clients] [events] [any|polling|websocket]`
//!
//! Each client sends `events` emits without an ack (the server answers `echo-back`, they must
//! come back complete and in order) and `events` emits with an ack, all at once (each reply
//! must equal what was sent). Payloads rotate through JSON with unicode, one buffer, buffers
//! nested in objects, and a 16 KiB string. `fake` runs against the in-process test server.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::join_all;
use serde_json::json;
use socketio_next::{Client, ClientBuilder, Event, Payload, TransportType};
use tokio::sync::mpsc;

fn payload(client: usize, i: usize) -> Payload {
    match i % 4 {
        0 => Payload::Text(vec![
            json!({ "c": client, "i": i, "s": "Привет 😀 てすと" }),
            json!([1, 2.5, null, true]),
        ]),
        1 => Payload::Binary(Bytes::from(format!("bin-{client}-{i}").into_bytes())),
        2 => Payload::Mixed {
            args: vec![
                json!({ "c": client, "a": Payload::placeholder(0), "b": [Payload::placeholder(1)] }),
                json!(i),
            ],
            attachments: vec![
                Bytes::from(vec![i as u8; 3]),
                Bytes::from_static(&[0x1e, 0, 255]),
            ],
        },
        _ => Payload::Text(vec![json!("x".repeat(16 * 1024)), json!(i)]),
    }
}

#[derive(Default, Debug)]
struct Report {
    acks_ok: usize,
    echoes_ok: usize,
    errors: Vec<String>,
    latencies: Vec<Duration>,
}

async fn run_client(url: String, transport: TransportType, client: usize, events: usize) -> Report {
    let mut report = Report::default();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let built = ClientBuilder::new(url)
        .transport_type(transport)
        .on("echo-back", move |event: Event, _: Client| {
            let _ = tx.send(event.into_payload());
        })
        .connect()
        .await;
    let socket = match built {
        Ok(socket) => socket,
        Err(e) => {
            report.errors.push(format!("client {client}: connect: {e}"));
            return report;
        }
    };
    for i in 0..events {
        if let Err(e) = socket.emit("echo", payload(client, i)) {
            report
                .errors
                .push(format!("client {client}: emit {i}: {e}"));
        }
    }
    let acks = (0..events).map(|i| {
        let socket = socket.clone();
        async move {
            let started = Instant::now();
            let reply = socket
                .emit_with_ack("echo", payload(client, i), Duration::from_secs(30))
                .await;
            (i, reply, started.elapsed())
        }
    });
    for (i, reply, took) in join_all(acks).await {
        match reply {
            Ok(got) if got == payload(client, i) => {
                report.acks_ok += 1;
                report.latencies.push(took);
            }
            Ok(got) => report
                .errors
                .push(format!("client {client}: ack {i} differs: {got:?}")),
            Err(e) => report.errors.push(format!("client {client}: ack {i}: {e}")),
        }
    }
    for i in 0..events {
        match tokio::time::timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(got)) if got == payload(client, i) => report.echoes_ok += 1,
            Ok(Some(got)) => {
                report.errors.push(format!(
                    "client {client}: echo {i} differs or out of order: {got:?}"
                ));
                break;
            }
            _ => {
                report
                    .errors
                    .push(format!("client {client}: echo {i} missing"));
                break;
            }
        }
    }
    if let Err(e) = socket.disconnect().await {
        report
            .errors
            .push(format!("client {client}: disconnect: {e}"));
    }
    report
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let target = args.next().unwrap_or_else(|| "fake".into());
    let url = if target == "fake" {
        socketio_next_testing::socket_io_server()
    } else {
        target.clone()
    };
    let clients: usize = args.next().map_or(50, |v| v.parse().unwrap());
    let events: usize = args.next().map_or(200, |v| v.parse().unwrap());
    let transport = match args.next().as_deref() {
        Some("polling") => TransportType::Polling,
        Some("websocket") => TransportType::Websocket,
        _ => TransportType::Any,
    };
    let started = Instant::now();
    let reports =
        join_all((0..clients).map(|c| tokio::spawn(run_client(url.clone(), transport, c, events))))
            .await;
    let elapsed = started.elapsed();
    let mut total = Report::default();
    for report in reports {
        let report = report.expect("client task");
        total.acks_ok += report.acks_ok;
        total.echoes_ok += report.echoes_ok;
        total.errors.extend(report.errors);
        total.latencies.extend(report.latencies);
    }
    total.latencies.sort();
    let pct = |p: f64| {
        total
            .latencies
            .get(((total.latencies.len() as f64 - 1.0) * p) as usize)
            .copied()
            .unwrap_or_default()
    };
    let expected = clients * events;
    println!(
        "target={target} transport={transport:?} clients={clients} events/client={events}\n\
         acks ok {}/{expected}, echoes ok {}/{expected}, errors {}\n\
         ack latency p50 {:?} p99 {:?} max {:?}; {:.0} messages/s; {:?} total",
        total.acks_ok,
        total.echoes_ok,
        total.errors.len(),
        pct(0.5),
        pct(0.99),
        total.latencies.last().copied().unwrap_or_default(),
        (2 * expected * 2) as f64 / elapsed.as_secs_f64(),
        elapsed,
    );
    for error in total.errors.iter().take(10) {
        println!("  {error}");
    }
    if !total.errors.is_empty() || total.acks_ok != expected || total.echoes_ok != expected {
        std::process::exit(1);
    }
}
