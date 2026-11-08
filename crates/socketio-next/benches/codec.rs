//! Codec benchmarks (no I/O, no server): `cargo bench -p socketio-next`.
use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use serde_json::json;
use socketio_next::Payload;
use socketio_next::protocol::engineio::Batch;
use socketio_next::protocol::socketio::Packet;

fn socketio(c: &mut Criterion) {
    let text = Payload::from(vec![
        json!({"id": 1, "name": "x".repeat(64)}),
        json!([1, 2, 3]),
    ]);
    c.bench_function("socket.io encode event", |b| {
        b.iter(|| {
            Bytes::from(
                Packet::new_event(black_box(text.clone()), "ev", "/admin", Some(7)).unwrap(),
            )
        })
    });
    let raw = Bytes::from(Packet::new_event(text.clone(), "ev", "/admin", Some(7)).unwrap());
    c.bench_function("socket.io decode event", |b| {
        b.iter(|| {
            Packet::try_from(black_box(&raw))
                .unwrap()
                .event_payload()
                .unwrap()
        })
    });
    let mixed = Payload::Mixed {
        args: vec![
            json!({"file": Payload::placeholder(0)}),
            Payload::placeholder(1),
        ],
        attachments: vec![Bytes::from(vec![7u8; 4096]), Bytes::from(vec![9u8; 64])],
    };
    c.bench_function("socket.io encode binary event", |b| {
        b.iter(|| Packet::new_event(black_box(mixed.clone()), "upload", "/", None).unwrap())
    });
}

fn engineio(c: &mut Criterion) {
    let body = Bytes::from(
        (0..32)
            .map(|i| format!("42[\"ev\",{i}]"))
            .collect::<Vec<_>>()
            .join("\u{1e}"),
    );
    c.bench_function("engine.io decode polling batch of 32", |b| {
        b.iter(|| Batch::try_from(black_box(body.clone())).unwrap().len())
    });
}

criterion_group!(benches, socketio, engineio);
criterion_main!(benches);
