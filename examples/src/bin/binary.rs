//! Binary arguments: one buffer, buffers next to JSON, buffers inside objects.
use std::time::Duration;

use bytes::Bytes;
use serde_json::json;
use socketio_next::{ClientBuilder, Payload};

#[tokio::main]
async fn main() -> socketio_next::Result<()> {
    examples::init_logging();
    let client = ClientBuilder::new(examples::url()).connect().await?;

    let reply = client
        .emit_with_ack(
            "echo",
            Bytes::from_static(&[1, 2, 3]),
            Duration::from_secs(5),
        )
        .await?;
    println!("one buffer back: {:?}", reply.binary());

    // `{"_placeholder":true,"num":n}` marks where attachments[n] goes
    let upload = Payload::Mixed {
        args: vec![
            json!({"name": "a.bin", "data": Payload::placeholder(0)}),
            json!(42),
        ],
        attachments: vec![Bytes::from(vec![0u8; 1024])],
    };
    let reply = client
        .emit_with_ack("echo", upload, Duration::from_secs(5))
        .await?;
    println!(
        "mixed back: {} args, {} attachments",
        reply.len(),
        reply.binary().len()
    );
    client.disconnect().await
}
