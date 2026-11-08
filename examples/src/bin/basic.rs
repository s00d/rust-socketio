//! Connect, receive events, emit, wait for an ack, disconnect.
use std::time::Duration;

use serde_json::json;
use socketio_next::{Client, ClientBuilder, Event};

#[tokio::main]
async fn main() -> socketio_next::Result<()> {
    examples::init_logging();
    let client = ClientBuilder::new(examples::url())
        // a plain closure runs on a blocking thread; an `async move` block runs on the event task
        .on("test", |event: Event, _: Client| {
            println!("test: {:?}", event.payload());
        })
        .on("echo-back", |event: Event, _: Client| async move {
            println!("echo-back: {:?}", event.payload());
        })
        .connect()
        .await?;
    println!("connected as {:?}", client.id());

    // emit never blocks; it fails fast when the link is down
    client.emit("echo", json!({"hello": "world"}))?;

    // an ack is a future with a timeout
    let reply = client
        .emit_with_ack("echo", vec![json!(1), json!("two")], Duration::from_secs(5))
        .await?;
    let (one, two): (i64, String) = reply.deserialize()?;
    println!("ack: {one} {two}");

    tokio::time::sleep(Duration::from_millis(500)).await;
    client.disconnect().await
}
