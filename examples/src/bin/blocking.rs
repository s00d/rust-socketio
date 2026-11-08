//! The client without async (feature `blocking`).
use std::time::Duration;

use socketio_next::Event;
use socketio_next::blocking::{Client, ClientBuilder};

fn main() -> socketio_next::Result<()> {
    examples::init_logging();
    let client = ClientBuilder::new(examples::url())
        .on("echo-back", |event: Event, _: Client| {
            println!("echo-back: {:?}", event.payload())
        })
        .connect()?;
    client.emit("echo", "from a thread")?;
    let reply = client.emit_with_ack("echo", 7, Duration::from_secs(5))?;
    println!("ack: {:?}", reply.arg::<i64>(0)?);
    std::thread::sleep(Duration::from_millis(300));
    client.disconnect()
}
