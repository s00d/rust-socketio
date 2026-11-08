//! Watch the connection state: reasons, reconnect attempts with their delays, health info.
//! Stop the server while it runs to see the reconnect loop; Ctrl-C to quit.
use std::time::Duration;

use socketio_next::{Client, ClientBuilder, ConnectionState};

#[tokio::main]
async fn main() -> socketio_next::Result<()> {
    examples::init_logging();
    let client = ClientBuilder::new(examples::url())
        .reconnection_delay(Duration::from_millis(500))
        .reconnection_delay_max(Duration::from_secs(5))
        .reconnection_attempts(20)
        // every transition, in order (the watch below only sees the latest)
        .on_state_change(|state: ConnectionState, _: Client| async move {
            match state {
                ConnectionState::Connected { sid, recovered } => {
                    println!("connected {sid} recovered={recovered}")
                }
                ConnectionState::Disconnected { reason, .. } => println!("lost: {reason}"),
                ConnectionState::Reconnecting {
                    attempt,
                    delay,
                    error,
                } => {
                    println!("attempt {attempt} in {delay:?} (last error: {error:?})")
                }
                ConnectionState::Closed { reason, error } => println!("closed: {reason} {error:?}"),
                other => println!("{other:?}"),
            }
        })
        .connect()
        .await?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                let info = client.link_info();
                println!("transport {:?}, last ping {:?} ago", info.transport, info.last_ping_at.map(|t| t.elapsed()));
            }
        }
    }
    // graceful: leaves the namespace, closes the engine, bounded on a dead link
    client.disconnect().await
}
