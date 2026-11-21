//! Stories: the cookie jar (feature `cookies`): the engine.io `io` cookie set on the handshake
//! goes back on every polling request and on the websocket upgrade.

use socketio_next::{ClientBuilder, TransportType};

use crate::common::*;

#[tokio::test]
async fn cookies_reach_polling_and_the_websocket() {
    for (transport, kind) in [
        (TransportType::Polling, "polling"),
        (TransportType::WebsocketUpgrade, "websocket"),
    ] {
        // the caller's own cookie and the jar's (the sticky one) both go out
        let client = connect(
            ClientBuilder::new(cookie_server())
                .header("Cookie", "user=1")
                .transport_type(transport),
        )
        .await;
        client.emit_with_ack("echo", 1, WAIT).await.unwrap();
        let sid = client.link_info().engine_sid.unwrap();
        let seen = cookies_seen(&sid).await;
        let cookie = format!("io={sid}");
        assert!(
            seen.iter().any(|l| l.starts_with(&format!("{kind} "))
                && l.contains(&cookie)
                && l.contains("user=1")),
            "{kind}: {seen:?}"
        );
        client.disconnect().await.unwrap();
    }
}
