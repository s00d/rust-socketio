//! Namespace, query and CONNECT auth; a rejected namespace is a typed error.
use serde_json::json;
use socketio_next::{Client, ClientBuilder, Error, Event};

#[tokio::main]
async fn main() -> socketio_next::Result<()> {
    examples::init_logging();
    // the auth server of ci/ listens on 4204 and checks `auth.password`
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://localhost:4204".into());
    let client = ClientBuilder::new(&url)
        .namespace("/admin")
        .query("token", "secret") // encoded for you, shown as `***` in logs
        .auth(json!({ "password": "123" })) // socket.handshake.auth on the server
        .on("auth", |event: Event, _: Client| {
            println!("server says: {:?}", event.arg::<String>(0));
        })
        .connect()
        .await?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    client.disconnect().await?;

    // a namespace the middleware refuses
    match ClientBuilder::new("http://localhost:4200")
        .namespace("/reject")
        .connect()
        .await
    {
        Err(Error::ConnectRejected { message, data }) => println!("rejected: {message} {data:?}"),
        other => println!("unexpected: {other:?}"),
    }
    Ok(())
}
