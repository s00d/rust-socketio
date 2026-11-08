//! Polling only (no websocket, e.g. behind a proxy that blocks upgrades), and TLS options.
use socketio_next::{ClientBuilder, TransportType};

#[tokio::main]
async fn main() -> socketio_next::Result<()> {
    examples::init_logging();
    let client = ClientBuilder::new(examples::url())
        .transport_type(TransportType::Polling)
        .connect()
        .await?;
    println!("transport: {:?}", client.link_info().transport);
    client.disconnect().await?;

    // https/wss use the Mozilla roots with the default `rustls` feature. A private CA:
    //   let pem = std::fs::read("ca.crt")?;
    //   ClientBuilder::new("https://host").tls_config(TlsConfig::with_root_certificates_pem(&pem)?)
    // or a `rustls::ClientConfig` / `native_tls::TlsConnector` of your own.
    Ok(())
}
