use rust_socketio::{ClientBuilder, TlsConfig};

fn main() {
    // A private CA that is not in the default roots (Mozilla roots with rustls, the OS store with
    // native-tls). Without `.tls_config(..)` the default roots are used.
    let pem = std::fs::read("ca.crt").expect("Failed to read cert");
    let tls = TlsConfig::with_root_certificates_pem(&pem).expect("Invalid certificate");

    let socket = ClientBuilder::new("https://localhost:4200")
        .tls_config(tls)
        .on("error", |err, _| eprintln!("Error: {:#?}", err))
        .connect()
        .expect("Connection failed");

    // use the socket

    socket.disconnect().expect("Disconnect failed")
}
