//! Shared bits of the examples.

/// The server URL: the first argument, or the ci/ test server.
pub fn url() -> String {
    std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://localhost:4200".to_owned())
}

/// `RUST_LOG=socketio_next=debug` shows connects, reconnects and close reasons.
pub fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "socketio_next=info".into()),
        )
        .init();
}
