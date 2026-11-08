// The Rust side: a "core" whose socket must not depend on Tauri builds its client itself and
// hands it to the registry read-only; the webview can watch it but never emit or disconnect it.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::time::Duration;

use tauri_plugin_socketio::socketio_next::{Client, ClientBuilder};
use tauri_plugin_socketio::{Access, SocketIoExt};

const URL: &str = "http://localhost:4210"; // `just servers` in the repository

/// Stands for a Tauri-free crate: it only knows socketio-next.
mod core {
    use super::*;

    pub async fn start() -> Result<Client, tauri_plugin_socketio::socketio_next::Error> {
        let client = ClientBuilder::new(URL).connect().await?;
        let ticker = client.clone();
        tokio::spawn(async move {
            for n in 1.. {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let _ = ticker.emit("tick", serde_json::json!(n));
            }
        });
        Ok(client)
    }
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_socketio::init())
        .setup(|app| {
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                match core::start().await {
                    Ok(client) => {
                        if let Err(e) = handle.socketio().insert("core", client, Access::ReadOnly) {
                            eprintln!("core: {e}");
                        }
                    }
                    Err(e) => eprintln!("core: {e}"),
                }
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
