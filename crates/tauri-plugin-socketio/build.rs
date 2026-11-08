const COMMANDS: &[&str] = &[
    "connect",
    "disconnect",
    "emit",
    "emit_with_ack",
    "ack",
    "subscribe",
    "unsubscribe",
    "list",
    "state",
    "link_info",
];

fn main() {
    tauri_plugin::Builder::new(COMMANDS).build();

    // Test binaries link tauri, which needs Common-Controls v6 on Windows: without this manifest
    // they fail to start (STATUS_ENTRYPOINT_NOT_FOUND). Same workaround as tauri's own build.rs.
    // Link args of a build script apply only to this crate's own linked targets (it is an rlib,
    // so its test binaries), never to apps that depend on it.
    let windows = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows");
    let msvc = std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc");
    if windows && msvc {
        let manifest =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("windows-test-manifest.xml");
        println!("cargo:rerun-if-changed={}", manifest.display());
        println!("cargo:rustc-link-arg=/MANIFEST:EMBED");
        println!("cargo:rustc-link-arg=/MANIFESTINPUT:{}", manifest.display());
    }
}
