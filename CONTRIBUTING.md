# Contributing

Contributions are welcome, under the terms of the [LICENSE](LICENSE).

## Layout

| Path | Contents |
|---|---|
| `crates/socketio-next` | the client: `src/` (socket.io layer, `engineio/`, `protocol/`), `tests/it/` (one test binary), `benches/` |
| `crates/socketio-next-testing` | test support, not published: in-process fake servers (`src/fake/`), the Node server URLs, a fault-injecting proxy, the differential harness |
| `crates/tauri-plugin-socketio` | the Tauri 2 plugin: Rust in `src/`, the webview API in `guest-js/`, tests in `tests/it/` and `tests/guest-js/` |
| `examples/` | runnable examples (`just example <name>`) and the load run (`just load`) |
| `ci/` | the Node test servers (socket.io 4.x fixtures, the 4.8 conformance server, TLS, the load server) |
| `docs/` | the documentation site (Nuxt Content) |

How the code is structured: `ARCHITECTURE.md`. How tests are written: `TESTING.md`. How releases are made: `PUBLISHING.md`.

## Recipes

CI runs `just` recipes; the same commands work locally.

```bash
cargo install just cargo-hack cargo-nextest cargo-deny cargo-shear cargo-semver-checks cargo-sweep
just servers-setup && just servers   # the Node servers (only some tests need them)
just ci                              # what CI runs
```

More in the [contributing guide](https://s00d.github.io/rust-socketio/guides/contributing).

## Commits

[Conventional commits](https://www.conventionalcommits.org/en/v1.0.0/); changes worth a release note go under `## [Unreleased]` in `CHANGELOG.md` (and the plugin's), by hand.
