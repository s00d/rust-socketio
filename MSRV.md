# Minimum supported Rust version

`socketio-next` needs **Rust 1.85** (edition 2024) with every feature (`rustls`, `native-tls`,
`blocking`).

The number assumes the MSRV-aware resolver picks your dependency versions: Cargo 1.84+ does that
by default for packages with `resolver = "3"` / edition 2024; otherwise set it once:

```sh
CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback cargo update
```

With newest-version resolution some dependencies may need a newer compiler.

`just msrv` (run in CI) builds the crate on 1.85 in a throwaway project whose lockfile is
resolved for 1.85, as a downstream project would, once with `rustls` and once with
`native-tls` + `blocking`.
