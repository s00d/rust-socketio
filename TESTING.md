# Testing

Behaviour is tested the way users use the crates: build a client, connect to a server, emit,
receive, lose the link, reconnect; for the plugin, call its IPC commands as a webview would.
Tests go through the public API only.

## Kinds

| Kind | What | Example |
|---|---|---|
| **model** (proptest) | Random inputs checked against a small model of the protocol rules: the model predicts the encoded packet, the decoded payload or the error. On failure proptest shrinks to the smallest case. | `crates/socketio-next/tests/it/model_socketio_codec.rs` |
| **story** | One user journey per test, start to end, with a `Given / When / Then` doc comment: connect with auth, emit with an ack, lose the link, buffer emits, reconnect, read the state transitions. A story checks every step's observable result, not one function. | `crates/socketio-next/tests/it/story_*.rs` |
| **corpus** | A differential comparison with the reference implementation: the Node socket.io-client 4.8 runs the same scenario and the packets the server recorded must match. | `crates/socketio-next/tests/it/corpus_conformance.rs` |

## Servers

- **In-process fakes** (`socketio-next-testing::fake`): the fixtures of `ci/socket-io.js`,
  `ci/socket-io-auth.js`, `ci/socket-io-restart.js`, `ci/engine-io.js`,
  `ci/engine-io-polling.js` written in Rust from the protocol (not with socketio-next's codecs).
  They start on first use; models and most stories need nothing else.
- **Node** (`just servers-setup`, `just servers`): only what the reference server settles: the
  corpus (differential) tests, connection state recovery, cookies (ci/conformance, 4210-4213)
  and TLS (ci/, 4202).

`SOCKET_IO_SERVER=http://localhost:4200` (and the other variables in
`socketio-next-testing/src/lib.rs`) runs the same stories against the Node fixtures.

## Layout

Each crate has **one** integration test binary, `tests/it/main.rs`, whose modules are the model,
story and corpus files, plus `tests/it/common/` for helpers. No `#[cfg(test)] mod tests` in
`src/` (doctests stay: they are documentation), except checks the public API cannot make; each
such module says why in its first line.

## Rules

- Compare payloads, states, disconnect reasons, error variants and recorded packets. Compare
  message texts only where the text is the contract (redaction, `connect_error` messages).
- A new regression is a new step in a story or a case the model now generates, never a new
  one-function test.
- No timing: no sleeps, no real-time limits. Wait for events (a state, an event, a packet the
  server recorded); per-test timeouts are in `.config/nextest.toml`. Delays a test needs
  (heartbeat, backoff) are small server or client settings, not sleeps in the test.
- Logs of long runs go to `target/logs/<task>.log`.

## Running

```bash
just test                                            # every test (Node servers up: just servers)
cargo nextest run -p socketio-next --all-features     # one crate
cargo nextest run -p socketio-next --all-features -E 'test(/^story_reconnect::/)'   # one area
```
