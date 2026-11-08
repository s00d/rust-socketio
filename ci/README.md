# Test servers

Node servers the integration tests run against. No Docker: `just servers-setup` (npm ci, test
certificates) once, then `just servers` / `just servers-stop` (or `ci/servers.sh start|stop`).
Logs go to `ci/logs/`.

| Port | Script | What |
|---|---|---|
| 4200 | `socket-io.js` | socket.io 4.0 with the event fixtures; namespaces `/`, `/admin`, `/slow`, `/reject` |
| 4201 | `engine-io.js` | engine.io 6.4 |
| 4202 | `engine-io-secure.js` | engine.io over TLS (`cert/`, made by `keygen.sh`) |
| 4203 | `engine-io-polling.js` | engine.io without websocket upgrades |
| 4204 | `socket-io-auth.js` | checks `auth.password` |
| 4205 | `socket-io-restart.js` | restarts on `restart_server`; echoes the `message_back` header |
| 4206 | `socket-io-restart-url-auth.js` | checks a `timestamp` query parameter |
| 4210 | `conformance/server.js` | socket.io 4.8, records each client's packets (`GET /log?tag=`) |
| 4211 | `conformance/server.js` | the same with connection state recovery |
| 4212 | `conformance/server.js` | the same with pingInterval 300 ms / pingTimeout 200 ms |

`conformance/client.js` is the Node side of the differential tests (socket.io-client 4.8).

TLS tests trust `cert/ca.crt` in-process (`TlsConfig::with_root_certificates_pem`); nothing is
installed into a system store. `CA_CERT_PATH` and the `*_SERVER` variables of
`crates/socketio-next-testing` point the tests elsewhere.
