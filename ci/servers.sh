#!/usr/bin/env bash
# Starts / stops the Node test servers in the background (no Docker):
#   ci/servers.sh start   ports 4200-4206 (ci/, socket.io 4.0 / engine.io 6.4) and
#                         4210-4211 (ci/conformance, socket.io 4.8); logs in ci/logs/
#   ci/servers.sh stop
# Needs `npm ci` in ci/ and ci/conformance, and ci/cert (./keygen.sh) for the TLS server.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p logs

servers=(engine-io.js engine-io-polling.js engine-io-secure.js socket-io.js socket-io-auth.js
  socket-io-restart.js socket-io-restart-url-auth.js conformance/server.js)

case "${1:-start}" in
start)
  [[ -f cert/server.crt ]] || ./keygen.sh node-engine-io-secure 127.0.0.1
  for s in "${servers[@]}"; do
    name="$(basename "${s}" .js)"
    [[ "${s}" == conformance/* ]] && name="conformance"
    node "${s}" > "logs/${name}.log" 2>&1 &
    echo $! > "logs/${name}.pid"
  done
  node wait-ports.js 4200 4201 4202 4203 4204 4205 4206 4210 4211
  ;;
stop)
  for pid in logs/*.pid; do
    [[ -f "${pid}" ]] || continue
    kill "$(cat "${pid}")" 2>/dev/null || true
    rm -f "${pid}"
  done
  ;;
*)
  echo "usage: $0 start|stop" >&2
  exit 2
  ;;
esac
