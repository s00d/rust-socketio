// Recording socket.io 4.8 server for the differential tests (crates/socketio-next-testing).
//
// :4210  every engine.io message packet of a client is logged under its `tag` query parameter
//        (GET /log?tag=<tag> returns them); the Node client and the Rust client run the same
//        scenario and the logs must match.
// :4211  the same events with connection state recovery enabled.
// :4212  the same events with pingInterval 300 ms, pingTimeout 200 ms.
const http = require("http");
const { Server } = require("socket.io");

const logs = new Map();

function normalize(dir, packet) {
  let data = packet.data;
  if (Buffer.isBuffer(data) || data instanceof ArrayBuffer || ArrayBuffer.isView(data)) {
    data = "<binary " + Buffer.from(data).toString("hex") + ">";
  } else if (typeof data === "string") {
    // session ids are random: CONNECT answers carry them
    data = data.replace(/"(sid|pid)":"[^"]+"/g, '"$1":"*"');
    // ack ids of server events come from a counter shared by every socket of the namespace;
    // the client's own ack ids (events in, acks out) stay as they are
    const serverId = dir === "out" ? /^(2|5\d+-)((?:\/[^,]*,)?)(\d+)/ : /^(3|6\d+-)((?:\/[^,]*,)?)(\d+)/;
    data = data.replace(serverId, "$1$2#");
  }
  return `${dir} ${packet.type}${data === undefined ? "" : " " + data}`;
}

function record(io) {
  io.engine.on("connection", (raw) => {
    const tag = raw.request._query.tag;
    if (!tag) return;
    if (!logs.has(tag)) logs.set(tag, []);
    const log = logs.get(tag);
    // pings, pongs, noops and upgrades depend on timing and transport, not on the client logic
    const keep = (p) => p.type === "message" || p.type === "close";
    raw.on("packet", (p) => keep(p) && log.push(normalize("in", p)));
    raw.on("packetCreate", (p) => keep(p) && log.push(normalize("out", p)));
  });
}

function events(socket) {
  socket.on("echo", (...args) => {
    const ack = typeof args[args.length - 1] === "function" ? args.pop() : null;
    if (ack) ack(...args);
    else socket.emit("echo-back", ...args);
  });
  socket.on("ask", () => {
    socket.emit("question", "why?", (...reply) => socket.emit("answered", ...reply));
  });
  socket.on("bin", () => {
    socket.emit("binary-mixed", { a: Buffer.from([1]), b: [Buffer.from([2, 3])] }, "tail");
  });
  socket.on("shapes", () => {
    socket.emit("no-args");
    socket.emit("scalars", 1, "x", true, null, 2.5);
    socket.emit("nested", { deep: { list: [1, { buf: Buffer.from([7, 8]) }] } });
    socket.emit(42, "numeric name");
    socket.emit("unicode", "てすと 😀");
    socket.emit("done");
  });
  socket.on("drop", () => socket.conn.close());
  socket.on("tick", (n) => socket.emit("tock", n, `offset-${n}`));
}

function serve(port, opts) {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, "http://x");
    if (url.pathname === "/log") {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify(logs.get(url.searchParams.get("tag")) || []));
      return;
    }
    res.statusCode = 404;
    res.end();
  });
  const io = new Server(server, opts);
  record(io);
  io.on("connection", events);
  io.of("/admin").on("connection", events);
  server.listen(port, () => console.log(`conformance server on ${port}`));
}

serve(4210, {});
serve(4211, { connectionStateRecovery: { maxDisconnectionDuration: 60000 } });
// fast heartbeat for ping-timeout tests (pingInterval + pingTimeout = 500 ms)
serve(4212, { pingInterval: 300, pingTimeout: 200 });
