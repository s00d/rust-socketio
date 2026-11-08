// The Node side of the differential tests: runs one scenario with socket.io-client 4.8.
// Usage: node client.js <scenario> <tag>    (CONFORMANCE_SERVER overrides the URL, TRANSPORTS=polling
// limits the transports)
const { io } = require("socket.io-client");

const url = process.env.CONFORMANCE_SERVER || "http://localhost:4210";
const [scenario, tag] = process.argv.slice(2);

const once = (socket, event) => new Promise((resolve) => socket.once(event, (...args) => resolve(args)));

const scenarios = {
  // emits, acks both ways, binary out and in
  async basic(socket) {
    socket.emit("echo", 1, "two", { three: 3 });
    await once(socket, "echo-back");
    const reply = await socket.timeout(5000).emitWithAck("echo", "ack-me", [1, 2]);
    socket.emit("echo", Buffer.from([1, 2, 3]), "x", reply);
    await once(socket, "echo-back");
    socket.on("question", (q, ack) => ack("because", q));
    socket.emit("ask");
    await once(socket, "answered");
    socket.emit("bin");
    const [mixed, tail] = await once(socket, "binary-mixed");
    socket.emit("echo", mixed, tail);
    await once(socket, "echo-back");
  },
  // every event the server sends goes back as `seen` with its name and arguments
  async shapes(socket) {
    const done = once(socket, "done");
    socket.onAny((name, ...args) => socket.emit("seen", String(name), ...args));
    socket.emit("shapes");
    await done;
    await socket.timeout(5000).emitWithAck("echo", "sync");
  },
};

async function main() {
  const transports = process.env.TRANSPORTS ? process.env.TRANSPORTS.split(",") : undefined;
  const socket = io(url, { query: { tag }, auth: { token: "t" }, reconnection: false, transports });
  await once(socket, "connect");
  await scenarios[scenario](socket);
  socket.disconnect();
  // let the disconnect packets reach the server before the log is read
  await new Promise((r) => setTimeout(r, 200));
}

main().then(
  () => process.exit(0),
  (err) => {
    console.error(err);
    process.exit(1);
  },
);
