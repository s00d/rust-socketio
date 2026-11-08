// Waits (up to 30 s) until every port given on the command line accepts TCP connections.
const net = require("net");
const ports = process.argv.slice(2).map(Number);
const deadline = Date.now() + 30000;

function probe(port) {
  return new Promise((resolve) => {
    const socket = net.connect(port, "127.0.0.1");
    socket.once("connect", () => { socket.destroy(); resolve(true); });
    socket.once("error", () => resolve(false));
  });
}

(async () => {
  for (const port of ports) {
    while (!(await probe(port))) {
      if (Date.now() > deadline) {
        console.error(`port ${port} not listening after 30 s`);
        process.exit(1);
      }
      await new Promise((r) => setTimeout(r, 200));
    }
  }
  console.log(`listening: ${ports.join(" ")}`);
})();
