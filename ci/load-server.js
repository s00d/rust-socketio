// A plain socket.io 4.8 server with default options (not a test fixture): `echo` answers
// through the ack, or as `echo-back` without one. For `just load`: the same client run against
// this server and against the in-process fake must give the same result.
const { Server } = require("./conformance/node_modules/socket.io");

const port = Number(process.env.PORT || 4220);
const io = new Server(port);
let sockets = 0;
io.on("connection", (socket) => {
  sockets++;
  socket.on("echo", (...args) => {
    const ack = typeof args[args.length - 1] === "function" ? args.pop() : null;
    if (ack) ack(...args);
    else socket.emit("echo-back", ...args);
  });
  socket.on("disconnect", () => sockets--);
});
console.log(`load server on ${port}`);
process.on("SIGTERM", () => {
  console.log(`connections still open at exit: ${sockets}`);
  process.exit(0);
});
