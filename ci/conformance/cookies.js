// socket.io 4.8 with engine.io's `io` cookie (a sticky-session load balancer sets one like it).
// :4213  GET /cookies?sid=<engine sid> lists "<transport> <Cookie header or ->" per request.
const http = require("http");
const { Server } = require("socket.io");

const seen = new Map();
const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://x");
  if (url.pathname === "/cookies") {
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(seen.get(url.searchParams.get("sid")) || []));
    return;
  }
  res.statusCode = 404;
  res.end();
});
const io = new Server(server, { cookie: { name: "io", httpOnly: true, path: "/" } });
const record = (req) => {
  const url = new URL(req.url, "http://x");
  const sid = url.searchParams.get("sid");
  if (!sid) return;
  if (!seen.has(sid)) seen.set(sid, []);
  seen.get(sid).push(`${url.searchParams.get("transport")} ${req.headers.cookie || "-"}`);
};
// after io attached its listeners, so these run first
server.prependListener("request", record);
server.prependListener("upgrade", record);
io.on("connection", (socket) => {
  socket.on("echo", (...args) => {
    const ack = typeof args[args.length - 1] === "function" ? args.pop() : null;
    if (ack) ack(...args);
  });
});
server.listen(4213, () => console.log("cookie server on 4213"));
