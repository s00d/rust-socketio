/**
 * This is an example server, used to test the current code.
 */
const engine = require('engine.io');
const http = require('http').createServer().listen(4203);
// the engine.io client runs on port 4203
const server = engine.attach(http, {
    allowUpgrades: false,
    transports: ["polling"]
});

console.log("Started")
server.on('connection', socket => {
    console.log("Connected");

    socket.on('message', message => {
        if (message !== undefined) {
            console.log(message.toString());
            if (message == "respond") {
                socket.send("Roger Roger");
            } else if (message == "respond_large") {
                // larger than one HTTP body chunk, so the polling response arrives in several frames
                socket.send("x".repeat(64 * 1024));
            } else if (message == "utf8") {
                socket.send("cash money €€€ てすと");
            } else if (message == "emoji") {
                socket.send("\uD83D\uDE00\uD83C\uDF89");
            } else if (message == "binary") {
                socket.send(Buffer.from([1, 2, 3]));
            } else if (message == "close") {
                socket.close();
            }
        } else {
            console.log("empty message received")
        }
    });

    socket.on('heartbeat', () => {
        console.log("heartbeat");
    });

    socket.on('error', message => {
        // Notify the client if there is an error so it's tests will fail
        socket.send("ERROR: Received error")
        console.log(message.toString());
    });

    socket.on('close', () => {
        console.log("Close");
        socket.close();
    });

    socket.send('hello client');
});
