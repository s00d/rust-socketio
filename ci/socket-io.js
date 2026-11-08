const server = require('http').createServer();
const io = require('socket.io')(server);

console.log('Started');
var callback = client => {
    console.log('Connected!');
    client.on('test', data => {
        // Send a message back to the server to confirm the message was received
        client.emit('test-received', data);
        console.log(['test', data]);
    });
    client.on('message', data => {
        client.emit('message-received', data);
        console.log(['message', data]);
    });
    client.on('test', function (arg, ack) {
        console.log('Ack received')
        if (ack) {
            ack('woot');
        }
    });

    client.on('binary', data => {
        var bufView = new Uint8Array(data);
        console.log(['binary', 'Yehaa binary payload!']);
        for (elem in bufView) {
            console.log(['binary', elem]);
        }
        client.emit('binary-received', data);
        console.log(['binary', data]);
    });

    client.on('binary', function (arg, ack) {
        console.log(['binary', 'Ack received, answer with binary'])
        if (ack) {
            ack(Buffer.from([1, 2, 3]));
        }
    });

    // This event allows the test framework to arbitrarily close the underlying connection
    client.on('close_transport', data => {
        console.log(['close_transport', 'Request to close transport received'])
        // Close underlying websocket connection
        client.client.conn.close();
    })

    // Binary attachment that contains the polling payload separator (0x1e).
    client.on('binary_delimiter', () => {
        client.emit('binary-delimiter', Buffer.from([0x1e, 1, 0x1e]));
    })

    // Binary event with an extra (non-binary) argument before the attachment.
    client.on('binary_with_args', () => {
        client.emit('binary-args', 'extra', Buffer.from([9]));
    })

    // JS-parity helpers: echo the arguments back through the ack (or as `echo-back`).
    client.on('echo', (...args) => {
        const ack = typeof args[args.length - 1] === 'function' ? args.pop() : null;
        if (ack) {
            ack(...args);
        } else {
            client.emit('echo-back', ...args);
        }
    })

    // Acknowledges after 500 ms.
    client.on('ack_late', (...args) => {
        const ack = args.pop();
        if (typeof ack === 'function') setTimeout(() => ack('late'), 500);
    })

    client.on('handshake_query', (ack) => {
        ack(client.handshake.query);
    })

    // Makes the server send a packet that socket.io-parser rejects.
    client.on('invalid_packet', () => {
        client.conn.send('2[true,"foo"]');
    })

    // The server asks with a callback; the client's ack comes back as `answered`.
    client.on('ask_me', (question) => {
        client.emit('question', question, (...reply) => client.emit('answered', ...reply));
    })

    // Binary in nested positions and two attachments.
    client.on('binary_mixed', () => {
        client.emit('binary-mixed', { a: Buffer.from([1]), b: [Buffer.from([2, 3])] }, 'tail');
    })

    // Server-side namespace disconnect: leaves the underlying engine.io connection open.
    client.on('server_disconnect', () => {
        console.log(['server_disconnect', 'Request to disconnect the namespace received'])
        client.disconnect();
    })

    client.emit('Hello from the message event!');
    client.emit('test', 'Hello from the test event!');
    client.emit(Buffer.from([4, 5, 6]));
    client.emit('test', Buffer.from([1, 2, 3]));
    client.emit('This is the first argument', 'This is the second argument', {
        argCount: 3
    });
    client.emit('on_abc_event', '', {
        abc: 0,
        some_other: 'value',
    });
};
io.on('connection', callback);
io.of('/admin').on('connection', callback);
// Namespace whose middleware answers CONNECT late (an emit sent before the ack must be buffered).
io.of('/slow').use((socket, next) => setTimeout(next, 200)).on('connection', callback);
// Namespace whose middleware rejects every connection with CONNECT_ERROR.
io.of('/reject').use((socket, next) => {
    setTimeout(() => {
        const err = new Error('not authorized');
        err.data = { reason: 'test' };
        next(err);
    }, 200);
});
// the socket.io client runs on port 4201
server.listen(4200);
