use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

use super::{ClientBuilder, RawClient};
use crate::backoff::Backoff;
use crate::{
    error::Result,
    packet::{Packet, PacketId},
    Error,
};
pub(crate) use crate::{event::Event, payload::Payload};

#[derive(Clone)]
pub struct Client {
    builder: Arc<Mutex<ClientBuilder>>,
    client: Arc<RwLock<RawClient>>,
    // set by disconnect(); outlives the RawClient, which a reconnect replaces
    disconnected: Arc<AtomicBool>,
}

impl Client {
    pub(crate) fn new(builder: ClientBuilder) -> Result<Self> {
        let builder_clone = builder.clone();
        let client = builder_clone.connect_raw()?;

        let s = Self {
            builder: Arc::new(Mutex::new(builder)),
            client: Arc::new(RwLock::new(client)),
            disconnected: Arc::new(AtomicBool::new(false)),
        };
        s.poll_callback();

        Ok(s)
    }

    /// Updates the URL the client will connect to when reconnecting.
    /// This is especially useful for updating query parameters.
    pub fn set_reconnect_url<T: Into<String>>(&self, address: T) -> Result<()> {
        self.builder.lock()?.address = address.into();
        Ok(())
    }

    /// Sends a message to the server using the underlying `engine.io` protocol.
    /// This message takes an event, which could either be one of the common
    /// events like "message" or "error" or a custom event like "foo". But be
    /// careful, the data string needs to be valid JSON. It's recommended to use
    /// a library like `serde_json` to serialize the data properly.
    ///
    /// # Example
    /// ```
    /// use rust_socketio::{ClientBuilder, RawClient, Payload};
    /// use serde_json::json;
    ///
    /// let mut socket = ClientBuilder::new("http://localhost:4200/")
    ///     .on("test", |payload: Payload, socket: RawClient| {
    ///         println!("Received: {:#?}", payload);
    ///         socket.emit("test", json!({"hello": true})).expect("Server unreachable");
    ///      })
    ///     .connect()
    ///     .expect("connection failed");
    ///
    /// let json_payload = json!({"token": 123});
    ///
    /// let result = socket.emit("foo", json_payload);
    ///
    /// assert!(result.is_ok());
    /// ```
    pub fn emit<E, D>(&self, event: E, data: D) -> Result<()>
    where
        E: Into<Event>,
        D: Into<Payload>,
    {
        let client = self.client.read()?;
        // TODO(#230): like js client, buffer emit, resend after reconnect
        client.emit(event, data)
    }

    /// Sends a message to the server but `alloc`s an `ack` to check whether the
    /// server responded in a given time span. This message takes an event, which
    /// could either be one of the common events like "message" or "error" or a
    /// custom event like "foo", as well as a data parameter. But be careful,
    /// in case you send a [`Payload::String`], the string needs to be valid JSON.
    /// It's even recommended to use a library like serde_json to serialize the data properly.
    /// It also requires a timeout `Duration` in which the client needs to answer.
    /// If the ack is acked in the correct time span, the specified callback is
    /// called. The callback consumes a [`Payload`] which represents the data send
    /// by the server.
    ///
    /// # Example
    /// ```
    /// use rust_socketio::{ClientBuilder, Payload, RawClient};
    /// use serde_json::json;
    /// use std::time::Duration;
    /// use std::thread::sleep;
    ///
    /// let mut socket = ClientBuilder::new("http://localhost:4200/")
    ///     .on("foo", |payload: Payload, _| println!("Received: {:#?}", payload))
    ///     .connect()
    ///     .expect("connection failed");
    ///
    /// let ack_callback = |message: Payload, socket: RawClient| {
    ///     match message {
    ///         Payload::Text(values) => println!("{:#?}", values),
    ///         Payload::Binary(bytes) => println!("Received bytes: {:#?}", bytes),
    ///         // This is deprecated, use Payload::Text instead.
    ///         Payload::String(str) => println!("{}", str),
    ///    }
    /// };
    ///
    /// let payload = json!({"token": 123});
    /// socket.emit_with_ack("foo", payload, Duration::from_secs(2), ack_callback).unwrap();
    ///
    /// sleep(Duration::from_secs(2));
    /// ```
    pub fn emit_with_ack<F, E, D>(
        &self,
        event: E,
        data: D,
        timeout: Duration,
        callback: F,
    ) -> Result<()>
    where
        F: FnMut(Payload, RawClient) + 'static + Send,
        E: Into<Event>,
        D: Into<Payload>,
    {
        let client = self.client.read()?;
        // TODO(#230): like js client, buffer emit, resend after reconnect
        client.emit_with_ack(event, data, timeout, callback)
    }

    /// Disconnects this client from the server by sending a `socket.io` closing
    /// packet.
    /// # Example
    /// ```rust
    /// use rust_socketio::{ClientBuilder, Payload, RawClient};
    /// use serde_json::json;
    ///
    /// fn handle_test(payload: Payload, socket: RawClient) {
    ///     println!("Received: {:#?}", payload);
    ///     socket.emit("test", json!({"hello": true})).expect("Server unreachable");
    /// }
    ///
    /// let mut socket = ClientBuilder::new("http://localhost:4200/")
    ///     .on("test", handle_test)
    ///     .connect()
    ///     .expect("connection failed");
    ///
    /// let json_payload = json!({"token": 123});
    ///
    /// socket.emit("foo", json_payload);
    ///
    /// // disconnect from the server
    /// socket.disconnect();
    ///
    /// ```
    pub fn disconnect(&self) -> Result<()> {
        // A manual disconnect is final: the poll thread must neither reconnect nor keep polling.
        self.disconnected.store(true, Ordering::Release);
        let client = self.client.read()?;
        client.disconnect()
    }

    fn disconnected_manually(&self) -> bool {
        self.disconnected.load(Ordering::Acquire)
    }

    /// Returns whether a new connection was established.
    fn reconnect(&mut self) -> Result<bool> {
        let (reconnect, max_reconnect_attempts, mut backoff) = {
            let builder = self.builder.lock()?;
            (
                builder.reconnect,
                builder.max_reconnect_attempts,
                Backoff::new(
                    builder.reconnect_delay_min,
                    builder.reconnect_delay_max,
                    builder.randomization_factor,
                ),
            )
        };

        if reconnect {
            loop {
                if max_reconnect_attempts.is_some_and(|max| backoff.attempts() >= u32::from(max)) {
                    break;
                }

                std::thread::sleep(backoff.duration());

                if self.disconnected_manually() {
                    break;
                }

                if self.do_reconnect().is_ok() {
                    // disconnect() raced with this attempt: close the new connection too.
                    if self.disconnected_manually() {
                        let _ = self.client.read()?.disconnect();
                        break;
                    }
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn do_reconnect(&self) -> Result<()> {
        let builder = self.builder.lock()?;
        let new_client = builder.clone().connect_raw()?;
        let mut client = self.client.write()?;
        *client = new_client;

        Ok(())
    }

    pub(crate) fn iter(&self) -> Iter {
        Iter {
            socket: self.client.clone(),
        }
    }

    fn poll_callback(&self) {
        let mut self_clone = self.clone();
        // Use thread to consume items in iterator in order to call callbacks
        std::thread::spawn(move || {
            // tries to restart a poll cycle whenever a 'normal' error occurs,
            // it just panics on network errors, in case the poll cycle returned
            // `Result::Ok`, the server receives a close frame so it's safe to
            // terminate
            for packet in self_clone.iter() {
                if self_clone.disconnected_manually() {
                    break;
                }
                let should_reconnect = match packet {
                    Err(Error::IncompleteResponseFromEngineIo(_)) => {
                        //TODO: 0.3.X handle errors
                        //TODO: logging error
                        true
                    }
                    // The engine.io socket stopped (server Close packet, failed reconnect): every
                    // further poll returns this error at once, so either reconnect or stop.
                    Err(Error::StoppedEngineIoSocket) => match self_clone.builder.lock() {
                        Ok(builder) if builder.reconnect => true,
                        _ => break,
                    },
                    Ok(Packet {
                        packet_type: PacketId::Disconnect,
                        ..
                    }) => match self_clone.builder.lock() {
                        Ok(builder) => builder.reconnect_on_disconnect,
                        Err(_) => false,
                    },
                    _ => false,
                };
                if should_reconnect {
                    // Close the old connection without marking a manual disconnect.
                    if let Ok(client) = self_clone.client.read() {
                        let _ = client.disconnect();
                    }
                    if !matches!(self_clone.reconnect(), Ok(true)) {
                        break;
                    }
                }
            }
        });
    }
}

pub(crate) struct Iter {
    socket: Arc<RwLock<RawClient>>,
}

impl Iterator for Iter {
    type Item = Result<Packet>;

    fn next(&mut self) -> Option<Self::Item> {
        let socket = self.socket.read();
        match socket {
            Ok(socket) => match socket.poll() {
                Err(err) => Some(Err(err)),
                Ok(Some(packet)) => Some(Ok(packet)),
                // If the underlying engineIO connection is closed,
                // throw an error so we know to reconnect
                Ok(None) => Some(Err(Error::StoppedEngineIoSocket)),
            },
            Err(_) => {
                // Lock is poisoned, our iterator is useless.
                None
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::UNIX_EPOCH,
    };

    use super::*;
    use crate::error::Result;
    use crate::ClientBuilder;
    use serde_json::json;
    use serial_test::serial;
    use std::time::{Duration, SystemTime};
    use url::Url;

    /// After disconnect() the poll thread ends instead of spinning on the stopped socket.
    #[test]
    fn socket_io_poll_thread_stops_after_disconnect() {
        let socket = ClientBuilder::new(crate::test::socket_io_server())
            .connect()
            .unwrap();
        socket.disconnect().unwrap();

        for _ in 0..50 {
            // the poll thread and its iterator hold the other references
            if Arc::strong_count(&socket.client) == 1 {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("poll thread still running after disconnect");
    }

    /// disconnect() while the client is retrying after an outage stops the reconnect loop.
    #[test]
    #[serial(reconnect)]
    fn socket_io_disconnect_stops_reconnect_loop() {
        let connects = Arc::new(AtomicUsize::new(0));
        let on_connect = connects.clone();
        let socket = ClientBuilder::new(crate::test::socket_io_restart_server())
            .reconnect(true)
            .reconnect_delay(100, 100)
            .on(Event::Connect, move |_, _| {
                on_connect.fetch_add(1, Ordering::SeqCst);
            })
            .connect()
            .unwrap();
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(connects.load(Ordering::SeqCst), 1);

        // The server goes away for 2 s; the client starts retrying.
        socket.emit("restart_server", json!("")).unwrap();
        std::thread::sleep(Duration::from_millis(500));
        let _ = socket.disconnect();

        std::thread::sleep(Duration::from_secs(4));
        assert_eq!(
            connects.load(Ordering::SeqCst),
            1,
            "reconnected after disconnect()"
        );
    }

    #[test]
    #[serial(reconnect)]
    fn socket_io_reconnect_integration() -> Result<()> {
        static CONNECT_NUM: AtomicUsize = AtomicUsize::new(0);
        static CLOSE_NUM: AtomicUsize = AtomicUsize::new(0);
        static MESSAGE_NUM: AtomicUsize = AtomicUsize::new(0);

        let url = crate::test::socket_io_restart_server();

        let socket = ClientBuilder::new(url)
            .reconnect(true)
            .max_reconnect_attempts(100)
            .reconnect_delay(100, 100)
            .on(Event::Connect, move |_, socket| {
                CONNECT_NUM.fetch_add(1, Ordering::Release);
                let r = socket.emit_with_ack(
                    "message",
                    json!(""),
                    Duration::from_millis(100),
                    |_, _| {},
                );
                assert!(r.is_ok(), "should emit message success");
            })
            .on(Event::Close, move |_, _| {
                CLOSE_NUM.fetch_add(1, Ordering::Release);
            })
            .on("message", move |_, _socket| {
                // test the iterator implementation and make sure there is a constant
                // stream of packets, even when reconnecting
                MESSAGE_NUM.fetch_add(1, Ordering::Release);
            })
            .connect();

        assert!(socket.is_ok(), "should connect success");
        let socket = socket.unwrap();

        // waiting for server to emit message
        std::thread::sleep(std::time::Duration::from_millis(500));

        assert_eq!(load(&CONNECT_NUM), 1, "should connect once");
        assert_eq!(load(&MESSAGE_NUM), 1, "should receive one");
        assert_eq!(load(&CLOSE_NUM), 0, "should not close");

        let r = socket.emit("restart_server", json!(""));
        assert!(r.is_ok(), "should emit restart success");

        // waiting for server to restart
        for _ in 0..10 {
            std::thread::sleep(std::time::Duration::from_millis(400));
            if load(&CONNECT_NUM) == 2 && load(&MESSAGE_NUM) == 2 {
                break;
            }
        }

        assert_eq!(load(&CONNECT_NUM), 2, "should connect twice");
        assert_eq!(load(&MESSAGE_NUM), 2, "should receive two messages");
        assert_eq!(load(&CLOSE_NUM), 1, "should close once");

        socket.disconnect()?;
        Ok(())
    }

    #[test]
    fn socket_io_reconnect_url_auth_integration() -> Result<()> {
        static CONNECT_NUM: AtomicUsize = AtomicUsize::new(0);
        static CLOSE_NUM: AtomicUsize = AtomicUsize::new(0);
        static MESSAGE_NUM: AtomicUsize = AtomicUsize::new(0);

        fn get_url() -> Url {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let mut url = crate::test::socket_io_restart_url_auth_server();
            url.set_query(Some(&format!("timestamp={timestamp}")));
            url
        }

        let socket = ClientBuilder::new(get_url())
            .reconnect(true)
            .max_reconnect_attempts(100)
            .reconnect_delay(100, 100)
            .on(Event::Connect, move |_, socket| {
                CONNECT_NUM.fetch_add(1, Ordering::Release);
                let result = socket.emit_with_ack(
                    "message",
                    json!(""),
                    Duration::from_millis(100),
                    |_, _| {},
                );
                assert!(result.is_ok(), "should emit message success");
            })
            .on(Event::Close, move |_, _| {
                CLOSE_NUM.fetch_add(1, Ordering::Release);
            })
            .on("message", move |_, _| {
                // test the iterator implementation and make sure there is a constant
                // stream of packets, even when reconnecting
                MESSAGE_NUM.fetch_add(1, Ordering::Release);
            })
            .connect();

        assert!(socket.is_ok(), "should connect success");
        let socket = socket.unwrap();

        // waiting for server to emit message
        std::thread::sleep(std::time::Duration::from_millis(500));

        assert_eq!(load(&CONNECT_NUM), 1, "should connect once");
        assert_eq!(load(&MESSAGE_NUM), 1, "should receive one");
        assert_eq!(load(&CLOSE_NUM), 0, "should not close");

        // waiting for timestamp in url to expire
        std::thread::sleep(std::time::Duration::from_secs(1));

        socket.set_reconnect_url(get_url())?;

        let result = socket.emit("restart_server", json!(""));
        assert!(result.is_ok(), "should emit restart success");

        // waiting for server to restart
        for _ in 0..10 {
            std::thread::sleep(std::time::Duration::from_millis(400));
            if load(&CONNECT_NUM) == 2 && load(&MESSAGE_NUM) == 2 {
                break;
            }
        }

        assert_eq!(load(&CONNECT_NUM), 2, "should connect twice");
        assert_eq!(load(&MESSAGE_NUM), 2, "should receive two messages");
        assert_eq!(load(&CLOSE_NUM), 1, "should close once");

        socket.disconnect()?;
        Ok(())
    }

    #[test]
    fn socket_io_iterator_integration() -> Result<()> {
        let url = crate::test::socket_io_server();
        let builder = ClientBuilder::new(url);
        let builder_clone = builder.clone();

        let client = Arc::new(RwLock::new(builder_clone.connect_raw()?));
        let mut socket = Client {
            builder: Arc::new(Mutex::new(builder)),
            client,
            disconnected: Default::default(),
        };
        let socket_clone = socket.clone();

        let packets: Arc<RwLock<Vec<Packet>>> = Default::default();
        let packets_clone = packets.clone();

        std::thread::spawn(move || {
            for packet in socket_clone.iter() {
                {
                    let mut packets = packets_clone.write().unwrap();
                    if let Ok(packet) = packet {
                        (*packets).push(packet);
                    }
                }
            }
        });

        // waiting for client to emit messages
        std::thread::sleep(Duration::from_millis(100));
        let lock = packets.read().unwrap();
        let pre_num = lock.len();
        drop(lock);

        // Drop the connection without a manual disconnect(), which is final.
        let _ = socket.client.read().unwrap().disconnect();
        socket.reconnect()?;

        // waiting for client to emit messages
        std::thread::sleep(Duration::from_millis(100));

        let lock = packets.read().unwrap();
        let post_num = lock.len();
        drop(lock);

        assert!(
            pre_num < post_num,
            "pre_num {} should less than post_num {}",
            pre_num,
            post_num
        );

        Ok(())
    }

    fn load(num: &AtomicUsize) -> usize {
        num.load(Ordering::Acquire)
    }
}
