use std::fmt::Debug;
use std::pin::Pin;

use futures_util::{Stream, StreamExt};
use tokio::time::Instant;

use crate::engineio::error::Result;
use crate::engineio::generator::{StreamGenerator, end_after_error};
use crate::engineio::socket::Socket as InnerSocket;
use crate::engineio::{HandshakePacket, Packet, TransportKind};

/// An engine.io client: a [`Stream`] of received packets plus `emit`.
///
/// Clones share one session (no need for an `Arc`): emit from any clone, but read the stream
/// from one task at a time; two tasks polling concurrently would steal each other's wakeups.
#[derive(Clone)]
pub struct Client {
    socket: InnerSocket,
    generator: StreamGenerator<Packet>,
}

impl Client {
    pub(crate) fn new(socket: InnerSocket) -> Self {
        Client {
            socket: socket.clone(),
            generator: StreamGenerator::new(Self::stream(socket)),
        }
    }

    #[cfg(test)]
    pub(crate) fn socket(&self) -> &InnerSocket {
        &self.socket
    }

    /// Closes the session (same as [`disconnect`](Self::disconnect)).
    pub async fn close(&self) -> Result<()> {
        self.socket.disconnect().await
    }

    /// Starts the session after `build*`: the heartbeat deadline runs from here.
    pub async fn connect(&self) -> Result<()> {
        self.socket.connect();
        Ok(())
    }

    /// Sends a close packet (best effort) and closes the session; the stream then ends.
    pub async fn disconnect(&self) -> Result<()> {
        self.socket.disconnect().await
    }

    /// Sends one packet.
    pub async fn emit(&self, packet: Packet) -> Result<()> {
        self.socket.emit(packet).await
    }

    /// Sends several packets in order: batched into as few polling requests as `maxPayload`
    /// allows, or one websocket frame each.
    pub async fn emit_all(&self, packets: Vec<Packet>) -> Result<()> {
        self.socket.emit_all(packets).await
    }

    /// Whether the session is open.
    pub fn is_connected(&self) -> bool {
        self.socket.is_connected()
    }

    /// The server's handshake: sid, upgrades, ping interval / timeout, max payload.
    pub fn handshake(&self) -> &HandshakePacket {
        self.socket.handshake()
    }

    /// The transport of this session.
    pub fn transport(&self) -> TransportKind {
        self.socket.transport().kind()
    }

    /// When the last ping arrived (the session start before the first one).
    pub fn last_ping(&self) -> Instant {
        self.socket.last_ping()
    }

    fn stream(socket: InnerSocket) -> Pin<Box<dyn Stream<Item = Result<Packet>> + 'static + Send>> {
        // Each packet is handled (pong, close, callbacks) before it is passed on; a failed
        // handling ends the stream like a transport error.
        let handled = socket.as_stream().then(move |item| {
            let socket = socket.clone();
            async move {
                let packet: Packet = item?;
                socket.handle_incoming_packet(packet.clone()).await?;
                Ok(packet)
            }
        });
        end_after_error(handled.boxed()).boxed()
    }
}

impl Stream for Client {
    type Item = Result<Packet>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.generator.poll_next_unpin(cx)
    }
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("socket", &self.socket)
            .finish()
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::engineio::{ClientBuilder, DEFAULT_TIMEOUT as HANDSHAKE_TIMEOUT, Error, PacketId};
    use bytes::Bytes;
    use futures_util::StreamExt;
    use http::HeaderMap;
    use url::Url;

    /// The purpose of this test is to check whether the Client is properly cloneable or not.
    /// As the documentation of the engine.io client states, the object needs to maintain it's internal
    /// state when cloned and the cloned object should reflect the same state throughout the lifetime
    /// of both objects (initial and cloned).
    /// Reads until `deadline`; `Some(err)` for an error, `None` when it stayed healthy.
    async fn watch(
        socket: &mut Client,
        deadline: std::time::Duration,
    ) -> Option<crate::engineio::Error> {
        let end = tokio::time::Instant::now() + deadline;
        loop {
            match tokio::time::timeout_at(end, socket.next()).await {
                Err(_) => return None,
                Ok(Some(Err(err))) => return Some(err),
                Ok(Some(Ok(_))) => continue,
                Ok(None) => return Some(crate::engineio::Error::NotConnected),
            }
        }
    }

    /// A system sleep on a dead link: the monotonic timer has most of pingInterval +
    /// pingTimeout (45 s here) to go, the wall clock says the deadline passed. The probe gets
    /// no answer and the link is declared dead after pingTimeout (20 s), not 45 s.
    #[tokio::test]
    async fn a_sleep_on_a_dead_link_is_a_ping_timeout_after_ping_timeout() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let tcp = socketio_next_testing::Proxy::start(&socketio_next_testing::host_port(
                crate::engineio::test::engine_io_server()?.as_str(),
            ))
            .await;
            let mut socket = builder(Url::parse(&tcp.url())?).build_polling().await?;
            socket.connect().await?;
            // the link is dead: open connections stall, new ones are refused
            tcp.refuse(true);
            tcp.freeze();
            let started = std::time::Instant::now();
            socket
                .socket
                .shift_wall_clock(std::time::Duration::from_secs(3600), true);
            let err = watch(&mut socket, std::time::Duration::from_secs(30)).await;
            assert!(
                matches!(err, Some(crate::engineio::Error::PingTimeout)),
                "{err:?}"
            );
            let took = started.elapsed();
            assert!(
                took >= std::time::Duration::from_secs(19),
                "probed first: {took:?}"
            );
            assert!(took < std::time::Duration::from_secs(25), "{took:?}");
            tcp.cut();
            Ok(())
        })
        .await
        .expect("test `a_sleep_on_a_dead_link_is_a_ping_timeout_after_ping_timeout` timed out")
    }

    /// A forward clock step (NTP on a gateway without RTC) on a live link: the probe is
    /// answered (a noop POST, or a websocket ping), nothing closes. On the fast-ping server
    /// (pingInterval 300 ms, pingTimeout 200 ms), stepped right after a ping: the next ping is
    /// ~300 ms away, so without the probe the 200 ms grace would end the link.
    #[tokio::test]
    async fn a_forward_clock_step_on_a_live_link_keeps_it() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let mut url = Url::parse(&socketio_next_testing::fast_ping_server())?;
            url.set_path("/socket.io/");
            for websocket in [false, true] {
                let mut socket = if websocket {
                    builder(url.clone()).build_websocket().await?
                } else {
                    builder(url.clone()).build_polling().await?
                };
                socket.connect().await?;
                // a ping arrived: the clock is stepped right after it
                loop {
                    let packet =
                        tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
                            .await
                            .expect("a ping")
                            .expect("open")?;
                    if packet.packet_id == crate::engineio::PacketId::Ping {
                        break;
                    }
                }
                socket
                    .socket
                    .shift_wall_clock(std::time::Duration::from_secs(3600), true);
                // longer than pingTimeout (200), shorter than pingInterval (300)
                let err = watch(&mut socket, std::time::Duration::from_millis(260)).await;
                assert!(err.is_none(), "websocket={websocket}: {err:?}");
                assert!(socket.is_connected());
                socket.disconnect().await?;
            }
            Ok(())
        })
        .await
        .expect("test `a_forward_clock_step_on_a_live_link_keeps_it` timed out")
    }

    /// A backward step changes nothing.
    #[tokio::test]
    async fn a_backward_clock_step_has_no_effect() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_server()?;
            let mut socket = builder(url).build_polling().await?;
            socket.connect().await?;
            socket
                .socket
                .shift_wall_clock(std::time::Duration::from_secs(3600), false);
            assert!(
                watch(&mut socket, std::time::Duration::from_secs(1))
                    .await
                    .is_none()
            );
            socket.disconnect().await?;
            Ok(())
        })
        .await
        .expect("test `a_backward_clock_step_has_no_effect` timed out")
    }

    #[tokio::test]
    async fn test_client_cloneable() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_server()?;

            let mut sut = builder(url).build().await?;
            let mut cloned = sut.clone();

            sut.connect().await?;

            // when the underlying socket is connected, the
            // state should also change on the cloned one
            assert!(sut.is_connected());
            assert!(cloned.is_connected());

            // both clients should reflect the same messages.
            assert_eq!(
                sut.next().await.unwrap()?,
                Packet::new(PacketId::Message, "hello client")
            );

            sut.emit(Packet::new(PacketId::Message, "respond")).await?;

            assert_eq!(
                cloned.next().await.unwrap()?,
                Packet::new(PacketId::Message, "Roger Roger")
            );

            cloned.disconnect().await?;

            // when the underlying socket is disconnected, the
            // state should also change on the cloned one
            assert!(!sut.is_connected());
            assert!(!cloned.is_connected());

            Ok(())
        })
        .await
        .expect("test `test_client_cloneable` timed out")
    }

    /// A polling response bigger than one HTTP body chunk must still be decoded as one payload.
    #[tokio::test]
    async fn test_polling_large_message() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_polling_server()?;
            let mut socket = builder(url).build_polling().await?;
            socket.connect().await?;

            assert_eq!(
                socket.next().await.unwrap()?,
                Packet::new(PacketId::Message, "hello client")
            );

            socket
                .emit(Packet::new(PacketId::Message, "respond_large"))
                .await?;

            assert_eq!(
                socket.next().await.unwrap()?,
                Packet::new(PacketId::Message, "x".repeat(64 * 1024))
            );

            socket.disconnect().await
        })
        .await
        .expect("test `test_polling_large_message` timed out")
    }

    /// A peer that accepts TCP but never answers (blackholed proxy, half-open link) must not hang
    /// the polling handshake forever.
    #[tokio::test(start_paused = true)]
    async fn test_polling_handshake_times_out() {
        let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = Url::parse(&format!("http://{}/", silent.local_addr().unwrap())).unwrap();

        let result = tokio::time::timeout(
            HANDSHAKE_TIMEOUT * 2,
            ClientBuilder::new(url).build_polling(),
        )
        .await
        .expect("polling handshake hung past its timeout");
        assert!(result.is_err());
    }

    /// After disconnect() the packet stream ends instead of polling a closed session.
    #[tokio::test]
    async fn test_stream_ends_after_disconnect() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            for url in [
                crate::engineio::test::engine_io_polling_server().unwrap(),
                crate::engineio::test::engine_io_server().unwrap(),
            ] {
                let mut socket = builder(url).build().await.unwrap();
                socket.connect().await.unwrap();
                socket.disconnect().await.unwrap();
                assert!(!socket.is_connected());

                let next = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
                    .await
                    .expect("stream still waiting after disconnect");
                assert!(next.is_none(), "stream yielded {next:?} after disconnect");
            }
        })
        .await
        .expect("test `test_stream_ends_after_disconnect` timed out")
    }

    #[tokio::test(start_paused = true)]
    async fn test_websocket_connect_times_out() {
        let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = Url::parse(&format!("http://{}/", silent.local_addr().unwrap())).unwrap();

        let result = tokio::time::timeout(
            HANDSHAKE_TIMEOUT * 2,
            ClientBuilder::new(url).build_websocket(),
        )
        .await
        .expect("websocket connect hung past its timeout");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_illegal_actions() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_server()?;
            let mut sut = builder(url.clone()).build().await?;

            assert!(
                sut.emit(Packet::new(PacketId::Close, Bytes::new()))
                    .await
                    .is_err()
            );

            sut.connect().await?;

            assert!(sut.next().await.unwrap().is_ok());

            assert!(
                builder(Url::parse("fake://fake.fake").unwrap())
                    .build_websocket()
                    .await
                    .is_err()
            );

            sut.disconnect().await?;

            Ok(())
        })
        .await
        .expect("test `test_illegal_actions` timed out")
    }

    use reqwest::header::HOST;

    fn builder(url: Url) -> ClientBuilder {
        ClientBuilder::new(url)
            .on_open(|_| async {
                println!("Open event!");
            })
            .on_packet(|packet| async move {
                println!("Received packet: {:?}", packet);
            })
            .on_data(|data| async move {
                println!("Received data: {:?}", std::str::from_utf8(&data));
            })
            .on_close(|_| async {
                println!("Close event!");
            })
            .on_error(|error| async move {
                println!("Error {}", error);
            })
    }

    async fn test_connection(socket: Client) -> Result<()> {
        let mut socket = socket;

        socket.connect().await.unwrap();

        assert_eq!(
            socket.next().await.unwrap()?,
            Packet::new(PacketId::Message, "hello client")
        );
        println!("received msg, about to send");

        socket
            .emit(Packet::new(PacketId::Message, "respond"))
            .await?;

        println!("send msg");

        assert_eq!(
            socket.next().await.unwrap()?,
            Packet::new(PacketId::Message, "Roger Roger")
        );
        println!("received 2");

        socket.close().await
    }

    #[tokio::test]
    async fn test_connection_long() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            // Long lived socket to receive pings
            let url = crate::engineio::test::engine_io_server()?;
            let mut socket = builder(url).build().await?;

            socket.connect().await?;

            // hello client
            assert!(matches!(
                socket.next().await.unwrap()?,
                Packet {
                    packet_id: PacketId::Message,
                    ..
                }
            ));
            // Ping
            assert!(matches!(
                socket.next().await.unwrap()?,
                Packet {
                    packet_id: PacketId::Ping,
                    ..
                }
            ));

            socket.disconnect().await?;

            assert!(!socket.is_connected());

            Ok(())
        })
        .await
        .expect("test `test_connection_long` timed out")
    }

    #[tokio::test]
    async fn test_connection_dynamic() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_server()?;
            let socket = builder(url).build().await?;
            test_connection(socket).await?;

            let url = crate::engineio::test::engine_io_polling_server()?;
            let socket = builder(url).build().await?;
            test_connection(socket).await
        })
        .await
        .expect("test `test_connection_dynamic` timed out")
    }

    #[tokio::test]
    async fn test_connection_fallback() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_server()?;
            let socket = builder(url).build_with_fallback().await?;
            test_connection(socket).await?;

            let url = crate::engineio::test::engine_io_polling_server()?;
            let socket = builder(url).build_with_fallback().await?;
            test_connection(socket).await
        })
        .await
        .expect("test `test_connection_fallback` timed out")
    }

    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    #[tokio::test]
    async fn test_connection_dynamic_secure() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_server_secure()?;
            let mut socket_builder = builder(url);
            socket_builder = socket_builder.tls_config(crate::engineio::test::tls_config()?);
            let socket = socket_builder.build().await?;
            test_connection(socket).await
        })
        .await
        .expect("test `test_connection_dynamic_secure` timed out")
    }

    #[tokio::test]
    async fn test_connection_polling() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_server()?;
            let socket = builder(url).build_polling().await?;
            test_connection(socket).await
        })
        .await
        .expect("test `test_connection_polling` timed out")
    }

    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    #[tokio::test]
    async fn test_connection_wss() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_polling_server()?;
            assert!(builder(url).build_websocket_with_upgrade().await.is_err());

            let host =
                std::env::var("ENGINE_IO_SECURE_HOST").unwrap_or_else(|_| "localhost".to_owned());
            let mut url = crate::engineio::test::engine_io_server_secure()?;

            let mut headers = HeaderMap::default();
            headers.insert(HOST, host.parse().unwrap());
            let mut builder = builder(url.clone());

            builder = builder.tls_config(crate::engineio::test::tls_config()?);
            builder = builder.headers(headers.clone());
            let socket = builder.clone().build_websocket_with_upgrade().await?;

            test_connection(socket).await?;

            let socket = builder.build_websocket().await?;

            test_connection(socket).await?;

            url.set_scheme("wss").unwrap();

            let builder = self::builder(url)
                .tls_config(crate::engineio::test::tls_config()?)
                .headers(headers);
            let socket = builder.clone().build_websocket().await?;

            test_connection(socket).await?;

            assert!(builder.build_websocket_with_upgrade().await.is_err());

            Ok(())
        })
        .await
        .expect("test `test_connection_wss` timed out")
    }

    #[tokio::test]
    async fn test_connection_ws() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_polling_server()?;
            assert!(builder(url.clone()).build_websocket().await.is_err());
            assert!(builder(url).build_websocket_with_upgrade().await.is_err());

            let mut url = crate::engineio::test::engine_io_server()?;

            let builder = builder(url.clone());
            let socket = builder.clone().build_websocket().await?;
            test_connection(socket).await?;

            let socket = builder.build_websocket_with_upgrade().await?;
            test_connection(socket).await?;

            url.set_scheme("ws").unwrap();

            let builder = self::builder(url);
            let socket = builder.clone().build_websocket().await?;

            test_connection(socket).await?;

            assert!(builder.build_websocket_with_upgrade().await.is_err());

            Ok(())
        })
        .await
        .expect("test `test_connection_ws` timed out")
    }

    #[tokio::test]
    async fn test_open_invariants() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_server()?;
            let illegal_url = "this is illegal";

            assert!(Url::parse(illegal_url).is_err());

            let invalid_protocol = "file:///tmp/foo";
            assert!(
                builder(Url::parse(invalid_protocol).unwrap())
                    .build()
                    .await
                    .is_err()
            );

            let sut = builder(url.clone()).build().await?;
            let _error = sut
                .emit(Packet::new(PacketId::Close, Bytes::new()))
                .await
                .expect_err("error");
            assert!(matches!(_error, Error::NotConnected));

            // test missing match arm in socket constructor
            let mut headers = HeaderMap::default();
            let host =
                std::env::var("ENGINE_IO_SECURE_HOST").unwrap_or_else(|_| "localhost".to_owned());
            headers.insert(HOST, host.parse().unwrap());

            let _ = builder(url.clone()).build().await?;
            let _ = builder(url).headers(headers).build().await?;
            Ok(())
        })
        .await
        .expect("test `test_open_invariants` timed out")
    }
}
