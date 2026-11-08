use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, Stream, StreamExt, ready};
use http::HeaderMap;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::engineio::error::Result;
use crate::engineio::tls::{self, TlsConfig};
use crate::engineio::transport::Frame;
use crate::engineio::{Error, Packet, PacketId};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// An asynchronous websocket transport, `ws://` or `wss://` after the URL scheme
/// (`http`/`ws` -> `ws`, `https`/`wss` -> `wss`).
#[derive(Clone)]
pub struct WebsocketTransport {
    sender: Arc<Mutex<SplitSink<WsStream, Message>>>,
    /// A std mutex held only for one synchronous poll: a tokio lock future polled once per
    /// `poll_next` and dropped while Pending would lose its waker (the reader would hang).
    receiver: Arc<StdMutex<SplitStream<WsStream>>>,
    base_url: Arc<RwLock<Url>>,
    secure: bool,
}

fn ws_scheme(secure: bool) -> &'static str {
    if secure { "wss" } else { "ws" }
}

impl WebsocketTransport {
    /// Opens the websocket. `tls_config` is used for `wss` (default: the backend's default roots).
    pub async fn new(
        base_url: Url,
        tls_config: Option<TlsConfig>,
        headers: Option<HeaderMap>,
    ) -> Result<Self> {
        Self::connect(base_url, tls_config, headers, None).await
    }

    /// Like [`new`](Self::new), through the HTTP proxy `route` (a `CONNECT` tunnel) if given.
    pub(crate) async fn connect(
        base_url: Url,
        tls_config: Option<TlsConfig>,
        headers: Option<HeaderMap>,
        route: Option<crate::engineio::http::Route>,
    ) -> Result<Self> {
        tls::check_scheme(&base_url)?;
        let secure = matches!(base_url.scheme(), "https" | "wss");
        let mut url = base_url;
        url.query_pairs_mut().append_pair("transport", "websocket");
        url.set_scheme(ws_scheme(secure))
            .map_err(|()| Error::InvalidUrlScheme(url.scheme().to_owned()))?;

        let mut req = url.as_str().into_client_request()?;
        if let Some(map) = headers {
            req.headers_mut().extend(map);
        }
        // TCP_NODELAY like the Node `ws` client: engine.io packets are small and latency-bound.
        let tcp = match &route {
            Some(route) => crate::engineio::http::tunnel(route, &url).await?,
            None => {
                let host = crate::engineio::http::connect_host(&url)?;
                let port = url.port_or_known_default().unwrap_or(80);
                let tcp = TcpStream::connect((host.as_str(), port))
                    .await
                    .map_err(|e| Error::from(tokio_tungstenite::tungstenite::Error::Io(e)))?;
                tcp.set_nodelay(true).ok();
                tcp
            }
        };
        #[cfg(any(feature = "rustls", feature = "native-tls"))]
        let (ws_stream, _) = {
            let connector = if secure {
                tls::ws_connector(tls_config.as_ref())
            } else {
                None
            };
            tokio_tungstenite::client_async_tls_with_config(req, tcp, None, connector).await?
        };
        #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
        let (ws_stream, _) = {
            let _ = tls_config;
            tokio_tungstenite::client_async_with_config(req, MaybeTlsStream::Plain(tcp), None)
                .await?
        };
        let (sender, receiver) = ws_stream.split();

        Ok(WebsocketTransport {
            sender: Arc::new(Mutex::new(sender)),
            receiver: Arc::new(StdMutex::new(receiver)),
            base_url: Arc::new(RwLock::new(url)),
            secure,
        })
    }

    /// Upgrade probe: `2probe`, expect `3probe`, then `5` (engine.io protocol v4).
    pub(crate) async fn upgrade(&self) -> Result<()> {
        let mut sender = self.sender.lock().await;

        sender
            .send(Message::text(Packet::text(PacketId::Ping, "probe")))
            .await?;

        // Skip control frames; the first data frame must be the probe answer.
        let answer = loop {
            match self.next_message().await.ok_or(Error::UpgradeFailed(
                "the connection closed during the probe",
            ))?? {
                Message::Text(text) => break Bytes::from(text),
                Message::Binary(data) => break data,
                Message::Close(frame) => {
                    return Err(closed(frame).unwrap_or(Error::UpgradeFailed(
                        "the server closed the websocket during the probe",
                    )));
                }
                _ => continue,
            }
        };
        if answer != Bytes::from(Packet::new(PacketId::Pong, Bytes::from_static(b"probe"))) {
            return Err(Error::UpgradeFailed("the server did not answer 3probe"));
        }

        sender
            .send(Message::text(Packet::text(PacketId::Upgrade, "")))
            .await?;

        Ok(())
    }

    fn poll_message(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>> {
        self.receiver
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .poll_next_unpin(cx)
    }

    async fn next_message(
        &self,
    ) -> Option<std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> {
        futures_util::future::poll_fn(|cx| self.poll_message(cx)).await
    }

    /// A websocket ping; the server's pong arrives as an engine.io noop frame.
    pub(crate) async fn ping(&self) -> Result<()> {
        self.sender
            .lock()
            .await
            .send(Message::Ping(Bytes::new()))
            .await?;
        Ok(())
    }

    pub(crate) async fn emit(&self, data: Bytes, is_binary_att: bool) -> Result<()> {
        let message = if is_binary_att {
            Message::binary(data)
        } else {
            Message::text(Utf8Bytes::try_from(data)?)
        };
        self.sender.lock().await.send(message).await?;
        Ok(())
    }

    /// Closes the websocket (close frame). engine.io-client sends no close packet over a
    /// websocket; the server sees the socket close.
    pub(crate) async fn close(&self) -> Result<()> {
        let mut sender = self.sender.lock().await;
        sender.send(Message::Close(None)).await?;
        sender.close().await?;
        Ok(())
    }

    pub async fn base_url(&self) -> Result<Url> {
        Ok(self.base_url.read().await.clone())
    }

    pub async fn set_base_url(&self, base_url: Url) -> Result<()> {
        let mut url = base_url;
        if !url
            .query_pairs()
            .any(|(k, v)| k == "transport" && v == "websocket")
        {
            url.query_pairs_mut().append_pair("transport", "websocket");
        }
        url.set_scheme(ws_scheme(self.secure))
            .map_err(|()| Error::InvalidUrlScheme(url.scheme().to_owned()))?;
        *self.base_url.write().await = url;
        Ok(())
    }
}

/// A close frame with a code is an error that says why; without one the stream just ends.
fn closed(frame: Option<tokio_tungstenite::tungstenite::protocol::CloseFrame>) -> Option<Error> {
    frame.map(|f| Error::WebsocketClosed {
        code: u16::from(f.code),
        reason: f.reason.to_string(),
    })
}

/// A text frame is an encoded engine.io packet; a binary frame is the raw data of one binary
/// message. Control frames carry nothing for engine.io.
fn frame_to_packet_bytes(message: Message) -> Option<Frame> {
    match message {
        Message::Text(text) => Some(Frame::Text(Bytes::from(text))),
        Message::Binary(data) => Some(Frame::Binary(data)),
        // a pong (to the liveness probe) is a sign of life: an engine.io noop
        Message::Pong(_) => Some(Frame::Text(Bytes::from_static(b"6"))),
        _ => None,
    }
}

impl Stream for WebsocketTransport {
    type Item = Result<Frame>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(self.poll_message(cx)) {
                Some(Ok(Message::Close(frame))) => return Poll::Ready(closed(frame).map(Err)),
                Some(Ok(message)) => {
                    if let Some(frame) = frame_to_packet_bytes(message) {
                        return Poll::Ready(Some(Ok(frame)));
                    }
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
                None => return Poll::Ready(None),
            }
        }
    }
}

impl Debug for WebsocketTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncWebsocketTransport")
            .field(
                "base_url",
                &self
                    .base_url
                    .try_read()
                    .map_or("Currently not available".to_owned(), |url| {
                        crate::redact::url(&url, true)
                    }),
            )
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::engineio::ENGINE_IO_VERSION;
    use std::str::FromStr;
    use std::time::Duration;

    async fn new() -> Result<WebsocketTransport> {
        let url = crate::engineio::test::engine_io_server()?.to_string()
            + "engine.io/?EIO="
            + &ENGINE_IO_VERSION.to_string();
        WebsocketTransport::new(Url::from_str(&url[..])?, None, None).await
    }

    #[test]
    fn frames_map_to_packets() {
        assert_eq!(
            frame_to_packet_bytes(Message::text("4hi")),
            Some(Frame::Text(Bytes::from_static(b"4hi")))
        );
        assert_eq!(
            frame_to_packet_bytes(Message::binary(vec![1u8, 2])),
            Some(Frame::Binary(Bytes::from_static(&[1, 2])))
        );
        // an empty binary message is valid
        assert_eq!(
            frame_to_packet_bytes(Message::binary(Vec::<u8>::new())),
            Some(Frame::Binary(Bytes::new()))
        );
        assert_eq!(frame_to_packet_bytes(Message::Ping(Bytes::new())), None);
    }

    /// A one-shot websocket server on localhost that runs `serve` on the accepted socket.
    async fn ws_server<F, Fut>(serve: F) -> Url
    where
        F: FnOnce(WebSocketStream<TcpStream>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            serve(tokio_tungstenite::accept_async(tcp).await.unwrap()).await;
        });
        Url::parse(&format!("http://{addr}/engine.io/?EIO=4")).unwrap()
    }

    fn close_frame() -> Message {
        use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};
        Message::Close(Some(CloseFrame {
            code: CloseCode::from(4001),
            reason: "kicked".into(),
        }))
    }

    /// A refused handshake (here a redirect) is `HttpStatus` with the status only: the
    /// response, with its `Location` and query, is not kept.
    #[tokio::test]
    async fn a_refused_handshake_keeps_only_the_status() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = tcp.read(&mut buf).await;
            let _ = tcp
                .write_all(b"HTTP/1.1 302 Found\r\nLocation: http://evil.example/?token=SECRET\r\nContent-Length: 0\r\n\r\n")
                .await;
        });
        let url = Url::parse(&format!("http://{addr}/engine.io/?EIO=4")).unwrap();
        let err = WebsocketTransport::new(url, None, None).await.unwrap_err();
        assert!(
            matches!(
                err,
                Error::HttpStatus {
                    status: 302,
                    message: None,
                    ..
                }
            ),
            "{err:?}"
        );
        let text = format!("{err} {err:?}");
        assert!(!text.contains("SECRET") && !text.contains("evil"), "{text}");
    })
    .await
    .expect("test `a_refused_handshake_keeps_only_the_status` timed out")
    }

    #[tokio::test]
    async fn a_close_frame_is_an_error_with_code_and_reason() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = ws_server(|mut ws| async move {
                ws.send(close_frame()).await.unwrap();
                let _ = ws.next().await;
            })
            .await;
            let mut transport = WebsocketTransport::new(url, None, None).await.unwrap();
            match transport.next().await {
                Some(Err(Error::WebsocketClosed { code: 4001, reason })) => {
                    assert_eq!(reason, "kicked")
                }
                other => panic!("{other:?}"),
            }
        })
        .await
        .expect("test `a_close_frame_is_an_error_with_code_and_reason` timed out")
    }

    #[tokio::test]
    async fn a_close_frame_during_the_probe_says_why() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = ws_server(|mut ws| async move {
                let _ = ws.next().await; // 2probe
                ws.send(close_frame()).await.unwrap();
                let _ = ws.next().await;
            })
            .await;
            let transport = WebsocketTransport::new(url, None, None).await.unwrap();
            assert!(matches!(
                transport.upgrade().await,
                Err(Error::WebsocketClosed { code: 4001, .. })
            ));
        })
        .await
        .expect("test `a_close_frame_during_the_probe_says_why` timed out")
    }

    /// Item: the reader must be woken after another holder released the receiver (a tokio
    /// lock future polled once and dropped lost its waker).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reader_is_not_lost_behind_a_held_receiver() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = ws_server(|mut ws| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                ws.send(Message::text("4hello")).await.unwrap();
                let _ = ws.next().await;
            })
            .await;
            let mut transport = WebsocketTransport::new(url, None, None).await.unwrap();
            let receiver = Arc::clone(&transport.receiver);
            let holder = std::thread::spawn(move || {
                let _guard = receiver.lock().unwrap();
                std::thread::sleep(Duration::from_millis(200));
            });
            std::thread::sleep(Duration::from_millis(20));
            let frame = tokio::time::timeout(Duration::from_secs(5), transport.next()).await;
            holder.join().unwrap();
            assert!(matches!(frame, Ok(Some(Ok(Frame::Text(_))))), "{frame:?}");
        })
        .await
        .expect("test `a_reader_is_not_lost_behind_a_held_receiver` timed out")
    }

    #[tokio::test]
    async fn websocket_transport_base_url() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let transport = new().await?;
            let mut url = crate::engineio::test::engine_io_server()?;
            url.set_path("/engine.io/");
            url.query_pairs_mut()
                .append_pair("EIO", &ENGINE_IO_VERSION.to_string())
                .append_pair("transport", "websocket");
            url.set_scheme("ws").unwrap();
            assert_eq!(transport.base_url().await?.to_string(), url.to_string());
            transport
                .set_base_url(Url::parse("https://127.0.0.1")?)
                .await?;
            assert_eq!(
                transport.base_url().await?.to_string(),
                "ws://127.0.0.1/?transport=websocket"
            );
            transport
                .set_base_url(Url::parse("http://127.0.0.1/?transport=websocket")?)
                .await?;
            assert_eq!(
                transport.base_url().await?.to_string(),
                "ws://127.0.0.1/?transport=websocket"
            );
            Ok(())
        })
        .await
        .expect("test `websocket_transport_base_url` timed out")
    }

    #[tokio::test]
    async fn websocket_debug_and_frames() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let mut transport = new().await?;
            assert_eq!(
                format!("{:?}", transport),
                format!(
                    "AsyncWebsocketTransport {{ base_url: {:?} }}",
                    crate::redact::url(&transport.base_url().await?, true)
                )
            );
            // The handshake frame, then the server's greeting message.
            assert!(
                matches!(transport.next().await.unwrap()?, Frame::Text(t) if t.starts_with(b"0{"))
            );
            transport.next().await.unwrap()?;
            Ok(())
        })
        .await
        .expect("test `websocket_debug_and_frames` timed out")
    }

    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    #[tokio::test]
    async fn websocket_secure_transport() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let url = crate::engineio::test::engine_io_server_secure()?.to_string()
                + "engine.io/?EIO="
                + &ENGINE_IO_VERSION.to_string();
            let mut transport = WebsocketTransport::new(
                Url::from_str(&url)?,
                Some(crate::engineio::test::tls_config()?),
                None,
            )
            .await?;
            let base = transport.base_url().await?;
            assert_eq!(base.scheme(), "wss");
            transport
                .set_base_url(Url::parse("http://127.0.0.1/?transport=websocket")?)
                .await?;
            assert_eq!(
                transport.base_url().await?.to_string(),
                "wss://127.0.0.1/?transport=websocket"
            );
            assert!(
                matches!(transport.next().await.unwrap()?, Frame::Text(t) if t.starts_with(b"0{"))
            );
            Ok(())
        })
        .await
        .expect("test `websocket_secure_transport` timed out")
    }

    #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
    #[tokio::test]
    async fn wss_without_tls_is_refused() {
        tokio::time::timeout(std::time::Duration::from_secs(90), async move {
            let result = WebsocketTransport::new(
                Url::parse("https://localhost:4202/engine.io/").unwrap(),
                None,
                None,
            )
            .await;
            assert!(matches!(result, Err(Error::TlsNotEnabled(_))));
        })
        .await
        .expect("test `wss_without_tls_is_refused` timed out")
    }
}
