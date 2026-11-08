use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{ready, FutureExt, SinkExt, Stream, StreamExt};
use http::HeaderMap;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::error::Result;
use crate::tls::{self, TlsConfig};
use crate::{Error, Packet, PacketId};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// An asynchronous websocket transport, `ws://` or `wss://` after the URL scheme
/// (`http`/`ws` -> `ws`, `https`/`wss` -> `wss`).
#[derive(Clone)]
pub struct WebsocketTransport {
    sender: Arc<Mutex<SplitSink<WsStream, Message>>>,
    receiver: Arc<Mutex<SplitStream<WsStream>>>,
    base_url: Arc<RwLock<Url>>,
    secure: bool,
}

fn ws_scheme(secure: bool) -> &'static str {
    if secure {
        "wss"
    } else {
        "ws"
    }
}

impl WebsocketTransport {
    /// Opens the websocket. `tls_config` is used for `wss` (default: the backend's default roots).
    pub async fn new(
        base_url: Url,
        tls_config: Option<TlsConfig>,
        headers: Option<HeaderMap>,
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
        #[cfg(any(feature = "rustls", feature = "native-tls"))]
        let (ws_stream, _) = {
            let connector = if secure {
                tls::ws_connector(tls_config.as_ref())
            } else {
                None
            };
            tokio_tungstenite::connect_async_tls_with_config(req, None, true, connector).await?
        };
        #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
        let (ws_stream, _) = {
            let _ = tls_config;
            tokio_tungstenite::connect_async_with_config(req, None, true).await?
        };
        let (sender, receiver) = ws_stream.split();

        Ok(WebsocketTransport {
            sender: Arc::new(Mutex::new(sender)),
            receiver: Arc::new(Mutex::new(receiver)),
            base_url: Arc::new(RwLock::new(url)),
            secure,
        })
    }

    /// Upgrade probe: `2probe`, expect `3probe`, then `5` (engine.io protocol v4).
    pub(crate) async fn upgrade(&self) -> Result<()> {
        let mut receiver = self.receiver.lock().await;
        let mut sender = self.sender.lock().await;

        sender
            .send(Message::text(Packet::text(PacketId::Ping, "probe")))
            .await?;

        // Skip control frames; the first data frame must be the probe answer.
        let answer = loop {
            match receiver
                .next()
                .await
                .ok_or(Error::IllegalWebsocketUpgrade())??
            {
                Message::Text(text) => break Bytes::from(text),
                Message::Binary(data) => break data,
                _ => continue,
            }
        };
        if answer != Bytes::from(Packet::new(PacketId::Pong, Bytes::from_static(b"probe"))) {
            return Err(Error::InvalidPacket());
        }

        sender
            .send(Message::text(Packet::text(PacketId::Upgrade, "")))
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

    pub(crate) async fn poll_next(&self) -> Result<Option<Bytes>> {
        let mut receiver = self.receiver.lock().await;
        loop {
            match receiver.next().await {
                Some(Ok(message)) => {
                    if let Some(frame) = frame_to_packet_bytes(message) {
                        return Ok(Some(frame));
                    }
                }
                Some(Err(err)) => return Err(err.into()),
                None => return Ok(None),
            }
        }
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

/// A text frame is an encoded engine.io packet; a binary frame is the raw data of a binary
/// message, so it gets the `4` message prefix. Control frames carry nothing for engine.io.
fn frame_to_packet_bytes(message: Message) -> Option<Bytes> {
    match message {
        Message::Text(text) => Some(Bytes::from(text)),
        Message::Binary(data) => {
            let mut msg = BytesMut::with_capacity(data.len() + 1);
            msg.put_u8(b'4');
            msg.put(data);
            Some(msg.freeze())
        }
        _ => None,
    }
}

impl Stream for WebsocketTransport {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut lock = ready!(Box::pin(self.receiver.lock()).poll_unpin(cx));
            match ready!(lock.poll_next_unpin(cx)) {
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
                    .map_or("Currently not available".to_owned(), |url| url.to_string()),
            )
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::ENGINE_IO_VERSION;
    use std::str::FromStr;

    async fn new() -> Result<WebsocketTransport> {
        let url = crate::test::engine_io_server()?.to_string()
            + "engine.io/?EIO="
            + &ENGINE_IO_VERSION.to_string();
        WebsocketTransport::new(Url::from_str(&url[..])?, None, None).await
    }

    #[test]
    fn frames_map_to_packets() {
        assert_eq!(
            frame_to_packet_bytes(Message::text("4hi")),
            Some(Bytes::from_static(b"4hi"))
        );
        assert_eq!(
            frame_to_packet_bytes(Message::binary(vec![1u8, 2])),
            Some(Bytes::from_static(&[b'4', 1, 2]))
        );
        assert_eq!(frame_to_packet_bytes(Message::Ping(Bytes::new())), None);
    }

    #[tokio::test]
    async fn websocket_transport_base_url() -> Result<()> {
        let transport = new().await?;
        let mut url = crate::test::engine_io_server()?;
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
    }

    #[tokio::test]
    async fn websocket_debug_and_frames() -> Result<()> {
        let mut transport = new().await?;
        assert_eq!(
            format!("{:?}", transport),
            format!(
                "AsyncWebsocketTransport {{ base_url: {:?} }}",
                transport.base_url().await?.to_string()
            )
        );
        // The handshake frame, then the server's greeting message.
        assert!(transport.next().await.unwrap()?.starts_with(b"0{"));
        transport.next().await.unwrap()?;
        Ok(())
    }

    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    #[tokio::test]
    async fn websocket_secure_transport() -> Result<()> {
        let url = crate::test::engine_io_server_secure()?.to_string()
            + "engine.io/?EIO="
            + &ENGINE_IO_VERSION.to_string();
        let mut transport =
            WebsocketTransport::new(Url::from_str(&url)?, Some(crate::test::tls_config()?), None)
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
        assert!(transport.next().await.unwrap()?.starts_with(b"0{"));
        Ok(())
    }

    #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
    #[tokio::test]
    async fn wss_without_tls_is_refused() {
        let result = WebsocketTransport::new(
            Url::parse("https://localhost:4202/engine.io/").unwrap(),
            None,
            None,
        )
        .await;
        assert!(matches!(result, Err(Error::TlsNotEnabled(_))));
    }
}
