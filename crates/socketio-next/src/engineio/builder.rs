use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http::HeaderMap;
use url::Url;

use crate::engineio::callback::{Callbacks, boxed};
use crate::engineio::error::Result;
use crate::engineio::http::Http;
use crate::engineio::socket::Socket as InnerSocket;
use crate::engineio::tls::{self, TlsConfig};
use crate::engineio::transport::Frame;
use crate::engineio::transports::{PollingTransport, WebsocketTransport};
use crate::engineio::{
    Client, DEFAULT_TIMEOUT, ENGINE_IO_VERSION, Error, HandshakePacket, Packet, PacketId,
};

/// Configures and opens an engine.io [`Client`].
///
/// `build*` performs the handshake; [`Client::connect`] then starts the session.
#[derive(Clone)]
pub struct ClientBuilder {
    url: Url,
    tls_config: Option<TlsConfig>,
    headers: Option<HeaderMap>,
    timeout: Duration,
    handshake: Option<HandshakePacket>,
    callbacks: Callbacks,
    /// Shared by every transport this builder opens (built on first use).
    http: Option<Http>,
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // query values and header values may be secrets
        f.debug_struct("ClientBuilder")
            .field("url", &crate::redact::url(&self.url, true))
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .flat_map(|h| h.keys())
                    .map(|k| k.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("tls_config", &self.tls_config)
            .field("timeout", &self.timeout)
            .field("handshake", &self.handshake.is_some())
            .field("callbacks", &self.callbacks)
            .finish()
    }
}

impl ClientBuilder {
    /// A builder for `url`. `EIO=4` is added to the query; an empty path becomes `/engine.io/`.
    pub fn new(url: Url) -> Self {
        let mut url = url;
        url.query_pairs_mut()
            .append_pair("EIO", &ENGINE_IO_VERSION.to_string());
        if url.path() == "/" {
            url.set_path("/engine.io/");
        }
        ClientBuilder {
            url,
            headers: None,
            tls_config: None,
            timeout: DEFAULT_TIMEOUT,
            handshake: None,
            callbacks: Callbacks::default(),
            http: None,
        }
    }

    /// The HTTP client (pool, cookie jar) and proxy of the socket.io client.
    pub(crate) fn http(mut self, http: Http) -> Self {
        self.http = Some(http);
        self
    }

    fn http_settings(&mut self) -> Result<Http> {
        if self.http.is_none() {
            self.http = Some(Http::new(self.tls_config.as_ref(), None)?);
        }
        Ok(self.http.clone().expect("set above"))
    }

    /// TLS configuration for `https`/`wss` (default: the backend's default roots).
    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "rustls", feature = "native-tls"))))]
    pub fn tls_config(mut self, tls_config: impl Into<TlsConfig>) -> Self {
        self.tls_config = Some(tls_config.into());
        self
    }

    /// HTTP headers for every polling request and the websocket handshake.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = Some(headers);
        self
    }

    /// Bound for the handshake and the websocket connect (default 20 s).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Called when the session closes (server close packet or [`Client::disconnect`]).
    pub fn on_close<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(()) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.on_close = Some(boxed(callback));
        self
    }

    /// Called with the data of every message packet.
    pub fn on_data<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(Bytes) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.on_data = Some(boxed(callback));
        self
    }

    /// Called with the text of every send error.
    pub fn on_error<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.on_error = Some(boxed(callback));
        self
    }

    /// Called when [`Client::connect`] opens the session.
    pub fn on_open<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(()) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.on_open = Some(boxed(callback));
        self
    }

    /// Called with every received packet.
    pub fn on_packet<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(Packet) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.on_packet = Some(boxed(callback));
        self
    }

    /// Reads the open packet from `transport` and adds the sid to the URL.
    async fn handshake_with_transport<T, I>(&mut self, transport: &mut T) -> Result<()>
    where
        T: Stream<Item = Result<I>> + Unpin,
        I: Into<Frame>,
    {
        if self.handshake.is_some() {
            return Ok(());
        }
        let first = transport.next().await.ok_or_else(|| {
            Error::InvalidHandshake("the connection closed before the open packet".into())
        })??;
        let handshake = match first.into() {
            Frame::Text(bytes) => parse_handshake(bytes)?,
            Frame::Binary(_) => {
                return Err(Error::InvalidHandshake(
                    "binary frame instead of the open packet".into(),
                ));
            }
        };
        self.url
            .query_pairs_mut()
            .append_pair("sid", &handshake.sid);
        tracing::debug!(
            sid = %handshake.sid,
            upgrades = ?handshake.upgrades,
            ping_interval_ms = handshake.ping_interval,
            ping_timeout_ms = handshake.ping_timeout,
            max_payload = handshake.max_payload,
            "engine.io handshake"
        );
        self.handshake = Some(handshake);
        Ok(())
    }

    async fn handshake(&mut self) -> Result<()> {
        if self.handshake.is_some() {
            return Ok(());
        }
        tls::check_scheme(&self.url)?;
        let http = self.http_settings()?;
        let mut transport = PollingTransport::with_client(
            self.url.clone(),
            &http,
            self.headers.clone().unwrap_or_default(),
            Some(self.timeout),
        )?;
        self.handshake_with_transport(&mut transport).await
    }

    /// Polling handshake, then a websocket upgrade when the server offers one.
    pub async fn build(mut self) -> Result<Client> {
        self.handshake().await?;
        if self.websocket_upgrade() {
            self.build_websocket_with_upgrade().await
        } else {
            self.build_polling().await
        }
    }

    /// Polling only.
    pub async fn build_polling(mut self) -> Result<Client> {
        self.handshake().await?;
        let handshake = self
            .handshake
            .take()
            .ok_or(Error::InvalidHandshake("missing".into()))?;

        // A long-poll GET is answered at least once per ping interval; a request still open after
        // the ping deadline belongs to a dead connection.
        let request_timeout = Duration::from_millis(
            handshake
                .ping_interval
                .saturating_add(handshake.ping_timeout),
        );
        let http = self.http_settings()?;
        let transport = PollingTransport::with_client(
            self.url,
            &http,
            self.headers.unwrap_or_default(),
            Some(request_timeout),
        )?;
        tracing::debug!(transport = "polling", "engine.io transport chosen");
        Ok(Client::new(InnerSocket::new(
            transport.into(),
            handshake,
            self.callbacks,
        )))
    }

    /// Polling handshake, then the websocket upgrade; fails when the server offers none.
    pub async fn build_websocket_with_upgrade(mut self) -> Result<Client> {
        self.handshake().await?;
        if self.websocket_upgrade() {
            self.build_websocket().await
        } else {
            Err(Error::UpgradeUnavailable)
        }
    }

    /// Websocket only: the handshake runs over the websocket, or (after a polling handshake)
    /// the upgrade probe does. Bounded by the timeout: a blackholed websocket (a proxy that
    /// accepts TCP and drops the upgrade) would otherwise hang and `build_with_fallback` would
    /// never fall back to polling.
    pub async fn build_websocket(mut self) -> Result<Client> {
        let timeout = self.timeout;
        let connect = async move {
            let http = self.http_settings()?;
            #[allow(unused_mut)]
            let mut headers = self.headers.clone().unwrap_or_default();
            #[cfg(feature = "cookies")]
            if let Some(cookies) = http.cookies(&self.url) {
                headers.insert(http::header::COOKIE, cookies);
            }
            let mut transport = WebsocketTransport::connect(
                self.url.clone(),
                self.tls_config.clone(),
                Some(headers),
                http.ws_proxy(&self.url),
            )
            .await?;
            if self.handshake.is_some() {
                transport.upgrade().await?;
                tracing::debug!(transport = "websocket", "engine.io upgraded");
            } else {
                self.handshake_with_transport(&mut transport).await?;
                tracing::debug!(transport = "websocket", "engine.io transport chosen");
            }
            let handshake = self
                .handshake
                .ok_or(Error::InvalidHandshake("missing".into()))?;
            Ok(Client::new(InnerSocket::new(
                transport.into(),
                handshake,
                self.callbacks,
            )))
        };
        tokio::time::timeout(timeout, connect)
            .await
            .map_err(|_| Error::Timeout(timeout))?
    }

    /// Polling handshake, then the websocket upgrade within half the timeout; when the upgrade
    /// fails (blocked or blackholed websocket) the session stays on polling with the same sid,
    /// as engine.io-client does after a failed probe.
    pub async fn build_with_fallback(mut self) -> Result<Client> {
        self.handshake().await?;
        if self.websocket_upgrade() {
            let mut upgrade = self.clone();
            upgrade.timeout = self.timeout / 2;
            match upgrade.build_websocket().await {
                Ok(client) => return Ok(client),
                Err(err) => {
                    tracing::debug!(error = %err, "websocket upgrade failed, staying on polling");
                }
            }
        }
        self.build_polling().await
    }

    fn websocket_upgrade(&self) -> bool {
        self.handshake.as_ref().is_some_and(|handshake| {
            handshake
                .upgrades
                .iter()
                .any(|upgrade| upgrade.eq_ignore_ascii_case("websocket"))
        })
    }
}

/// The open packet: `0{"sid":..}`. Anything else (an Engine.IO v3 `96:0{..}` length-prefixed
/// payload, a proxy error page) is an invalid handshake with a hint.
fn parse_handshake(bytes: Bytes) -> Result<HandshakePacket> {
    let hint = || {
        let head = String::from_utf8_lossy(&bytes[..bytes.len().min(32)]).into_owned();
        let v3 = head
            .split_once(':')
            .is_some_and(|(len, _)| len.bytes().all(|b| b.is_ascii_digit()));
        if v3 {
            format!(
                "{head:?}: looks like an Engine.IO v3 server (socket.io 2.x); this client needs v4 (socket.io 3+)"
            )
        } else {
            format!("expected an open packet `0{{...}}`, got {head:?}")
        }
    };
    let packet = Packet::try_from(bytes.clone()).map_err(|_| Error::InvalidHandshake(hint()))?;
    if packet.packet_id != PacketId::Open {
        return Err(Error::InvalidHandshake(hint()));
    }
    HandshakePacket::try_from(packet).map_err(|e| Error::InvalidHandshake(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_errors_explain() {
        let err = parse_handshake(Bytes::from_static(b"96:0{\"sid\":\"x\"}")).unwrap_err();
        assert!(err.to_string().contains("Engine.IO v3"), "{err}");
        let err = parse_handshake(Bytes::from_static(b"<html>")).unwrap_err();
        assert!(err.to_string().contains("expected an open packet"), "{err}");
        let ok = parse_handshake(Bytes::from_static(
            b"0{\"sid\":\"a\",\"upgrades\":[\"websocket\"],\"pingInterval\":25000,\"pingTimeout\":20000,\"maxPayload\":1000000}",
        ))
        .unwrap();
        assert_eq!((ok.sid.as_str(), ok.max_payload), ("a", Some(1_000_000)));
    }
}
