//! What the transports of one client share: one reqwest client (its connection pool and, with
//! the `cookies` feature, its cookie jar) and the proxy settings.

use std::fmt;
#[cfg(feature = "cookies")]
use std::sync::Arc;

use base64::Engine as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use url::Url;

use crate::engineio::tls::{self, TlsConfig, apply_reqwest_tls};
use crate::engineio::{Error, error::Result};

/// One client's HTTP settings; cheap to clone (the reqwest client is reference counted).
#[derive(Clone)]
pub(crate) struct Http {
    pub(crate) client: reqwest::Client,
    /// An explicit proxy for every transport; `None`: the environment's (`HTTP_PROXY`, ...).
    proxy: Option<Url>,
    #[cfg(feature = "cookies")]
    jar: Arc<reqwest::cookie::Jar>,
}

impl fmt::Debug for Http {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // the proxy URL may carry credentials
        f.debug_struct("Http")
            .field("proxy", &self.proxy.is_some())
            .finish_non_exhaustive()
    }
}

impl Http {
    /// The reqwest client for `tls` and `proxy` (no proxy: the system / environment one).
    pub(crate) fn new(tls: Option<&TlsConfig>, proxy: Option<Url>) -> Result<Http> {
        let mut builder = apply_reqwest_tls!(reqwest::ClientBuilder::new(), tls);
        if let Some(proxy) = &proxy {
            check_proxy(proxy)?;
            let mut all = reqwest::Proxy::all(proxy.as_str())?;
            if !proxy.username().is_empty() {
                all = all.basic_auth(
                    &percent_decode(proxy.username()),
                    &percent_decode(proxy.password().unwrap_or("")),
                );
            }
            builder = builder.proxy(all);
        }
        #[cfg(feature = "cookies")]
        let jar = Arc::new(reqwest::cookie::Jar::default());
        #[cfg(feature = "cookies")]
        {
            builder = builder.cookie_provider(Arc::clone(&jar));
        }
        Ok(Http {
            client: tls::build_reqwest(builder)?,
            proxy,
            #[cfg(feature = "cookies")]
            jar,
        })
    }

    /// The proxy a websocket to `url` goes through: the explicit one, else `HTTPS_PROXY` (wss)
    /// or `HTTP_PROXY` (ws) unless `NO_PROXY` matches the host.
    pub(crate) fn ws_proxy(&self, url: &Url) -> Option<Url> {
        self.proxy
            .clone()
            .or_else(|| env_proxy(url, |k| std::env::var(k).ok()))
    }

    /// The `Cookie` header the jar holds for `url` (polling answers set them).
    #[cfg(feature = "cookies")]
    pub(crate) fn cookies(&self, url: &Url) -> Option<http::HeaderValue> {
        use reqwest::cookie::CookieStore;
        let mut http_url = url.clone();
        let scheme = if matches!(url.scheme(), "wss" | "https") {
            "https"
        } else {
            "http"
        };
        let _ = http_url.set_scheme(scheme);
        self.jar.cookies(&http_url)
    }
}

/// Only `http://` proxies are spoken (as reqwest and most clients do for CONNECT).
fn check_proxy(proxy: &Url) -> Result<()> {
    match proxy.scheme() {
        "http" => Ok(()),
        other => Err(Error::InvalidUrlScheme(format!("proxy {other}"))),
    }
}

/// `HTTPS_PROXY` / `HTTP_PROXY` (either case) for `url`, unless `NO_PROXY` lists its host.
pub(crate) fn env_proxy(url: &Url, var: impl Fn(&str) -> Option<String>) -> Option<Url> {
    let get = |name: &str| {
        var(name)
            .or_else(|| var(&name.to_ascii_lowercase()))
            .filter(|v| !v.is_empty())
    };
    let host = url.host_str()?;
    if let Some(no_proxy) = get("NO_PROXY") {
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let excluded = no_proxy.split(',').map(str::trim).any(|entry| {
            let entry = entry
                .split(':')
                .next()
                .unwrap_or(entry)
                .trim_start_matches('.');
            entry == "*"
                || (!entry.is_empty() && (host == entry || host.ends_with(&format!(".{entry}"))))
        });
        if excluded {
            return None;
        }
    }
    let secure = matches!(url.scheme(), "wss" | "https");
    let value = if secure {
        get("HTTPS_PROXY").or_else(|| get("ALL_PROXY"))
    } else {
        get("HTTP_PROXY").or_else(|| get("ALL_PROXY"))
    }?;
    let value = if value.contains("://") {
        value
    } else {
        format!("http://{value}")
    };
    Url::parse(&value).ok().filter(|p| check_proxy(p).is_ok())
}

/// A TCP connection to `host:port` through the HTTP proxy `proxy` (`CONNECT`, basic auth from
/// the proxy URL's user info).
pub(crate) async fn tunnel(proxy: &Url, host: &str, port: u16) -> Result<TcpStream> {
    check_proxy(proxy)?;
    let proxy_host = proxy
        .host_str()
        .ok_or_else(|| Error::ProxyFailed("the proxy URL has no host".into()))?;
    let proxy_port = proxy.port_or_known_default().unwrap_or(80);
    let mut stream = TcpStream::connect((proxy_host, proxy_port))
        .await
        .map_err(|e| Error::ProxyFailed(format!("cannot reach the proxy: {}", e.kind())))?;
    stream.set_nodelay(true).ok();
    let authority = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if !proxy.username().is_empty() {
        let decode = |s: &str| percent_decode(s);
        let credentials = format!(
            "{}:{}",
            decode(proxy.username()),
            decode(proxy.password().unwrap_or(""))
        );
        let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
        request.push_str(&format!("Proxy-Authorization: Basic {encoded}\r\n"));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| Error::ProxyFailed(format!("proxy write failed: {}", e.kind())))?;
    // the response head, read byte by byte so nothing of the tunnel is consumed
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() > 8192 {
            return Err(Error::ProxyFailed("the proxy answer is too long".into()));
        }
        let mut byte = [0u8];
        let n = stream
            .read(&mut byte)
            .await
            .map_err(|e| Error::ProxyFailed(format!("proxy read failed: {}", e.kind())))?;
        if n == 0 {
            return Err(Error::ProxyFailed(
                "the proxy closed the connection during CONNECT".into(),
            ));
        }
        head.push(byte[0]);
    }
    let status = String::from_utf8_lossy(&head)
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if status != 200 {
        return Err(Error::ProxyFailed(format!(
            "the proxy answered CONNECT with HTTP {status}"
        )));
    }
    Ok(stream)
}

fn percent_decode(s: &str) -> String {
    url::form_urlencoded::parse(format!("x={}", s.replace('+', "%2B")).as_bytes())
        .next()
        .map(|(_, v)| v.into_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| map.get(k).cloned()
    }

    #[test]
    fn environment_proxies() {
        let ws = Url::parse("ws://api.example.com/socket.io/").unwrap();
        let wss = Url::parse("wss://api.example.com/socket.io/").unwrap();
        let vars = env(&[
            ("HTTP_PROXY", "proxy:3128"),
            ("https_proxy", "http://u:p@sec:8080"),
        ]);
        assert_eq!(
            env_proxy(&ws, &vars).unwrap().as_str(),
            "http://proxy:3128/"
        );
        assert_eq!(env_proxy(&wss, &vars).unwrap().host_str(), Some("sec"));
        let no = env(&[
            ("HTTP_PROXY", "proxy:3128"),
            ("NO_PROXY", "localhost, .example.com"),
        ]);
        assert!(env_proxy(&ws, &no).is_none());
        let other = Url::parse("ws://example.org/").unwrap();
        assert!(env_proxy(&other, &no).is_some());
        assert!(env_proxy(&ws, env(&[("HTTP_PROXY", "proxy"), ("NO_PROXY", "*")])).is_none());
        assert!(env_proxy(&ws, env(&[])).is_none());
        // https proxies are not spoken
        assert!(env_proxy(&ws, env(&[("HTTP_PROXY", "https://p:1")])).is_none());
    }

    #[test]
    fn credentials_are_decoded() {
        assert_eq!(percent_decode("p%40ss+word"), "p@ss+word");
    }
}
