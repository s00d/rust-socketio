//! What the transports of one client share: one reqwest client (its connection pool and, with
//! the `cookies` feature, its cookie jar) and the proxy decision.

use std::fmt;
use std::sync::Arc;

use base64::Engine as _;
use http::{HeaderMap, HeaderValue};
use hyper_util::client::proxy::matcher::Matcher;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use url::{Host, Url};

use crate::engineio::tls::{self, TlsConfig, apply_reqwest_tls};
use crate::engineio::{Error, error::Result};

/// Which proxy the transports use.
#[derive(Clone, Debug, Default)]
pub(crate) enum ProxyMode {
    /// `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY` / `NO_PROXY`, read by the same matcher for
    /// polling (reqwest) and the websocket.
    #[default]
    Env,
    /// This one for every request.
    Explicit(Url),
    /// None, whatever the environment says.
    Off,
}

/// One client's HTTP settings; cheap to clone (the reqwest client is reference counted).
#[derive(Clone)]
pub(crate) struct Http {
    pub(crate) client: reqwest::Client,
    mode: Arc<ProxyMode>,
    #[cfg(feature = "cookies")]
    jar: Arc<reqwest::cookie::Jar>,
}

impl fmt::Debug for Http {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // the proxy URL may carry credentials
        let mode = match *self.mode {
            ProxyMode::Env => "env",
            ProxyMode::Explicit(_) => "explicit",
            ProxyMode::Off => "off",
        };
        f.debug_struct("Http")
            .field("proxy", &mode)
            .finish_non_exhaustive()
    }
}

/// Where a websocket's TCP connection goes when it is proxied.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Route {
    /// The proxy host, without IPv6 brackets.
    host: String,
    port: u16,
    /// `Proxy-Authorization` value.
    auth: Option<HeaderValue>,
}

impl Http {
    /// The reqwest client for `tls` and `mode`. Redirects are not followed: engine.io never
    /// sends one, and following one would send the query (tokens) to another place.
    pub(crate) fn new(tls: Option<&TlsConfig>, mode: ProxyMode) -> Result<Http> {
        let mut builder = apply_reqwest_tls!(reqwest::ClientBuilder::new(), tls)
            .redirect(reqwest::redirect::Policy::none());
        match &mode {
            ProxyMode::Env => {}
            ProxyMode::Off => builder = builder.no_proxy(),
            ProxyMode::Explicit(proxy) => {
                check_proxy_scheme(proxy.scheme())?;
                let mut all = reqwest::Proxy::all(proxy.as_str())?;
                if !proxy.username().is_empty() {
                    all = all.basic_auth(
                        &percent_decode(proxy.username()),
                        &percent_decode(proxy.password().unwrap_or("")),
                    );
                }
                builder = builder.proxy(all);
            }
        }
        #[cfg(feature = "cookies")]
        let jar = Arc::new(reqwest::cookie::Jar::default());
        #[cfg(feature = "cookies")]
        {
            builder = builder.cookie_provider(Arc::clone(&jar));
        }
        Ok(Http {
            client: tls::build_reqwest(builder)?,
            mode: Arc::new(mode),
            #[cfg(feature = "cookies")]
            jar,
        })
    }

    /// The proxy a websocket to `url` goes through, decided like polling's: the explicit one,
    /// none, or the environment's (the same matcher reqwest uses). An `https://` (TLS) proxy
    /// is an error here rather than a silent direct connection.
    pub(crate) fn ws_route(&self, url: &Url) -> Result<Option<Route>> {
        match &*self.mode {
            ProxyMode::Off => Ok(None),
            ProxyMode::Explicit(proxy) => {
                check_proxy_scheme(proxy.scheme())?;
                let auth = (!proxy.username().is_empty()).then(|| {
                    basic(
                        &percent_decode(proxy.username()),
                        &percent_decode(proxy.password().unwrap_or("")),
                    )
                });
                Ok(Some(Route {
                    host: connect_host(proxy)?,
                    port: proxy.port_or_known_default().unwrap_or(80),
                    auth,
                }))
            }
            ProxyMode::Env => matcher_route(&Matcher::from_env(), url),
        }
    }

    /// The headers of a request to `url` with the jar's cookies added to the caller's own
    /// `Cookie` header (`user; jar`), so neither replaces the other.
    pub(crate) fn with_cookies(&self, headers: &HeaderMap, url: &Url) -> HeaderMap {
        #[allow(unused_mut)]
        let mut headers = headers.clone();
        #[cfg(feature = "cookies")]
        {
            use reqwest::cookie::CookieStore;
            let mut http_url = url.clone();
            let scheme = if matches!(url.scheme(), "wss" | "https") {
                "https"
            } else {
                "http"
            };
            let _ = http_url.set_scheme(scheme);
            if let Some(jar) = self.jar.cookies(&http_url) {
                let merged = merge_cookies(&headers, jar);
                headers.insert(http::header::COOKIE, merged);
            }
        }
        #[cfg(not(feature = "cookies"))]
        let _ = url;
        headers
    }
}

/// The caller's `Cookie` values, then the jar's.
#[cfg_attr(not(feature = "cookies"), allow(dead_code))]
fn merge_cookies(headers: &HeaderMap, jar: HeaderValue) -> HeaderValue {
    let user: Vec<&str> = headers
        .get_all(http::header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .collect();
    if user.is_empty() {
        return jar;
    }
    let jar = jar.to_str().unwrap_or_default();
    HeaderValue::try_from(format!("{}; {jar}", user.join("; ")))
        .unwrap_or_else(|_| HeaderValue::from_static(""))
}

/// The route the environment matcher picks for `url` (`ws`/`wss` count as `http`/`https`).
fn matcher_route(matcher: &Matcher, url: &Url) -> Result<Option<Route>> {
    let secure = matches!(url.scheme(), "wss" | "https");
    let authority = authority(url)?;
    let dst: http::Uri = format!("{}://{authority}/", if secure { "https" } else { "http" })
        .parse()
        .map_err(|_| Error::ProxyFailed("cannot build the proxy target".into()))?;
    let Some(intercept) = matcher.intercept(&dst) else {
        return Ok(None);
    };
    let proxy = intercept.uri();
    check_proxy_scheme(proxy.scheme_str().unwrap_or("http"))?;
    let host = proxy
        .host()
        .ok_or_else(|| Error::ProxyFailed("the proxy has no host".into()))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    Ok(Some(Route {
        host,
        port: proxy.port_u16().unwrap_or(80),
        auth: intercept
            .basic_auth()
            .cloned()
            .or_else(|| intercept.raw_auth().map(|(u, p)| basic(u, p))),
    }))
}

fn basic(user: &str, password: &str) -> HeaderValue {
    let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    HeaderValue::try_from(format!("Basic {encoded}")).expect("base64 is a valid header value")
}

/// Only `http://` proxies are spoken for the websocket `CONNECT` (no TLS to the proxy, no
/// SOCKS); anything else is an error, never a silent direct connection.
fn check_proxy_scheme(scheme: &str) -> Result<()> {
    match scheme {
        "http" => Ok(()),
        other => Err(Error::ProxyFailed(format!(
            "{other} proxies are not supported (only http:// proxies)"
        ))),
    }
}

/// `host:port` of `url` for a `CONNECT` line and `Host` header (`[v6]:port`).
pub(crate) fn authority(url: &Url) -> Result<String> {
    let port = url.port_or_known_default().unwrap_or(80);
    match url.host() {
        Some(Host::Ipv6(addr)) => Ok(format!("[{addr}]:{port}")),
        Some(Host::Ipv4(addr)) => Ok(format!("{addr}:{port}")),
        Some(Host::Domain(domain)) => Ok(format!("{domain}:{port}")),
        None => Err(Error::InvalidHandshake("the URL has no host".into())),
    }
}

/// The host of `url` to give `TcpStream::connect` (IPv6 without brackets).
pub(crate) fn connect_host(url: &Url) -> Result<String> {
    match url.host() {
        Some(Host::Ipv6(addr)) => Ok(addr.to_string()),
        Some(Host::Ipv4(addr)) => Ok(addr.to_string()),
        Some(Host::Domain(domain)) => Ok(domain.to_owned()),
        None => Err(Error::InvalidHandshake("the URL has no host".into())),
    }
}

/// The `CONNECT` request for `target` (an authority) through `route`.
fn connect_request(route: &Route, target: &str) -> String {
    let mut request = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n");
    if let Some(auth) = route.auth.as_ref().and_then(|a| a.to_str().ok()) {
        request.push_str(&format!("Proxy-Authorization: {auth}\r\n"));
    }
    request.push_str("\r\n");
    request
}

/// A TCP connection to `target` (a URL) through the HTTP proxy `route` (`CONNECT`).
pub(crate) async fn tunnel(route: &Route, target: &Url) -> Result<TcpStream> {
    let authority = authority(target)?;
    let mut stream = TcpStream::connect((route.host.as_str(), route.port))
        .await
        .map_err(|e| Error::ProxyFailed(format!("cannot reach the proxy: {}", e.kind())))?;
    stream.set_nodelay(true).ok();
    stream
        .write_all(connect_request(route, &authority).as_bytes())
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

    fn matcher(http: &str, https: &str, no: &str) -> Matcher {
        Matcher::builder().http(http).https(https).no(no).build()
    }

    /// The websocket uses the matcher reqwest uses for polling: same proxy, same exclusions
    /// (lower case, IPv6, CIDR come from hyper-util).
    #[test]
    fn environment_routes() {
        let ws = Url::parse("ws://api.example.com/socket.io/").unwrap();
        let wss = Url::parse("wss://api.example.com/socket.io/").unwrap();
        let m = matcher("http://proxy:3128", "http://u:p@sec:8080", "");
        let route = matcher_route(&m, &ws).unwrap().unwrap();
        assert_eq!((route.host.as_str(), route.port), ("proxy", 3128));
        let route = matcher_route(&m, &wss).unwrap().unwrap();
        assert_eq!(route.host, "sec");
        assert_eq!(route.auth.unwrap(), "Basic dTpw");
        let m = matcher(
            "http://proxy:3128",
            "",
            "localhost, .example.com, 10.0.0.0/8, ::1",
        );
        assert!(matcher_route(&m, &ws).unwrap().is_none());
        for direct in ["ws://10.1.2.3/", "ws://[::1]:4200/"] {
            let url = Url::parse(direct).unwrap();
            assert!(matcher_route(&m, &url).unwrap().is_none(), "{direct}");
        }
        let other = Url::parse("ws://example.org/").unwrap();
        assert!(matcher_route(&m, &other).unwrap().is_some());
        // a TLS proxy is refused, not bypassed
        let m = matcher("https://p:1", "", "");
        assert!(matches!(matcher_route(&m, &ws), Err(Error::ProxyFailed(_))));
    }

    #[test]
    fn ipv6_targets_and_proxies() {
        let target = Url::parse("ws://[::1]:4200/socket.io/").unwrap();
        assert_eq!(authority(&target).unwrap(), "[::1]:4200");
        assert_eq!(connect_host(&target).unwrap(), "::1");
        let route = Route {
            host: "::1".into(),
            port: 3128,
            auth: Some(basic("u", "p")),
        };
        assert_eq!(
            connect_request(&route, &authority(&target).unwrap()),
            "CONNECT [::1]:4200 HTTP/1.1\r\nHost: [::1]:4200\r\nProxy-Authorization: Basic dTpw\r\n\r\n"
        );
        let m = matcher("http://[::1]:3128", "", "");
        let other = Url::parse("ws://example.org/").unwrap();
        let route = matcher_route(&m, &other).unwrap().unwrap();
        assert_eq!((route.host.as_str(), route.port), ("::1", 3128));
        let explicit = Http::new(
            None,
            ProxyMode::Explicit(Url::parse("http://[::1]:3128").unwrap()),
        )
        .unwrap()
        .ws_route(&other)
        .unwrap()
        .unwrap();
        assert_eq!((explicit.host.as_str(), explicit.port), ("::1", 3128));
    }

    #[test]
    fn credentials_are_decoded() {
        assert_eq!(percent_decode("p%40ss+word"), "p@ss+word");
    }

    #[test]
    fn user_and_jar_cookies_are_merged() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::COOKIE, "user=1".parse().unwrap());
        let merged = merge_cookies(&headers, HeaderValue::from_static("io=abc"));
        assert_eq!(merged, "user=1; io=abc");
        let jar_only = merge_cookies(&HeaderMap::new(), HeaderValue::from_static("io=abc"));
        assert_eq!(jar_only, "io=abc");
    }
}
