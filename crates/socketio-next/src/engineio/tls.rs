//! TLS backend selection for `https://` and `wss://` connections.
//!
//! The backend is a cargo feature: `rustls` (default, pure Rust, ring provider, Mozilla roots from
//! `webpki-roots`) or `native-tls` (OpenSSL on Linux, Security.framework on macOS, SChannel on
//! Windows). With neither feature the crate builds without TLS and refuses `https`/`wss` URLs with
//! [`Error::TlsNotEnabled`].

use std::fmt;
#[cfg(feature = "rustls")]
use std::sync::{Arc, LazyLock};

use url::Url;

use crate::engineio::error::{Error, Result};

/// A TLS client configuration for one of the compiled-in backends.
///
/// Built from a backend type with `From` (`rustls::ClientConfig`, `Arc<rustls::ClientConfig>`,
/// `native_tls::TlsConnector`), or with [`TlsConfig::with_root_certificates_pem`] for a private CA.
#[derive(Clone)]
#[non_exhaustive]
pub enum TlsConfig {
    /// A rustls client configuration.
    #[cfg(feature = "rustls")]
    Rustls(Arc<rustls::ClientConfig>),
    /// A native-tls connector.
    #[cfg(feature = "native-tls")]
    NativeTls(native_tls::TlsConnector),
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[allow(unreachable_patterns)]
        match self {
            #[cfg(feature = "rustls")]
            TlsConfig::Rustls(_) => f.write_str("TlsConfig::Rustls"),
            #[cfg(feature = "native-tls")]
            TlsConfig::NativeTls(_) => f.write_str("TlsConfig::NativeTls"),
            _ => f.write_str("TlsConfig"),
        }
    }
}

#[cfg(feature = "rustls")]
impl From<rustls::ClientConfig> for TlsConfig {
    fn from(config: rustls::ClientConfig) -> Self {
        TlsConfig::Rustls(Arc::new(config))
    }
}

#[cfg(feature = "rustls")]
impl From<Arc<rustls::ClientConfig>> for TlsConfig {
    fn from(config: Arc<rustls::ClientConfig>) -> Self {
        TlsConfig::Rustls(config)
    }
}

#[cfg(feature = "native-tls")]
impl From<native_tls::TlsConnector> for TlsConfig {
    fn from(connector: native_tls::TlsConnector) -> Self {
        TlsConfig::NativeTls(connector)
    }
}

impl TlsConfig {
    /// A configuration that trusts only the PEM certificates in `pem` (a private or self-signed
    /// CA). Uses rustls when it is compiled in, native-tls otherwise.
    pub fn with_root_certificates_pem(pem: &[u8]) -> Result<Self> {
        #[cfg(feature = "rustls")]
        {
            use rustls_pki_types::{CertificateDer, pem::PemObject};
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(pem) {
                let cert = cert.map_err(|e| Error::InvalidTlsConfig(e.to_string()))?;
                roots
                    .add(cert)
                    .map_err(|e| Error::InvalidTlsConfig(e.to_string()))?;
            }
            if roots.is_empty() {
                return Err(Error::InvalidTlsConfig("no certificate in PEM".into()));
            }
            Ok(TlsConfig::Rustls(Arc::new(rustls_config(roots)?)))
        }
        #[cfg(all(feature = "native-tls", not(feature = "rustls")))]
        {
            let cert = native_tls::Certificate::from_pem(pem)
                .map_err(|e| Error::InvalidTlsConfig(e.to_string()))?;
            let connector = native_tls::TlsConnector::builder()
                .add_root_certificate(cert)
                .build()
                .map_err(|e| Error::InvalidTlsConfig(e.to_string()))?;
            Ok(TlsConfig::NativeTls(connector))
        }
        #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
        {
            let _ = pem;
            Err(Error::TlsNotEnabled("custom root certificates".into()))
        }
    }
}

#[cfg(feature = "rustls")]
fn rustls_config(roots: rustls::RootCertStore) -> Result<rustls::ClientConfig> {
    // An explicit provider: `ClientConfig::builder()` panics when another crate in the graph
    // also enables aws-lc-rs and no process default is installed.
    Ok(rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| Error::InvalidTlsConfig(e.to_string()))?
    .with_root_certificates(roots)
    .with_no_client_auth())
}

/// The rustls configuration used when the caller gives none: Mozilla roots, or the operating
/// system's verifier with `platform-verifier` (not on Android).
#[cfg(feature = "rustls")]
fn default_rustls() -> Arc<rustls::ClientConfig> {
    static CONFIG: LazyLock<Arc<rustls::ClientConfig>> = LazyLock::new(|| {
        #[cfg(all(feature = "platform-verifier", not(target_os = "android")))]
        match platform_config() {
            Ok(config) => return Arc::new(config),
            Err(err) => {
                tracing::warn!(error = %err, "platform verifier unavailable; using webpki roots")
            }
        }
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        Arc::new(rustls_config(roots).expect("ring supports the default protocol versions"))
    });
    Arc::clone(&CONFIG)
}

#[cfg(all(feature = "platform-verifier", not(target_os = "android")))]
fn platform_config() -> std::result::Result<rustls::ClientConfig, rustls::Error> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = rustls_platform_verifier::Verifier::new(Arc::clone(&provider))?;
    Ok(rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth())
}

/// The configuration to use: the caller's, or the default of the preferred backend.
pub(crate) fn effective(config: Option<&TlsConfig>) -> Option<TlsConfig> {
    if let Some(config) = config {
        return Some(config.clone());
    }
    #[cfg(feature = "rustls")]
    {
        Some(TlsConfig::Rustls(default_rustls()))
    }
    #[cfg(all(feature = "native-tls", not(feature = "rustls")))]
    {
        native_tls::TlsConnector::new()
            .ok()
            .map(TlsConfig::NativeTls)
    }
    #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
    {
        None
    }
}

/// Fails for `https`/`wss` when no TLS backend is compiled in.
pub(crate) fn check_scheme(url: &Url) -> Result<()> {
    match url.scheme() {
        "http" | "ws" => Ok(()),
        "https" | "wss" if cfg!(any(feature = "rustls", feature = "native-tls")) => Ok(()),
        "https" | "wss" => Err(Error::TlsNotEnabled(url.scheme().to_owned())),
        other => Err(Error::InvalidUrlScheme(other.to_owned())),
    }
}

/// Builds the reqwest client. reqwest panics when a crate elsewhere in the graph enabled its
/// `rustls-no-provider` feature and no provider is installed (feature unification); that becomes
/// [`Error::TlsProviderMissing`] instead of a panic in the caller's task.
// ponytail: catch_unwind does not help under `panic = "abort"`; the README names the feature fix.
pub(crate) fn build_reqwest(builder: reqwest::ClientBuilder) -> Result<reqwest::Client> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| builder.build())) {
        Ok(client) => Ok(client?),
        Err(_) => Err(Error::TlsProviderMissing),
    }
}

/// Applies the TLS configuration to a reqwest client builder.
macro_rules! apply_reqwest_tls {
    ($builder:expr, $config:expr) => {{
        #[allow(unused_mut)]
        let mut builder = $builder;
        #[allow(unreachable_patterns, unused_variables)]
        match $crate::engineio::tls::effective($config) {
            #[cfg(feature = "rustls")]
            Some($crate::engineio::tls::TlsConfig::Rustls(config)) => {
                builder = builder.tls_backend_preconfigured((*config).clone());
            }
            #[cfg(feature = "native-tls")]
            Some($crate::engineio::tls::TlsConfig::NativeTls(connector)) => {
                builder = builder.tls_backend_preconfigured(connector);
            }
            _ => {}
        }
        builder
    }};
}
pub(crate) use apply_reqwest_tls;

/// The tokio-tungstenite connector for a `wss` connection.
#[cfg(any(feature = "rustls", feature = "native-tls"))]
pub(crate) fn ws_connector(config: Option<&TlsConfig>) -> Option<tokio_tungstenite::Connector> {
    #[allow(unreachable_patterns, unused_variables)]
    match effective(config)? {
        #[cfg(feature = "rustls")]
        TlsConfig::Rustls(config) => Some(tokio_tungstenite::Connector::Rustls(config)),
        #[cfg(feature = "native-tls")]
        TlsConfig::NativeTls(connector) => Some(tokio_tungstenite::Connector::NativeTls(connector)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // reqwest's own builder with rustls compiled in and no provider installed: what a downstream
    // build gets when another crate enables `rustls-no-provider` (nothing here installs a default).
    #[cfg(all(feature = "rustls", not(feature = "native-tls")))]
    #[test]
    fn missing_provider_is_an_error_not_a_panic() {
        assert!(matches!(
            build_reqwest(reqwest::Client::builder()),
            Err(Error::TlsProviderMissing)
        ));
        assert!(build_reqwest(apply_reqwest_tls!(reqwest::Client::builder(), None)).is_ok());
    }

    #[test]
    fn scheme_check() {
        assert!(check_scheme(&Url::parse("http://h/").unwrap()).is_ok());
        assert!(check_scheme(&Url::parse("ws://h/").unwrap()).is_ok());
        assert!(matches!(
            check_scheme(&Url::parse("ftp://h/").unwrap()),
            Err(Error::InvalidUrlScheme(_))
        ));
        let secure = check_scheme(&Url::parse("https://h/").unwrap());
        if cfg!(any(feature = "rustls", feature = "native-tls")) {
            assert!(secure.is_ok());
        } else {
            assert!(matches!(secure, Err(Error::TlsNotEnabled(_))));
        }
    }

    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    #[test]
    fn pem_without_certificates_is_an_error() {
        assert!(TlsConfig::with_root_certificates_pem(b"not a pem").is_err());
    }

    /// `platform-verifier`: the default config verifies through the OS, not the bundled roots.
    #[cfg(feature = "rustls")]
    #[test]
    fn default_verifier_matches_the_features() {
        let debug = format!("{:?}", default_rustls());
        let platform = cfg!(all(
            feature = "platform-verifier",
            not(target_os = "android")
        ));
        assert_eq!(!debug.contains("WebPkiServerVerifier"), platform, "{debug}");
    }

    #[cfg(any(feature = "rustls", feature = "native-tls"))]
    #[test]
    fn default_config_exists() {
        assert!(effective(None).is_some());
    }
}
