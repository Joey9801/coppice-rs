//! Outbound HTTP(S) clients on the workspace's ring rustls provider.
//!
//! reqwest takes its rustls `CryptoProvider` from the process-wide default and,
//! without its aws-lc-rs feature, panics at `build()` when none is installed.
//! The workspace never installs a process default (every rustls config here
//! names the ring provider explicitly) and never builds aws-lc-rs, so every
//! reqwest client is handed a preconfigured [`ClientConfig`] built by
//! [`HttpTls`] instead — which also keeps the trust store this workspace has
//! always used for outbound HTTPS: the Mozilla bundle *and* the platform's
//! native roots (a cluster fronted by a corporate or private-PKI certificate
//! keeps its roots in the OS store), plus whatever extra roots a caller adds.
//!
//! Every TLS option is set here, on the rustls config. reqwest's own TLS
//! builder methods (`tls_certs_*`, `identity`, …) do not apply to a client
//! built with a preconfigured config.

use std::io::Cursor;
use std::sync::Arc;

use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};

/// Why an [`HttpTls`] could not be turned into a client.
#[derive(Debug, thiserror::Error)]
pub enum HttpTlsError {
    /// A PEM blob held no certificate, or a certificate/key that does not
    /// parse.
    #[error("invalid PEM: {0}")]
    Pem(String),
    /// rustls refused the configuration (e.g. a key that does not match the
    /// client certificate).
    #[error("TLS configuration: {0}")]
    Rustls(#[from] rustls::Error),
    /// reqwest could not build the client.
    #[error("HTTP client: {0}")]
    Build(#[from] reqwest::Error),
}

/// The TLS posture of an outbound reqwest client: which roots it trusts and
/// which client certificate, if any, it presents.
#[derive(Debug, Clone)]
pub struct HttpTls {
    built_in_roots: bool,
    extra_roots: Vec<CertificateDer<'static>>,
    identity: Option<(Vec<CertificateDer<'static>>, Arc<PrivateKeyDer<'static>>)>,
}

impl HttpTls {
    /// Trust the built-in roots: the Mozilla bundle plus the platform's native
    /// trust store.
    pub fn built_in_roots() -> HttpTls {
        HttpTls {
            built_in_roots: true,
            extra_roots: Vec::new(),
            identity: None,
        }
    }

    /// Trust nothing beyond roots added with [`extra_roots_pem`]
    /// (Self::extra_roots_pem). The posture for a plain-HTTP endpoint: no
    /// trust store is loaded, which on macOS skips enumerating the keychain.
    pub fn no_roots() -> HttpTls {
        HttpTls {
            built_in_roots: false,
            extra_roots: Vec::new(),
            identity: None,
        }
    }

    /// Additionally trust every certificate in `pem`.
    pub fn extra_roots_pem(mut self, pem: &[u8]) -> Result<HttpTls, HttpTlsError> {
        let certs = parse_certs(pem)?;
        if certs.is_empty() {
            return Err(HttpTlsError::Pem("no certificate in the root PEM".into()));
        }
        self.extra_roots.extend(certs);
        Ok(self)
    }

    /// Present a client certificate: `pem` holds the private key and the
    /// certificate chain, leaf first, in any order relative to each other.
    pub fn identity_pem(mut self, pem: &[u8]) -> Result<HttpTls, HttpTlsError> {
        let chain = parse_certs(pem)?;
        if chain.is_empty() {
            return Err(HttpTlsError::Pem(
                "no certificate in the identity PEM".into(),
            ));
        }
        let key = rustls_pemfile::private_key(&mut Cursor::new(pem))
            .map_err(|e| HttpTlsError::Pem(e.to_string()))?
            .ok_or_else(|| HttpTlsError::Pem("no private key in the identity PEM".into()))?;
        self.identity = Some((chain, Arc::new(key)));
        Ok(self)
    }

    /// The rustls client config for this posture, on the ring provider.
    pub fn client_config(&self) -> Result<ClientConfig, HttpTlsError> {
        let mut roots = RootCertStore::empty();
        if self.built_in_roots {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            // As reqwest itself always did: an unreadable or partially
            // unparsable platform store degrades to the certificates that did
            // load (plus the bundle above), rather than failing the client.
            let native = rustls_native_certs::load_native_certs();
            for error in &native.errors {
                tracing::debug!(%error, "skipping part of the platform trust store");
            }
            roots.add_parsable_certificates(native.certs);
        }
        for root in &self.extra_roots {
            roots.add(root.clone())?;
        }
        let builder = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots);
        Ok(match &self.identity {
            Some((chain, key)) => builder.with_client_auth_cert(chain.clone(), key.clone_key())?,
            None => builder.with_no_client_auth(),
        })
    }

    /// A reqwest client builder carrying this posture. Configure everything
    /// but TLS on the returned builder.
    pub fn reqwest_builder(&self) -> Result<reqwest::ClientBuilder, HttpTlsError> {
        Ok(reqwest::Client::builder().tls_backend_preconfigured(self.client_config()?))
    }

    /// A reqwest client with this posture and every other setting at its
    /// default.
    pub fn reqwest_client(&self) -> Result<reqwest::Client, HttpTlsError> {
        Ok(self.reqwest_builder()?.build()?)
    }
}

fn parse_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, HttpTlsError> {
    rustls_pemfile::certs(&mut Cursor::new(pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| HttpTlsError::Pem(e.to_string()))
}
