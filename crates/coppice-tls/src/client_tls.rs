//! The **public client listener's** own serving material (ADR 0037 §4
//! `[client_tls]`).
//!
//! Deliberately a separate store from [`TlsStore`](crate::TlsStore), because
//! the two are separate certificates with opposite trust stories and must
//! never be conflated:
//!
//! - the machine plane serves the cluster-minted leaf and **requires** a
//!   cluster-CA client certificate;
//! - this listener serves an *externally signed* certificate (browsers will
//!   never trust the cluster's private root) and merely **requests** a client
//!   certificate — the cluster CA is its verifier when one is presented, so
//!   ADR 0022's operator-profile certificates can later authenticate here,
//!   while every ordinary user and every enrolling machine connects with no
//!   certificate at all.
//!
//! Only the cert/key are watched here. The client-auth trust anchor is the
//! cluster CA from *replicated state*, which arrives per accept (and is absent
//! before formation), so the assembled [`ServerConfig`] is a function of both
//! the on-disk generation and the current CA — hence the one-entry cache in
//! [`ClientTlsStore::server_config`] rather than a config baked at load.

use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use arc_swap::ArcSwap;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};

use crate::{leaf_validity_unix, ReloadableMaterial, TlsError};

/// The two `[client_tls]` file paths. There is no CA path: this listener's
/// serving chain comes from a public issuer, and its client-certificate trust
/// anchor is the cluster CA held in replicated state.
#[derive(Debug, Clone)]
pub struct ClientTlsPaths {
    /// The externally-signed serving certificate chain (PEM).
    pub cert: PathBuf,
    /// Its private key (PEM).
    pub key: PathBuf,
}

/// One immutable generation of the client listener's serving material.
struct ClientTlsMaterial {
    cert_chain: Vec<CertificateDer<'static>>,
    key_pem: Vec<u8>,
    not_after_unix: Option<i64>,
}

/// The assembled config for one (material generation, client-CA) pair.
struct Cached {
    generation: u64,
    roots_key: u64,
    config: Arc<ServerConfig>,
}

/// The live `[client_tls]` store: hot-reloaded serving material plus the
/// per-accept [`ServerConfig`] resolution the listener performs.
pub struct ClientTlsStore {
    paths: ClientTlsPaths,
    current: ArcSwap<ClientTlsMaterial>,
    fingerprint: Mutex<Fingerprint>,
    generation: AtomicU64,
    cached: Mutex<Option<Cached>>,
}

/// A `(mtime, len)` pair per watched file, as in the machine-plane store.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fingerprint {
    cert: (Option<SystemTime>, u64),
    key: (Option<SystemTime>, u64),
}

impl ClientTlsStore {
    /// Load the serving material. Fails fast naming the offending path: a
    /// coordinator configured for TLS on its public listener must not fall
    /// back to plain HTTP (ADR 0037 §4 — the posture is explicit either way).
    pub fn load(paths: ClientTlsPaths) -> Result<Arc<ClientTlsStore>, TlsError> {
        let fingerprint = fingerprint_of(&paths);
        let material = read_material(&paths)?;
        Ok(Arc::new(ClientTlsStore {
            paths,
            current: ArcSwap::from_pointee(material),
            fingerprint: Mutex::new(fingerprint),
            generation: AtomicU64::new(0),
            cached: Mutex::new(None),
        }))
    }

    /// The paths this store watches.
    pub fn paths(&self) -> &ClientTlsPaths {
        &self.paths
    }

    /// The serving config for one accept.
    ///
    /// `client_ca_pem` is the cluster CA from replicated state, or `None`
    /// before formation has minted one — in which case no client certificate
    /// is requested at all, because there is no root to judge one against.
    /// When it is present, client auth is **requested, never required**
    /// (`allow_unauthenticated`): the overwhelming majority of connections
    /// here are certless browsers, CLIs, and enrolling machines, and refusing
    /// them would close the listener ADR 0037 §4 places `/enroll` on.
    pub fn server_config(
        &self,
        client_ca_pem: Option<&[u8]>,
    ) -> Result<Arc<ServerConfig>, TlsError> {
        let generation = self.generation.load(Ordering::Acquire);
        let roots_key = roots_key(client_ca_pem);

        let mut cached = self
            .cached
            .lock()
            .expect("client tls config cache poisoned");
        if let Some(hit) = cached.as_ref() {
            if hit.generation == generation && hit.roots_key == roots_key {
                return Ok(Arc::clone(&hit.config));
            }
        }

        let config = Arc::new(self.build_config(client_ca_pem)?);
        *cached = Some(Cached {
            generation,
            roots_key,
            config: Arc::clone(&config),
        });
        Ok(config)
    }

    fn build_config(&self, client_ca_pem: Option<&[u8]>) -> Result<ServerConfig, TlsError> {
        let material = self.current.load_full();

        let key: PrivateKeyDer<'static> =
            rustls_pemfile::private_key(&mut Cursor::new(&material.key_pem))
                .map_err(|e| TlsError::Key {
                    path: self.paths.key.clone(),
                    reason: e.to_string(),
                })?
                .ok_or_else(|| TlsError::Key {
                    path: self.paths.key.clone(),
                    reason: "no PKCS#8/PKCS#1/SEC1 private key found in PEM".to_string(),
                })?;

        let verifier = match client_ca_pem {
            Some(ca_pem) => {
                let mut roots = RootCertStore::empty();
                for ca in rustls_pemfile::certs(&mut Cursor::new(ca_pem))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| TlsError::RustlsConfig(format!("parsing the cluster CA: {e}")))?
                {
                    roots.add(ca).map_err(|e| {
                        TlsError::RustlsConfig(format!("adding the cluster CA: {e}"))
                    })?;
                }
                if roots.is_empty() {
                    WebPkiClientVerifier::no_client_auth()
                } else {
                    WebPkiClientVerifier::builder(Arc::new(roots))
                        .allow_unauthenticated()
                        .build()
                        .map_err(|e| TlsError::RustlsConfig(e.to_string()))?
                }
            }
            None => WebPkiClientVerifier::no_client_auth(),
        };

        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| TlsError::RustlsConfig(e.to_string()))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(material.cert_chain.clone(), key)
            .map_err(|e| TlsError::RustlsConfig(e.to_string()))?;
        // Both, in preference order: this listener serves browsers and CLIs,
        // not only the h2 machine plane.
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(config)
    }

    fn reload_inner(&self, force: bool) -> Result<bool, TlsError> {
        let fingerprint = fingerprint_of(&self.paths);
        if !force
            && *self
                .fingerprint
                .lock()
                .expect("client tls fingerprint poisoned")
                == fingerprint
        {
            return Ok(false);
        }
        let material = read_material(&self.paths)?;
        self.current.store(Arc::new(material));
        *self
            .fingerprint
            .lock()
            .expect("client tls fingerprint poisoned") = fingerprint;
        self.generation.fetch_add(1, Ordering::Release);
        Ok(true)
    }
}

impl ReloadableMaterial for ClientTlsStore {
    fn label(&self) -> &'static str {
        "client tls"
    }

    fn cert_path(&self) -> &Path {
        &self.paths.cert
    }

    fn not_after_unix(&self) -> Option<i64> {
        self.current.load().not_after_unix
    }

    fn reload(&self) -> Result<bool, TlsError> {
        self.reload_inner(false)
    }

    fn force_reload(&self) -> Result<bool, TlsError> {
        self.reload_inner(true)
    }
}

fn read_material(paths: &ClientTlsPaths) -> Result<ClientTlsMaterial, TlsError> {
    let cert_pem = std::fs::read(&paths.cert).map_err(|source| TlsError::Read {
        kind: "client-listener certificate",
        path: paths.cert.clone(),
        source,
    })?;
    let key_pem = std::fs::read(&paths.key).map_err(|source| TlsError::Read {
        kind: "client-listener private key",
        path: paths.key.clone(),
        source,
    })?;

    let cert_chain: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut Cursor::new(&cert_pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TlsError::Cert {
                path: paths.cert.clone(),
                reason: e.to_string(),
            })?;
    if cert_chain.is_empty() {
        return Err(TlsError::Cert {
            path: paths.cert.clone(),
            reason: "no certificates found in PEM".to_string(),
        });
    }
    // Parsed once here so a broken key fails at load, not at the first
    // handshake — the same fail-fast the machine plane gets.
    if rustls_pemfile::private_key(&mut Cursor::new(&key_pem))
        .map_err(|e| TlsError::Key {
            path: paths.key.clone(),
            reason: e.to_string(),
        })?
        .is_none()
    {
        return Err(TlsError::Key {
            path: paths.key.clone(),
            reason: "no PKCS#8/PKCS#1/SEC1 private key found in PEM".to_string(),
        });
    }

    let (_, not_after_unix) = leaf_validity_unix(&cert_chain[0]);
    Ok(ClientTlsMaterial {
        cert_chain,
        key_pem,
        not_after_unix,
    })
}

fn fingerprint_of(paths: &ClientTlsPaths) -> Fingerprint {
    Fingerprint {
        cert: stat(&paths.cert),
        key: stat(&paths.key),
    }
}

fn stat(path: &Path) -> (Option<SystemTime>, u64) {
    match std::fs::metadata(path) {
        Ok(m) => (m.modified().ok(), m.len()),
        Err(_) => (None, 0),
    }
}

/// A cheap identity for the client-auth roots, so the assembled config is
/// rebuilt when (and only when) the cluster CA changes — a re-root, or the
/// first CA appearing after formation.
fn roots_key(client_ca_pem: Option<&[u8]>) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match client_ca_pem {
        Some(pem) => {
            1u8.hash(&mut hasher);
            pem.hash(&mut hasher);
        }
        None => 0u8.hash(&mut hasher),
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn material(dir: &std::path::Path) -> (ClientTlsPaths, Vec<u8>) {
        let ca = crate::pki::mint_root_ca().expect("mint ca");
        let signer = crate::pki::CaSigner::load(&ca.cert_pem, &ca.key_pem).expect("load ca signer");
        let (cert_pem, key_pem) =
            crate::pki::mint_operator_local(&signer, "api.example.com").expect("mint serving leaf");
        let cert = dir.join("api.crt");
        let key = dir.join("api.key");
        std::fs::write(&cert, &cert_pem).expect("write cert");
        std::fs::write(&key, &key_pem).expect("write key");
        (ClientTlsPaths { cert, key }, ca.cert_pem)
    }

    #[test]
    fn a_config_is_built_and_then_served_from_the_cache() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (paths, ca_pem) = material(dir.path());
        let store = ClientTlsStore::load(paths).expect("load");

        let first = store.server_config(Some(&ca_pem)).expect("build");
        let second = store.server_config(Some(&ca_pem)).expect("cached");
        assert!(
            Arc::ptr_eq(&first, &second),
            "an unchanged generation and CA must reuse the assembled config"
        );

        // A different trust anchor is a different config.
        let other = store.server_config(None).expect("no client auth");
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[test]
    fn a_missing_certificate_fails_at_load_naming_the_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let paths = ClientTlsPaths {
            cert: dir.path().join("absent.crt"),
            key: dir.path().join("absent.key"),
        };
        let err = match ClientTlsStore::load(paths) {
            Err(e) => e,
            Ok(_) => panic!("missing material must fail"),
        };
        assert!(format!("{err}").contains("absent.crt"), "{err}");
    }

    #[test]
    fn a_rewritten_certificate_is_picked_up_and_bumps_the_generation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (paths, ca_pem) = material(dir.path());
        let store = ClientTlsStore::load(paths.clone()).expect("load");
        let before = store.server_config(Some(&ca_pem)).expect("build");

        // Rotate: a second serving leaf under a second root.
        let (rotated, _) = material(dir.path());
        assert_eq!(rotated.cert, paths.cert, "the rotation writes in place");
        assert!(store.force_reload().expect("reload"), "material swapped");

        let after = store.server_config(Some(&ca_pem)).expect("rebuild");
        assert!(
            !Arc::ptr_eq(&before, &after),
            "a rotation must not keep serving the previous config"
        );
    }
}
