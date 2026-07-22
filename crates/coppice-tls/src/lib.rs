//! # coppice-tls
//!
//! Hot-reloadable mutual-TLS material for the coordinator control plane
//! (ADR 0037 §6, "cert reload"). Certificate issuance stays external — the two
//! documented deployment paths mint short-lived platform leaves or long-lived
//! config-management leaves — but the coordinator must pick a rotated
//! cert/key/CA up *without a restart*: short-lived externally-rotated
//! certificates then require no process choreography, and in-flight connections
//! finish on the old leaf.
//!
//! The crate is a small shared dependency of both `coppice-coordinator` (the
//! two mTLS listeners) and `coppice-consensus` (the outbound raft peer mesh),
//! so neither has to reach into the other for the reload store.
//!
//! ## Shape
//!
//! - [`TlsStore`] holds the current [`TlsMaterial`] behind an [`arc_swap`]
//!   cell, loaded from the `[tls]` paths. [`TlsStore::reload`] re-reads the
//!   files and swaps in freshly-parsed material *only when it parses cleanly*:
//!   a broken, half-written file never takes down serving — it is logged and
//!   the old material keeps serving. [`spawn_reload_task`] drives reloads from
//!   an mtime poll and (on the daemon path) `SIGHUP`.
//! - **Server side** is connection-time resolution. tonic's `ServerTlsConfig`
//!   cannot swap identities on a live listener, so [`serve`] hand-rolls a
//!   [`tokio_rustls`] acceptor: it accepts TCP, does the rustls handshake with
//!   the *current* material's [`rustls::ServerConfig`], and yields the
//!   resulting [`tokio_rustls::server::TlsStream`] for
//!   `Server::serve_with_incoming`. Because tonic ships a blanket
//!   `Connected for TlsStream<T>` impl, `request.peer_certs()` keeps working
//!   unchanged — the agent gateway's CN-based authentication is preserved with
//!   no newtype. Client auth is mandatory: a handshake presenting no client
//!   certificate is refused by the CA verifier built from current material.
//! - **Outbound** channels read [`TlsStore::current`] at (re)dial time rather
//!   than freezing a `ClientTlsConfig` at startup, so a reconnect after a
//!   rotation uses the new leaf. In-flight connections finishing on the old
//!   leaf is automatic — their TLS session is already established.

use std::io::{self, Cursor};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::server::TlsStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;

// ---------------------------------------------------------------------------
// Metrics (crate-root describe/gather pair, mounted into the coordinator's own
// `describe_metrics`/`gather_metrics` per the repo-wide convention).
// ---------------------------------------------------------------------------

const RELOADS: &str = "coordinator_tls_reloads_total";
const RELOAD_FAILURES: &str = "coordinator_tls_reload_failures_total";
const CERT_NOT_AFTER: &str = "coordinator_tls_cert_not_after_seconds";

/// Register the TLS-reload metric descriptions (wire into the coordinator's
/// crate-root `describe_metrics`).
pub fn describe_metrics() {
    metrics::describe_counter!(
        RELOADS,
        "TLS material reloads that swapped in freshly-parsed cert/key/CA."
    );
    metrics::describe_counter!(
        RELOAD_FAILURES,
        "TLS material reload attempts that failed to read or parse and were \
         ignored (the previous material kept serving)."
    );
    metrics::describe_gauge!(
        CERT_NOT_AFTER,
        "Unix timestamp (seconds) of the currently-served leaf certificate's \
         notAfter, for a rotation/expiry alert."
    );
}

/// No point-in-time sampling behind the TLS metrics — the expiry gauge is set
/// eagerly on load and on every swap. Present for the crate-root pattern.
pub fn gather_metrics() {}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// A failure loading or parsing TLS material. Each variant names the path so an
/// operator's first error line is actionable.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A cert/key/CA file could not be read.
    #[error("reading TLS {kind} {}: {source}", path.display())]
    Read {
        kind: &'static str,
        path: PathBuf,
        source: io::Error,
    },

    /// The certificate chain PEM was empty or unparseable.
    #[error("parsing TLS certificate {}: {reason}", path.display())]
    Cert { path: PathBuf, reason: String },

    /// The private key PEM held no usable key.
    #[error("parsing TLS private key {}: {reason}", path.display())]
    Key { path: PathBuf, reason: String },

    /// The CA bundle PEM was empty or unparseable.
    #[error("parsing TLS CA bundle {}: {reason}", path.display())]
    Ca { path: PathBuf, reason: String },

    /// rustls rejected the assembled server config (bad key/cert pairing, etc).
    #[error("building the rustls server config: {0}")]
    RustlsConfig(String),
}

// ---------------------------------------------------------------------------
// Paths + material
// ---------------------------------------------------------------------------

/// The three `[tls]` file paths, as loaded from config.
#[derive(Debug, Clone)]
pub struct TlsPaths {
    /// This coordinator's leaf certificate chain (PEM).
    pub cert: PathBuf,
    /// This coordinator's private key (PEM).
    pub key: PathBuf,
    /// The cluster CA bundle used to verify peers (PEM).
    pub ca: PathBuf,
}

/// One immutable generation of parsed mTLS material.
///
/// Holds the raw PEM bytes (so an outbound `ClientTlsConfig` can be rebuilt at
/// dial time) and a pre-assembled [`rustls::ServerConfig`] (so a handshake is a
/// cheap `Arc` clone, not a per-connection re-parse). Published as
/// `Arc<TlsMaterial>` through the [`TlsStore`]'s [`ArcSwap`].
pub struct TlsMaterial {
    ca_pem: Vec<u8>,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    server_config: Arc<ServerConfig>,
    /// The leaf's `notAfter` as a Unix timestamp (seconds), when parseable.
    not_after_unix: Option<i64>,
}

impl TlsMaterial {
    /// The CA bundle PEM (trust root for outbound `ClientTlsConfig`).
    pub fn ca_pem(&self) -> &[u8] {
        &self.ca_pem
    }

    /// This node's leaf certificate chain PEM (client identity for outbound).
    pub fn cert_pem(&self) -> &[u8] {
        &self.cert_pem
    }

    /// This node's private key PEM.
    pub fn key_pem(&self) -> &[u8] {
        &self.key_pem
    }

    /// The current server config for a fresh handshake. Cheap `Arc` clone.
    pub fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.server_config)
    }

    /// The served leaf's `notAfter` (Unix seconds), when parseable.
    pub fn not_after_unix(&self) -> Option<i64> {
        self.not_after_unix
    }

    /// The subject of this material's own leaf certificate (ADR 0037 §6): the
    /// common name (a coordinator machine identity) and organizational unit (the
    /// certificate-profile marker). `None` fields when the leaf carries neither.
    /// Used to wire the machine-identity binding and to run the startup profile
    /// lint.
    pub fn leaf_subject(&self) -> LeafSubject {
        parse_leaf_subject(&self.cert_pem).unwrap_or_default()
    }

    /// Assemble parsed material from the three PEM blobs.
    ///
    /// Mirrors tonic's own server-TLS assembly (rustls-pemfile decode, a
    /// [`WebPkiClientVerifier`] built from the CA, `with_single_cert`, `h2`
    /// ALPN) so behaviour matches the previous `ServerTlsConfig` path exactly —
    /// only now the config is rebuildable on demand. Client auth is
    /// **mandatory** (no `allow_unauthenticated`): a handshake with no client
    /// cert is refused, preserving ADR 0011's no-unauthenticated-peer rule.
    pub fn from_pem(
        paths: &TlsPaths,
        ca_pem: Vec<u8>,
        cert_pem: Vec<u8>,
        key_pem: Vec<u8>,
    ) -> Result<TlsMaterial, TlsError> {
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

        let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut Cursor::new(&key_pem))
            .map_err(|e| TlsError::Key {
                path: paths.key.clone(),
                reason: e.to_string(),
            })?
            .ok_or_else(|| TlsError::Key {
                path: paths.key.clone(),
                reason: "no PKCS#8/PKCS#1/SEC1 private key found in PEM".to_string(),
            })?;

        let mut roots = RootCertStore::empty();
        for ca in rustls_pemfile::certs(&mut Cursor::new(&ca_pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TlsError::Ca {
                path: paths.ca.clone(),
                reason: e.to_string(),
            })?
        {
            roots.add(ca).map_err(|e| TlsError::Ca {
                path: paths.ca.clone(),
                reason: e.to_string(),
            })?;
        }
        if roots.is_empty() {
            return Err(TlsError::Ca {
                path: paths.ca.clone(),
                reason: "no CA certificates found in PEM".to_string(),
            });
        }

        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| TlsError::RustlsConfig(e.to_string()))?;

        // Pin the ring provider explicitly (the workspace deliberately excludes
        // aws-lc-rs) rather than relying on process-default provider
        // resolution, so config assembly can never panic on an ambiguous or
        // absent default.
        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| TlsError::RustlsConfig(e.to_string()))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain.clone(), key)
            .map_err(|e| TlsError::RustlsConfig(e.to_string()))?;
        config.alpn_protocols.push(b"h2".to_vec());

        let not_after_unix = leaf_not_after_unix(&cert_chain[0]);

        Ok(TlsMaterial {
            ca_pem,
            cert_pem,
            key_pem,
            server_config: Arc::new(config),
            not_after_unix,
        })
    }
}

/// Read the leaf's `notAfter` as Unix seconds. Best-effort: the TLS layer has
/// already validated the chain, so a parse miss here only skips the gauge.
fn leaf_not_after_unix(leaf: &CertificateDer<'_>) -> Option<i64> {
    x509_parser::parse_x509_certificate(leaf.as_ref())
        .ok()
        .map(|(_, cert)| cert.validity().not_after.timestamp())
}

/// A certificate leaf's subject fields relevant to the ADR 0037 §6 profile
/// convention: the common name (`CN`) and organizational unit (`OU`).
///
/// The profile convention as implemented: coordinator *machine* leaves carry
/// `OU=coppice-coordinator` with `CN` = the stable machine identity;
/// operator-profile leaves carry `OU=coppice-operator`; agent leaves carry no
/// `OU` (their `CN` remains the compute-node id).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeafSubject {
    /// The subject common name, if present.
    pub common_name: Option<String>,
    /// The subject organizational unit, if present — the profile marker.
    pub org_unit: Option<String>,
}

/// Parse the `CN`/`OU` out of a leaf certificate chain's first (leaf) cert.
pub fn parse_leaf_subject(cert_pem: &[u8]) -> Option<LeafSubject> {
    let leaf = rustls_pemfile::certs(&mut Cursor::new(cert_pem))
        .next()?
        .ok()?;
    parse_leaf_subject_der(leaf.as_ref())
}

/// Parse the `CN`/`OU` out of a DER-encoded leaf certificate.
pub fn parse_leaf_subject_der(der: &[u8]) -> Option<LeafSubject> {
    let (_, cert) = x509_parser::parse_x509_certificate(der).ok()?;
    let common_name = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(str::to_string);
    let org_unit = cert
        .subject()
        .iter_organizational_unit()
        .next()
        .and_then(|ou| ou.as_str().ok())
        .map(str::to_string);
    Some(LeafSubject {
        common_name,
        org_unit,
    })
}

// ---------------------------------------------------------------------------
// The reload store
// ---------------------------------------------------------------------------

/// A per-file `(mtime, len)` fingerprint used to skip a re-parse when nothing
/// changed on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fingerprint {
    cert: (Option<SystemTime>, u64),
    key: (Option<SystemTime>, u64),
    ca: (Option<SystemTime>, u64),
}

/// The live TLS material store: an [`ArcSwap`] of the current [`TlsMaterial`]
/// plus the source paths and a change-detection fingerprint.
///
/// Cheap to clone the `Arc<TlsStore>`; reads ([`current`](Self::current)) are
/// lock-free.
pub struct TlsStore {
    paths: TlsPaths,
    current: ArcSwap<TlsMaterial>,
    /// Last fingerprint that produced the current material. Only advanced on a
    /// successful swap (or an initial load), so a broken write is retried every
    /// poll until it parses cleanly.
    fingerprint: Mutex<Fingerprint>,
}

impl TlsStore {
    /// Load the initial material from `paths`. Fails fast if any file is
    /// missing or unparseable — a coordinator with no valid TLS material must
    /// not start (ADR 0011: no insecure fallback).
    pub fn load(paths: TlsPaths) -> Result<Arc<TlsStore>, TlsError> {
        let (material, fingerprint) = Self::read_and_parse(&paths)?;
        set_expiry_gauge(&material);
        Ok(Arc::new(TlsStore {
            paths,
            current: ArcSwap::from_pointee(material),
            fingerprint: Mutex::new(fingerprint),
        }))
    }

    /// Build a store from PEM bytes already in memory, bypassing the
    /// filesystem. The paths are recorded for error messages only; [`reload`]
    /// against such a store re-reads those paths, so this is intended for tests
    /// and in-memory material.
    ///
    /// [`reload`]: TlsStore::reload
    pub fn from_pem(
        paths: TlsPaths,
        ca_pem: Vec<u8>,
        cert_pem: Vec<u8>,
        key_pem: Vec<u8>,
    ) -> Result<Arc<TlsStore>, TlsError> {
        let material = TlsMaterial::from_pem(&paths, ca_pem, cert_pem, key_pem)?;
        set_expiry_gauge(&material);
        let fingerprint = fingerprint_of(&paths);
        Ok(Arc::new(TlsStore {
            paths,
            current: ArcSwap::from_pointee(material),
            fingerprint: Mutex::new(fingerprint),
        }))
    }

    /// The paths this store watches.
    pub fn paths(&self) -> &TlsPaths {
        &self.paths
    }

    /// The current material. Lock-free; call at every handshake and every
    /// outbound dial so rotations are picked up.
    pub fn current(&self) -> Arc<TlsMaterial> {
        self.current.load_full()
    }

    /// Re-read the source files and swap in freshly-parsed material.
    ///
    /// Returns `Ok(true)` when new material was swapped in, `Ok(false)` when
    /// nothing on disk changed since the last successful load, and `Err` when a
    /// file could not be read or parsed. On `Err` the previous material keeps
    /// serving and the fingerprint is left unchanged, so the next poll retries
    /// the same (still-broken, or now-fixed) files rather than latching a bad
    /// state. This is the guarantee that a half-written cert can never take
    /// down serving.
    pub fn reload(&self) -> Result<bool, TlsError> {
        let fingerprint = fingerprint_of(&self.paths);
        if *self.fingerprint.lock().expect("tls fingerprint poisoned") == fingerprint {
            return Ok(false);
        }

        let (material, fingerprint) = Self::read_and_parse(&self.paths)?;
        set_expiry_gauge(&material);
        self.current.store(Arc::new(material));
        *self.fingerprint.lock().expect("tls fingerprint poisoned") = fingerprint;
        metrics::counter!(RELOADS).increment(1);
        Ok(true)
    }

    /// Read the three files and parse them into material plus the fingerprint
    /// captured at read time.
    fn read_and_parse(paths: &TlsPaths) -> Result<(TlsMaterial, Fingerprint), TlsError> {
        let cert_pem = read(&paths.cert, "certificate")?;
        let key_pem = read(&paths.key, "private key")?;
        let ca_pem = read(&paths.ca, "CA certificate")?;
        // Fingerprint after reading, so a write racing between stat and read is
        // caught on the next poll (the fingerprint we store reflects the bytes
        // we actually parsed).
        let fingerprint = fingerprint_of(paths);
        let material = TlsMaterial::from_pem(paths, ca_pem, cert_pem, key_pem)?;
        Ok((material, fingerprint))
    }
}

fn read(path: &std::path::Path, kind: &'static str) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::Read {
        kind,
        path: path.to_path_buf(),
        source,
    })
}

fn fingerprint_of(paths: &TlsPaths) -> Fingerprint {
    Fingerprint {
        cert: stat(&paths.cert),
        key: stat(&paths.key),
        ca: stat(&paths.ca),
    }
}

/// `(mtime, len)` of a file, or `(None, 0)` if it cannot be stat'd (missing
/// mid-rotation) — an absent file simply reads as "changed" next poll.
fn stat(path: &std::path::Path) -> (Option<SystemTime>, u64) {
    match std::fs::metadata(path) {
        Ok(m) => (m.modified().ok(), m.len()),
        Err(_) => (None, 0),
    }
}

fn set_expiry_gauge(material: &TlsMaterial) {
    if let Some(ts) = material.not_after_unix {
        metrics::gauge!(CERT_NOT_AFTER).set(ts as f64);
    }
}

// ---------------------------------------------------------------------------
// The reload task
// ---------------------------------------------------------------------------

/// How the reload task is driven.
#[derive(Debug, Clone)]
pub struct ReloadOptions {
    /// How often to poll the source files' mtimes for a change.
    pub poll_interval: Duration,
    /// Install a `SIGHUP` handler that forces an immediate reload. The daemon
    /// path sets this `true`; callers that run several replicas in one process
    /// (integration tests, `coppice dev` is one process but one coordinator)
    /// leave it `false`, mirroring how `runtime` gates its own signal install.
    pub sighup: bool,
}

impl Default for ReloadOptions {
    fn default() -> Self {
        ReloadOptions {
            poll_interval: Duration::from_secs(2),
            sighup: false,
        }
    }
}

/// Spawn the background task that keeps `store` fresh: an mtime poll every
/// `opts.poll_interval` and, when `opts.sighup` is set on a Unix daemon, a
/// `SIGHUP` that forces an immediate reload. A read/parse failure is logged and
/// the old material keeps serving; identical consecutive failures are logged
/// once to avoid flooding while a cert stays broken.
pub fn spawn_reload_task(store: Arc<TlsStore>, opts: ReloadOptions) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(opts.poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        #[cfg(unix)]
        let mut sighup = if opts.sighup {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::error!(error = %e, "tls reload: failed to install SIGHUP handler");
                    None
                }
            }
        } else {
            None
        };

        let mut last_failure: Option<String> = None;
        loop {
            #[cfg(unix)]
            let forced = {
                // `recv()` on `None` never resolves, so a store without SIGHUP
                // just polls; with it, either arm triggers a reload.
                let hup = async {
                    match sighup.as_mut() {
                        Some(s) => {
                            s.recv().await;
                        }
                        None => std::future::pending::<()>().await,
                    }
                };
                tokio::select! {
                    _ = ticker.tick() => false,
                    _ = hup => true,
                }
            };
            #[cfg(not(unix))]
            let forced = {
                ticker.tick().await;
                false
            };

            if forced {
                tracing::info!("tls reload: SIGHUP received, re-reading cert/key/CA");
            }
            match store.reload() {
                Ok(true) => {
                    last_failure = None;
                    let not_after = store.current().not_after_unix();
                    tracing::info!(
                        cert = %store.paths().cert.display(),
                        not_after_unix = ?not_after,
                        "tls reload: swapped in new certificate material"
                    );
                }
                Ok(false) => {
                    if forced {
                        tracing::info!(
                            "tls reload: SIGHUP forced a reload but files were unchanged"
                        );
                    }
                }
                Err(e) => {
                    metrics::counter!(RELOAD_FAILURES).increment(1);
                    let msg = e.to_string();
                    if last_failure.as_deref() != Some(msg.as_str()) {
                        tracing::warn!(
                            error = %msg,
                            "tls reload: could not load new material; keeping the current \
                             certificate (a half-written file is retried on the next poll)"
                        );
                        last_failure = Some(msg);
                    }
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Endpoint verification (ADR 0037 §6)
// ---------------------------------------------------------------------------

/// Dial `addr` (`host:port`) over mTLS using `store`'s current material and
/// return the *serving* leaf's subject (ADR 0037 §6 endpoint verification).
///
/// The leader calls this before admitting a learner: it must confirm the
/// advertised endpoint actually presents the requester's machine identity (a
/// claimed node id without the matching CA-attested subject is not proof of
/// ownership). The handshake trusts the cluster CA and presents this node's own
/// leaf, so it exercises the same mutual-auth the raft listener requires; the
/// peer chain is validated by rustls against the CA before we read its subject.
pub async fn read_serving_leaf(store: &TlsStore, addr: &str) -> Result<LeafSubject, String> {
    let material = store.current();
    let host = addr
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(addr)
        .to_string();

    let mut roots = RootCertStore::empty();
    for ca in rustls_pemfile::certs(&mut Cursor::new(material.ca_pem()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parsing CA for endpoint verification: {e}"))?
    {
        roots
            .add(ca)
            .map_err(|e| format!("adding CA for endpoint verification: {e}"))?;
    }

    let cert_chain: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut Cursor::new(material.cert_pem()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("parsing client cert for endpoint verification: {e}"))?;
    let key = rustls_pemfile::private_key(&mut Cursor::new(material.key_pem()))
        .map_err(|e| format!("parsing client key for endpoint verification: {e}"))?
        .ok_or_else(|| "no client private key for endpoint verification".to_string())?;

    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("building endpoint-verification client config: {e}"))?
        .with_root_certificates(roots)
        .with_client_auth_cert(cert_chain, key)
        .map_err(|e| format!("building endpoint-verification client identity: {e}"))?;

    let server_name = ServerName::try_from(host.clone())
        .map_err(|e| format!("invalid endpoint host {host}: {e}"))?
        .to_owned();

    let tcp = tokio::time::timeout(HANDSHAKE_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| format!("timed out connecting to endpoint {addr}"))?
        .map_err(|e| format!("connecting to endpoint {addr}: {e}"))?;
    let connector = TlsConnector::from(Arc::new(config));
    let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, connector.connect(server_name, tcp))
        .await
        .map_err(|_| format!("timed out in TLS handshake to endpoint {addr}"))?
        .map_err(|e| format!("TLS handshake to endpoint {addr}: {e}"))?;

    let (_, session) = stream.get_ref();
    let leaf = session
        .peer_certificates()
        .and_then(|chain| chain.first())
        .ok_or_else(|| format!("endpoint {addr} presented no certificate"))?;
    parse_leaf_subject_der(leaf.as_ref())
        .ok_or_else(|| format!("endpoint {addr} leaf subject did not parse"))
}

// ---------------------------------------------------------------------------
// The connection-time server acceptor
// ---------------------------------------------------------------------------

/// Handshakes that stall are dropped after this bound so a slow or malicious
/// client cannot leak accept tasks.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// Bound on completed-but-not-yet-consumed handshakes buffered ahead of the
/// tonic server. Ample at coordinator connection rates.
const INCOMING_BUFFER: usize = 128;

/// A stream of established mTLS connections for `Server::serve_with_incoming`.
///
/// Each item is a [`tokio_rustls::server::TlsStream`] whose peer already
/// presented a CA-valid client certificate; tonic's blanket `Connected` impl
/// turns it into a `TlsConnectInfo`, so `request.peer_certs()` and
/// `remote_addr()` work exactly as under the built-in TLS path. The item type
/// is `Result<_, io::Error>` to satisfy tonic's incoming bound, but only `Ok`
/// items are ever produced — a failed handshake closes that one socket and is
/// logged, never surfaced as a stream error that could stop the server.
pub struct TlsIncoming {
    inner: ReceiverStream<Result<TlsStream<TcpStream>, io::Error>>,
}

impl Stream for TlsIncoming {
    type Item = Result<TlsStream<TcpStream>, io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Serve TLS connections off `listener`, resolving the server certificate from
/// `store` at handshake time.
///
/// Spawns a detached accept loop that performs each rustls handshake on its own
/// task (so one slow handshake never blocks accepting others) with the material
/// current *at that moment*. The returned [`TlsIncoming`] feeds
/// `Server::builder().add_service(..).serve_with_incoming[_shutdown]` — do
/// **not** also call `.tls_config(..)` on that builder; TLS is already
/// terminated here. When the tonic server stops and drops the incoming stream,
/// the accept loop observes the closed channel and exits.
pub fn serve(listener: TcpListener, store: Arc<TlsStore>) -> TlsIncoming {
    let (tx, rx) = mpsc::channel(INCOMING_BUFFER);
    tokio::spawn(accept_loop(listener, store, tx));
    TlsIncoming {
        inner: ReceiverStream::new(rx),
    }
}

async fn accept_loop(
    listener: TcpListener,
    store: Arc<TlsStore>,
    tx: mpsc::Sender<Result<TlsStream<TcpStream>, io::Error>>,
) {
    loop {
        tokio::select! {
            // The consumer (tonic server) dropped the incoming stream: stop
            // accepting.
            _ = tx.closed() => break,
            accepted = listener.accept() => match accepted {
                Ok((tcp, peer)) => {
                    // Connection-time resolution: read the CURRENT material now,
                    // so a rotation between accepts takes effect on new
                    // connections without touching the ones already handshaking.
                    let config = store.current().server_config();
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let acceptor = TlsAcceptor::from(config);
                        match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                            Ok(Ok(stream)) => {
                                // Ignore send failure: it only means the server
                                // shut down between accept and handshake.
                                let _ = tx.send(Ok(stream)).await;
                            }
                            Ok(Err(e)) => {
                                // No/invalid client cert, protocol error, etc:
                                // refuse this one connection, keep serving.
                                tracing::debug!(%peer, error = %e, "tls handshake failed");
                            }
                            Err(_) => {
                                tracing::debug!(%peer, "tls handshake timed out");
                            }
                        }
                    });
                }
                Err(e) => {
                    // A per-accept error (e.g. transient fd exhaustion) must not
                    // kill the listener; back off a touch to avoid a hot loop.
                    tracing::warn!(error = %e, "tls listener: tcp accept error");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            },
        }
    }
}

#[cfg(test)]
mod tests;
