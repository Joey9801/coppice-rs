//! The enrolling machine's side of `POST /api/v1/enroll` (ADR 0037 §4).
//!
//! A machine that holds a role-scoped token and the cluster's address, and
//! nothing else, becomes a machine that speaks cluster-CA mTLS. That is the
//! whole of this module: [`ensure_enrolled`] generates a keypair and CSR, posts
//! them with the token, and installs the returned leaf into the `[tls]` paths a
//! [`TlsStore`](coppice_tls::TlsStore) watches.
//!
//! Three rules shape it, all from §4.
//!
//! **The endpoint's posture is declared, never inferred.** [`validate_endpoint`]
//! is the single decision: `https` is always verified against system roots —
//! `insecure` does not weaken it, there is no pin and no cluster-CA
//! distribution — and `http` is refused outright unless the operator wrote
//! `insecure = true`, which is documented dev/test-only because it puts the
//! token on the wire in the clear. Both daemons' config validation calls this
//! same function, so a bad posture fails at startup rather than at first
//! enrollment.
//!
//! **Verification failure must precede the token.** This is inherent rather
//! than defended: the TLS handshake completes before any request byte is
//! written, so an untrusted or misnamed server certificate aborts the
//! connection with the token still in memory. `posture.rs` proves it against a
//! capture server rather than trusting the argument.
//!
//! **The token is never rendered.** [`Secret`] has a hand-written `Debug`, no
//! type here derives `Debug` over it, no tracing field carries it, and it
//! travels only in an `Authorization: Bearer` header — never a query parameter,
//! and never the request body's `token` field, which exists for other clients
//! and stays `None` here.
//!
//! Redirects are refused rather than followed: a 3xx would hand a
//! `Authorization` header to whatever host the response named.

use std::path::{Path, PathBuf};
use std::time::Duration;

use coppice_core::id::{MachineId, NodeId};
use coppice_tls::pki;
use coppice_tls::TlsPaths;
use serde::{Deserialize, Serialize};

use crate::{EnrollRequest, EnrollResponse, ENROLL_PATH};

/// How long a single enrollment attempt may take, handshake included. Long
/// enough for a leader proxy hop and an argon2 token scan, short enough that a
/// black-holed endpoint does not hang daemon startup indefinitely.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// The secret
// ---------------------------------------------------------------------------

/// An enrollment-token secret. A newtype purely so it cannot be rendered:
/// `Debug` and `Display` are hand-written to redact, and the inner string is
/// reachable only through [`expose`](Secret::expose), which is deliberately
/// awkward to type at a `tracing` field.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(secret: impl Into<String>) -> Secret {
        Secret(secret.into())
    }

    /// The raw secret. The only caller is the `Authorization` header assembly.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Secret {
        Secret(value)
    }
}

/// Where the token comes from. A path is the preferred production form — it
/// keeps the secret out of a config file that gets committed, diffed, and
/// attached to support bundles (ADR 0020) — and inline exists for development.
#[derive(Debug, Clone)]
pub enum TokenSource {
    /// The secret written straight into the config file.
    Inline(Secret),
    /// A file holding nothing but the secret. Read at enrollment time, not at
    /// config load, so a token delivered late (a cloud-init drop, a mounted
    /// secret) is still found, and trailing whitespace is trimmed.
    Path(PathBuf),
}

impl TokenSource {
    /// Resolve to the secret itself.
    fn read(&self) -> Result<Secret, EnrollClientError> {
        match self {
            TokenSource::Inline(secret) => Ok(secret.clone()),
            TokenSource::Path(path) => {
                let raw = std::fs::read_to_string(path).map_err(|source| {
                    EnrollClientError::ReadToken {
                        path: path.clone(),
                        source,
                    }
                })?;
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    return Err(EnrollClientError::EmptyToken { path: path.clone() });
                }
                Ok(Secret::new(trimmed))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Endpoint posture
// ---------------------------------------------------------------------------

/// A rejected enrollment endpoint (ADR 0037 §4 transport bullet).
#[derive(Debug, thiserror::Error)]
pub enum EndpointError {
    /// A plain-HTTP endpoint without the conspicuous opt-in.
    #[error(
        "enrollment.endpoint {endpoint:?} is plain HTTP, which puts the enrollment token on the \
         wire in the clear; set enrollment.insecure = true to accept that (development and test \
         only), or use an https endpoint"
    )]
    InsecureNotOptedIn { endpoint: String },

    /// Anything that is not `http` or `https`.
    #[error("enrollment.endpoint {endpoint:?} must be an http:// or https:// URL")]
    UnsupportedScheme { endpoint: String },
}

/// Decide whether `endpoint` may be enrolled against under `insecure`
/// (ADR 0037 §4) — the single posture decision, shared by the agent's and the
/// coordinator's config validation so neither can drift from the other.
///
/// - `https://…` is always accepted and is always verified against system
///   roots. `insecure` does **not** weaken it: the flag is about tolerating a
///   cleartext endpoint, not about tolerating an unverified one, and there is
///   no trust-on-first-use and no pinned cluster CA.
/// - `http://…` is accepted only with `insecure = true`, and the refusal names
///   the flag so an operator who meant it can opt in deliberately.
/// - Anything else is refused.
pub fn validate_endpoint(endpoint: &str, insecure: bool) -> Result<(), EndpointError> {
    let lower = endpoint.trim().to_ascii_lowercase();
    if lower.starts_with("https://") {
        Ok(())
    } else if lower.starts_with("http://") {
        if insecure {
            Ok(())
        } else {
            Err(EndpointError::InsecureNotOptedIn {
                endpoint: endpoint.to_string(),
            })
        }
    } else {
        Err(EndpointError::UnsupportedScheme {
            endpoint: endpoint.to_string(),
        })
    }
}

/// The `[enrollment]` table (ADR 0037 §4), as both daemons parse it.
///
/// Defined here rather than in either daemon's config module because the table
/// is identical on both sides and its validation is a security decision: one
/// definition, one [`validate`](EnrollmentConfig::validate), no chance of the
/// agent and the coordinator disagreeing about what `insecure` means.
///
/// `Debug` is derived, which is safe because [`Secret`] redacts itself.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollmentConfig {
    /// The cluster's client-listener base URL, e.g.
    /// `https://coppice.example.com:7070`. `/api/v1/enroll` is appended.
    pub endpoint: String,

    /// The token written inline. Mutually exclusive with
    /// [`token_path`](Self::token_path); exactly one is required.
    #[serde(default)]
    pub token: Option<Secret>,

    /// A file holding the token — the preferred form, so the secret never
    /// enters the config file.
    #[serde(default)]
    pub token_path: Option<PathBuf>,

    /// Accept a plain-HTTP endpoint. Development and test only; see
    /// [`validate_endpoint`].
    #[serde(default)]
    pub insecure: bool,
}

impl EnrollmentConfig {
    /// Reject an endpoint posture or token configuration that cannot work, at
    /// config-load time rather than at first enrollment.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_endpoint(&self.endpoint, self.insecure)?;
        match (&self.token, &self.token_path) {
            (Some(_), None) | (None, Some(_)) => Ok(()),
            (Some(_), Some(_)) => Err(ConfigError::AmbiguousToken),
            (None, None) => Err(ConfigError::NoToken),
        }
    }

    /// The token source. Only meaningful after [`validate`](Self::validate);
    /// an unvalidated config with neither field set yields `None`.
    pub fn token_source(&self) -> Option<TokenSource> {
        match (&self.token, &self.token_path) {
            (Some(secret), _) => Some(TokenSource::Inline(secret.clone())),
            (None, Some(path)) => Some(TokenSource::Path(path.clone())),
            (None, None) => None,
        }
    }

    /// How this configuration describes itself in a startup log line: the
    /// endpoint, the declared posture, and *which kind* of token source is
    /// configured — never the token.
    pub fn token_kind(&self) -> &'static str {
        match (&self.token, &self.token_path) {
            (Some(_), _) => "inline",
            (None, Some(_)) => "path",
            (None, None) => "none",
        }
    }
}

/// An unusable `[enrollment]` table.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The endpoint's scheme and the `insecure` flag disagree.
    #[error(transparent)]
    Endpoint(#[from] EndpointError),

    /// Both token forms were given.
    #[error("set exactly one of enrollment.token or enrollment.token_path, not both")]
    AmbiguousToken,

    /// Neither token form was given.
    #[error(
        "enrollment requires a token: set enrollment.token_path (preferred — the secret stays out \
         of the config file) or enrollment.token"
    )]
    NoToken,
}

// ---------------------------------------------------------------------------
// The client
// ---------------------------------------------------------------------------

/// The identity an enrollee claims. The cluster dictates the subject either
/// way; this only says *which* role's subject is being asked for, and the
/// token's own role has the final word (a mismatch is refused uniformly,
/// server-side).
#[derive(Debug, Clone, Copy)]
pub enum Claim {
    /// An agent claiming its configured node id (ADR 0011): the issued leaf's
    /// CN comes back equal to it.
    Node(NodeId),
    /// A coordinator claiming the machine identity it minted for itself
    /// (ADR 0037 §7).
    Machine(MachineId),
}

/// What [`ensure_enrolled`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// A usable leaf was already on disk; no network call was made. This is
    /// what makes startup enrollment idempotent across restarts.
    AlreadyEnrolled,
    /// A leaf was obtained and installed.
    Enrolled,
}

/// A failure on the enrolling machine's side.
#[derive(Debug, thiserror::Error)]
pub enum EnrollClientError {
    /// The configured endpoint is not one this client may use.
    #[error(transparent)]
    Endpoint(#[from] EndpointError),

    /// The token file could not be read.
    #[error("reading the enrollment token from {}: {source}", path.display())]
    ReadToken {
        path: PathBuf,
        source: std::io::Error,
    },

    /// The token file held nothing.
    #[error("the enrollment token file {} is empty", path.display())]
    EmptyToken { path: PathBuf },

    /// Generating the keypair or CSR failed.
    #[error(transparent)]
    Csr(#[from] pki::CsrError),

    /// The HTTP client could not be built.
    #[error("building the enrollment HTTP client: {0}")]
    BuildClient(String),

    /// The request never completed — including a TLS verification failure,
    /// which by construction happens before any token byte is sent.
    #[error("enrolling at {endpoint}: {source}")]
    Transport {
        endpoint: String,
        #[source]
        source: reqwest::Error,
    },

    /// The endpoint answered with a redirect. Never followed: the next hop
    /// would receive the `Authorization` header (ADR 0037 §4).
    #[error(
        "the enrollment endpoint answered {status} with a redirect; refusing to resend the token \
         to another host"
    )]
    Redirected { status: u16 },

    /// The endpoint refused the enrollment. A credential refusal (401) stays
    /// deliberately opaque — one uniform refusal for every credential
    /// failure — but an *operational* refusal (429, 503, 5xx) carries the
    /// server's stated reason: discarding it would force exactly the log
    /// archaeology ADR 0037 §9 removes.
    #[error("the enrollment endpoint refused the request: HTTP {status}{detail}")]
    Refused { status: u16, detail: String },

    /// A 200 whose body was not an [`EnrollResponse`].
    #[error("the enrollment endpoint returned an unreadable body: {0}")]
    BadResponse(String),

    /// The issued material could not be written to the `[tls]` paths.
    #[error(transparent)]
    Install(#[from] pki::CustodyError),
}

/// A configured client for one enrollment endpoint.
///
/// Holds the `reqwest` client rather than rebuilding it per call, so the root
/// store is assembled once, and centralises the two transport rules: no
/// redirect following, and a bounded attempt.
pub struct EnrollClient {
    http: reqwest::Client,
    endpoint: String,
}

impl EnrollClient {
    /// Build a client for `endpoint`, verifying `https` against the platform's
    /// system roots (ADR 0037 §4). Refuses an endpoint [`validate_endpoint`]
    /// rejects, so a misconfigured posture cannot reach the network even if
    /// config validation was somehow skipped.
    pub fn new(endpoint: &str, insecure: bool) -> Result<EnrollClient, EnrollClientError> {
        Self::build(endpoint, insecure, None)
    }

    /// **Test only.** As [`new`](Self::new), plus `root_ca_pem` as an
    /// additional trust anchor.
    ///
    /// Production never calls this: a cluster's own CA is exactly what an
    /// enrolling machine does not yet have, which is why §4 puts enrollment on
    /// the system-root-verified public listener. It exists so the posture tests
    /// can stand up a real HTTPS endpoint under a throwaway CA instead of
    /// asserting against a mock.
    #[doc(hidden)]
    pub fn with_extra_root_ca(
        endpoint: &str,
        insecure: bool,
        root_ca_pem: &[u8],
    ) -> Result<EnrollClient, EnrollClientError> {
        Self::build(endpoint, insecure, Some(root_ca_pem))
    }

    fn build(
        endpoint: &str,
        insecure: bool,
        extra_root: Option<&[u8]>,
    ) -> Result<EnrollClient, EnrollClientError> {
        validate_endpoint(endpoint, insecure)?;

        let mut builder = reqwest::Client::builder()
            // A 3xx carrying an `Authorization: Bearer` header to a host the
            // response chose is exactly the leak §4 forbids. `none()` makes the
            // redirect visible to us as a status, which `enroll` turns into an
            // error rather than a hop.
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .use_rustls_tls()
            .tls_built_in_root_certs(true);

        if let Some(pem) = extra_root {
            let cert = reqwest::Certificate::from_pem(pem)
                .map_err(|e| EnrollClientError::BuildClient(e.to_string()))?;
            builder = builder.add_root_certificate(cert);
        }

        let http = builder
            .build()
            .map_err(|e| EnrollClientError::BuildClient(e.to_string()))?;
        Ok(EnrollClient {
            http,
            endpoint: endpoint.trim_end_matches('/').to_string(),
        })
    }

    /// The endpoint this client posts to, `/api/v1/enroll` included.
    fn url(&self) -> String {
        format!("{}{ENROLL_PATH}", self.endpoint)
    }

    /// Post one CSR with one token and return the issued material.
    ///
    /// The token goes in the `Authorization` header only. The body's `token`
    /// field stays `None`: the wire contract accepts it, but a redacted body
    /// field is still a body field, and headers are what proxies and access
    /// logs are configured to omit.
    pub async fn enroll(
        &self,
        token: &Secret,
        csr_pem: &[u8],
        claim: Claim,
        sans: &[String],
    ) -> Result<EnrollResponse, EnrollClientError> {
        let (node_id, machine_id) = match claim {
            Claim::Node(node) => (Some(node), None),
            Claim::Machine(machine) => (None, Some(machine)),
        };
        let body = EnrollRequest {
            csr_pem: String::from_utf8_lossy(csr_pem).into_owned(),
            token: None,
            node_id,
            machine_id,
            sans: sans.to_vec(),
        };

        let response = self
            .http
            .post(self.url())
            .bearer_auth(token.expose())
            .json(&body)
            .send()
            .await
            .map_err(|source| EnrollClientError::Transport {
                endpoint: self.endpoint.clone(),
                source,
            })?;

        let status = response.status();
        if status.is_redirection() {
            return Err(EnrollClientError::Redirected {
                status: status.as_u16(),
            });
        }
        if !status.is_success() {
            // See `Refused`: the 401 body is the uniform refusal and stays
            // unread; any other refusal's body is the server's reason,
            // surfaced (bounded) so a stuck convergence loop names its
            // blocker in status output instead of requiring server logs.
            let detail = if status.as_u16() == 401 {
                String::new()
            } else {
                match response.text().await {
                    Ok(body) if !body.is_empty() => {
                        let mut body = body;
                        body.truncate(512);
                        format!(": {body}")
                    }
                    _ => String::new(),
                }
            };
            return Err(EnrollClientError::Refused {
                status: status.as_u16(),
                detail,
            });
        }
        response
            .json::<EnrollResponse>()
            .await
            .map_err(|e| EnrollClientError::BadResponse(e.to_string()))
    }
}

/// Whether `paths` already holds a leaf this machine can serve and dial with.
///
/// "Usable" means: all three files present, the leaf parses with a subject,
/// and it has **not expired**. The expiry check matters because renewal only
/// runs over an established mTLS session — an expired leaf cannot open one,
/// so a machine restarting past expiry has exactly one way back in:
/// enrollment. Treating the dead leaf as "enrolled" would strand the daemon
/// in a reconnect loop forever, token in hand, never using it. (An expired
/// leaf re-spends the token on restart; that is the correct cost, not a leak
/// — a *live* leaf still skips the network entirely.) It is deliberately not
/// a chain check: a live leaf a re-rooted cluster no longer trusts is still
/// renewal's problem, not enrollment's.
pub fn has_usable_leaf(paths: &TlsPaths) -> bool {
    fn present(path: &Path) -> bool {
        std::fs::metadata(path)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    }
    if !present(&paths.cert) || !present(&paths.key) || !present(&paths.ca) {
        return false;
    }
    let Ok(cert_pem) = std::fs::read(&paths.cert) else {
        return false;
    };
    if !coppice_tls::parse_leaf_subject(&cert_pem).is_some_and(|s| s.common_name.is_some()) {
        return false;
    }
    match coppice_tls::leaf_not_after_unix_pem(&cert_pem) {
        // An unreadable expiry on a parseable cert should not force a
        // re-enrollment loop; the daemon's own load will complain.
        None => true,
        Some(not_after) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            not_after > now
        }
    }
}

/// Obtain a cluster-signed leaf into `paths` if there is not one already
/// (ADR 0037 §4).
///
/// Idempotent by design: a machine that restarts with its leaf intact makes no
/// network call and never touches its token. Otherwise this is the whole
/// first-contact flow — generate a keypair and CSR, post them to the endpoint
/// with the token, and install the returned leaf and CA bundle through
/// [`pki::install_leaf_material`], which writes the key last so a crash
/// mid-install cannot leave a leaf without its key.
pub async fn ensure_enrolled(
    paths: &TlsPaths,
    config: &EnrollmentConfig,
    claim: Claim,
    sans: &[String],
) -> Result<Outcome, EnrollClientError> {
    let client = EnrollClient::new(&config.endpoint, config.insecure)?;
    let token = config.token_source().ok_or(EnrollClientError::EmptyToken {
        path: PathBuf::new(),
    })?;
    ensure_enrolled_with(paths, &client, &token, claim, sans).await
}

/// [`ensure_enrolled`] against an already-built client. Separated so the tests
/// can supply a client that trusts a throwaway CA, and so a caller that enrolls
/// repeatedly need not rebuild the root store.
pub async fn ensure_enrolled_with(
    paths: &TlsPaths,
    client: &EnrollClient,
    token: &TokenSource,
    claim: Claim,
    sans: &[String],
) -> Result<Outcome, EnrollClientError> {
    if has_usable_leaf(paths) {
        tracing::debug!(
            cert = %paths.cert.display(),
            "a usable leaf is already installed; skipping enrollment"
        );
        return Ok(Outcome::AlreadyEnrolled);
    }

    let secret = token.read()?;
    let (key_pem, csr_pem) = pki::generate_key_and_csr()?;

    tracing::info!(
        endpoint = %client.endpoint,
        claim = ?claim,
        "enrolling for a cluster-signed leaf (ADR 0037 §4)"
    );
    let issued = client.enroll(&secret, &csr_pem, claim, sans).await?;

    pki::install_leaf_material(
        paths,
        issued.ca_pem.as_bytes(),
        issued.cert_pem.as_bytes(),
        &key_pem,
    )?;
    tracing::info!(
        cert = %paths.cert.display(),
        "enrolled; the issued leaf and CA bundle are installed"
    );
    Ok(Outcome::Enrolled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn https_is_always_verified_whatever_the_insecure_flag_says() {
        validate_endpoint("https://coppice.example.com:7070", false).expect("https is accepted");
        validate_endpoint("https://coppice.example.com:7070", true)
            .expect("insecure does not make https invalid — it simply does not apply");
    }

    #[test]
    fn plain_http_needs_the_conspicuous_opt_in() {
        let err = validate_endpoint("http://10.0.0.1:7070", false)
            .expect_err("plain http without the flag is refused");
        let rendered = err.to_string();
        assert!(rendered.contains("enrollment.insecure"), "{rendered}");
        assert!(rendered.contains("test"), "{rendered}");

        validate_endpoint("http://10.0.0.1:7070", true).expect("the opt-in allows it");
    }

    #[test]
    fn other_schemes_are_refused_either_way() {
        for insecure in [false, true] {
            validate_endpoint("grpc://coppice.example.com", insecure)
                .expect_err("only http and https are enrollment transports");
            validate_endpoint("coppice.example.com:7070", insecure)
                .expect_err("a bare host is not a URL");
        }
    }

    #[test]
    fn exactly_one_token_form_is_required() {
        let base = EnrollmentConfig {
            endpoint: "https://coppice.example.com".to_string(),
            token: None,
            token_path: None,
            insecure: false,
        };

        let err = base.validate().expect_err("no token at all is refused");
        assert!(err.to_string().contains("token_path"), "{err}");

        let both = EnrollmentConfig {
            token: Some(Secret::new("cpk_inline")),
            token_path: Some(PathBuf::from("/etc/coppice/token")),
            ..base.clone()
        };
        both.validate().expect_err("both forms is ambiguous");

        let inline = EnrollmentConfig {
            token: Some(Secret::new("cpk_inline")),
            ..base.clone()
        };
        inline.validate().expect("inline alone is valid");
        assert_eq!(inline.token_kind(), "inline");

        let by_path = EnrollmentConfig {
            token_path: Some(PathBuf::from("/etc/coppice/token")),
            ..base
        };
        by_path.validate().expect("a path alone is valid");
        assert_eq!(by_path.token_kind(), "path");
    }

    #[test]
    fn nothing_holding_the_secret_renders_it() {
        let config = EnrollmentConfig {
            endpoint: "https://coppice.example.com".to_string(),
            token: Some(Secret::new("cpk_super_secret")),
            token_path: None,
            insecure: false,
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("cpk_"), "{rendered}");

        let source = config.token_source().unwrap();
        assert!(!format!("{source:?}").contains("cpk_"));
    }

    #[test]
    fn a_token_file_is_trimmed_and_an_empty_one_is_an_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("token");
        std::fs::write(&path, "  cpk_from_a_file\n").expect("write the token");
        let secret = TokenSource::Path(path.clone()).read().expect("read");
        assert_eq!(secret.expose(), "cpk_from_a_file");

        std::fs::write(&path, "\n \n").expect("write an empty token");
        TokenSource::Path(path)
            .read()
            .expect_err("empty is refused");

        TokenSource::Path(dir.path().join("absent"))
            .read()
            .expect_err("a missing file is refused");
    }

    #[test]
    fn an_incomplete_or_unparseable_trio_is_not_a_usable_leaf() {
        let dir = tempfile::tempdir().expect("temp dir");
        let paths = TlsPaths {
            cert: dir.path().join("node.crt"),
            key: dir.path().join("node.key"),
            ca: dir.path().join("ca.crt"),
        };
        assert!(!has_usable_leaf(&paths), "nothing on disk");

        let ca = pki::mint_root_ca().expect("mint a CA");
        let signer = pki::CaSigner::load(&ca.cert_pem, &ca.key_pem).expect("load the CA");
        let node = NodeId::new();
        let (cert_pem, key_pem) =
            pki::mint_agent_local(&signer, &node, &[]).expect("mint an agent leaf");

        std::fs::write(&paths.cert, &cert_pem).expect("write the leaf");
        assert!(!has_usable_leaf(&paths), "the key and CA are still missing");

        std::fs::write(&paths.key, &key_pem).expect("write the key");
        std::fs::write(&paths.ca, &ca.cert_pem).expect("write the CA");
        assert!(has_usable_leaf(&paths), "the full trio parses");

        std::fs::write(&paths.cert, b"not a certificate").expect("corrupt the leaf");
        assert!(!has_usable_leaf(&paths), "an unparseable leaf is unusable");
    }

    /// An expired leaf is NOT usable: renewal runs over an mTLS session the
    /// dead leaf can no longer open, so treating it as "enrolled" would
    /// strand the daemon reconnecting forever — enrollment is the only way
    /// back (ADR 0037 §4/§8).
    #[test]
    fn an_expired_leaf_is_not_usable() {
        let dir = tempfile::tempdir().expect("temp dir");
        let paths = TlsPaths {
            cert: dir.path().join("node.crt"),
            key: dir.path().join("node.key"),
            ca: dir.path().join("ca.crt"),
        };
        let ca = pki::mint_root_ca().expect("mint a CA");
        let signer = pki::CaSigner::load(&ca.cert_pem, &ca.key_pem).expect("load the CA");
        let node = NodeId::new();
        let (cert_pem, key_pem) =
            pki::mint_agent_local(&signer, &node, &[]).expect("mint an agent leaf");
        std::fs::write(&paths.key, &key_pem).expect("write the key");
        std::fs::write(&paths.ca, &ca.cert_pem).expect("write the CA");

        // Live leaf: usable.
        std::fs::write(&paths.cert, &cert_pem).expect("write the leaf");
        assert!(has_usable_leaf(&paths), "a live trio is usable");

        // The same subject, expired years ago: unusable, so startup enrolls.
        let expired = {
            let key = rcgen::KeyPair::generate().expect("keypair");
            let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, node.to_string());
            params.not_before = rcgen::date_time_ymd(2020, 1, 1);
            params.not_after = rcgen::date_time_ymd(2020, 1, 2);
            params.self_signed(&key).expect("self-sign").pem()
        };
        std::fs::write(&paths.cert, expired).expect("write the expired leaf");
        assert!(
            !has_usable_leaf(&paths),
            "an expired leaf cannot renew, so it must re-enroll"
        );
    }
}
