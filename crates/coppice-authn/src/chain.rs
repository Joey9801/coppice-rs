//! The authenticator chain: credentials in, [`Actor`] out.

use std::sync::Arc;

use coppice_tls::pki::{self, Profile};

use crate::actor;
use crate::metrics;
use crate::validator::{ValidateError, Validator};
use crate::{Actor, ActorExt, AuthMethod, AuthMode, OidcConfig};

/// What the HTTP edge pulled off one request.
///
/// Borrowed rather than owned: the extractor builds this from the request's
/// headers and connection extensions and it does not outlive the call.
#[derive(Debug, Clone, Copy, Default)]
pub struct Credentials<'a> {
    /// The `Authorization: Bearer` value, header prefix already stripped.
    pub bearer: Option<&'a str>,
    /// DER of the peer's leaf client certificate, if the connection presented
    /// one. The TLS layer has already established that the peer holds the
    /// matching private key; whether the certificate *means* anything to this
    /// cluster is decided here, against the cluster CA.
    pub peer_leaf_der: Option<&'a [u8]>,
}

/// Supplies the cluster CA bundle (PEM) as of now.
///
/// A closure and not a `Vec<u8>` because the bundle is replicated state that
/// changes under the edge's feet: `rotate-ca` publishes a two-root bundle and
/// later a one-root bundle, and an operator certificate presented in the
/// middle of that must be verified against whatever is committed at that
/// instant. `None` means there is no cluster CA yet (pre-formation).
pub type CaProvider = Arc<dyn Fn() -> Option<Vec<u8>> + Send + Sync>;

/// Supplies the current groups-claim name from replicated policy.
///
/// Same reasoning as [`CaProvider`]: `groups_claim` is `PolicyConfig`, so it
/// can change at runtime and the next request must see the new value.
pub type GroupsClaimProvider = Arc<dyn Fn() -> String + Send + Sync>;

/// A `CaProvider` that never yields a CA — for open-mode deployments with no
/// cluster PKI, and for tests.
pub fn no_ca() -> CaProvider {
    Arc::new(|| None)
}

/// A `GroupsClaimProvider` pinned to one name.
///
/// For tests and for any caller with no replicated view to read: a serving
/// coordinator builds the policy-backed provider instead
/// (`coppice_coordinator::runtime::groups_claim_provider`), so a `groups_claim`
/// change takes effect on the next request without a restart.
pub fn static_groups_claim(name: impl Into<String>) -> GroupsClaimProvider {
    let name = name.into();
    Arc::new(move || name.clone())
}

/// The result of offering one request's credentials to one mechanism.
enum Step {
    /// This mechanism has nothing to say about this request — no certificate
    /// was presented, no bearer header was sent, or the mechanism cannot run
    /// in this posture. The chain moves on.
    NotApplicable,
    /// This mechanism authenticated the request. The chain stops.
    Authenticated(Box<Actor>),
    /// A credential *for this mechanism* was presented and it is bad. The
    /// chain stops with a 401; it does not fall through to a weaker mechanism.
    Rejected(Unauthenticated),
}

/// One authentication mechanism.
///
/// An enum rather than a trait object because the set is closed and small; the
/// point of modelling it at all is that the chain is an **ordered, additive
/// list**, so adding a mechanism (mTLS service identities, a signed-request
/// scheme) is appending a variant and an arm, not editing a conditional that
/// three other things also depend on.
enum Authenticator {
    /// Verify a presented client certificate against the cluster CA and accept
    /// operator leaves.
    OperatorCert {
        ca: CaProvider,
        /// In the OIDC posture a presented-but-unverifiable certificate is a
        /// hard 401. In open mode it falls through: the listener *requests*
        /// client certificates from everyone, so browsers and stray clients
        /// will hand over certificates that have nothing to do with this
        /// cluster, and refusing them in a posture that requires no
        /// credentials at all would be a failure mode invented out of thin
        /// air.
        strict: bool,
    },
    /// Validate an `Authorization: Bearer` JWT.
    Bearer {
        validator: Validator,
        groups_claim: GroupsClaimProvider,
    },
    /// Open mode's static anonymous actor.
    Open,
}

/// The ordered set of authentication mechanisms this deployment runs.
///
/// Order is operator certificate, then bearer, then (open mode only) the
/// anonymous fallback. The certificate wins over a bearer token presented on
/// the same request: it is the break-glass path of ADR 0022, it is the
/// stronger proof of the two, and a break-glass credential that could be
/// shadowed by a stale token in a client's environment would not be much of a
/// break-glass.
pub struct AuthnChain {
    mode: AuthMode,
    authenticators: Vec<Authenticator>,
}

impl AuthnChain {
    /// The OIDC posture: operator certificates and bearer tokens, nothing else.
    pub fn oidc(
        validator: Validator,
        groups_claim: GroupsClaimProvider,
        ca: CaProvider,
        mode_config: OidcConfig,
    ) -> AuthnChain {
        AuthnChain {
            mode: AuthMode::Oidc(mode_config),
            authenticators: vec![
                Authenticator::OperatorCert { ca, strict: true },
                Authenticator::Bearer {
                    validator,
                    groups_claim,
                },
            ],
        }
    }

    /// The open posture: everything authenticates.
    ///
    /// The operator-certificate mechanism is still in the chain, and still
    /// first. An operator who presents a certificate to an open cluster gets
    /// `cert:<CN>` in the audit trail rather than `anonymous`, which is
    /// strictly more information at no cost — open mode grants unscoped admin
    /// either way.
    ///
    /// There is deliberately **no** bearer mechanism here: open mode has no
    /// `[sso]` block, so there is no issuer, no audience and no key set to
    /// validate a token against. A bearer header on an open-mode request is
    /// therefore ignored rather than rejected — the alternative would be to
    /// 401 a credential we have no means of judging, on a deployment that
    /// asked for no credentials at all.
    pub fn open(ca: CaProvider) -> AuthnChain {
        AuthnChain {
            mode: AuthMode::Open,
            authenticators: vec![
                Authenticator::OperatorCert { ca, strict: false },
                Authenticator::Open,
            ],
        }
    }

    /// The posture this chain was built for. `GET /api/v1/auth/config` serves
    /// this.
    pub fn mode(&self) -> &AuthMode {
        &self.mode
    }

    /// Run the chain.
    ///
    /// The first mechanism that either authenticates or rejects decides the
    /// request. "Rejects" stopping the chain is the important half: a
    /// presented-and-invalid credential must never degrade into a weaker
    /// identity than the caller asked for, so a bad bearer token in the OIDC
    /// posture is a 401 and not, say, an anonymous read.
    pub async fn authenticate(&self, creds: Credentials<'_>) -> Result<Actor, Unauthenticated> {
        for authenticator in &self.authenticators {
            match authenticator.attempt(creds).await {
                Step::NotApplicable => continue,
                Step::Authenticated(actor) => {
                    metrics::record_auth_outcome(actor.method().as_str(), "ok");
                    return Ok(*actor);
                }
                Step::Rejected(err) => {
                    metrics::record_auth_outcome(err.attempted_method().as_str(), "rejected");
                    return Err(err);
                }
            }
        }
        metrics::record_auth_outcome("none", "no_credentials");
        Err(Unauthenticated::NoCredentials)
    }
}

impl Authenticator {
    async fn attempt(&self, creds: Credentials<'_>) -> Step {
        match self {
            Authenticator::OperatorCert { ca, strict } => operator_cert(ca, *strict, creds),
            Authenticator::Bearer {
                validator,
                groups_claim,
            } => bearer(validator, groups_claim, creds).await,
            Authenticator::Open => Step::Authenticated(Box::new(actor::anonymous())),
        }
    }
}

fn operator_cert(ca: &CaProvider, strict: bool, creds: Credentials<'_>) -> Step {
    let Some(leaf) = creds.peer_leaf_der else {
        return Step::NotApplicable;
    };
    let Some(ca_pem) = ca() else {
        // Pre-formation there is no cluster CA, so no certificate can be
        // judged. Falling through (rather than rejecting) keeps the bearer
        // path — the one that still works — reachable.
        tracing::debug!("a client certificate was presented before a cluster CA exists; ignoring");
        return Step::NotApplicable;
    };

    match pki::verify_leaf(&ca_pem, leaf) {
        Ok(verified) => match verified.profile {
            Profile::Operator { cn } => Step::Authenticated(Box::new(actor::operator(&cn))),
            // A coordinator or agent leaf is a real cluster identity, but not
            // an *operator* one: those certificates authenticate machines on
            // the internal planes and carry no human accountability. Granting
            // them the break-glass admin grant would make every agent host an
            // admin credential.
            other => reject_cert(
                strict,
                format!(
                    "the client certificate is a {} leaf, not an operator leaf",
                    profile_name(&other)
                ),
            ),
        },
        Err(e) => reject_cert(
            strict,
            format!("the client certificate did not verify against the cluster CA: {e}"),
        ),
    }
}

fn reject_cert(strict: bool, why: String) -> Step {
    if strict {
        Step::Rejected(Unauthenticated::ClientCertificate(why))
    } else {
        tracing::debug!(reason = %why, "ignoring an unusable client certificate in open mode");
        Step::NotApplicable
    }
}

fn profile_name(profile: &Profile) -> &'static str {
    match profile {
        Profile::Coordinator(_) => "coordinator",
        Profile::Agent(_) => "agent",
        Profile::Operator { .. } => "operator",
    }
}

async fn bearer(
    validator: &Validator,
    groups_claim: &GroupsClaimProvider,
    creds: Credentials<'_>,
) -> Step {
    let Some(token) = creds.bearer else {
        return Step::NotApplicable;
    };
    if token.trim().is_empty() {
        return Step::Rejected(Unauthenticated::InvalidBearer(ValidateError::Malformed(
            "the Authorization header carries an empty bearer token".to_string(),
        )));
    }
    match validator.validate(token, &groups_claim()).await {
        Ok(validated) => {
            Step::Authenticated(Box::new(actor::bearer(validated.sub, validated.groups)))
        }
        Err(e) => Step::Rejected(Unauthenticated::InvalidBearer(e)),
    }
}

/// Why a request has no identity. Every variant maps to HTTP 401
/// (`ErrorCode::Unauthenticated`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unauthenticated {
    /// Nothing usable was presented at all.
    #[error("no credentials were presented")]
    NoCredentials,
    /// A bearer token was presented and rejected.
    #[error("the bearer token was rejected: {0}")]
    InvalidBearer(#[source] ValidateError),
    /// A client certificate was presented and is not an operator credential
    /// for this cluster.
    #[error("{0}")]
    ClientCertificate(String),
}

impl Unauthenticated {
    /// The mechanism that failed, for metric labelling.
    pub fn attempted_method(&self) -> AuthMethod {
        match self {
            Unauthenticated::InvalidBearer(_) => AuthMethod::Bearer,
            Unauthenticated::ClientCertificate(_) => AuthMethod::OperatorCert,
            // Nothing was attempted; `open` is the closest honest label and
            // the counter's `result` label carries the real story.
            Unauthenticated::NoCredentials => AuthMethod::Open,
        }
    }
}
