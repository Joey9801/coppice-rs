//! How a request's identity was proved, and the constructors that build it.
//!
//! The identity itself is **not** defined here: it is
//! [`coppice_state::Actor`], the same struct that rides actor-carrying
//! commands into the Raft log (ADR 0023). That unification is deliberate. The
//! chain resolves a request's credentials into an `Actor`, the API layer puts
//! it in request extensions, and the proposer attaches *that very value* to the
//! command it proposes — so what apply re-checks on every replica is what the
//! edge actually authenticated, with no transcription step in between that
//! could drift.
//!
//! What lives here is what only the authentication edge cares about: the
//! mechanism that proved the identity ([`AuthMethod`], reported on
//! `GET /api/v1/session` and on the auth-outcome metric), exposed as an
//! extension trait because `Actor` is a foreign type now.

use coppice_state::Actor;

/// The principal of the open-mode anonymous actor.
pub const ANONYMOUS_PRINCIPAL: &str = "anonymous";

/// The authentication edge's additions to the replicated [`Actor`].
///
/// An extension trait rather than inherent methods: `Actor` belongs to
/// `coppice-state`, which knows nothing about HTTP credentials and must not
/// grow a dependency on this crate to learn about them.
pub trait ActorExt {
    /// Which mechanism authenticated this actor.
    ///
    /// Derived from the flags rather than stored, so the method reported on
    /// `/session` can never disagree with the flags authorization reads.
    fn method(&self) -> AuthMethod;
}

impl ActorExt for Actor {
    fn method(&self) -> AuthMethod {
        if self.operator_cert {
            AuthMethod::OperatorCert
        } else if self.auth_disabled {
            AuthMethod::Open
        } else {
            AuthMethod::Bearer
        }
    }
}

/// The anonymous actor of an open-mode deployment.
///
/// Crate-internal, like its two siblings: the chain is the only thing that may
/// mint an actor, because an actor that no mechanism produced is an
/// unauthenticated request wearing an identity.
pub(crate) fn anonymous() -> Actor {
    Actor {
        principal: ANONYMOUS_PRINCIPAL.to_string(),
        groups: Vec::new(),
        operator_cert: false,
        auth_disabled: true,
    }
}

/// The actor for a verified operator leaf with common name `cn`.
pub(crate) fn operator(cn: &str) -> Actor {
    Actor {
        principal: format!("cert:{cn}"),
        groups: Vec::new(),
        operator_cert: true,
        auth_disabled: false,
    }
}

/// The actor for a validated bearer token.
pub(crate) fn bearer(sub: String, groups: Vec<String>) -> Actor {
    Actor {
        principal: sub,
        groups,
        operator_cert: false,
        auth_disabled: false,
    }
}

/// The mechanism that authenticated a request. The `as_str` spellings are the
/// wire values of `GET /api/v1/session`'s `method` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    /// A validated OIDC bearer token.
    Bearer,
    /// A client certificate that verified against the cluster CA as an
    /// operator leaf.
    OperatorCert,
    /// No credential: the deployment is in open mode.
    Open,
}

impl AuthMethod {
    /// The wire spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthMethod::Bearer => "bearer",
            AuthMethod::OperatorCert => "operator_cert",
            AuthMethod::Open => "open",
        }
    }
}

impl std::fmt::Display for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_is_derived_from_the_flags() {
        assert_eq!(anonymous().method(), AuthMethod::Open);
        assert_eq!(operator("alice").method(), AuthMethod::OperatorCert);
        assert_eq!(bearer("sub-1".into(), vec![]).method(), AuthMethod::Bearer);
        assert_eq!(operator("alice").principal, "cert:alice");
    }

    /// The two implicit-unscoped-admin grants of ADR 0022 are exactly the two
    /// credential-less mechanisms — asserted here, on the actors this crate
    /// actually mints, because it is `coppice-state`'s
    /// [`is_implicit_admin`](Actor::is_implicit_admin) that reads them.
    #[test]
    fn open_mode_and_operator_certs_mint_implicit_admins() {
        assert!(anonymous().is_implicit_admin());
        assert!(operator("alice").is_implicit_admin());
        assert!(!bearer("sub-1".into(), vec![]).is_implicit_admin());
    }
}
