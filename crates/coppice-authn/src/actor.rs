//! The authenticated identity a request carries, and how it was proved.

/// The identity resolved for one request.
///
/// This is a **local** definition. The replicated `Actor` (the proto message
/// that rides actor-carrying commands, ADR 0023) lands separately; the two
/// unify in a later change, at which point this struct becomes a re-export.
/// It is defined here so the authentication edge can be built and tested
/// without waiting on the state-machine half.
///
/// The last two fields are the two implicit-unscoped-admin grants: an operator
/// certificate (the break-glass path of ADR 0022) and an explicitly open
/// deployment. Both are recorded *in* the actor rather than consulted from
/// node config at authorization time, so authorization stays a pure function
/// of `(state, command)` on every replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor {
    /// The principal string: an OIDC `sub` for bearer auth, `cert:<CN>` for an
    /// operator certificate, `anonymous` in open mode. Opaque — there is no
    /// user table behind it.
    pub principal: String,
    /// Group names read from the token's groups claim. Empty for the
    /// non-bearer mechanisms, which do not carry groups.
    pub groups: Vec<String>,
    /// The principal proved itself with an operator certificate.
    pub operator_cert: bool,
    /// The deployment is running in open mode; no credential was required.
    pub auth_disabled: bool,
}

impl Actor {
    /// The anonymous actor of an open-mode deployment.
    pub fn anonymous() -> Actor {
        Actor {
            principal: ANONYMOUS_PRINCIPAL.to_string(),
            groups: Vec::new(),
            operator_cert: false,
            auth_disabled: true,
        }
    }

    /// The actor for a verified operator leaf with common name `cn`.
    pub fn operator(cn: &str) -> Actor {
        Actor {
            principal: format!("cert:{cn}"),
            groups: Vec::new(),
            operator_cert: true,
            auth_disabled: false,
        }
    }

    /// The actor for a validated bearer token.
    pub fn bearer(sub: String, groups: Vec<String>) -> Actor {
        Actor {
            principal: sub,
            groups,
            operator_cert: false,
            auth_disabled: false,
        }
    }

    /// Which mechanism authenticated this actor.
    ///
    /// Derived from the flags rather than stored, so the method reported on
    /// `/session` can never disagree with the flags authorization reads.
    pub fn method(&self) -> AuthMethod {
        if self.operator_cert {
            AuthMethod::OperatorCert
        } else if self.auth_disabled {
            AuthMethod::Open
        } else {
            AuthMethod::Bearer
        }
    }
}

/// The principal of the open-mode anonymous actor.
pub const ANONYMOUS_PRINCIPAL: &str = "anonymous";

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
        assert_eq!(Actor::anonymous().method(), AuthMethod::Open);
        assert_eq!(Actor::operator("alice").method(), AuthMethod::OperatorCert);
        assert_eq!(
            Actor::bearer("sub-1".into(), vec![]).method(),
            AuthMethod::Bearer
        );
        assert_eq!(Actor::operator("alice").principal, "cert:alice");
    }
}
