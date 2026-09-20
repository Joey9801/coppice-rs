//! Session, authentication configuration, and authorization policy
//! (`/api/v1/auth/config`, `/api/v1/session`, `/api/v1/authorization`).

use serde::{Deserialize, Serialize};

use crate::id::QuotaEntityId;

/// The authentication posture a deployment runs under (`GetAuthConfigResponse::mode`).
///
/// Client-authored-closed on the server (`coppice_authn::AuthMode` has no
/// third case today), but this client still carries `Unknown` — a future
/// posture is exactly the kind of change an old client must degrade against
/// rather than fail to decode `/auth/config` at all.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum AuthMode {
    /// Bearer tokens validated against `issuer`, plus operator certificates.
    Oidc,
    /// Authentication is switched off: every request resolves to the
    /// anonymous actor with implicit unscoped admin.
    Open,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl AuthMode {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, AuthMode::Unknown(_))
    }
}

/// `GET /api/v1/auth/config` — the public, pre-authentication description of
/// a deployment's auth posture. A client cannot obtain a credential without
/// it: a web UI bootstraps its authorization-code + PKCE login from
/// `issuer`/`client_id`/`audience`, and a CLI learns from `mode` alone
/// whether a token is wanted at all.
///
/// The OIDC fields are **omitted**, not null, in open mode — the one
/// documented exception to this crate's "absent optionals are explicit
/// null" rule: `{"mode":"open"}` says there is no OIDC configuration, where
/// three nulls would invite a client to render an empty login form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetAuthConfigResponse {
    /// `"oidc"` or `"open"`.
    pub mode: AuthMode,
    /// The OIDC issuer URL; present in OIDC mode only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    /// The client id a UI logs in with; present in OIDC mode only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// The audience this cluster requires in an access token; present in
    /// OIDC mode only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
}

impl GetAuthConfigResponse {
    /// Whether this deployment has authentication disabled.
    pub fn is_open(&self) -> bool {
        matches!(self.mode, AuthMode::Open)
    }

    /// Whether this deployment authenticates via OIDC.
    pub fn is_oidc(&self) -> bool {
        matches!(self.mode, AuthMode::Oidc)
    }
}

/// `GET /api/v1/session` — the resolved identity of the calling request,
/// echoed back. Reachable only with a valid credential: this endpoint
/// reports *who the credential proved you are*.
///
/// `bindings` + `implicit_admin` are the resolved-authority summary: the
/// replicated bindings whose subject matches this actor's principal or
/// groups, reported faithfully (role + scope each, one entry per matching
/// binding) rather than collapsed into a single effective role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetSessionResponse {
    /// The principal: an OIDC `sub`, `cert:<CN>` for an operator
    /// certificate, or `anonymous` in open mode. Opaque — there is no user
    /// table behind it, so this is kept as a free-form string.
    pub principal: String,
    /// Groups from the token's groups claim; `[]` for the mechanisms that
    /// carry none.
    pub groups: Vec<String>,
    /// How the principal proved itself.
    pub auth_method: AuthMethod,
    /// Display name from the token's `name` claim; `null` when absent, and
    /// always `null` for the operator-cert and open mechanisms.
    pub name: Option<String>,
    /// As `name`, for the token's `email` claim.
    pub email: Option<String>,
    /// The replicated bindings matching this actor, in stored order.
    pub bindings: Vec<SessionBinding>,
    /// `true` when the actor is an unscoped admin outside the bindings
    /// list: an operator certificate, or the open (auth-disabled) posture.
    pub implicit_admin: bool,
}

/// One matching binding in the session's resolved-authority summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionBinding {
    /// The role this binding grants.
    pub role: BindingRole,
    /// Subtree root the role is scoped to; `null` means cluster-wide.
    pub scope: Option<QuotaEntityId>,
}

/// The mechanism that authenticated a request (`GetSessionResponse::auth_method`).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum AuthMethod {
    /// A validated OIDC bearer token.
    Bearer,
    /// A client certificate that verified against the cluster CA as an
    /// operator leaf.
    OperatorCert,
    /// No credential: the deployment is in open mode.
    Open,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl AuthMethod {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, AuthMethod::Unknown(_))
    }
}

/// The closed role set a binding can grant.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum BindingRole {
    /// May submit and manage its own jobs within scope.
    Submitter,
    /// May also manage nodes and other users' jobs within scope.
    Operator,
    /// Unrestricted within scope, including authorization policy itself.
    Admin,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl BindingRole {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, BindingRole::Unknown(_))
    }
}

/// One role binding on the wire.
///
/// Renamed from the server's `BindingDto`: `Dto` is server-internal jargon,
/// and the wire shape is unchanged under the new name.
///
/// Flat subject: exactly one of `group`/`principal` must be present — serde
/// cannot express "exactly one", so [`Binding::validate`] checks it, mirroring
/// the server's own rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Binding {
    /// Group-claim subject; exactly one of `group`/`principal`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// Principal (`sub`) subject; exactly one of `group`/`principal`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// The role this binding grants.
    pub role: BindingRole,
    /// Subtree root the role is scoped to; absent means unscoped
    /// (cluster-wide).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<QuotaEntityId>,
}

impl Binding {
    /// A binding on a group-claim subject.
    pub fn for_group(name: impl Into<String>, role: BindingRole) -> Binding {
        Binding {
            group: Some(name.into()),
            principal: None,
            role,
            scope: None,
        }
    }

    /// A binding on a principal (`sub`) subject.
    pub fn for_principal(sub: impl Into<String>, role: BindingRole) -> Binding {
        Binding {
            group: None,
            principal: Some(sub.into()),
            role,
            scope: None,
        }
    }

    /// Scope this binding to a subtree, rather than leaving it cluster-wide.
    pub fn with_scope(mut self, scope: QuotaEntityId) -> Binding {
        self.scope = Some(scope);
        self
    }

    /// Enforce the exactly-one-subject rule serde cannot express, with the
    /// server's own error texts.
    pub fn validate(&self) -> Result<(), String> {
        match (&self.group, &self.principal) {
            (Some(_), Some(_)) => {
                Err("a binding names exactly one of `group`/`principal`, not both".to_string())
            }
            (None, None) => Err("a binding names exactly one of `group`/`principal`".to_string()),
            _ => Ok(()),
        }
    }
}

/// `GET /api/v1/authorization` — the current replicated authorization
/// policy: the full bindings list plus the groups-claim name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetAuthorizationResponse {
    /// The token claim group names are read from.
    pub groups_claim: String,
    /// Every binding, in stored order.
    pub bindings: Vec<Binding>,
}

/// `PUT /api/v1/authorization` — a full-replacement update: `bindings`
/// wholly replace the replicated list, and `groups_claim`, when present,
/// rides the same command so a rename and a binding swap can never be
/// half-applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct UpdateAuthorizationRequest {
    /// Absent = leave the current groups-claim name unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups_claim: Option<String>,
    /// The full replacement bindings list.
    pub bindings: Vec<Binding>,
}

impl UpdateAuthorizationRequest {
    /// A request replacing the bindings list, leaving `groups_claim`
    /// unchanged.
    pub fn new(bindings: impl IntoIterator<Item = Binding>) -> UpdateAuthorizationRequest {
        UpdateAuthorizationRequest {
            groups_claim: None,
            bindings: bindings.into_iter().collect(),
        }
    }

    /// Also rename the groups claim.
    pub fn with_groups_claim(mut self, claim: impl Into<String>) -> UpdateAuthorizationRequest {
        self.groups_claim = Some(claim.into());
        self
    }

    /// Validate every binding, prefixing each error the way the server's
    /// handler does.
    pub fn validate(&self) -> Result<(), String> {
        for (i, binding) in self.bindings.iter().enumerate() {
            binding
                .validate()
                .map_err(|e| format!("binding {i}: {e}"))?;
        }
        Ok(())
    }
}

/// `PUT /api/v1/authorization` response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct UpdateAuthorizationResponse {
    /// Raft log index at which the update applied — bindings and any
    /// `groups_claim` rename together, since one command carries both; pair
    /// with a subsequent read's minimum-index option for read-your-writes.
    pub log_index: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_mode_omits_the_oidc_fields_rather_than_nulling_them() {
        let config = GetAuthConfigResponse {
            mode: AuthMode::Open,
            issuer: None,
            client_id: None,
            audience: None,
        };
        let value = serde_json::to_value(&config).unwrap();
        assert_eq!(value, serde_json::json!({ "mode": "open" }));
        assert!(config.is_open());
        assert!(!config.is_oidc());
    }

    #[test]
    fn oidc_mode_includes_the_oidc_fields() {
        let config = GetAuthConfigResponse {
            mode: AuthMode::Oidc,
            issuer: Some("https://idp.example".to_string()),
            client_id: Some("coppice".to_string()),
            audience: Some("coppice-api".to_string()),
        };
        let value = serde_json::to_value(&config).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "mode": "oidc",
                "issuer": "https://idp.example",
                "client_id": "coppice",
                "audience": "coppice-api",
            })
        );
        assert!(config.is_oidc());
    }

    #[test]
    fn a_binding_needs_exactly_one_subject() {
        let neither = Binding {
            group: None,
            principal: None,
            role: BindingRole::Operator,
            scope: None,
        };
        assert_eq!(
            neither.validate().unwrap_err(),
            "a binding names exactly one of `group`/`principal`"
        );

        let both = Binding {
            group: Some("g".to_string()),
            principal: Some("p".to_string()),
            role: BindingRole::Operator,
            scope: None,
        };
        assert_eq!(
            both.validate().unwrap_err(),
            "a binding names exactly one of `group`/`principal`, not both"
        );

        assert!(Binding::for_group("g", BindingRole::Submitter)
            .validate()
            .is_ok());
        assert!(Binding::for_principal("p", BindingRole::Admin)
            .validate()
            .is_ok());
    }

    #[test]
    fn update_request_prefixes_binding_errors_with_their_index() {
        let bad = Binding {
            group: None,
            principal: None,
            role: BindingRole::Operator,
            scope: None,
        };
        let req = UpdateAuthorizationRequest::new([
            Binding::for_group("ok", BindingRole::Submitter),
            bad,
        ]);
        assert_eq!(
            req.validate().unwrap_err(),
            "binding 1: a binding names exactly one of `group`/`principal`"
        );
    }

    #[test]
    fn binding_omits_absent_subject_and_scope() {
        let binding = Binding::for_principal("sub-123", BindingRole::Submitter);
        assert_eq!(
            serde_json::to_value(&binding).unwrap(),
            serde_json::json!({ "principal": "sub-123", "role": "submitter" })
        );
    }
}
