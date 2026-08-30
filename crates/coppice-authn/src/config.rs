//! The resolved authentication posture this process runs under.

/// The OIDC parameters an offline resource server needs.
///
/// No client secret: validating someone else's access token needs only the
/// issuer's public keys (ADR 0022). The `audience` here is the **effective**
/// audience — the caller has already applied the "defaults to `client_id`"
/// rule from node config, so this type never has to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcConfig {
    /// The issuer URL, exactly as configured. Discovery is fetched relative to
    /// it, and a token's `iss` must equal this string.
    pub issuer: String,
    /// The client id the UI bootstraps its PKCE login with.
    pub client_id: String,
    /// The audience every accepted token must list.
    pub audience: String,
}

/// The posture the authentication edge runs in. There is no default: node
/// config forces an explicit choice between the two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMode {
    /// Bearer tokens validated against `issuer`, plus operator certificates.
    Oidc(OidcConfig),
    /// Authentication is switched off: every request resolves to the anonymous
    /// actor with implicit unscoped admin.
    Open,
}

impl AuthMode {
    /// The wire spelling used by `GET /api/v1/auth/config`.
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthMode::Oidc(_) => "oidc",
            AuthMode::Open => "open",
        }
    }

    /// The OIDC parameters, when this is the OIDC posture.
    pub fn oidc(&self) -> Option<&OidcConfig> {
        match self {
            AuthMode::Oidc(c) => Some(c),
            AuthMode::Open => None,
        }
    }
}
