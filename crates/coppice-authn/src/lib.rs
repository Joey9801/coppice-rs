//! # coppice-authn
//!
//! The authentication edge for the public HTTP API (ADR 0022): OIDC discovery,
//! a JWKS cache, offline bearer-token validation, operator-certificate
//! break-glass, and the ordered [`AuthnChain`] that turns one request's
//! credentials into an [`Actor`].
//!
//! ## The shape of the thing
//!
//! ```text
//!  coordinator                    coppice-api                 this crate
//!  ───────────                    ───────────                 ──────────
//!  builds JwksCache ──spawn──▶ (background refresh) ─────────▶ JwksCache
//!  builds AuthnChain ─────────▶ axum extractor ──Credentials──▶ AuthnChain
//!                                     ◀──────────Actor─────────
//! ```
//!
//! Nothing here knows about axum, the router, or the replicated state machine.
//! The long-lived pieces are constructed by the coordinator and injected into
//! router state; the extractor in `coppice-api` calls
//! [`AuthnChain::authenticate`] and puts the resulting actor in request
//! extensions.
//!
//! ## Invariants worth keeping
//!
//! - **No IdP call on the request path.** Validation reads a cached key. The
//!   only exception is a rate-limited refetch when a token names a `kid` we
//!   have never seen (see [`jwks`]).
//! - **An IdP outage does not invalidate anything.** A failed refresh keeps
//!   serving the previous key set, so already-issued tokens keep working. The
//!   staleness gauge, not a fetch-failure counter, is what an operator alerts
//!   on.
//! - **The chain is an ordered, additive list**, not a conditional. A new
//!   mechanism is a new variant appended to it.
//! - **Errors never carry credential material** — no token text, no claim
//!   values (claim *names* are fine).

mod actor;
mod chain;
mod config;
mod http;
pub mod jwks;
mod metrics;
mod validator;

pub use actor::{ActorExt, AuthMethod, ANONYMOUS_PRINCIPAL};
pub use chain::{
    no_ca, static_groups_claim, AuthnChain, CaProvider, Credentials, GroupsClaimProvider,
    Unauthenticated,
};

/// The identity one request resolved to — **the replicated actor itself**
/// ([`coppice_state::Actor`]), not a copy of its shape.
///
/// Re-exported so a consumer of the authentication edge needs one import, and
/// so it is visible here that the chain's output and the value that rides a
/// proposed command (ADR 0023) are the same type. The edge's own additions to
/// it are the [`ActorExt`] trait.
pub use coppice_state::Actor;

pub use config::{AuthMode, OidcConfig};
/// The default groups-claim name (ADR 0022) — owned by `coppice-state`, where
/// it is also what an absent `PolicyConfig.groups_claim` decodes to.
///
/// Re-exported rather than restated: two constants that must agree is one
/// constant with extra steps.
pub use coppice_state::DEFAULT_GROUPS_CLAIM;
pub use http::default_http_client;
pub use jwks::{JwksCache, JwksError, JwksTimings};
pub use validator::{ValidateError, ValidatedToken, Validator, CLOCK_SKEW_LEEWAY_SECS};

pub use metrics::{describe_metrics, gather_metrics};
