//! The `/api/v1` request and response bodies, and the query parameters that
//! go with them.
//!
//! These are hand-written copies of the coordinator's own DTOs. This crate
//! deliberately depends on no `coppice-*` crate, so that it can be published
//! on its own; a contract test inside the server's `coppice-api` builds every
//! server value, hands the JSON to the type here, and asserts the round trip
//! is byte-identical, which is what keeps the copies honest.
//!
//! Three conventions run through everything here, and knowing them explains
//! most of the shapes:
//!
//! - **Responses tolerate the future.** No response type rejects an unknown
//!   field, every wire enum has an `Unknown` catch-all that keeps the
//!   unrecognized spelling, and the structs are `#[non_exhaustive]`. An old
//!   client reading a newer server degrades; it does not fail.
//! - **Requests are exact.** Every write body is checked field-for-field by
//!   the server — a misspelled key is an error there rather than a silently
//!   defaulted field — so request types are constructed through a
//!   constructor or builder rather than a struct literal. The tolerance
//!   above does not cross over: an enum's `Unknown` value is a
//!   *response-side* catch-all, and a request carrying one is refused by
//!   this crate's own validation before it is sent (see below).
//! - **Absent is `null`, not missing.** A read field with no value is an
//!   explicit `null`; an empty list is `[]`. The one documented exception is
//!   [`GetAuthConfigResponse`]'s OIDC fields, which are omitted in open mode
//!   so nothing renders an empty login form.
//!
//! ## The shape every wire enum shares
//!
//! The server's vocabularies are closed *for it* and open *for us*: the
//! coordinator may add a phase, an outcome, or a health verdict in a release
//! this client predates, and a client that answered a new value with a decode
//! error would fail to read an otherwise perfectly good response. So every
//! enum that arrives in a response ends in an `Unknown(String)` variant that
//! keeps the unrecognized value verbatim — through `Deserialize`, `Serialize`,
//! `FromStr` and `Display` alike — so nothing is lost, nothing fails, and a
//! caller that cares can see exactly what it did not recognize.
//!
//! Three of those enums are reused in requests — [`JobPhase`] in a
//! [`PhaseFilter`], [`BindingRole`] on a [`Binding`], [`LogStreamName`] as
//! the `stream=` log filter — and in that direction `Unknown` is refused:
//! [`JobFilter::validate`], [`Binding::validate`] and
//! [`LogsParams::validate`] each reject it by name before anything is sent.
//! The server's vocabularies are closed *for it*, so such a request could
//! only ever come back as an opaque `400`. A caller who genuinely needs to
//! send a value a newer server grew reaches it untyped, through
//! [`Client::get_value`](crate::Client::get_value),
//! [`Client::post_value`](crate::Client::post_value) or
//! [`Client::put_value`](crate::Client::put_value) — the same escape hatch
//! that covers any other shape these types cannot express.
//!
//! `Unknown` holds the string rather than being a bare unit variant for one
//! concrete reason: [`JobPhase`] is a **map key** in `by_state`, and a unit
//! catch-all would silently fold two future phases into one entry, losing a
//! count. Carrying the spelling keeps the map faithful — this is verified
//! (`BTreeMap<JobPhase, u32>`, both directions, including an unknown key) by
//! this module's tests.
//!
//! The enums are `#[non_exhaustive]` as well, so promoting a value out of
//! `Unknown` in a later release is not a breaking change. A handful of
//! client-authored vocabularies that never travel on a response ([`LogOrder`],
//! [`EntityScope`], [`RequestsResource`]) have no `Unknown` catch-all — there
//! is no future-server-value case to tolerate — but still derive
//! `strum::Display`/`strum::EnumString` for the same spelling, on the same
//! terms.
//!
//! `is_unknown()` (a one-line `matches!`) is kept per enum where it is used;
//! there is no `known()`/`VariantNames` facility — [`JobPhase::ALL`] plays
//! that role for the one enum that needs it, spelled out explicitly rather
//! than through a derive that would have to special-case the data-carrying
//! `Unknown` variant.

mod auth;
mod cluster;
mod common;
mod filter;
mod jobs;
mod logs;
mod nodes;
mod quota;
mod usage;

pub use auth::*;
pub use cluster::*;
pub use common::*;
pub use filter::*;
pub use jobs::*;
pub use logs::*;
pub use nodes::*;
pub use quota::*;
pub use usage::*;
