//! The HTTP client edge (ADR 0031): axum router, JSON DTO bodies, and
//! the wire error/consistency contracts.
//!
//! This module owns everything transport: the `/api/v1` route map
//! ([`router`]), the JSON error contract ([`HttpError`]/[`ErrorCode`]),
//! and the ADR 0007 read parameters ([`ReadParams`]). The coordinator
//! binds the router on `listen.client_addr` and injects its
//! [`crate::ControlPlane`]. Every request and response body is a
//! handwritten serde DTO in [`dto`] (ADR 0031, "Wire format") —
//! protobuf idioms never leak into them.
//!
//! Most read routes are `UNIMPLEMENTED` stubs today — the ADR 0031 route
//! table is authoritative for their consistency class and message names,
//! and `web/src/api/types.ts` for their response shapes until each DTO
//! lands.

pub mod dto;
mod error;
mod extract;
mod logs;
mod metrics;
mod project;
mod read;
mod routes;
mod ui;
mod usage;

pub use error::{ErrorCode, HttpError};
pub use extract::{IdPath, ReadIndexes, ReadQuery};
pub use metrics::MetricsEndpoint;
pub use read::{Consistency, ReadParams};
pub use routes::router;
pub use ui::ui_available;

/// Header carrying the applied index of the view a read was served from.
pub const COPPICE_APPLIED_INDEX: &str = "coppice-applied-index";
/// Header carrying the serving replica's last-known committed index.
pub const COPPICE_COMMITTED_INDEX: &str = "coppice-committed-index";
/// Header carrying the leader hint on a `NOT_LEADER` error.
pub const COPPICE_LEADER: &str = "coppice-leader";
