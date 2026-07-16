//! Conversions between generated wire types ([`crate::pb`]) and the domain
//! types in `coppice-core` / `coppice-state`.
//!
//! The direction of fallibility is the contract (ADR 0003, and the
//! migration decision in `docs/architecture/schema-style.md`):
//!
//! - **domain → pb is infallible.** Domain types uphold their invariants,
//!   and every domain value is representable on the wire. Encoding also
//!   *canonicalizes*: repeated key-sorted entries are emitted in key order
//!   with zero/empty entries omitted, so identical domain values encode to
//!   identical bytes. Time is part of this: `Timestamp` and `Duration` are
//!   microsecond-quantised and bounded to the `int64` µs range precisely so
//!   that every value of them encodes exactly (`coppice_core::time`).
//! - **pb → domain is fallible.** The wire admits shapes the domain does
//!   not (malformed or wrongly-prefixed ids, missing required messages, unknown enum
//!   values, duplicate keys). What a [`ConvertError`] means depends on
//!   where the bytes came from: on a committed log entry it becomes a
//!   deterministic `InvalidCommand` rejection (decode is a pure function,
//!   so every replica refuses identically); on a snapshot or manifest it is
//!   fail-stop corruption; at the API or agent edge it is a bad request.
//!
//! Shape rules the *catalog* assigns to apply (e.g. the v1
//! single-allocation placement) are deliberately **not** enforced here:
//! apply must see those payloads to reject them as
//! `UnsupportedPlacementShape` per the apply contract.

use coppice_core::time::{Duration, Timestamp};
use thiserror::Error;

mod command;
mod core;
mod snapshot;

pub use command::{command_from_pb, command_to_pb};
pub use snapshot::{
    allocation_records, attempt_records, cluster_record, job_records, node_records,
    quota_entity_records, record_counts, state_from_records, state_to_records, RecordCounts,
    StateRecords,
};

/// Why a wire value could not become a domain value.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConvertError {
    #[error("missing required field {0}")]
    MissingField(&'static str),
    #[error("{0} is not a typed `<prefix>-<uuid>` id")]
    InvalidId(&'static str),
    #[error("unknown enum value {value} in {field}")]
    UnknownEnum { field: &'static str, value: i32 },
    #[error("duplicate entry in {0}")]
    DuplicateEntry(&'static str),
    #[error("invalid {field}: {reason}")]
    Invalid {
        field: &'static str,
        reason: &'static str,
    },
}

/// Unwrap a required message field (prost decodes them as `Option`).
fn req<T>(field: Option<T>, name: &'static str) -> Result<T, ConvertError> {
    field.ok_or(ConvertError::MissingField(name))
}

/// Decode an `int64` Unix-microseconds field into a [`Timestamp`].
///
/// The wire type is wider than the domain type: `i64` µs reaches ~±292 000
/// years, `DateTime<Utc>` only ~±262 000. The gap is unreachable by any
/// honest producer, but it is reachable by a corrupt record or a hostile
/// peer, so it is rejected here rather than saturated — a timestamp from the
/// year 300 000 is a decoding failure, not a very old job.
fn timestamp(micros: i64, field: &'static str) -> Result<Timestamp, ConvertError> {
    Timestamp::from_micros(micros).ok_or(ConvertError::Invalid {
        field,
        reason: "timestamp is outside the representable range",
    })
}

/// Decode a `uint64` microseconds field into a non-negative [`Duration`].
///
/// Zero is legal (an attempt that never ran has zero runtime); only the range
/// is checked, since the signed domain type stops at `i64::MAX` µs.
fn nonnegative_duration(micros: u64, field: &'static str) -> Result<Duration, ConvertError> {
    let micros = i64::try_from(micros).map_err(|_| ConvertError::Invalid {
        field,
        reason: "duration is outside the representable range",
    })?;
    Ok(Duration::from_micros(micros))
}

/// Decode an optional `uint64` microseconds field into a positive [`Duration`].
///
/// Rejects zero (a zero-length runtime bound is not a bound; absence is how
/// "no bound" is spelled) and any value past `i64::MAX` µs, which the signed
/// domain type cannot hold.
fn positive_duration(micros: u64, field: &'static str) -> Result<Duration, ConvertError> {
    let micros = i64::try_from(micros).map_err(|_| ConvertError::Invalid {
        field,
        reason: "duration is outside the representable range",
    })?;
    let duration = Duration::from_micros(micros);
    if !duration.is_positive() {
        return Err(ConvertError::Invalid {
            field,
            reason: "duration must be positive",
        });
    }
    Ok(duration)
}
