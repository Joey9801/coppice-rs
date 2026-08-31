//! `coppice.agent.v1` ↔ domain conversions.
//!
//! The agent protocol is mostly built inline at its two edges (the session
//! constructs reports, ingestion reads them) because its messages are protocol
//! envelopes rather than domain values. The exceptions live here: payloads
//! whose *fields* carry domain types and unit-bearing scalars, where the
//! encode/decode pair is worth stating once.
//!
//! Same contract as the rest of `convert`: domain → pb is infallible, pb →
//! domain is fallible, and a failure at this edge is a bad report, rejected at
//! ingress (never a replicated fact — `NodeUsage` in particular is peeled off
//! at ingestion and never becomes a command).

use coppice_core::resource::Resources;
use coppice_core::time::Timestamp;

use super::{req, timestamp, ConvertError};
use crate::pb::agent::v1 as pb;

/// Encode a folded job-attributable usage reading as a [`pb::NodeUsage`].
///
/// Absence is the caller's decision, not this function's: a node with nothing
/// fresh to report omits the whole message rather than encoding a zero vector
/// (the "absent = not measured, never zero" rule in `agent.proto`).
pub fn node_usage_to_pb(used: &Resources, sampled_at: Timestamp) -> pb::NodeUsage {
    pb::NodeUsage {
        used: Some(used.into()),
        sampled_at_us: sampled_at.as_micros(),
    }
}

/// Decode a [`pb::NodeUsage`] into its folded vector and the time it was taken.
///
/// `used` is required: a `NodeUsage` with no vector is a malformed report, not
/// a report of zero usage.
pub fn node_usage_from_pb(usage: pb::NodeUsage) -> Result<(Resources, Timestamp), ConvertError> {
    let used = Resources::try_from(req(usage.used, "NodeUsage.used")?)?;
    let sampled_at = timestamp(usage.sampled_at_us, "NodeUsage.sampled_at_us")?;
    Ok((used, sampled_at))
}

#[cfg(test)]
mod tests {
    use super::*;
    use coppice_core::bytes::ByteSize;

    fn resources() -> Resources {
        Resources {
            cpu_millis: 2_500,
            memory: ByteSize::from_bytes(4 << 30),
            disk: ByteSize::from_bytes(9 << 30),
        }
    }

    #[test]
    fn node_usage_round_trips() {
        let at = Timestamp::from_micros(1_700_000_000_000_000).expect("representable");
        let encoded = node_usage_to_pb(&resources(), at);
        let (used, sampled_at) = node_usage_from_pb(encoded).expect("decodes");
        assert_eq!(used, resources());
        assert_eq!(sampled_at, at);
    }

    #[test]
    fn node_usage_without_a_vector_is_malformed() {
        let err = node_usage_from_pb(pb::NodeUsage {
            used: None,
            sampled_at_us: 1,
        })
        .expect_err("a NodeUsage with no vector is not a report of zero");
        assert_eq!(err, ConvertError::MissingField("NodeUsage.used"));
    }
}
