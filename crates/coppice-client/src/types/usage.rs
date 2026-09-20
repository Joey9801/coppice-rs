//! `GET /api/v1/jobs/{job}/usage` — best-effort job usage-metrics retrieval,
//! the metrics twin of the job-logs pipeline (see [`super::logs`]).
//!
//! The response mirrors [`super::GetJobLogsResponse`] shape-for-shape — a
//! samples list plus the same per-attempt `sources` availability accounting
//! — because the underlying walk is identical: resolve the job's attempts,
//! walk them under a bounded RPC budget, join "where it ran" with "what data
//! survives" into a per-attempt verdict.

use serde::{Deserialize, Serialize};

use crate::id::{AttemptId, NodeId};
use crate::pagination::UsageCursor;
use crate::time::Timestamp;

// The scan direction is shared vocabulary between the logs and usage
// endpoints — one enum, reused, rather than two identical ones. Only each
// endpoint's *default* differs (logs: desc, usage: asc), which is why
// `LogOrder` does not derive `Default`; see its own docs.
use super::logs::LogOrder;

/// The availability verdict for one attempt's samples — the metrics
/// twin of [`super::LogAvailability`], with identical verdicts and
/// semantics.
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
pub enum UsageAvailability {
    /// Samples were returned (possibly zero within the requested
    /// range); see the source record's `truncated`.
    Available,
    /// The attempt ran, but its telemetry has fallen out of retention
    /// (or was never captured — indistinguishable from the client's
    /// side, and the answer is the same: gone).
    Expired,
    /// No advertised endpoint, a dial/deadline failure, or the node
    /// that ran the attempt is no longer in the cluster; the source
    /// record's `reason` carries the detail.
    Unreachable,
    /// The attempt never reached `running`; nothing was captured.
    NotStarted,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl UsageAvailability {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, UsageAvailability::Unknown(_))
    }
}

/// One resource sample, mirroring the agent's stored row field-for-field.
/// Counters are cumulative — derive a rate by differencing consecutive
/// samples, so a dropped sample loses resolution, never mass.
///
/// The CPU counters carry an `_us` (microsecond-integer) suffix rather than
/// this wire's usual `_seconds` whole-second duration convention: these are
/// cumulative CPU-time totals a reader differences to derive utilization,
/// and whole-second quantization would erase sub-second deltas between
/// adjacent samples. They are integer counters, not instants, so the
/// string-instant rule does not apply to them either.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct UsagePoint {
    /// The attempt this sample came from.
    pub attempt: AttemptId,
    /// When the sample was taken.
    pub at: Timestamp,
    /// Cumulative CPU time consumed, in microseconds.
    pub cpu_usage_total_us: u64,
    /// Cumulative CPU time the container was throttled, in microseconds.
    pub cpu_throttled_total_us: u64,
    /// Current resident memory, in bytes.
    pub memory_used_bytes: u64,
    /// Peak resident memory over the attempt so far, in bytes.
    pub memory_peak_bytes: u64,
    /// Writable-layer bytes from the disk poller's last reading.
    pub disk_writable_bytes: u64,
    /// Image bytes — constant per attempt; writable + image = disk usage.
    pub disk_image_bytes: u64,
    /// Cumulative bytes received on the container network.
    pub net_rx_bytes_total: u64,
    /// Cumulative bytes transmitted on the container network.
    pub net_tx_bytes_total: u64,
    /// Cumulative block-I/O bytes read.
    pub blkio_read_bytes_total: u64,
    /// Cumulative block-I/O bytes written.
    pub blkio_write_bytes_total: u64,
}

/// One per attempt a page covered, in page order — the metrics twin of
/// [`super::LogSourceRecord`], with identical fields and semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct UsageSourceRecord {
    /// The attempt this record covers.
    pub attempt: AttemptId,
    /// The node the attempt ran on; `null` only when the attempt record
    /// itself is missing from replicated state, so no node is known.
    pub node: Option<NodeId>,
    /// The verdict.
    pub availability: UsageAvailability,
    /// True when older samples the client asked for have already been
    /// pruned: the store's oldest retained instant lies inside the
    /// requested range.
    pub truncated: bool,
    /// The store's oldest retained instant for this attempt, when known;
    /// advisory.
    pub earliest_available_at: Option<Timestamp>,
    /// Human-readable detail for an `expired`/`unreachable`/`not_started`
    /// verdict; `null` for a plain `available`.
    pub reason: Option<String>,
}

/// `GET /api/v1/jobs/{job}/usage` — the never-bare-array envelope, the
/// metrics twin of [`super::GetJobLogsResponse`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetJobUsageResponse {
    /// The samples this page covers.
    pub samples: Vec<UsagePoint>,
    /// The per-attempt availability accounting for this page.
    pub sources: Vec<UsageSourceRecord>,
    /// The in-walk continuation token; `null` iff the walk is truly
    /// complete. A short page with a non-null cursor means "continue",
    /// never "done".
    pub next_cursor: Option<UsageCursor>,
}

/// The parameters of a `GET /api/v1/jobs/{job}/usage` request.
///
/// Shaped like [`super::LogsParams`], minus `stream` (there is no analogous
/// filter for samples) and using [`UsageCursor`]. The contract differs from
/// logs in three ways: `limit` defaults to 1000 and must fall in
/// `1..=5000`; `order` defaults to `asc`, not `desc` — a chart-ordered time
/// series, not logs' newest-first; and there is no page byte cap (samples
/// are fixed-size rows, so only the sample count bounds a page). `to` is
/// still exclusive unless `to_inclusive` is set, and a `cursor` whose
/// embedded order or attempt disagrees with `order`/`attempt` is still a
/// `400`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct UsageParams {
    /// Resume from a previous page's `next_cursor`.
    pub cursor: Option<UsageCursor>,
    /// Page size; server default 1000, valid range `1..=5000`.
    pub limit: Option<u32>,
    /// Restrict the walk to one attempt; it must belong to the job.
    pub attempt: Option<AttemptId>,
    /// Inclusive lower bound.
    pub from: Option<Timestamp>,
    /// Upper bound; exclusive unless `to_inclusive` is set.
    pub to: Option<Timestamp>,
    /// Close the `to` bound inclusively rather than exclusively.
    pub to_inclusive: bool,
    /// Scan direction; server default `asc`.
    pub order: Option<LogOrder>,
}

impl UsageParams {
    /// The default parameters: server defaults throughout.
    pub fn new() -> UsageParams {
        UsageParams::default()
    }

    /// Resume from a previous page's cursor.
    pub fn with_cursor(mut self, cursor: UsageCursor) -> UsageParams {
        self.cursor = Some(cursor);
        self
    }

    /// Set the page size.
    pub fn with_limit(mut self, limit: u32) -> UsageParams {
        self.limit = Some(limit);
        self
    }

    /// Restrict the walk to one attempt.
    pub fn with_attempt(mut self, attempt: AttemptId) -> UsageParams {
        self.attempt = Some(attempt);
        self
    }

    /// Set the inclusive lower bound.
    pub fn with_from(mut self, from: Timestamp) -> UsageParams {
        self.from = Some(from);
        self
    }

    /// Set the upper bound (exclusive unless `with_to_inclusive` is set).
    pub fn with_to(mut self, to: Timestamp) -> UsageParams {
        self.to = Some(to);
        self
    }

    /// Close the `to` bound inclusively.
    pub fn with_to_inclusive(mut self, to_inclusive: bool) -> UsageParams {
        self.to_inclusive = to_inclusive;
        self
    }

    /// Set the scan direction.
    pub fn with_order(mut self, order: LogOrder) -> UsageParams {
        self.order = Some(order);
        self
    }

    /// The `?…` query pairs for this request, in wire order, omitting every
    /// field left at its default.
    pub fn query_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        if let Some(cursor) = &self.cursor {
            pairs.push(("cursor", cursor.as_str().to_string()));
        }
        if let Some(limit) = self.limit {
            pairs.push(("limit", limit.to_string()));
        }
        if let Some(attempt) = self.attempt {
            pairs.push(("attempt", attempt.to_string()));
        }
        if let Some(from) = self.from {
            pairs.push(("from", from.to_rfc3339()));
        }
        if let Some(to) = self.to {
            pairs.push(("to", to.to_rfc3339()));
        }
        if self.to_inclusive {
            pairs.push(("to_inclusive", "true".to_string()));
        }
        if let Some(order) = self.order {
            pairs.push(("order", order.to_string()));
        }
        pairs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(micros: i64) -> Timestamp {
        Timestamp::from_micros(micros).unwrap()
    }

    #[test]
    fn usage_point_round_trips_through_json() {
        let point = UsagePoint {
            attempt: AttemptId::new(),
            at: ts(1_000_000),
            cpu_usage_total_us: 1_000_000,
            cpu_throttled_total_us: 0,
            memory_used_bytes: 2_000_000,
            memory_peak_bytes: 3_000_000,
            disk_writable_bytes: 0,
            disk_image_bytes: 0,
            net_rx_bytes_total: 0,
            net_tx_bytes_total: 0,
            blkio_read_bytes_total: 0,
            blkio_write_bytes_total: 0,
        };
        let value = serde_json::to_value(point).unwrap();
        assert_eq!(value["at"], "1970-01-01T00:00:01.000000Z");
        assert_eq!(value["cpu_usage_total_us"], 1_000_000);
        assert_eq!(value["memory_used_bytes"], 2_000_000);
        let back: UsagePoint = serde_json::from_value(value).unwrap();
        assert_eq!(back, point);
    }

    #[test]
    fn query_pairs_emits_only_whats_set_in_wire_order() {
        assert_eq!(UsageParams::new().query_pairs(), Vec::new());

        let attempt = AttemptId::new();
        let params = UsageParams::new()
            .with_limit(500)
            .with_attempt(attempt)
            .with_from(ts(1_000_000))
            .with_to(ts(2_000_000))
            .with_to_inclusive(true)
            .with_order(LogOrder::Desc);
        assert_eq!(
            params.query_pairs(),
            vec![
                ("limit", "500".to_string()),
                ("attempt", attempt.to_string()),
                ("from", "1970-01-01T00:00:01.000000Z".to_string()),
                ("to", "1970-01-01T00:00:02.000000Z".to_string()),
                ("to_inclusive", "true".to_string()),
                ("order", "desc".to_string()),
            ]
        );
    }

    #[test]
    fn to_inclusive_is_omitted_when_false() {
        let params = UsageParams::new().with_to(ts(2_000_000));
        assert_eq!(
            params.query_pairs(),
            vec![("to", "1970-01-01T00:00:02.000000Z".to_string())]
        );
    }
}
