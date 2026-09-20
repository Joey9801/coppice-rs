//! `GET /api/v1/jobs/{job}/logs` — best-effort job log retrieval.
//!
//! Every read is served best-effort from whatever a node's agent still has:
//! the response's `sources` list is a per-attempt honesty accounting (what
//! was actually retrievable), separate from `entries` (what was retrieved).
//! A request that finds nothing is still a normal response with a full
//! `sources` breakdown, not an error.

use serde::{Deserialize, Serialize};

use crate::id::{AttemptId, NodeId};
use crate::pagination::LogCursor;
use crate::time::Timestamp;

/// The scan direction, shared by the `order=` query parameter and the
/// cursor. Unlike every other wire enum in this crate, a client only ever
/// *authors* this value (into a query string) and never reads one back from
/// a response body, so it stays a closed enum with no `Unknown` catch-all.
///
/// The default differs per endpoint: [`GetJobLogsResponse`] defaults to
/// `Desc` (newest first), while [`super::GetJobUsageResponse`] defaults to
/// `Asc` (a chart-ordered time series) — so this type deliberately does not
/// derive `Default`; pick explicitly with `LogsParams::with_order` /
/// `UsageParams::with_order`, or leave it `None` to take the endpoint's own
/// default.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum LogOrder {
    /// Oldest first.
    Asc,
    /// Newest first.
    Desc,
}

impl LogOrder {
    /// Whether this order walks oldest-to-newest.
    pub fn is_ascending(self) -> bool {
        matches!(self, LogOrder::Asc)
    }
}

/// One of an attempt's two output streams, on both a log entry and the
/// `stream=` filter.
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
pub enum LogStreamName {
    /// The container's standard output.
    Stdout,
    /// The container's standard error.
    Stderr,
    /// A value this client does not know — a newer server's vocabulary, kept
    /// verbatim rather than rejected. See [`super`] for why this carries the
    /// spelling rather than being a bare unit variant.
    ///
    /// Decoding only. `LogStreamName` also travels *out*, as the `stream=`
    /// filter, and [`LogsParams::validate`] refuses this before the request
    /// is sent; a caller who genuinely means a stream a newer server grew
    /// asks for it through
    /// [`Client::get_value`](crate::Client::get_value).
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl LogStreamName {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, LogStreamName::Unknown(_))
    }
}

/// The availability verdict for one attempt's logs — the join of "which
/// attempts exist and where they ran" with "what data still exists".
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
pub enum LogAvailability {
    /// Chunks were returned (possibly zero within the requested range);
    /// see the source record's `truncated`.
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

impl LogAvailability {
    /// Whether this value fell into the `Unknown` catch-all.
    pub fn is_unknown(&self) -> bool {
        matches!(self, LogAvailability::Unknown(_))
    }
}

/// One captured log chunk. Bytes are decoded UTF-8-lossily into `text`; raw
/// bytes are not recoverable through this API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LogEntry {
    /// Stable identity; distinct repeated writes remain distinct entries.
    pub id: String,
    /// The attempt this entry came from.
    pub attempt: AttemptId,
    /// When the line was written.
    pub at: Timestamp,
    /// Which stream it came from.
    pub stream: LogStreamName,
    /// The line's text.
    pub text: String,
    /// True when this entry's `text` was cut to fit the page's byte budget
    /// — the underlying chunk alone exceeded it, so the dropped tail is not
    /// retrievable through this API. Distinct from a source record's
    /// `truncated`, which reports that older *lines* were pruned from the
    /// store.
    pub truncated: bool,
}

/// One per attempt a page covered, in page order — the per-attempt
/// availability accounting that makes a best-effort answer honest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LogSourceRecord {
    /// The attempt this record covers.
    pub attempt: AttemptId,
    /// The node the attempt ran on; `null` only when the attempt record
    /// itself is missing from replicated state, so no node is known.
    pub node: Option<NodeId>,
    /// The verdict.
    pub availability: LogAvailability,
    /// True when older lines the client asked for have already been
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

// Serde specially treats a field of type `Option<T>` as optional to omit
// entirely — a bare `Option<T>` field silently defaults to `None` when the
// key is missing, `#[serde(default)]` or not. `resume_cursor` is always
// present on the wire (only its value is nullable), so routing it through
// this identity `deserialize_with` opts it out of that leniency: serde no
// longer sees a plain `Option<T>` field to special-case, so a missing key
// is a decode error, while `null` still decodes to `None` as normal.
fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// `GET /api/v1/jobs/{job}/logs` — the never-bare-array envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetJobLogsResponse {
    /// The ascending high-water mark, valid even when this page exhausts
    /// current output — what makes following possible (see
    /// [`crate::LogFollower`]). This key is always present on the wire,
    /// though nullable: `required_option` enforces that on decode.
    #[serde(deserialize_with = "required_option")]
    pub resume_cursor: Option<LogCursor>,
    /// Whether the job can still produce output (polling is best effort).
    pub live: bool,
    /// The log lines this page covers.
    pub entries: Vec<LogEntry>,
    /// The per-attempt availability accounting for this page.
    pub sources: Vec<LogSourceRecord>,
    /// The in-walk continuation token; `null` iff the walk is truly
    /// complete. A short page with a non-null cursor means "continue",
    /// never "done" — the server can end a page early (an RPC budget, a
    /// byte cap, a source cap) for reasons that have nothing to do with the
    /// requested `limit`.
    pub next_cursor: Option<LogCursor>,
}

/// The parameters of a `GET /api/v1/jobs/{job}/logs` request.
///
/// Server contract this mirrors: `limit` defaults to 200 and must fall in
/// `1..=1000` — out of range is a `400`, never silently clamped; `order`
/// defaults to `desc` (newest first); `to` is exclusive unless
/// `to_inclusive` is set; a `cursor` whose embedded order disagrees with
/// `order` is a `400`, and so is one whose embedded attempt disagrees with
/// `attempt`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LogsParams {
    /// Resume from a previous page's `next_cursor`.
    pub cursor: Option<LogCursor>,
    /// Page size; server default 200, valid range `1..=1000`.
    pub limit: Option<u32>,
    /// Restrict to one output stream.
    pub stream: Option<LogStreamName>,
    /// Restrict the walk to one attempt; it must belong to the job.
    pub attempt: Option<AttemptId>,
    /// Inclusive lower bound.
    pub from: Option<Timestamp>,
    /// Upper bound; exclusive unless `to_inclusive` is set.
    pub to: Option<Timestamp>,
    /// Close the `to` bound inclusively rather than exclusively.
    pub to_inclusive: bool,
    /// Scan direction; server default `desc`.
    pub order: Option<LogOrder>,
}

impl LogsParams {
    /// The default parameters: server defaults throughout.
    pub fn new() -> LogsParams {
        LogsParams::default()
    }

    /// Resume from a previous page's cursor.
    pub fn with_cursor(mut self, cursor: LogCursor) -> LogsParams {
        self.cursor = Some(cursor);
        self
    }

    /// Set the page size.
    pub fn with_limit(mut self, limit: u32) -> LogsParams {
        self.limit = Some(limit);
        self
    }

    /// Restrict to one output stream.
    pub fn with_stream(mut self, stream: LogStreamName) -> LogsParams {
        self.stream = Some(stream);
        self
    }

    /// Restrict the walk to one attempt.
    pub fn with_attempt(mut self, attempt: AttemptId) -> LogsParams {
        self.attempt = Some(attempt);
        self
    }

    /// Set the inclusive lower bound.
    pub fn with_from(mut self, from: Timestamp) -> LogsParams {
        self.from = Some(from);
        self
    }

    /// Set the upper bound (exclusive unless `with_to_inclusive` is set).
    pub fn with_to(mut self, to: Timestamp) -> LogsParams {
        self.to = Some(to);
        self
    }

    /// Close the `to` bound inclusively.
    pub fn with_to_inclusive(mut self, to_inclusive: bool) -> LogsParams {
        self.to_inclusive = to_inclusive;
        self
    }

    /// Set the scan direction.
    pub fn with_order(mut self, order: LogOrder) -> LogsParams {
        self.order = Some(order);
        self
    }

    /// Refuse a `stream=` this client only knows how to *decode*.
    ///
    /// [`LogStreamName::Unknown`] is the response-side catch-all; the
    /// server's own stream vocabulary is closed, so asking it to filter on a
    /// spelling it does not know is a `400` with nothing useful in it.
    /// [`Client::job_logs`](crate::Client::job_logs) calls this before
    /// sending, which covers the pager and the log follower too — both make
    /// their requests through it.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(stream) = &self.stream {
            if stream.is_unknown() {
                return Err(format!(
                    "`stream` names the unrecognized stream `{stream}`, which no request \
                     may carry: `LogStreamName::Unknown` exists only to decode a newer \
                     server's response"
                ));
            }
        }
        Ok(())
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
        if let Some(stream) = &self.stream {
            pairs.push(("stream", stream.to_string()));
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
    fn log_contract_requires_identity_liveness_and_the_nullable_resume_field() {
        let entry = serde_json::json!({
            "id": "segment:1",
            "attempt": AttemptId::new(),
            "at": "2026-01-01T00:00:00.000000Z",
            "stream": "stdout",
            "text": "line",
            "truncated": false,
        });
        assert!(serde_json::from_value::<LogEntry>(entry.clone()).is_ok());
        let mut missing_id = entry.clone();
        missing_id.as_object_mut().unwrap().remove("id");
        assert!(serde_json::from_value::<LogEntry>(missing_id).is_err());

        let response = serde_json::json!({
            "entries": [entry],
            "sources": [],
            "next_cursor": null,
            "resume_cursor": null,
            "live": false,
        });
        assert!(serde_json::from_value::<GetJobLogsResponse>(response.clone()).is_ok());
        for field in ["live", "resume_cursor"] {
            let mut missing = response.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<GetJobLogsResponse>(missing).is_err(),
                "{field} must be present"
            );
        }
    }

    /// `Unknown` decodes a newer server's entries; it is not something to
    /// filter *on*, and the refusal names the value.
    #[test]
    fn validate_rejects_an_unknown_stream_filter() {
        let params = LogsParams::new().with_stream(LogStreamName::Unknown("audit".to_string()));
        let err = params.validate().expect_err("an unknown stream");
        assert!(
            err.starts_with("`stream` names the unrecognized stream `audit`"),
            "{err}"
        );
        assert!(LogsParams::new()
            .with_stream(LogStreamName::Stderr)
            .validate()
            .is_ok());
        assert!(LogsParams::new().validate().is_ok());
    }

    #[test]
    fn log_order_parses_and_rejects_other_spellings() {
        assert_eq!("asc".parse::<LogOrder>().unwrap(), LogOrder::Asc);
        assert_eq!("desc".parse::<LogOrder>().unwrap(), LogOrder::Desc);
        assert!("sideways".parse::<LogOrder>().is_err());
    }

    #[test]
    fn query_pairs_emits_only_whats_set_in_wire_order() {
        assert_eq!(LogsParams::new().query_pairs(), Vec::new());

        let attempt = AttemptId::new();
        let params = LogsParams::new()
            .with_limit(50)
            .with_stream(LogStreamName::Stderr)
            .with_attempt(attempt)
            .with_from(ts(1_000_000))
            .with_to(ts(2_000_000))
            .with_to_inclusive(true)
            .with_order(LogOrder::Asc);
        assert_eq!(
            params.query_pairs(),
            vec![
                ("limit", "50".to_string()),
                ("stream", "stderr".to_string()),
                ("attempt", attempt.to_string()),
                ("from", "1970-01-01T00:00:01.000000Z".to_string()),
                ("to", "1970-01-01T00:00:02.000000Z".to_string()),
                ("to_inclusive", "true".to_string()),
                ("order", "asc".to_string()),
            ]
        );
    }

    #[test]
    fn to_inclusive_is_omitted_when_false() {
        let params = LogsParams::new().with_to(ts(2_000_000));
        assert_eq!(
            params.query_pairs(),
            vec![("to", "1970-01-01T00:00:02.000000Z".to_string())]
        );
    }
}
