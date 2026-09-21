//! The three frames of `GET /api/v1/events` — the filtered job-event
//! subscription (ADR 0043).
//!
//! Unlike everything else in [`crate::types`], these bodies never arrive on
//! their own: each is the `data:` payload of one named SSE frame, and the
//! frame's name is what says which of the three it is. The
//! [`events`](crate::events) module is what turns a response body into them;
//! they are public because the items that module yields carry them.
//!
//! All three are hand-written copies of the server's DTOs like every other
//! type here, and held to them by the same contract test.

use serde::{Deserialize, Serialize};

use crate::time::Timestamp;

use super::TimelineEvent;

/// The `event: batch` frame: everything one applied command produced that the
/// subscription's filter admitted.
///
/// One frame per Raft index, **never split** — a command's events are one unit
/// on this stream (ADR 0008) — which is what makes the bare index a safe
/// cursor: a resume can never land inside a command.
///
/// The events are the same [`TimelineEvent`] shape `GetJobTimeline` serves
/// (ADR 0032): thin, identified by `(index, ordinal)`, with no job snapshot
/// attached. A consumer that needs the job's state reads it with
/// [`ReadOptions::at_least`](crate::ReadOptions::at_least) set to this frame's
/// `index`, so a lagging replica cannot answer with state older than the
/// event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct EventBatchFrame {
    /// The producing command's Raft log index — the resume cursor, and the
    /// frame's SSE event id.
    pub index: u64,
    /// The command's advisory proposer stamp (ADR 0032). It may run backwards
    /// as `index` advances; never reorder by it.
    pub at: Timestamp,
    /// Ascending by `ordinal`, with gaps wherever the filter excluded an
    /// event — ordinals are batch positions, never renumbered per
    /// subscription.
    pub events: Vec<TimelineEvent>,
}

/// The `event: progress` frame: a bookmark saying everything this
/// subscription matches at or below `index` has already been sent.
///
/// Sent when a subscription finishes catching up and periodically after, which
/// is also what keeps an idle connection alive. It carries a resumable SSE id
/// exactly as a batch does, and that is the whole point: a subscriber to a
/// quiet set would otherwise reconnect with an ever-staler cursor and be
/// forced to resync having missed nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct EventProgressFrame {
    /// The covered-through applied index.
    pub index: u64,
}

/// The `event: gap` frame: delivery was discontinuous.
///
/// Carries no SSE event id, deliberately — a gap is not a position anything
/// can resume from, which is what makes it a gap. The stream stays open and
/// continues live; what a consumer owes is a re-query of state with the same
/// filter — which is why a subscription's filter is restricted to leaves
/// `ListJobs` also accepts — and a resubscribe from *that* read's applied
/// index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct EventGapFrame {
    /// The oldest index this replica could still have served — everything
    /// below it is gone from the reconnection ring (ADR 0008).
    pub earliest_available: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TimelineEventBody;

    #[test]
    fn a_batch_frame_is_the_index_the_stamp_and_its_events() {
        let json = serde_json::json!({
            "index": 42,
            "at": "1970-01-01T00:00:01.000000Z",
            "events": [{
                "index": 42,
                "ordinal": 0,
                "at": "1970-01-01T00:00:01.000000Z",
                "kind": "job_submitted",
                "job": "job-00000000-0000-0000-0000-000000000001",
            }],
        });
        let frame: EventBatchFrame = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(frame.index, 42);
        assert_eq!(frame.at, Timestamp::from_micros(1_000_000).unwrap());
        assert!(matches!(
            frame.events[0].body,
            TimelineEventBody::JobSubmitted { .. }
        ));
        assert_eq!(serde_json::to_value(&frame).unwrap(), json);
    }

    /// The two single-field frames are exactly that: no id, no stamp, nothing
    /// a reader has to reconcile with the batch shape.
    #[test]
    fn the_bookmark_and_the_gap_carry_one_index_each() {
        let progress: EventProgressFrame =
            serde_json::from_value(serde_json::json!({ "index": 7 })).unwrap();
        assert_eq!(progress.index, 7);
        let gap: EventGapFrame =
            serde_json::from_value(serde_json::json!({ "earliest_available": 9 })).unwrap();
        assert_eq!(gap.earliest_available, 9);
    }
}
