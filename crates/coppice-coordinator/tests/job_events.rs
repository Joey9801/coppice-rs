//! End-to-end `GET /api/v1/events` (ADR 0043), against a real daemon.
//!
//! Everything below the HTTP call is production: a live coordinator runtime,
//! its apply loop stamping scope keys onto the derived stream, its event
//! fanout, its reconnection ring, and the real axum router. The test is a
//! plain HTTP client reading SSE frames off the wire, because the wire
//! contract — which frames arrive, which carry a resumable id, and what a
//! reconnect with that id does *not* re-deliver — is the thing worth pinning
//! down, and none of it is observable from inside the process.
//!
//! One test, because the bootstrap is the expensive part: subscribe with a
//! metadata filter, submit a matching and a non-matching job, then reconnect
//! with the last event id.

mod common;

use std::time::Duration;

use coppice_consensus::Consensus;
use coppice_core::id::{ClusterId, JobId, QuotaEntityId};
use coppice_core::job::Job;
use coppice_core::metadata::JobMetadata;
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::resource::Resources;
use coppice_core::time::Timestamp;
use coppice_state::command::{ConfigureQuotaEntity, SubmitJob};
use coppice_state::Command;

use common::{poll, Ca, RunningCoordinator};

const DEADLINE: Duration = Duration::from_secs(20);

/// One parsed SSE frame: its `event:` name, its `id:` if it carried one, and
/// its `data:` payload decoded as JSON.
#[derive(Debug, Clone)]
struct Frame {
    event: String,
    id: Option<String>,
    data: serde_json::Value,
}

/// An SSE response being read incrementally.
///
/// Frames are separated by a blank line; a partial frame stays in the buffer
/// until the rest of it arrives. Deliberately hand-rolled: a client-side SSE
/// implementation is exactly what this test must *not* share with the server
/// it is checking.
struct Sse {
    response: reqwest::Response,
    buffer: String,
    pending: std::collections::VecDeque<Frame>,
}

impl Sse {
    async fn open(url: &str, last_event_id: Option<&str>) -> Sse {
        let mut request = reqwest::Client::new().get(url);
        if let Some(id) = last_event_id {
            request = request.header("last-event-id", id);
        }
        let response = request
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {url}: {e}"));
        assert_eq!(
            response.status().as_u16(),
            200,
            "subscription refused: {url}"
        );
        assert!(
            response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("text/event-stream")),
            "not an event stream: {:?}",
            response.headers()
        );
        Sse {
            response,
            buffer: String::new(),
            pending: std::collections::VecDeque::new(),
        }
    }

    /// The next frame, reading more of the body if none is buffered.
    async fn next(&mut self) -> Frame {
        loop {
            if let Some(frame) = self.pending.pop_front() {
                return frame;
            }
            let chunk = tokio::time::timeout(DEADLINE, self.response.chunk())
                .await
                .expect("the stream must produce a frame")
                .expect("reading the stream")
                .expect("the stream ended early");
            self.buffer.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(end) = self.buffer.find("\n\n") {
                let raw = self.buffer[..end].to_string();
                self.buffer.drain(..end + 2);
                self.pending.push_back(parse(&raw));
            }
        }
    }

    /// The next frame named `event`, skipping (and returning) whatever came
    /// before it.
    async fn next_named(&mut self, event: &str) -> (Frame, Vec<Frame>) {
        let mut skipped = Vec::new();
        loop {
            let frame = self.next().await;
            if frame.event == event {
                return (frame, skipped);
            }
            skipped.push(frame);
        }
    }
}

fn parse(raw: &str) -> Frame {
    let mut event = String::new();
    let mut id = None;
    let mut data = String::new();
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix("event:") {
            event = rest.trim().to_string();
        } else if let Some(rest) = line.strip_prefix("id:") {
            id = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            data.push_str(rest.trim());
        }
    }
    Frame {
        event,
        id,
        data: serde_json::from_str(&data)
            .unwrap_or_else(|e| panic!("frame data is not JSON ({e}): {data}")),
    }
}

/// `?jobs=` for `{"metadata": {"key": "team"}}`, percent-encoded.
fn jobs_query() -> String {
    let filter = serde_json::json!({"metadata": {"key": "team"}}).to_string();
    let encoded: String = filter
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect();
    format!("jobs={encoded}")
}

async fn seed_quota(coord: &RunningCoordinator, entity: QuotaEntityId) {
    let applied = coord
        .consensus()
        .propose(Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
            entity,
            parent: None,
            name: "root".into(),
            quota: CostUnits(1_000_000_000_000),
            updated_at: Timestamp::now(),
            actor: None,
        }))
        .await
        .expect("propose ConfigureQuotaEntity");
    assert!(
        applied.outcome.is_ok(),
        "quota rejected: {:?}",
        applied.outcome
    );
}

/// Submit one job, optionally carrying the `team` metadata key the
/// subscription filters on. Returns the applied log index — the same number
/// the subscription reports as its frame id.
async fn submit(coord: &RunningCoordinator, job: JobId, entity: QuotaEntityId, team: bool) -> u64 {
    let mut metadata = JobMetadata::new();
    if team {
        metadata.insert("team".into(), "platform".into());
    }
    let applied = coord
        .consensus()
        .propose(Command::SubmitJob(SubmitJob {
            job: Job {
                id: job,
                image: "registry/img:latest".into(),
                command: vec!["run".into()],
                entrypoint: None,
                env: Default::default(),
                requests: Resources {
                    cpu_millis: 1_000,
                    ..Default::default()
                },
                priority: 0,
                max_runtime: None,
                quota_entity: entity,
                retry: Default::default(),
                abort_requested: None,
                submitted_by: None,
                metadata,
            },
            multiplier: PriorityMultiplier::ONE,
            submitted_at: Timestamp::now(),
            actor: None,
        }))
        .await
        .expect("propose SubmitJob");
    assert!(
        applied.outcome.is_ok(),
        "SubmitJob rejected: {:?}",
        applied.outcome
    );
    applied.log_index
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_filtered_subscription_delivers_matching_jobs_and_resumes_without_duplicates() {
    let ca = Ca::new();
    let coord = RunningCoordinator::start(ClusterId::new(), &ca).await;
    poll(DEADLINE, "the single-node replica leads", || async {
        coord.is_leader()
    })
    .await;

    let entity = QuotaEntityId::new();
    seed_quota(&coord, entity).await;

    let url = coord.api(&format!("/api/v1/events?{}", jobs_query()));
    let mut stream = Sse::open(&url, None).await;

    // A fresh subscription opens with a bookmark, so the client has a cursor
    // before anything it cares about has happened.
    let opening = stream.next().await;
    assert_eq!(opening.event, "progress", "opening frame: {opening:?}");
    assert!(opening.id.is_some(), "a bookmark is resumable: {opening:?}");

    // One job with the key, one without. Only the first may be delivered —
    // and the second is submitted *first*, so "nothing arrived yet" cannot be
    // mistaken for "the stream is merely slow".
    let unmatched = JobId::new();
    submit(&coord, unmatched, entity, false).await;
    let matched = JobId::new();
    let matched_index = submit(&coord, matched, entity, true).await;

    let (batch, skipped) = stream.next_named("batch").await;
    for frame in &skipped {
        assert_eq!(
            frame.event, "progress",
            "only bookmarks may precede the batch: {frame:?}"
        );
    }
    assert_eq!(
        batch.data["index"].as_u64(),
        Some(matched_index),
        "the frame's index is the submitting command's log index: {batch:?}"
    );
    assert_eq!(
        batch.id.as_deref(),
        Some(matched_index.to_string().as_str()),
        "the SSE id is the resume cursor: {batch:?}"
    );
    // One frame per command, never split: the submission's whole event run
    // (submitted, then the accept/queue transitions apply derived from it)
    // arrives together, each at its batch-assigned ordinal.
    let events = batch.data["events"].as_array().expect("events array");
    assert_eq!(events[0]["kind"], "job_submitted", "{batch:?}");
    assert_eq!(
        events
            .iter()
            .map(|e| e["ordinal"].as_u64())
            .collect::<Vec<_>>(),
        (0..events.len() as u64).map(Some).collect::<Vec<_>>(),
        "ordinals are the full batch's positions: {batch:?}"
    );
    for event in events {
        assert_eq!(
            event["job"],
            matched.to_string(),
            "the job without the key must never appear ({unmatched}): {batch:?}"
        );
    }

    // -- Reconnect from the last id. ---------------------------------------
    //
    // The whole point of the cursor: the catch-up must not re-deliver what
    // this client already has, while still covering anything that happened
    // while it was away.
    drop(stream);
    let away = JobId::new();
    let away_index = submit(&coord, away, entity, true).await;

    let mut resumed = Sse::open(&url, batch.id.as_deref()).await;
    let (caught_up, skipped) = resumed.next_named("batch").await;
    for frame in &skipped {
        assert_ne!(
            frame.event, "batch",
            "a resume must not re-deliver a batch at or below its cursor: {frame:?}"
        );
    }
    assert_eq!(
        caught_up.data["index"].as_u64(),
        Some(away_index),
        "the first batch after the cursor is the one submitted while away: {caught_up:?}"
    );
    assert_eq!(
        caught_up.data["events"][0]["job"],
        away.to_string(),
        "{caught_up:?}"
    );

    drop(resumed);
    coord.shutdown().await;
}
