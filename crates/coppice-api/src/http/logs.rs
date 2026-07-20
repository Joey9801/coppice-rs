//! `GET /api/v1/jobs/{job}/logs` — best-effort job log retrieval (ADR 0034).
//!
//! Every replica serves this from its own applied state (eventual class,
//! ADR 0031 — no leader involvement, no `NOT_LEADER` outcome). The handler
//! resolves the job's attempts from replicated state, then walks them
//! direction-matched from the cursor position, making at most four
//! [`ControlPlane::fetch_logs`] RPCs to the agents that ran them. The join of
//! "which attempts exist and where they ran" (replicated state) with "what
//! data still exists" (the agent's answer) is the per-attempt availability
//! verdict; a request that retrieves nothing is still `200` with the full
//! `sources` accounting.
//!
//! Page orchestration lives here, not in the plane: the plane is one RPC.

use std::sync::Arc;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;

use coppice_core::id::{AttemptId, JobId, NodeId};
use coppice_core::time::Timestamp;

use crate::{
    Consistency, ControlPlane, LogFetchError, LogFetchOutcome, LogFetchRequest, LogPage,
    LogResumePosition, LogStreamSelector,
};

use super::dto::{
    GetJobLogsResponse, LogAvailability, LogCursor, LogEntry, LogOrder, LogSourceRecord,
    LogStreamName,
};
use super::error::HttpError;
use super::extract::{IdPath, ReadIndexes, ReadQuery};

/// Default page size when `?limit=` is absent (ADR 0034).
const DEFAULT_LOG_LIMIT: u64 = 200;
/// Valid `?limit=` range; out of range is `INVALID_ARGUMENT`, never clamped.
const LOG_LIMIT_RANGE: std::ops::RangeInclusive<u64> = 1..=1000;
/// At most this many `FetchLogs` RPCs per request (ADR 0034's bounded work);
/// attempts that resolve without an RPC do not count against it.
const MAX_FETCH_RPCS: u32 = 4;
/// Server-side cap on chunk bytes served per page (~256 KiB, ADR 0034). Fed to
/// the RPC as `max_bytes`; when it trips, the page ends early with a cursor.
const PAGE_BYTE_CAP: usize = 256 * 1024;

/// The raw `?…` parameters, all optional strings/numbers so each can carry its
/// own `INVALID_ARGUMENT` message (rather than a serde-flavored one).
#[derive(Debug, Default, Deserialize)]
pub(crate) struct LogsParams {
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    stream: Option<String>,
    #[serde(default)]
    attempt: Option<String>,
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    to: Option<String>,
    #[serde(default)]
    to_inclusive: Option<bool>,
    #[serde(default)]
    order: Option<String>,
}

/// The validated request: everything parsed, ranges and cross-field rules
/// (cursor order vs `order`, `attempt` scope vs cursor) already enforced.
struct LogsRequest {
    limit: usize,
    /// Inclusive lower bound (µs), `None` = open.
    from_us: Option<i64>,
    /// Exclusive upper bound (µs), `None` = open. `to_inclusive` is already
    /// folded in as `to + 1µs`.
    until_us: Option<i64>,
    stream: Option<LogStreamSelector>,
    /// Scope the walk to one attempt; it must belong to the job.
    attempt: Option<AttemptId>,
    order: LogOrder,
    cursor: Option<LogCursor>,
}

/// `GET /api/v1/jobs/{job}/logs`.
pub(crate) async fn get_job_logs<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    IdPath(job): IdPath<JobId>,
    ReadQuery(read): ReadQuery,
    params: Result<Query<LogsParams>, QueryRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Query(params) = params.map_err(|e: QueryRejection| HttpError::invalid(e.body_text()))?;
    let request = parse_request(params)?;

    // Logs are the eventual class (ADR 0031): serve from the latest published
    // view, honoring `?consistency=`/`?min_index=` like every read.
    let view = plane
        .read_state(read.into_options(Consistency::Eventual))
        .await?;

    let response = walk(plane.as_ref(), view.state(), job, &request).await?;

    Ok((
        ReadIndexes {
            applied_index: view.applied_index(),
            committed_index: view.committed_index(),
        },
        Json(response),
    ))
}

/// Parse and cross-validate the query parameters.
fn parse_request(params: LogsParams) -> Result<LogsRequest, HttpError> {
    let limit = match params.limit {
        None => DEFAULT_LOG_LIMIT,
        Some(n) if LOG_LIMIT_RANGE.contains(&n) => n,
        Some(n) => {
            return Err(HttpError::invalid(format!(
                "limit {n} is out of range {}..={}",
                LOG_LIMIT_RANGE.start(),
                LOG_LIMIT_RANGE.end(),
            )))
        }
    } as usize;

    let order = match &params.order {
        None => LogOrder::Desc,
        Some(raw) => LogOrder::parse(raw).map_err(HttpError::invalid)?,
    };

    let stream = match &params.stream {
        None => None,
        Some(raw) => Some(
            match LogStreamName::parse(raw).map_err(HttpError::invalid)? {
                LogStreamName::Stdout => LogStreamSelector::Stdout,
                LogStreamName::Stderr => LogStreamSelector::Stderr,
            },
        ),
    };

    let attempt = match &params.attempt {
        None => None,
        Some(raw) => Some(
            raw.parse::<AttemptId>()
                .map_err(|e| HttpError::invalid(format!("invalid attempt id: {e}")))?,
        ),
    };

    let from_us = match &params.from {
        None => None,
        Some(raw) => Some(parse_instant(raw, "from")?.as_micros()),
    };

    // `to` is exclusive by default; `to_inclusive=true` closes the bound by
    // mapping it to `to + 1µs` (timestamps are µs-quantised).
    let until_us =
        match &params.to {
            None => None,
            Some(raw) => {
                let to = parse_instant(raw, "to")?.as_micros();
                if params.to_inclusive.unwrap_or(false) {
                    Some(to.checked_add(1).ok_or_else(|| {
                        HttpError::invalid("to is too large to close inclusively")
                    })?)
                } else {
                    Some(to)
                }
            }
        };

    let cursor = match &params.cursor {
        None => None,
        Some(token) => {
            let cursor = LogCursor::parse(token).map_err(HttpError::invalid)?;
            // A cursor whose direction disagrees with `order` is a caller error
            // (ADR 0034): the two must describe the same walk.
            if cursor.order != order {
                return Err(HttpError::invalid(format!(
                    "cursor order `{}` does not match order `{}`",
                    cursor.order.as_str(),
                    order.as_str(),
                )));
            }
            // Scoping to one attempt and resuming a different one is
            // contradictory.
            if let Some(scoped) = attempt {
                if cursor.attempt != scoped {
                    return Err(HttpError::invalid(
                        "cursor attempt does not match the `attempt` scope",
                    ));
                }
            }
            Some(cursor)
        }
    };

    Ok(LogsRequest {
        limit,
        from_us,
        until_us,
        stream,
        attempt,
        order,
        cursor,
    })
}

/// Parse one RFC 3339 instant, reusing [`Timestamp`]'s canonical parse (µs
/// precision, the same the DTOs serialize). No new dependency: the string is
/// fed through serde rather than a second RFC 3339 parser.
fn parse_instant(raw: &str, field: &str) -> Result<Timestamp, HttpError> {
    serde_json::from_value::<Timestamp>(serde_json::Value::String(raw.to_string()))
        .map_err(|e| HttpError::invalid(format!("invalid `{field}` timestamp: {e}")))
}

/// The ordered list of attempt ids to walk, direction-matched and optionally
/// scoped to a single attempt. `Err` when a scoped `attempt=` is not one of the
/// job's attempts (`INVALID_ARGUMENT`).
fn attempt_walk(
    record: &coppice_state::JobRecord,
    request: &LogsRequest,
) -> Result<Vec<AttemptId>, HttpError> {
    if let Some(scoped) = request.attempt {
        if !record.attempts.contains(&scoped) {
            return Err(HttpError::invalid(format!(
                "attempt {scoped} does not belong to this job"
            )));
        }
        return Ok(vec![scoped]);
    }
    let mut ids = record.attempts.clone();
    if request.order == LogOrder::Desc {
        ids.reverse();
    }
    Ok(ids)
}

/// Walk the attempts, gathering entries and per-attempt source records until
/// the page fills, the RPC budget is spent, or the byte cap trips.
async fn walk<P: ControlPlane>(
    plane: &P,
    state: &coppice_state::StateMachine,
    job: JobId,
    request: &LogsRequest,
) -> Result<GetJobLogsResponse, HttpError> {
    let record = state
        .jobs
        .get(&job)
        .ok_or_else(|| HttpError::not_found(format!("job {job} not found")))?;

    let list = attempt_walk(record, request)?;

    // Resolve the resume point from the cursor: which attempt to start at, and
    // whether from its edge or a mid-attempt position.
    let (start_index, start_resume) = match request.cursor {
        None => (0usize, None),
        Some(cursor) => {
            let idx = list
                .iter()
                .position(|a| *a == cursor.attempt)
                .ok_or_else(|| {
                    HttpError::invalid("cursor attempt is not among this job's attempts")
                })?;
            let resume = if cursor.is_edge() {
                None
            } else {
                Some(LogResumePosition {
                    at_us: cursor.at_us,
                    skip: cursor.skip,
                })
            };
            (idx, resume)
        }
    };

    let order = request.order;
    let mut entries: Vec<LogEntry> = Vec::new();
    let mut sources: Vec<LogSourceRecord> = Vec::new();
    let mut rpcs: u32 = 0;
    let mut bytes: usize = 0;
    let mut next_cursor: Option<LogCursor> = None;

    let n = list.len();
    let mut i = start_index;
    while i < n {
        let attempt_id = list[i];
        // Only the first attempt of the walk inherits the cursor's resume
        // position; later attempts start from their edge.
        let resume = if i == start_index { start_resume } else { None };

        // Resolve the attempt record. It should exist while the job does; if it
        // somehow does not, we cannot reach its logs — an honest `unreachable`.
        let Some(ar) = state.attempts.get(&attempt_id) else {
            sources.push(source(
                attempt_id,
                None,
                LogAvailability::Unreachable,
                false,
                None,
                Some("attempt record is not in the replicated state".to_string()),
            ));
            i += 1;
            continue;
        };
        let node_id = ar.attempt.node;

        // An attempt that never reached `Running` captured nothing — no RPC.
        if ar.started_at.is_none() {
            sources.push(source(
                attempt_id,
                Some(node_id),
                LogAvailability::NotStarted,
                false,
                None,
                Some("attempt never reached Running; nothing was captured".to_string()),
            ));
            i += 1;
            continue;
        }

        // Resolve the node and its advertised log service — both are
        // `unreachable` verdicts made without an RPC.
        let Some(node_record) = state.nodes.get(&node_id) else {
            sources.push(source(
                attempt_id,
                Some(node_id),
                LogAvailability::Unreachable,
                false,
                None,
                Some(format!("node {node_id} is no longer in the cluster")),
            ));
            i += 1;
            continue;
        };
        let addr = node_record
            .node
            .service_addr
            .as_deref()
            .filter(|a| !a.is_empty());
        let Some(addr) = addr else {
            sources.push(source(
                attempt_id,
                Some(node_id),
                LogAvailability::Unreachable,
                false,
                None,
                Some(format!("node {node_id} advertises no log service")),
            ));
            i += 1;
            continue;
        };

        // The budget is only spent by an attempt that actually needs an RPC.
        // When it is exhausted, end the page here and resume at this attempt.
        if rpcs >= MAX_FETCH_RPCS {
            next_cursor = Some(cursor_at(order, attempt_id, resume));
            break;
        }

        let remaining_limit = request.limit - entries.len();
        let remaining_bytes = PAGE_BYTE_CAP - bytes;
        let rpc = LogFetchRequest {
            job,
            attempt: attempt_id,
            from_us: request.from_us,
            until_us: request.until_us,
            stream: request.stream,
            resume,
            ascending: order.ascending(),
            max_chunks: clamp_u32(remaining_limit),
            max_bytes: clamp_u32(remaining_bytes),
        };
        rpcs += 1;

        match plane.fetch_logs(node_id, addr, rpc).await {
            Err(LogFetchError::Unreachable { reason }) => {
                sources.push(source(
                    attempt_id,
                    Some(node_id),
                    LogAvailability::Unreachable,
                    false,
                    None,
                    Some(reason),
                ));
                i += 1;
            }
            Ok(LogFetchOutcome::UnknownAttempt) => {
                sources.push(source(
                    attempt_id,
                    Some(node_id),
                    LogAvailability::Expired,
                    false,
                    None,
                    Some(format!(
                        "node {node_id} no longer retains logs for this attempt \
                         (it ran there; telemetry has fallen out of retention)"
                    )),
                ));
                i += 1;
            }
            Ok(LogFetchOutcome::Chunks(page)) => {
                // The store honored `max_chunks`/`max_bytes`, so every returned
                // chunk fits the remaining page budget.
                let truncated = matches!(
                    (request.from_us, page.earliest_at_us),
                    (Some(from), Some(earliest)) if from < earliest
                );
                sources.push(LogSourceRecord {
                    attempt: attempt_id,
                    node: Some(node_id),
                    availability: LogAvailability::Available,
                    truncated,
                    earliest_available_at: page.earliest_at_us.and_then(Timestamp::from_micros),
                    reason: None,
                });

                for chunk in &page.chunks {
                    bytes += chunk.payload.len();
                    entries.push(LogEntry {
                        attempt: attempt_id,
                        at: Timestamp::from_micros(chunk.at_us)
                            .unwrap_or_else(Timestamp::min_value),
                        stream: match chunk.stream {
                            LogStreamSelector::Stdout => LogStreamName::Stdout,
                            LogStreamSelector::Stderr => LogStreamName::Stderr,
                        },
                        text: String::from_utf8_lossy(&chunk.payload).into_owned(),
                        truncated: chunk.truncated,
                    });
                }

                match page_disposition(&page, resume, entries.len(), request.limit, bytes) {
                    Disposition::MoreInAttempt { at_us, skip } => {
                        next_cursor = Some(LogCursor {
                            order,
                            attempt: attempt_id,
                            at_us,
                            skip,
                        });
                        break;
                    }
                    Disposition::AttemptDoneButFull => {
                        next_cursor = if i + 1 < n {
                            Some(LogCursor::edge(order, list[i + 1]))
                        } else {
                            None
                        };
                        break;
                    }
                    Disposition::Continue => {
                        i += 1;
                    }
                }
            }
        }
    }

    Ok(GetJobLogsResponse {
        entries,
        sources,
        next_cursor: next_cursor.map(|c| c.format()),
    })
}

/// What to do after appending an attempt's page of chunks.
enum Disposition {
    /// This attempt has more chunks in range; resume here at `(at_us, skip)`.
    MoreInAttempt { at_us: i64, skip: u64 },
    /// This attempt is fully consumed, but the page is full — resume at the
    /// next attempt's edge.
    AttemptDoneButFull,
    /// This attempt is done and there is room; move to the next attempt.
    Continue,
}

/// Decide how the walk proceeds after one attempt's page.
///
/// `resume` is the exclusive lower position that was sent (so a mid-attempt
/// resume cursor can extend the `skip` when the page ends at the same
/// microsecond it resumed from).
fn page_disposition(
    page: &LogPage,
    resume: Option<LogResumePosition>,
    entries_len: usize,
    limit: usize,
    bytes: usize,
) -> Disposition {
    if !page.exhausted {
        // A cap cut this attempt short; more chunks exist in range. Resume at
        // the last chunk's coordinate. A well-behaved store makes progress
        // (returns at least one chunk) when the range is non-empty; an empty
        // short page has no coordinate to resume from, so we advance rather
        // than wedge.
        if let Some(last) = page.chunks.last() {
            let at_us = last.at_us;
            let count_at_last = page
                .chunks
                .iter()
                .rev()
                .take_while(|c| c.at_us == at_us)
                .count() as u64;
            let base_skip = match resume {
                Some(r) if r.at_us == at_us => r.skip,
                _ => 0,
            };
            return Disposition::MoreInAttempt {
                at_us,
                skip: base_skip + count_at_last,
            };
        }
        return Disposition::Continue;
    }

    // Attempt fully consumed within the range.
    if entries_len >= limit || bytes >= PAGE_BYTE_CAP {
        Disposition::AttemptDoneButFull
    } else {
        Disposition::Continue
    }
}

/// The cursor for "resume at `attempt` at `resume`" — its edge when there is no
/// resume position.
fn cursor_at(order: LogOrder, attempt: AttemptId, resume: Option<LogResumePosition>) -> LogCursor {
    match resume {
        Some(r) => LogCursor {
            order,
            attempt,
            at_us: r.at_us,
            skip: r.skip,
        },
        None => LogCursor::edge(order, attempt),
    }
}

/// A source record with the given verdict.
fn source(
    attempt: AttemptId,
    node: Option<NodeId>,
    availability: LogAvailability,
    truncated: bool,
    earliest_available_at: Option<Timestamp>,
    reason: Option<String>,
) -> LogSourceRecord {
    LogSourceRecord {
        attempt,
        node,
        availability,
        truncated,
        earliest_available_at,
        reason,
    }
}

/// Saturate a page-budget count into the RPC's `u32` cap field.
fn clamp_u32(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;

    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use coppice_core::attempt::{Attempt, AttemptState};
    use coppice_core::id::{AllocationId, ClusterId, GroupId};
    use coppice_core::job::{Job, JobState};
    use coppice_core::node::Node;
    use coppice_core::quota::{ChargeRecord, CostUnits, PriorityMultiplier, FULL_REFUND_MILLI};
    use coppice_core::resource::Resources;
    use coppice_state::{AttemptRecord, JobRecord, NodeRecord, StateMachine};

    use crate::http::dto::{AbortJobRequest, ConfigureQuotaEntityRequest, SubmitJobRequest};
    use crate::http::dto::{ConfigureQuotaEntityResponse, SubmitJobResponse};
    use crate::{
        ApiError, CoordinatorSummary, LogChunk, LogPage, QueueWindow, ReadOptions, ReadView,
        RecentClusterEvents,
    };

    /// A `ControlPlane` that serves a seeded state and canned per-attempt log
    /// outcomes, counting fetch RPCs so a test can assert the 4-RPC budget.
    struct FakePlane {
        state: StateMachine,
        /// Per-attempt FIFO of canned outcomes; each `fetch_logs` pops one.
        outcomes: Mutex<HashMap<AttemptId, VecDeque<Result<LogFetchOutcome, LogFetchError>>>>,
        fetches: AtomicU32,
    }

    impl FakePlane {
        fn new(state: StateMachine) -> FakePlane {
            FakePlane {
                state,
                outcomes: Mutex::new(HashMap::new()),
                fetches: AtomicU32::new(0),
            }
        }

        fn seed(&self, attempt: AttemptId, outcome: Result<LogFetchOutcome, LogFetchError>) {
            self.outcomes
                .lock()
                .unwrap()
                .entry(attempt)
                .or_default()
                .push_back(outcome);
        }

        fn fetch_count(&self) -> u32 {
            self.fetches.load(Ordering::SeqCst)
        }
    }

    impl ControlPlane for FakePlane {
        fn cluster_id(&self) -> ClusterId {
            ClusterId::new()
        }
        fn queue_window(&self) -> QueueWindow {
            QueueWindow::default()
        }
        async fn recent_events(&self, _limit: usize) -> RecentClusterEvents {
            RecentClusterEvents {
                floor_index: 1,
                events: Vec::new(),
            }
        }
        fn coordinator_status(&self) -> Result<CoordinatorSummary, ApiError> {
            Err(ApiError::Unavailable("no consensus handle".into()))
        }
        async fn submit_job(&self, _req: SubmitJobRequest) -> Result<SubmitJobResponse, ApiError> {
            unimplemented!("logs tests never submit")
        }
        async fn abort_job(&self, _req: AbortJobRequest) -> Result<(), ApiError> {
            unimplemented!("logs tests never abort")
        }
        async fn configure_quota_entity(
            &self,
            _req: ConfigureQuotaEntityRequest,
        ) -> Result<ConfigureQuotaEntityResponse, ApiError> {
            unimplemented!("logs tests never configure")
        }
        async fn read_state(&self, _opts: ReadOptions) -> Result<ReadView, ApiError> {
            Ok(ReadView::new(self.state.clone(), 1, 1))
        }
        async fn fetch_logs(
            &self,
            _node: NodeId,
            _addr: &str,
            req: LogFetchRequest,
        ) -> Result<LogFetchOutcome, LogFetchError> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.outcomes
                .lock()
                .unwrap()
                .get_mut(&req.attempt)
                .and_then(|q| q.pop_front())
                .unwrap_or(Err(LogFetchError::Unreachable {
                    reason: "no canned outcome seeded".into(),
                }))
        }
    }

    // ---- state builders --------------------------------------------------

    const GOOD_ADDR: &str = "10.0.0.1:9100";

    fn good_node() -> (NodeId, NodeRecord) {
        node_with_service(Some(GOOD_ADDR.to_string()))
    }

    fn silent_node() -> (NodeId, NodeRecord) {
        node_with_service(None)
    }

    fn node_with_service(service_addr: Option<String>) -> (NodeId, NodeRecord) {
        let id = NodeId::new();
        (
            id,
            NodeRecord {
                node: Node {
                    id,
                    capacity: Resources::ZERO,
                    labels: Default::default(),
                    schedulable: true,
                    service_addr,
                },
                epoch: 1,
            },
        )
    }

    fn attempt_rec(id: AttemptId, job: JobId, node: NodeId, started: bool) -> AttemptRecord {
        AttemptRecord {
            attempt: Attempt {
                id,
                job,
                allocation: AllocationId::new(),
                node,
                state: if started {
                    AttemptState::Running
                } else {
                    AttemptState::Accruing
                },
            },
            group: GroupId(job.0),
            charge: ChargeRecord {
                amount: CostUnits(0),
                charged_at: ts(0),
                refund_fraction_milli: FULL_REFUND_MILLI,
            },
            rate_ucu_per_second: 0,
            multiplier: PriorityMultiplier::ONE,
            started_at: started.then(|| ts(1000)),
        }
    }

    fn job_rec(id: JobId, attempts: Vec<AttemptId>) -> JobRecord {
        JobRecord {
            spec: Job {
                id,
                image: "busybox".to_string(),
                command: vec!["run".to_string()],
                entrypoint: None,
                requests: Resources::ZERO,
                priority: 0,
                max_runtime: None,
                quota_entity: coppice_core::id::QuotaEntityId::new(),
                retry: Default::default(),
                abort_requested: None,
            },
            state: JobState::Queued,
            multiplier: PriorityMultiplier::ONE,
            submitted_at: ts(0),
            terminal_at: None,
            retries_used: 0,
            attempts,
        }
    }

    fn ts(micros: i64) -> Timestamp {
        Timestamp::from_micros(micros).expect("fixture timestamp in range")
    }

    fn chunk(at_us: i64, text: &str) -> LogChunk {
        LogChunk {
            at_us,
            stream: LogStreamSelector::Stdout,
            payload: text.as_bytes().to_vec(),
            truncated: false,
        }
    }

    /// A one-page `Chunks` outcome that is fully consumed within range.
    fn page(chunks: Vec<LogChunk>) -> Result<LogFetchOutcome, LogFetchError> {
        let earliest = chunks.iter().map(|c| c.at_us).min();
        let latest = chunks.iter().map(|c| c.at_us).max();
        Ok(LogFetchOutcome::Chunks(LogPage {
            chunks,
            exhausted: true,
            earliest_at_us: earliest,
            latest_at_us: latest,
        }))
    }

    // ---- request helpers -------------------------------------------------

    async fn get(plane: Arc<FakePlane>, uri: &str) -> (StatusCode, serde_json::Value) {
        let router = crate::http::router(plane);
        let response = router
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        // Some 200s and all errors are JSON; empty bodies never happen here.
        let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    // ---- tests -----------------------------------------------------------

    #[tokio::test]
    async fn unknown_job_is_404() {
        let plane = Arc::new(FakePlane::new(StateMachine::default()));
        let job = JobId::new();
        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/logs")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "NOT_FOUND");
    }

    #[tokio::test]
    async fn zero_attempt_job_is_empty_but_ok() {
        let job = JobId::new();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, Vec::new()));
        let plane = Arc::new(FakePlane::new(state));
        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/logs")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["entries"], serde_json::json!([]));
        assert_eq!(body["sources"], serde_json::json!([]));
        assert_eq!(body["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn not_started_attempt_reports_without_an_rpc() {
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let attempt = AttemptId::new();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, vec![attempt]));
        state
            .attempts
            .insert(attempt, attempt_rec(attempt, job, node, false));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));

        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/logs")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 0);
        assert_eq!(body["sources"][0]["availability"], "not_started");
        assert_eq!(body["entries"], serde_json::json!([]));
        assert_eq!(body["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn available_attempt_returns_entries() {
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let attempt = AttemptId::new();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, vec![attempt]));
        state
            .attempts
            .insert(attempt, attempt_rec(attempt, job, node, true));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));
        plane.seed(attempt, page(vec![chunk(1_000_000, "hello")]));

        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/logs")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 1);
        assert_eq!(body["sources"][0]["availability"], "available");
        assert_eq!(body["sources"][0]["truncated"], false);
        assert_eq!(body["entries"][0]["text"], "hello");
        assert_eq!(body["entries"][0]["stream"], "stdout");
        assert_eq!(body["entries"][0]["attempt"], attempt.to_string());
        assert_eq!(body["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn oversized_chunk_entry_is_flagged_truncated_and_stays_within_the_cap() {
        // The store cut a single oversized chunk down to the page byte budget and
        // marked it (ADR 0034 bypass fix). The handler must surface that on the
        // entry and the page must stay within `PAGE_BYTE_CAP`.
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let attempt = AttemptId::new();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, vec![attempt]));
        state
            .attempts
            .insert(attempt, attempt_rec(attempt, job, node, true));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));
        // A chunk already truncated by the store to a modest prefix.
        let cut = LogChunk {
            at_us: 1_000_000,
            stream: LogStreamSelector::Stdout,
            payload: b"0123".to_vec(),
            truncated: true,
        };
        plane.seed(
            attempt,
            Ok(LogFetchOutcome::Chunks(LogPage {
                chunks: vec![cut],
                exhausted: false,
                earliest_at_us: Some(1_000_000),
                latest_at_us: Some(1_000_000),
            })),
        );

        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/logs")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["entries"][0]["truncated"], true);
        assert_eq!(body["entries"][0]["text"], "0123");
        let served: usize = body["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["text"].as_str().unwrap().len())
            .sum();
        assert!(
            served <= PAGE_BYTE_CAP,
            "the page stays within the byte cap"
        );
    }

    #[tokio::test]
    async fn truncated_when_earliest_is_after_the_from_bound() {
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let attempt = AttemptId::new();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, vec![attempt]));
        state
            .attempts
            .insert(attempt, attempt_rec(attempt, job, node, true));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));
        // Store's earliest (5_000_000) is later than the requested `from`
        // (1970-01-01T00:00:01Z = 1_000_000µs): older lines were pruned.
        plane.seed(attempt, page(vec![chunk(5_000_000, "later")]));

        let (status, body) = get(
            plane,
            &format!("/api/v1/jobs/{job}/logs?from=1970-01-01T00:00:01Z"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["sources"][0]["availability"], "available");
        assert_eq!(body["sources"][0]["truncated"], true);
        assert_eq!(
            body["sources"][0]["earliest_available_at"],
            "1970-01-01T00:00:05.000000Z"
        );
    }

    #[tokio::test]
    async fn unknown_attempt_answer_is_expired() {
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let attempt = AttemptId::new();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, vec![attempt]));
        state
            .attempts
            .insert(attempt, attempt_rec(attempt, job, node, true));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));
        plane.seed(attempt, Ok(LogFetchOutcome::UnknownAttempt));

        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/logs")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["sources"][0]["availability"], "expired");
        assert!(body["sources"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("retention"));
    }

    #[tokio::test]
    async fn no_service_addr_is_unreachable_without_an_rpc() {
        let job = JobId::new();
        let (node, node_rec) = silent_node();
        let attempt = AttemptId::new();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, vec![attempt]));
        state
            .attempts
            .insert(attempt, attempt_rec(attempt, job, node, true));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));

        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/logs")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 0);
        assert_eq!(body["sources"][0]["availability"], "unreachable");
    }

    #[tokio::test]
    async fn missing_node_record_is_unreachable() {
        let job = JobId::new();
        let node = NodeId::new(); // never inserted
        let attempt = AttemptId::new();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, vec![attempt]));
        state
            .attempts
            .insert(attempt, attempt_rec(attempt, job, node, true));
        let plane = Arc::new(FakePlane::new(state));

        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/logs")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 0);
        assert_eq!(body["sources"][0]["availability"], "unreachable");
        assert!(body["sources"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("no longer in the cluster"));
    }

    /// Build a job whose attempts are all available on one good node, seeding
    /// each with a single distinguishable chunk.
    fn multi_available(count: usize) -> (JobId, Vec<AttemptId>, Arc<FakePlane>) {
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let mut state = StateMachine::default();
        let mut attempts = Vec::new();
        for _ in 0..count {
            let a = AttemptId::new();
            attempts.push(a);
            state.attempts.insert(a, attempt_rec(a, job, node, true));
        }
        state.jobs.insert(job, job_rec(job, attempts.clone()));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));
        for (i, a) in attempts.iter().enumerate() {
            plane.seed(
                *a,
                page(vec![chunk(1_000_000 + i as i64, &format!("a{i}"))]),
            );
        }
        (job, attempts, plane)
    }

    #[tokio::test]
    async fn desc_walk_orders_attempts_newest_first() {
        let (job, attempts, plane) = multi_available(2);
        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/logs")).await;
        assert_eq!(status, StatusCode::OK);
        // Desc: last-created attempt first.
        assert_eq!(body["sources"][0]["attempt"], attempts[1].to_string());
        assert_eq!(body["sources"][1]["attempt"], attempts[0].to_string());
        assert_eq!(body["entries"][0]["attempt"], attempts[1].to_string());
        assert_eq!(body["entries"][1]["attempt"], attempts[0].to_string());
        assert_eq!(body["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn asc_walk_orders_attempts_oldest_first() {
        let (job, attempts, plane) = multi_available(2);
        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/logs?order=asc")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["sources"][0]["attempt"], attempts[0].to_string());
        assert_eq!(body["sources"][1]["attempt"], attempts[1].to_string());
    }

    #[tokio::test]
    async fn rpc_budget_ends_the_page_with_a_cursor() {
        // Five available attempts; the 4-RPC budget stops after four, and the
        // page ends with a cursor rather than fetching the fifth.
        let (job, _attempts, plane) = multi_available(5);
        let (status, body) =
            get(plane.clone(), &format!("/api/v1/jobs/{job}/logs?order=asc")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 4);
        assert_eq!(body["entries"].as_array().unwrap().len(), 4);
        assert_eq!(body["sources"].as_array().unwrap().len(), 4);
        assert!(
            body["next_cursor"].is_string(),
            "a short page carries a cursor"
        );
    }

    #[tokio::test]
    async fn budget_cursor_resumes_the_remaining_attempts() {
        let (job, attempts, plane) = multi_available(5);
        let (_s1, page1) = get(plane.clone(), &format!("/api/v1/jobs/{job}/logs?order=asc")).await;
        let cursor = page1["next_cursor"].as_str().unwrap().to_string();
        // The cursor names the fifth (unfetched) attempt at its edge.
        assert!(cursor.contains(&attempts[4].to_string()));

        let (status, page2) = get(
            plane.clone(),
            &format!("/api/v1/jobs/{job}/logs?order=asc&cursor={cursor}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // Only the fifth attempt remained.
        assert_eq!(page2["sources"].as_array().unwrap().len(), 1);
        assert_eq!(page2["sources"][0]["attempt"], attempts[4].to_string());
        assert_eq!(page2["entries"][0]["attempt"], attempts[4].to_string());
        assert_eq!(page2["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn cursor_advances_past_an_unreachable_attempt() {
        // Seven attempts: a0..a3 spend the budget; a4 is the next RPC-requiring
        // attempt so page 1 stops there with a cursor to a4. On page 2 the walk
        // serves a4, hits an unreachable a5 (no service addr, no RPC), and
        // still reaches a6 — proving the cursor advanced past the dead node
        // mid-page rather than wedging on it.
        let job = JobId::new();
        let (good, good_rec) = good_node();
        let (silent, silent_rec) = silent_node();
        let mut state = StateMachine::default();
        let mut attempts = Vec::new();
        for i in 0..7 {
            let a = AttemptId::new();
            attempts.push(a);
            let node = if i == 5 { silent } else { good };
            state.attempts.insert(a, attempt_rec(a, job, node, true));
        }
        state.jobs.insert(job, job_rec(job, attempts.clone()));
        state.nodes.insert(good, good_rec);
        state.nodes.insert(silent, silent_rec);
        let plane = Arc::new(FakePlane::new(state));
        for (i, a) in attempts.iter().enumerate() {
            if i != 5 {
                plane.seed(
                    *a,
                    page(vec![chunk(1_000_000 + i as i64, &format!("a{i}"))]),
                );
            }
        }

        let (_s1, page1) = get(plane.clone(), &format!("/api/v1/jobs/{job}/logs?order=asc")).await;
        // Page 1 fetched a0..a3 (budget) and stopped at a4.
        assert_eq!(plane.fetch_count(), 4);
        assert_eq!(page1["sources"].as_array().unwrap().len(), 4);
        let cursor = page1["next_cursor"].as_str().unwrap().to_string();
        assert!(cursor.contains(&attempts[4].to_string()));

        let (status, page2) = get(
            plane.clone(),
            &format!("/api/v1/jobs/{job}/logs?order=asc&cursor={cursor}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // a4 available, a5 unreachable (no new RPC), a6 available.
        let sources = page2["sources"].as_array().unwrap();
        assert_eq!(sources.len(), 3);
        assert_eq!(sources[0]["attempt"], attempts[4].to_string());
        assert_eq!(sources[0]["availability"], "available");
        assert_eq!(sources[1]["attempt"], attempts[5].to_string());
        assert_eq!(sources[1]["availability"], "unreachable");
        assert_eq!(sources[2]["attempt"], attempts[6].to_string());
        assert_eq!(sources[2]["availability"], "available");
        // page 2 made two RPCs (a4, a6); a5 needed none.
        assert_eq!(plane.fetch_count(), 6);
        assert_eq!(page2["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn byte_cap_ends_the_page_before_the_next_attempt() {
        // The first attempt returns a page that exactly fills the byte cap;
        // the walk stops there with a cursor to the second, without fetching it.
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let a0 = AttemptId::new();
        let a1 = AttemptId::new();
        let mut state = StateMachine::default();
        state.attempts.insert(a0, attempt_rec(a0, job, node, true));
        state.attempts.insert(a1, attempt_rec(a1, job, node, true));
        state.jobs.insert(job, job_rec(job, vec![a0, a1]));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));
        // A payload the size of the whole page cap, returned as exhausted.
        let big = "x".repeat(PAGE_BYTE_CAP);
        plane.seed(a0, page(vec![chunk(1_000_000, &big)]));
        plane.seed(a1, page(vec![chunk(2_000_000, "second")]));

        let (status, body) =
            get(plane.clone(), &format!("/api/v1/jobs/{job}/logs?order=asc")).await;
        assert_eq!(status, StatusCode::OK);
        // Only the first attempt was fetched; the byte cap ended the page.
        assert_eq!(plane.fetch_count(), 1);
        assert_eq!(body["sources"].as_array().unwrap().len(), 1);
        assert_eq!(body["sources"][0]["attempt"], a0.to_string());
        assert!(body["next_cursor"].is_string());
    }

    #[tokio::test]
    async fn short_store_page_yields_a_mid_attempt_cursor() {
        // The store cuts the attempt short (`exhausted = false`): the cursor
        // resumes within the same attempt, past the last chunk shown.
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let attempt = AttemptId::new();
        let mut state = StateMachine::default();
        state
            .attempts
            .insert(attempt, attempt_rec(attempt, job, node, true));
        state.jobs.insert(job, job_rec(job, vec![attempt]));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));
        plane.seed(
            attempt,
            Ok(LogFetchOutcome::Chunks(LogPage {
                chunks: vec![chunk(7_000_000, "partial")],
                exhausted: false,
                earliest_at_us: Some(7_000_000),
                latest_at_us: Some(7_000_000),
            })),
        );

        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/logs")).await;
        assert_eq!(status, StatusCode::OK);
        let cursor = body["next_cursor"].as_str().unwrap();
        // Mid-attempt: same attempt, the last chunk's µs, skip = 1.
        let parsed = LogCursor::parse(cursor).unwrap();
        assert_eq!(parsed.attempt, attempt);
        assert_eq!(parsed.at_us, 7_000_000);
        assert_eq!(parsed.skip, 1);
        assert!(!parsed.is_edge());
    }

    // ---- parameter validation -------------------------------------------

    async fn invalid(uri_suffix: &str) {
        let job = JobId::new();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, Vec::new()));
        let plane = Arc::new(FakePlane::new(state));
        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/logs?{uri_suffix}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri_suffix}");
        assert_eq!(body["code"], "INVALID_ARGUMENT", "{uri_suffix}");
    }

    #[tokio::test]
    async fn rejects_bad_parameters() {
        invalid("limit=0").await;
        invalid("limit=1001").await;
        invalid("stream=stdlog").await;
        invalid("order=sideways").await;
        invalid("from=not-a-timestamp").await;
        invalid("to=2026-13-40T99:99:99Z").await;
        invalid("attempt=not-an-attempt").await;
        invalid("cursor=garbage").await;
        invalid("cursor=v2%3Adesc%3Aattempt").await;
    }

    #[tokio::test]
    async fn cursor_order_mismatch_is_invalid() {
        let job = JobId::new();
        let attempt = AttemptId::new();
        // A desc cursor with an asc request.
        let cursor = LogCursor::edge(LogOrder::Desc, attempt).format();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, vec![attempt]));
        let plane = Arc::new(FakePlane::new(state));
        let (status, body) = get(
            plane,
            &format!("/api/v1/jobs/{job}/logs?order=asc&cursor={cursor}"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn attempt_not_of_job_is_invalid() {
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let mine = AttemptId::new();
        let stranger = AttemptId::new();
        let mut state = StateMachine::default();
        state
            .attempts
            .insert(mine, attempt_rec(mine, job, node, true));
        state.jobs.insert(job, job_rec(job, vec![mine]));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));
        let (status, body) = get(
            plane,
            &format!("/api/v1/jobs/{job}/logs?attempt={stranger}"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn attempt_scope_limits_the_walk_to_one_attempt() {
        let (job, attempts, plane) = multi_available(3);
        let (status, body) = get(
            plane.clone(),
            &format!("/api/v1/jobs/{job}/logs?attempt={}", attempts[1]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 1);
        assert_eq!(body["sources"].as_array().unwrap().len(), 1);
        assert_eq!(body["sources"][0]["attempt"], attempts[1].to_string());
    }

    #[test]
    fn to_inclusive_maps_to_plus_one_microsecond() {
        // Directly exercise the parser's inclusive-bound arithmetic.
        let params = LogsParams {
            to: Some("1970-01-01T00:00:02Z".to_string()),
            to_inclusive: Some(true),
            ..Default::default()
        };
        let req = parse_request(params).unwrap();
        assert_eq!(req.until_us, Some(2_000_001));

        let exclusive = parse_request(LogsParams {
            to: Some("1970-01-01T00:00:02Z".to_string()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(exclusive.until_us, Some(2_000_000));
    }

    #[test]
    fn cursor_round_trips_through_format_and_parse() {
        let attempt = AttemptId::new();
        let cursor = LogCursor {
            order: LogOrder::Desc,
            attempt,
            at_us: 1_753_003_872_123_456,
            skip: 2,
        };
        let parsed = LogCursor::parse(&cursor.format()).unwrap();
        assert_eq!(parsed, cursor);
    }
}
