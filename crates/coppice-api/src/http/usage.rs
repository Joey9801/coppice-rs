//! `GET /api/v1/jobs/{job}/usage` — best-effort job usage-metrics retrieval
//! (ADR 0031's reserved `GetJobUsage`), the metrics twin of the job-logs
//! pipeline (ADR 0034).
//!
//! Every replica serves this from its own applied state (eventual class,
//! ADR 0031 — no leader involvement). The handler resolves the job's attempts
//! from replicated state, then walks them direction-matched from the cursor
//! position, making at most four [`ControlPlane::fetch_metrics`] RPCs to the
//! agents that ran them. The join of "which attempts exist and where they ran"
//! (replicated state) with "what data still exists" (the agent's answer) is the
//! per-attempt availability verdict; a request that retrieves nothing is still
//! `200` with the full `sources` accounting.
//!
//! Page orchestration lives here, not in the plane: the plane is one RPC. The
//! structure mirrors [`super::logs`] exactly, save two deliberate divergences:
//! there is no page byte cap (samples are fixed-size rows, so only the sample
//! count bounds a page), and `order` defaults to `asc` (a chart-ordered time
//! series) rather than logs' newest-first `desc`.

use std::sync::Arc;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;

use coppice_core::id::{AttemptId, JobId, NodeId};
use coppice_core::time::Timestamp;

use crate::{
    Consistency, ControlPlane, LogResumePosition, MetricsFetchError, MetricsFetchOutcome,
    MetricsFetchRequest, MetricsPage,
};

use super::dto::{
    GetJobUsageResponse, LogOrder, UsageAvailability, UsageCursor, UsagePoint, UsageSourceRecord,
};
use super::error::HttpError;
use super::extract::{IdPath, ReadIndexes, ReadQuery};

/// Default page size when `?limit=` is absent.
const DEFAULT_USAGE_LIMIT: u64 = 1000;
/// Valid `?limit=` range; out of range is `INVALID_ARGUMENT`, never clamped.
const USAGE_LIMIT_RANGE: std::ops::RangeInclusive<u64> = 1..=5000;
/// At most this many `FetchMetrics` RPCs per request (the bounded work);
/// attempts that resolve without an RPC do not count against it.
const MAX_FETCH_RPCS: u32 = 4;
/// At most this many source records (i.e. attempts examined) per response.
/// The no-RPC branches (not-started, missing node/service, missing attempt
/// record) append a source record without spending an RPC, so without this
/// ceiling an uncapped retry history could make one response scan and
/// serialize every attempt regardless of `limit`. Bounding the source count
/// keeps a single response's work bounded; unexamined attempts stay reachable
/// through the minted edge cursor.
const MAX_SOURCES: usize = 32;

/// The raw `?…` parameters, all optional strings/numbers so each can carry its
/// own `INVALID_ARGUMENT` message (rather than a serde-flavored one).
#[derive(Debug, Default, Deserialize)]
pub(crate) struct UsageParams {
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
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
struct UsageRequest {
    limit: usize,
    /// Inclusive lower bound (µs), `None` = open.
    from_us: Option<i64>,
    /// Exclusive upper bound (µs), `None` = open. `to_inclusive` is already
    /// folded in as `to + 1µs`.
    until_us: Option<i64>,
    /// Scope the walk to one attempt; it must belong to the job.
    attempt: Option<AttemptId>,
    order: LogOrder,
    cursor: Option<UsageCursor>,
}

/// `GET /api/v1/jobs/{job}/usage`.
pub(crate) async fn get_job_usage<P: ControlPlane>(
    State(plane): State<Arc<P>>,
    IdPath(job): IdPath<JobId>,
    ReadQuery(read): ReadQuery,
    params: Result<Query<UsageParams>, QueryRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Query(params) = params.map_err(|e: QueryRejection| HttpError::invalid(e.body_text()))?;
    let request = parse_request(params)?;

    // Usage is the eventual class (ADR 0031): serve from the latest published
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
fn parse_request(params: UsageParams) -> Result<UsageRequest, HttpError> {
    let limit = match params.limit {
        None => DEFAULT_USAGE_LIMIT,
        Some(n) if USAGE_LIMIT_RANGE.contains(&n) => n,
        Some(n) => {
            return Err(HttpError::invalid(format!(
                "limit {n} is out of range {}..={}",
                USAGE_LIMIT_RANGE.start(),
                USAGE_LIMIT_RANGE.end(),
            )))
        }
    } as usize;

    // Time series are chart-ordered, so `asc` (oldest-first) is the default —
    // a deliberate divergence from logs, which default `desc` (newest-first).
    let order = match &params.order {
        None => LogOrder::Asc,
        Some(raw) => LogOrder::parse(raw).map_err(HttpError::invalid)?,
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
            let cursor = UsageCursor::parse(token).map_err(HttpError::invalid)?;
            // A cursor whose direction disagrees with `order` is a caller error:
            // the two must describe the same walk.
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

    Ok(UsageRequest {
        limit,
        from_us,
        until_us,
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
    request: &UsageRequest,
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

/// Walk the attempts, gathering samples and per-attempt source records until
/// the page fills or the RPC budget is spent.
async fn walk<P: ControlPlane>(
    plane: &P,
    state: &coppice_state::StateMachine,
    job: JobId,
    request: &UsageRequest,
) -> Result<GetJobUsageResponse, HttpError> {
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
    let mut samples: Vec<UsagePoint> = Vec::new();
    let mut sources: Vec<UsageSourceRecord> = Vec::new();
    let mut rpcs: u32 = 0;
    let mut next_cursor: Option<UsageCursor> = None;

    let n = list.len();
    let mut i = start_index;
    while i < n {
        // Bound the per-response source count so an uncapped retry history of
        // no-RPC attempts cannot make this response's work unbounded. The
        // current attempt `list[i]` is still unexamined, so mint an edge cursor
        // for it and stop. Placed above the RPC-budget break (which resumes the
        // current attempt too), the two bounds compose; and since the first
        // iteration has an empty `sources`, every response examines at least one
        // attempt — forward progress is guaranteed.
        if sources.len() >= MAX_SOURCES {
            next_cursor = Some(UsageCursor::edge(order, list[i]));
            break;
        }

        let attempt_id = list[i];
        // Only the first attempt of the walk inherits the cursor's resume
        // position; later attempts start from their edge.
        let resume = if i == start_index { start_resume } else { None };

        // Resolve the attempt record. It should exist while the job does; if it
        // somehow does not, we cannot reach its samples — an honest `unreachable`.
        let Some(ar) = state.attempts.get(&attempt_id) else {
            sources.push(source(
                attempt_id,
                None,
                UsageAvailability::Unreachable,
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
                UsageAvailability::NotStarted,
                false,
                None,
                Some("attempt never reached Running; nothing was captured".to_string()),
            ));
            i += 1;
            continue;
        }

        // Resolve the node and its advertised service — both are `unreachable`
        // verdicts made without an RPC.
        let Some(node_record) = state.nodes.get(&node_id) else {
            sources.push(source(
                attempt_id,
                Some(node_id),
                UsageAvailability::Unreachable,
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
                UsageAvailability::Unreachable,
                false,
                None,
                Some(format!("node {node_id} advertises no metrics service")),
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

        let remaining_limit = request.limit - samples.len();
        let rpc = MetricsFetchRequest {
            job,
            attempt: attempt_id,
            from_us: request.from_us,
            until_us: request.until_us,
            resume,
            ascending: order.ascending(),
            max_samples: clamp_u32(remaining_limit),
        };
        rpcs += 1;

        match plane.fetch_metrics(node_id, addr, rpc).await {
            Err(MetricsFetchError::Unreachable { reason }) => {
                sources.push(source(
                    attempt_id,
                    Some(node_id),
                    UsageAvailability::Unreachable,
                    false,
                    None,
                    Some(reason),
                ));
                i += 1;
            }
            Ok(MetricsFetchOutcome::UnknownAttempt) => {
                sources.push(source(
                    attempt_id,
                    Some(node_id),
                    UsageAvailability::Expired,
                    false,
                    None,
                    Some(format!(
                        "node {node_id} no longer retains metrics for this attempt \
                         (it ran there; telemetry has fallen out of retention)"
                    )),
                ));
                i += 1;
            }
            Ok(MetricsFetchOutcome::Samples(page)) => {
                // The store honored `max_samples`, so every returned sample fits
                // the remaining page budget.
                let truncated = matches!(
                    (request.from_us, page.earliest_at_us),
                    (Some(from), Some(earliest)) if from < earliest
                );
                sources.push(UsageSourceRecord {
                    attempt: attempt_id,
                    node: Some(node_id),
                    availability: UsageAvailability::Available,
                    truncated,
                    earliest_available_at: page.earliest_at_us.and_then(Timestamp::from_micros),
                    reason: None,
                });

                for sample in &page.samples {
                    samples.push(point(attempt_id, sample));
                }

                match page_disposition(&page, resume, samples.len(), request.limit) {
                    Disposition::MoreInAttempt { at_us, skip } => {
                        next_cursor = Some(UsageCursor {
                            order,
                            attempt: attempt_id,
                            at_us,
                            skip,
                        });
                        break;
                    }
                    Disposition::AttemptDoneButFull => {
                        next_cursor = if i + 1 < n {
                            Some(UsageCursor::edge(order, list[i + 1]))
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

    Ok(GetJobUsageResponse {
        samples,
        sources,
        next_cursor: next_cursor.map(|c| c.format()),
    })
}

/// What to do after appending an attempt's page of samples.
enum Disposition {
    /// This attempt has more samples in range; resume here at `(at_us, skip)`.
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
    page: &MetricsPage,
    resume: Option<LogResumePosition>,
    samples_len: usize,
    limit: usize,
) -> Disposition {
    if !page.exhausted {
        // `max_samples` cut this attempt short; more samples exist in range.
        // Resume at the last sample's coordinate. A well-behaved store makes
        // progress (returns at least one sample) when the range is non-empty;
        // an empty short page has no coordinate to resume from, so we advance
        // rather than wedge.
        if let Some(last) = page.samples.last() {
            let at_us = last.at_us;
            let count_at_last = page
                .samples
                .iter()
                .rev()
                .take_while(|s| s.at_us == at_us)
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
    if samples_len >= limit {
        Disposition::AttemptDoneButFull
    } else {
        Disposition::Continue
    }
}

/// The cursor for "resume at `attempt` at `resume`" — its edge when there is no
/// resume position.
fn cursor_at(
    order: LogOrder,
    attempt: AttemptId,
    resume: Option<LogResumePosition>,
) -> UsageCursor {
    match resume {
        Some(r) => UsageCursor {
            order,
            attempt,
            at_us: r.at_us,
            skip: r.skip,
        },
        None => UsageCursor::edge(order, attempt),
    }
}

/// A client sample point from a stored metric sample.
fn point(attempt: AttemptId, sample: &crate::MetricSample) -> UsagePoint {
    UsagePoint {
        attempt,
        at: Timestamp::from_micros(sample.at_us).unwrap_or_else(Timestamp::min_value),
        cpu_usage_total_us: sample.cpu_usage_total_us,
        cpu_throttled_total_us: sample.cpu_throttled_total_us,
        memory_used_bytes: sample.memory_used_bytes,
        memory_peak_bytes: sample.memory_peak_bytes,
        disk_writable_bytes: sample.disk_writable_bytes,
        disk_image_bytes: sample.disk_image_bytes,
        net_rx_bytes_total: sample.net_rx_bytes_total,
        net_tx_bytes_total: sample.net_tx_bytes_total,
        blkio_read_bytes_total: sample.blkio_read_bytes_total,
        blkio_write_bytes_total: sample.blkio_write_bytes_total,
    }
}

/// A source record with the given verdict.
fn source(
    attempt: AttemptId,
    node: Option<NodeId>,
    availability: UsageAvailability,
    truncated: bool,
    earliest_available_at: Option<Timestamp>,
    reason: Option<String>,
) -> UsageSourceRecord {
    UsageSourceRecord {
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
        ApiError, CoordinatorSummary, JobTimelineWindow, LogFetchError, LogFetchOutcome,
        LogFetchRequest, MetricSample, QueueWindow, ReadOptions, ReadView, RecentClusterEvents,
    };

    /// A `ControlPlane` that serves a seeded state and canned per-attempt
    /// metrics outcomes, counting fetch RPCs so a test can assert the 4-RPC
    /// budget.
    struct FakePlane {
        state: StateMachine,
        /// Per-attempt FIFO of canned outcomes; each `fetch_metrics` pops one.
        outcomes:
            Mutex<HashMap<AttemptId, VecDeque<Result<MetricsFetchOutcome, MetricsFetchError>>>>,
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

        fn seed(
            &self,
            attempt: AttemptId,
            outcome: Result<MetricsFetchOutcome, MetricsFetchError>,
        ) {
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
        async fn job_timeline(
            &self,
            _job: JobId,
            _after: Option<(u64, u32)>,
            _limit: usize,
        ) -> JobTimelineWindow {
            JobTimelineWindow {
                floor_index: 1,
                events: Vec::new(),
                next: None,
            }
        }
        fn coordinator_status(&self) -> Result<CoordinatorSummary, ApiError> {
            Err(ApiError::Unavailable("no consensus handle".into()))
        }
        async fn submit_job(&self, _req: SubmitJobRequest) -> Result<SubmitJobResponse, ApiError> {
            unimplemented!("usage tests never submit")
        }
        async fn abort_job(&self, _req: AbortJobRequest) -> Result<(), ApiError> {
            unimplemented!("usage tests never abort")
        }
        async fn configure_quota_entity(
            &self,
            _req: ConfigureQuotaEntityRequest,
        ) -> Result<ConfigureQuotaEntityResponse, ApiError> {
            unimplemented!("usage tests never configure")
        }
        async fn read_state(&self, _opts: ReadOptions) -> Result<ReadView, ApiError> {
            Ok(ReadView::new(self.state.clone(), 1, 1))
        }
        async fn fetch_logs(
            &self,
            _node: NodeId,
            _addr: &str,
            _req: LogFetchRequest,
        ) -> Result<LogFetchOutcome, LogFetchError> {
            // The usage walk never touches the log seam.
            Err(LogFetchError::Unreachable {
                reason: "usage plane serves no logs".to_string(),
            })
        }
        async fn fetch_metrics(
            &self,
            _node: NodeId,
            _addr: &str,
            req: MetricsFetchRequest,
        ) -> Result<MetricsFetchOutcome, MetricsFetchError> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.outcomes
                .lock()
                .unwrap()
                .get_mut(&req.attempt)
                .and_then(|q| q.pop_front())
                .unwrap_or(Err(MetricsFetchError::Unreachable {
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

    /// A metric sample distinguishable by `at_us`; the counter payload rises
    /// with `at_us` so a test can assert order and values.
    fn sample(at_us: i64) -> MetricSample {
        MetricSample {
            at_us,
            cpu_usage_total_us: at_us as u64,
            cpu_throttled_total_us: 0,
            memory_used_bytes: at_us as u64 * 2,
            memory_peak_bytes: at_us as u64 * 3,
            disk_writable_bytes: 0,
            disk_image_bytes: 0,
            net_rx_bytes_total: 0,
            net_tx_bytes_total: 0,
            blkio_read_bytes_total: 0,
            blkio_write_bytes_total: 0,
        }
    }

    /// A one-page `Samples` outcome fully consumed within range.
    fn page(samples: Vec<MetricSample>) -> Result<MetricsFetchOutcome, MetricsFetchError> {
        let earliest = samples.iter().map(|s| s.at_us).min();
        let latest = samples.iter().map(|s| s.at_us).max();
        Ok(MetricsFetchOutcome::Samples(MetricsPage {
            samples,
            exhausted: true,
            earliest_at_us: earliest,
            latest_at_us: latest,
        }))
    }

    // ---- request helpers -------------------------------------------------

    async fn get(plane: Arc<FakePlane>, uri: &str) -> (StatusCode, serde_json::Value) {
        let router = crate::http::router(
            plane,
            crate::http::MetricsEndpoint::detached_for_tests(),
            None,
        );
        let response = router
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    // ---- tests -----------------------------------------------------------

    #[tokio::test]
    async fn unknown_job_is_404() {
        let plane = Arc::new(FakePlane::new(StateMachine::default()));
        let job = JobId::new();
        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/usage")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "NOT_FOUND");
    }

    #[tokio::test]
    async fn zero_attempt_job_is_empty_but_ok() {
        let job = JobId::new();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, Vec::new()));
        let plane = Arc::new(FakePlane::new(state));
        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/usage")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["samples"], serde_json::json!([]));
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

        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/usage")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 0);
        assert_eq!(body["sources"][0]["availability"], "not_started");
        assert_eq!(body["samples"], serde_json::json!([]));
        assert_eq!(body["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn available_attempt_returns_samples() {
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
        plane.seed(attempt, page(vec![sample(1_000_000)]));

        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/usage")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 1);
        assert_eq!(body["sources"][0]["availability"], "available");
        assert_eq!(body["sources"][0]["truncated"], false);
        assert_eq!(body["samples"][0]["attempt"], attempt.to_string());
        assert_eq!(body["samples"][0]["at"], "1970-01-01T00:00:01.000000Z");
        assert_eq!(body["samples"][0]["cpu_usage_total_us"], 1_000_000);
        assert_eq!(body["samples"][0]["memory_used_bytes"], 2_000_000);
        assert_eq!(body["next_cursor"], serde_json::Value::Null);
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
        // (1970-01-01T00:00:01Z = 1_000_000µs): older samples were pruned.
        plane.seed(attempt, page(vec![sample(5_000_000)]));

        let (status, body) = get(
            plane,
            &format!("/api/v1/jobs/{job}/usage?from=1970-01-01T00:00:01Z"),
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
        plane.seed(attempt, Ok(MetricsFetchOutcome::UnknownAttempt));

        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/usage")).await;
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

        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/usage")).await;
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

        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/usage")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 0);
        assert_eq!(body["sources"][0]["availability"], "unreachable");
        assert!(body["sources"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("no longer in the cluster"));
    }

    /// Build a job whose attempts are all available on one good node, seeding
    /// each with a single distinguishable sample.
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
            plane.seed(*a, page(vec![sample(1_000_000 + i as i64)]));
        }
        (job, attempts, plane)
    }

    #[tokio::test]
    async fn asc_walk_orders_attempts_oldest_first_by_default() {
        // Usage defaults to `asc` (chart order), unlike logs' `desc`.
        let (job, attempts, plane) = multi_available(2);
        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/usage")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["sources"][0]["attempt"], attempts[0].to_string());
        assert_eq!(body["sources"][1]["attempt"], attempts[1].to_string());
        assert_eq!(body["samples"][0]["attempt"], attempts[0].to_string());
        assert_eq!(body["samples"][1]["attempt"], attempts[1].to_string());
        assert_eq!(body["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn desc_walk_orders_attempts_newest_first() {
        let (job, attempts, plane) = multi_available(2);
        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/usage?order=desc")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["sources"][0]["attempt"], attempts[1].to_string());
        assert_eq!(body["sources"][1]["attempt"], attempts[0].to_string());
    }

    #[tokio::test]
    async fn rpc_budget_ends_the_page_with_a_cursor() {
        // Five available attempts; the 4-RPC budget stops after four, and the
        // page ends with a cursor rather than fetching the fifth.
        let (job, _attempts, plane) = multi_available(5);
        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/usage")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 4);
        assert_eq!(body["samples"].as_array().unwrap().len(), 4);
        assert_eq!(body["sources"].as_array().unwrap().len(), 4);
        assert!(
            body["next_cursor"].is_string(),
            "a short page carries a cursor"
        );
    }

    #[tokio::test]
    async fn budget_cursor_resumes_the_remaining_attempts() {
        let (job, attempts, plane) = multi_available(5);
        let (_s1, page1) = get(plane.clone(), &format!("/api/v1/jobs/{job}/usage")).await;
        let cursor = page1["next_cursor"].as_str().unwrap().to_string();
        // The cursor names the fifth (unfetched) attempt at its edge.
        assert!(cursor.contains(&attempts[4].to_string()));

        let (status, page2) = get(
            plane.clone(),
            &format!("/api/v1/jobs/{job}/usage?cursor={cursor}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // Only the fifth attempt remained.
        assert_eq!(page2["sources"].as_array().unwrap().len(), 1);
        assert_eq!(page2["sources"][0]["attempt"], attempts[4].to_string());
        assert_eq!(page2["samples"][0]["attempt"], attempts[4].to_string());
        assert_eq!(page2["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn cursor_advances_past_an_unreachable_attempt() {
        // Seven attempts: a0..a3 spend the budget; a4 is the next RPC-requiring
        // attempt so page 1 stops there with a cursor to a4. On page 2 the walk
        // serves a4, hits an unreachable a5 (no service addr, no RPC), and still
        // reaches a6 — proving the cursor advanced past the dead node mid-page.
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
                plane.seed(*a, page(vec![sample(1_000_000 + i as i64)]));
            }
        }

        let (_s1, page1) = get(plane.clone(), &format!("/api/v1/jobs/{job}/usage")).await;
        // Page 1 fetched a0..a3 (budget) and stopped at a4.
        assert_eq!(plane.fetch_count(), 4);
        assert_eq!(page1["sources"].as_array().unwrap().len(), 4);
        let cursor = page1["next_cursor"].as_str().unwrap().to_string();
        assert!(cursor.contains(&attempts[4].to_string()));

        let (status, page2) = get(
            plane.clone(),
            &format!("/api/v1/jobs/{job}/usage?cursor={cursor}"),
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
    async fn limit_clamp_ends_the_page_before_the_next_attempt() {
        // The first attempt returns a page that exactly fills the limit; the
        // walk stops there with a cursor to the second, without fetching it.
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
        // Two samples fill a `limit=2` page exactly, returned as exhausted.
        plane.seed(a0, page(vec![sample(1_000_000), sample(1_000_001)]));
        plane.seed(a1, page(vec![sample(2_000_000)]));

        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/usage?limit=2")).await;
        assert_eq!(status, StatusCode::OK);
        // Only the first attempt was fetched; the limit ended the page.
        assert_eq!(plane.fetch_count(), 1);
        assert_eq!(body["sources"].as_array().unwrap().len(), 1);
        assert_eq!(body["sources"][0]["attempt"], a0.to_string());
        assert!(body["next_cursor"].is_string());
    }

    #[tokio::test]
    async fn short_store_page_yields_a_mid_attempt_cursor() {
        // The store cuts the attempt short (`exhausted = false`): the cursor
        // resumes within the same attempt, past the last sample shown.
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
            Ok(MetricsFetchOutcome::Samples(MetricsPage {
                samples: vec![sample(7_000_000)],
                exhausted: false,
                earliest_at_us: Some(7_000_000),
                latest_at_us: Some(7_000_000),
            })),
        );

        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/usage")).await;
        assert_eq!(status, StatusCode::OK);
        let cursor = body["next_cursor"].as_str().unwrap();
        // Mid-attempt: same attempt, the last sample's µs, skip = 1.
        let parsed = UsageCursor::parse(cursor).unwrap();
        assert_eq!(parsed.attempt, attempt);
        assert_eq!(parsed.at_us, 7_000_000);
        assert_eq!(parsed.skip, 1);
        assert!(!parsed.is_edge());
    }

    #[tokio::test]
    async fn from_to_window_passes_through_to_the_rpc() {
        // The parsed half-open window reaches the RPC as µs bounds.
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
        plane.seed(attempt, page(vec![sample(3_000_000)]));

        let (status, _body) = get(
            plane.clone(),
            &format!(
                "/api/v1/jobs/{job}/usage?from=1970-01-01T00:00:01Z\
                 &to=1970-01-01T00:00:05Z&to_inclusive=true"
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 1);
    }

    #[tokio::test]
    async fn source_bound_caps_no_rpc_attempts_and_mints_a_cursor() {
        // A job with more no-RPC (not-started) attempts than MAX_SOURCES: the
        // response returns exactly MAX_SOURCES source records, makes no RPCs,
        // and carries an edge cursor to the first unexamined attempt.
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let mut state = StateMachine::default();
        let mut attempts = Vec::new();
        for _ in 0..(MAX_SOURCES + 5) {
            let a = AttemptId::new();
            attempts.push(a);
            // `started = false` → not_started → no RPC.
            state.attempts.insert(a, attempt_rec(a, job, node, false));
        }
        state.jobs.insert(job, job_rec(job, attempts.clone()));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));

        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/usage")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 0);
        assert_eq!(body["sources"].as_array().unwrap().len(), MAX_SOURCES);
        // The cursor points at the first unexamined attempt (index MAX_SOURCES).
        let cursor = body["next_cursor"].as_str().unwrap();
        assert!(cursor.contains(&attempts[MAX_SOURCES].to_string()));
        let parsed = UsageCursor::parse(cursor).unwrap();
        assert!(parsed.is_edge());
        assert_eq!(parsed.attempt, attempts[MAX_SOURCES]);
    }

    #[tokio::test]
    async fn source_bound_cursor_walks_the_remainder() {
        // Following the minted cursor examines the remaining attempts, all
        // within a single further page (5 remain, under MAX_SOURCES).
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let mut state = StateMachine::default();
        let mut attempts = Vec::new();
        for _ in 0..(MAX_SOURCES + 5) {
            let a = AttemptId::new();
            attempts.push(a);
            state.attempts.insert(a, attempt_rec(a, job, node, false));
        }
        state.jobs.insert(job, job_rec(job, attempts.clone()));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));

        let (_s1, page1) = get(plane.clone(), &format!("/api/v1/jobs/{job}/usage")).await;
        let cursor = page1["next_cursor"].as_str().unwrap().to_string();

        let (status, page2) = get(
            plane.clone(),
            &format!("/api/v1/jobs/{job}/usage?cursor={cursor}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let sources = page2["sources"].as_array().unwrap();
        assert_eq!(sources.len(), 5);
        assert_eq!(sources[0]["attempt"], attempts[MAX_SOURCES].to_string());
        assert_eq!(sources[4]["attempt"], attempts[MAX_SOURCES + 4].to_string());
        assert_eq!(page2["next_cursor"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn source_bound_composes_with_the_rpc_budget() {
        // Mixed no-RPC + RPC attempts. A no-RPC prefix of MAX_SOURCES-2
        // not-started attempts fills all but two source slots; two available
        // attempts then consume the last two slots (two RPCs, under the 4-RPC
        // budget), and the source ceiling ends the page before the RPC budget
        // is spent. This proves the two bounds compose: no-RPC attempts do not
        // count against the RPC budget but do count against the source ceiling.
        let job = JobId::new();
        let (node, node_rec) = good_node();
        let mut state = StateMachine::default();
        let mut attempts = Vec::new();
        let not_started = MAX_SOURCES - 2;
        for _ in 0..not_started {
            let a = AttemptId::new();
            attempts.push(a);
            state.attempts.insert(a, attempt_rec(a, job, node, false));
        }
        // Three available attempts follow; only two fit before the ceiling.
        for _ in 0..3 {
            let a = AttemptId::new();
            attempts.push(a);
            state.attempts.insert(a, attempt_rec(a, job, node, true));
        }
        state.jobs.insert(job, job_rec(job, attempts.clone()));
        state.nodes.insert(node, node_rec);
        let plane = Arc::new(FakePlane::new(state));
        for a in attempts.iter().skip(not_started) {
            plane.seed(*a, page(vec![sample(1_000_000)]));
        }

        let (status, body) = get(plane.clone(), &format!("/api/v1/jobs/{job}/usage")).await;
        assert_eq!(status, StatusCode::OK);
        // Only the two available attempts that fit the ceiling were fetched —
        // well under the RPC budget, so the source ceiling (not the budget)
        // ended the page.
        assert_eq!(plane.fetch_count(), 2);
        assert_eq!(body["sources"].as_array().unwrap().len(), MAX_SOURCES);
        // The cursor points at the third available attempt (the first unexamined
        // one), so the remainder stays reachable.
        let cursor = body["next_cursor"].as_str().unwrap();
        assert!(cursor.contains(&attempts[MAX_SOURCES].to_string()));
    }

    // ---- parameter validation -------------------------------------------

    async fn invalid(uri_suffix: &str) {
        let job = JobId::new();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, Vec::new()));
        let plane = Arc::new(FakePlane::new(state));
        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/usage?{uri_suffix}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri_suffix}");
        assert_eq!(body["code"], "INVALID_ARGUMENT", "{uri_suffix}");
    }

    #[tokio::test]
    async fn rejects_bad_parameters() {
        invalid("limit=0").await;
        invalid("limit=5001").await;
        invalid("order=sideways").await;
        invalid("from=not-a-timestamp").await;
        invalid("to=2026-13-40T99:99:99Z").await;
        invalid("attempt=not-an-attempt").await;
        invalid("cursor=garbage").await;
        invalid("cursor=v2%3Aasc%3Aattempt").await;
    }

    #[tokio::test]
    async fn cursor_order_mismatch_is_invalid() {
        let job = JobId::new();
        let attempt = AttemptId::new();
        // A desc cursor with an (default) asc request.
        let cursor = UsageCursor::edge(LogOrder::Desc, attempt).format();
        let mut state = StateMachine::default();
        state.jobs.insert(job, job_rec(job, vec![attempt]));
        let plane = Arc::new(FakePlane::new(state));
        let (status, body) = get(plane, &format!("/api/v1/jobs/{job}/usage?cursor={cursor}")).await;
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
            &format!("/api/v1/jobs/{job}/usage?attempt={stranger}"),
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
            &format!("/api/v1/jobs/{job}/usage?attempt={}", attempts[1]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(plane.fetch_count(), 1);
        assert_eq!(body["sources"].as_array().unwrap().len(), 1);
        assert_eq!(body["sources"][0]["attempt"], attempts[1].to_string());
    }

    #[test]
    fn to_inclusive_maps_to_plus_one_microsecond() {
        let params = UsageParams {
            to: Some("1970-01-01T00:00:02Z".to_string()),
            to_inclusive: Some(true),
            ..Default::default()
        };
        let req = parse_request(params).unwrap();
        assert_eq!(req.until_us, Some(2_000_001));

        let exclusive = parse_request(UsageParams {
            to: Some("1970-01-01T00:00:02Z".to_string()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(exclusive.until_us, Some(2_000_000));
    }

    #[test]
    fn default_order_is_ascending() {
        let req = parse_request(UsageParams::default()).unwrap();
        assert_eq!(req.order, LogOrder::Asc);
    }

    #[test]
    fn cursor_round_trips_through_format_and_parse() {
        let attempt = AttemptId::new();
        let cursor = UsageCursor {
            order: LogOrder::Asc,
            attempt,
            at_us: 1_753_003_872_123_456,
            skip: 2,
        };
        let parsed = UsageCursor::parse(&cursor.format()).unwrap();
        assert_eq!(parsed, cursor);
    }
}
