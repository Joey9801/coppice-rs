//! End-to-end best-effort job usage-metrics retrieval (ADR 0034), the metrics
//! twin of `job_logs.rs`.
//!
//! The full read path, daemonless and over real mTLS:
//!
//! - a real [`FilesystemSink`] seeded with [`MetricSample`] rows (no Docker —
//!   the sink is the same store the Docker collectors write, exercised directly
//!   through the [`MetricsSink`] append seam);
//! - the agent-hosted [`NodeService`](coppice_agent::node_service) served over a
//!   real mTLS listener whose leaf carries the node id as a dNSName SAN;
//! - a real node registered through the live coordinator runtime, so its
//!   advertised `service_addr` lands in replicated state;
//! - `GET /api/v1/jobs/{job}/usage` driven through the real axum router, the real
//!   [`CoordinatorControlPlane`], and the real [`NodeClient`] dialing that
//!   listener id-pinned (the metric fetch rides the same client as logs).
//!
//! One test shares the (expensive) bootstrap + register + run + retry setup, so
//! the job carries **two** attempts on the single node, and then asserts, in
//! sequence: exact sample round-trip and ascending cross-attempt order with two
//! `available` sources, cursor paging across the attempt boundary (asc and
//! desc), the `[from, until)` window, `expired` after the attempts' telemetry is
//! deleted, and `unreachable` after the listener stops. The verdict mechanisms
//! mirror `job_logs.rs` exactly (delete the attempt directory → `expired`; stop
//! the listener + a fresh client → `unreachable`).

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use coppice_agent::config::{CapacityConfig, Config, TlsConfig};
use coppice_agent::executor::{ExitCause, ExitInfo, FakeExecutor};
use coppice_agent::journal::Journal;
use coppice_agent::node_service::{serve, NodeServiceListener};
use coppice_agent::session::{run, Session};
use coppice_agent::telemetry::{FilesystemSink, FilesystemSinkOptions, MetricSample, MetricsSink};
use coppice_consensus::fs::RealFs;
use coppice_consensus::{Consensus, StateViews};
use coppice_coordinator::{CoordinatorControlPlane, NodeClient};
use coppice_core::attempt::AttemptState;
use coppice_core::bytes::ByteSize;
use coppice_core::id::{AllocationId, AttemptId, ClusterId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::{Job, RetryPolicy};
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration as CoreDuration, Timestamp};
use coppice_state::command::{ConfigureQuotaEntity, SubmitJob};
use coppice_state::Command;

use common::{poll, Ca, RunningCoordinator};

/// Generous per-wait deadline, matching `job_logs.rs`.
const DEADLINE: Duration = Duration::from_secs(20);

/// Sample count seeded per attempt.
const PER_ATTEMPT: usize = 3;

/// Sample epoch (µs) and spacing: one sample per second, so each sample carries
/// a distinct `at` and window bounds are exact RFC 3339 seconds.
const BASE_US: i64 = 1_700_000_000_000_000;
const STEP_US: i64 = 1_000_000;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

fn requested() -> Resources {
    Resources {
        cpu_millis: 500,
        memory: ByteSize::from_mib(1),
        disk: ByteSize::ZERO,
    }
}

// ---- agent harness (advertises a NodeService address, ADR 0034) ----------

/// Build an agent config whose client leaf's CN is the typed node id (the
/// gateway binds it, ADR 0011). The `[listen]` table is left `None`: this test
/// binds and serves the NodeService listener itself (so it can seed the sink
/// after learning the coordinator-minted attempt id), and advertises the
/// address through [`Session::with_service_addr`] directly.
fn agent_config(
    node_id: NodeId,
    data_dir: std::path::PathBuf,
    endpoint: &str,
    ca: &Ca,
    pki_dir: &std::path::Path,
) -> Config {
    let leaf = ca.leaf_with_cn(&node_id.to_string());
    let cert_path = pki_dir.join("agent.crt");
    let key_path = pki_dir.join("agent.key");
    let ca_path = pki_dir.join("agent-ca.crt");
    std::fs::write(&cert_path, &leaf.cert_pem).expect("write agent cert");
    std::fs::write(&key_path, &leaf.key_pem).expect("write agent key");
    std::fs::write(&ca_path, &ca.pem).expect("write agent ca");

    Config {
        node_id,
        data_dir,
        coordinators: vec![endpoint.to_string()],
        tls: TlsConfig {
            cert_path,
            key_path,
            ca_path,
        },
        capacity: CapacityConfig {
            cpu_millis: 16_000,
            memory: ByteSize::from_gib(16),
            disk: ByteSize::from_tib(1),
        },
        reservation: Default::default(),
        heartbeat_interval: Duration::from_millis(300),
        reconnect_backoff_min: Duration::from_millis(100),
        reconnect_backoff_max: Duration::from_millis(500),
        labels: BTreeMap::new(),
        executor: Default::default(),
        pressure: Default::default(),
        image_cache: Default::default(),
        telemetry: Default::default(),
        listen: None,
    }
}

/// Spawn the real agent session runner, advertising `service_addr` at
/// registration so it lands in replicated `Node.service_addr`.
fn spawn_agent(
    config: Config,
    executor: FakeExecutor,
    service_addr: String,
) -> tokio::task::JoinHandle<()> {
    std::fs::create_dir_all(&config.data_dir).expect("create agent data dir");
    let fs = RealFs::new(config.data_dir.clone());
    let (journal, state) = Journal::open(fs).expect("open agent journal");
    let session = Session::new(
        config.node(),
        config.advertised_resources(),
        Vec::new(),
        journal,
        state,
        executor,
    )
    .with_service_addr(Some(service_addr));
    tokio::spawn(async move {
        let _ = run(session, &config).await;
    })
}

// ---- state readers -------------------------------------------------------

fn node_epoch(views: &StateViews, node: NodeId) -> Option<u64> {
    views.latest().state().nodes.get(&node).map(|n| n.epoch)
}

fn current_attempt_id(views: &StateViews, job: JobId) -> Option<AttemptId> {
    views.latest().state().jobs.get(&job)?.current_attempt()
}

fn attempt_ids(views: &StateViews, job: JobId) -> Vec<AttemptId> {
    views
        .latest()
        .state()
        .jobs
        .get(&job)
        .map(|j| j.attempts.clone())
        .unwrap_or_default()
}

fn attempt_state(views: &StateViews, attempt: AttemptId) -> Option<AttemptState> {
    views
        .latest()
        .state()
        .attempts
        .get(&attempt)
        .map(|a| a.attempt.state.clone())
}

fn attempt_alloc(views: &StateViews, attempt: AttemptId) -> Option<AllocationId> {
    views
        .latest()
        .state()
        .attempts
        .get(&attempt)
        .map(|a| a.attempt.allocation)
}

// ---- command proposers ---------------------------------------------------

async fn seed_quota(coord: &RunningCoordinator, entity: QuotaEntityId) {
    let applied = coord
        .consensus()
        .propose(Command::ConfigureQuotaEntity(ConfigureQuotaEntity {
            entity,
            parent: None,
            name: "root".into(),
            quota: CostUnits(1_000_000_000_000),
            updated_at: Timestamp::now(),
        }))
        .await
        .expect("propose ConfigureQuotaEntity");
    assert!(
        applied.outcome.is_ok(),
        "quota rejected: {:?}",
        applied.outcome
    );
}

/// Submit a job whose retry policy permits exactly one user-error retry, so a
/// non-zero exit on the first attempt requeues the job onto a second attempt
/// (the walk then spans two attempts on the one node).
async fn submit_retrying_job(coord: &RunningCoordinator, job: JobId, entity: QuotaEntityId) {
    let applied = coord
        .consensus()
        .propose(Command::SubmitJob(SubmitJob {
            job: Job {
                id: job,
                image: "registry/img:latest".into(),
                command: vec!["run".into()],
                entrypoint: None,
                requests: requested(),
                priority: 0,
                max_runtime: None,
                quota_entity: entity,
                retry: RetryPolicy {
                    max_retries: 1,
                    retry_user_errors: true,
                },
                abort_requested: None,
            },
            multiplier: PriorityMultiplier::ONE,
            submitted_at: Timestamp::now(),
        }))
        .await
        .expect("propose SubmitJob");
    assert!(
        applied.outcome.is_ok(),
        "SubmitJob rejected: {:?}",
        applied.outcome
    );
}

// ---- sample seeding + comparison -----------------------------------------

/// A comparable projection of every metric field. `sample_key` and `point_key`
/// project a stored [`MetricSample`] and a returned [`UsagePoint`] into the same
/// tuple, so an exact field-for-field round-trip is a plain equality — the CPU
/// counters cross as integer µs (`Duration` → `_us`), the gauges as raw bytes.
type Key = (
    AttemptId,
    i64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
);

fn sample_key(s: &MetricSample) -> Key {
    (
        s.attempt,
        s.at.as_micros(),
        s.cpu_usage_total.as_micros() as u64,
        s.cpu_throttled_total.as_micros() as u64,
        s.memory_used_bytes,
        s.memory_peak_bytes,
        s.disk_writable_bytes,
        s.disk_image_bytes,
        s.net_rx_bytes_total,
        s.net_tx_bytes_total,
        s.blkio_read_bytes_total,
        s.blkio_write_bytes_total,
    )
}

fn point_key(p: &coppice_api::http::dto::UsagePoint) -> Key {
    (
        p.attempt,
        p.at.as_micros(),
        p.cpu_usage_total_us,
        p.cpu_throttled_total_us,
        p.memory_used_bytes,
        p.memory_peak_bytes,
        p.disk_writable_bytes,
        p.disk_image_bytes,
        p.net_rx_bytes_total,
        p.net_tx_bytes_total,
        p.blkio_read_bytes_total,
        p.blkio_write_bytes_total,
    )
}

/// One sample carrying a globally unique `seq` woven through every field, so a
/// mis-ordered, dropped, or duplicated sample is caught by value alone. `at`
/// steps one second per index, distinct within an attempt.
fn seed_sample(
    job: JobId,
    attempt: AttemptId,
    alloc: AllocationId,
    index: usize,
    seq: u64,
) -> MetricSample {
    MetricSample {
        allocation: alloc,
        attempt,
        job,
        at: Timestamp::from_micros(BASE_US + index as i64 * STEP_US).expect("valid timestamp"),
        // Counters are cumulative µs; a reader differences them. Distinct per
        // seq and non-zero so the µs round-trip is load-bearing.
        cpu_usage_total: CoreDuration::from_micros(seq as i64 * 1_000 + 7),
        cpu_throttled_total: CoreDuration::from_micros(seq as i64 * 11),
        memory_used_bytes: seq * 4_096,
        memory_peak_bytes: seq * 4_096 + 2_048,
        disk_writable_bytes: seq * 512,
        disk_image_bytes: 1_000_000, // constant per attempt (image bytes)
        net_rx_bytes_total: seq * 10,
        net_tx_bytes_total: seq * 20,
        blkio_read_bytes_total: seq * 30,
        blkio_write_bytes_total: seq * 40,
    }
}

// ---- HTTP driver ---------------------------------------------------------

/// Drive one `GET /api/v1/jobs/{job}/usage?{query}` through the router and decode
/// the JSON body.
async fn get_usage(
    router: &axum::Router,
    job: JobId,
    query: &str,
) -> (StatusCode, coppice_api::http::dto::GetJobUsageResponse) {
    let uri = format!("/api/v1/jobs/{job}/usage?{query}");
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("router response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let decoded = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("decode body ({e}): {}", String::from_utf8_lossy(&bytes)));
    (status, decoded)
}

// ---- the test ------------------------------------------------------------

#[tokio::test]
async fn best_effort_job_usage_full_read_path() {
    use coppice_api::http::dto::UsageAvailability;

    init_tracing();

    // -- Seed the sink and stand up the mTLS NodeService (empty for now). ---
    let ca = Ca::new();
    let node = NodeId::new();

    let tel_dir = tempfile::tempdir().expect("telemetry tempdir");
    let sink_root = tel_dir.path().join("tel");
    let sink = FilesystemSink::new(FilesystemSinkOptions::new(sink_root.clone()))
        .await
        .expect("build filesystem sink");

    // The NodeService server leaf carries the node id as a dNSName SAN so the
    // coordinator's id-pinned dial validates (ADR 0034). Both stores are the one
    // sink; only the metric store is exercised here.
    let server_leaf = ca.leaf_with_cn_and_sans("node-service", &[node.to_string()]);
    let listener = NodeServiceListener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &server_leaf.cert_pem,
        &server_leaf.key_pem,
        &ca.pem,
    )
    .expect("bind NodeService listener");
    let service_addr = format!("127.0.0.1:{}", listener.local_addr().port());
    let mut server = serve(listener, Some(sink.clone()), Some(sink.clone()));

    // -- Boot the coordinator runtime and register the agent. --------------
    let cluster_id = ClusterId::new();
    let coord = RunningCoordinator::start(cluster_id, &ca).await;
    poll(DEADLINE, "coordinator leadership", || {
        let coord = &coord;
        async move { coord.is_leader() }
    })
    .await;

    let agent_dir = tempfile::tempdir().expect("agent tempdir");
    let executor = FakeExecutor::new();
    let config = agent_config(
        node,
        agent_dir.path().join("data"),
        &coord.agent_endpoint,
        &ca,
        agent_dir.path(),
    );
    let agent = spawn_agent(config, executor.clone(), service_addr.clone());

    let views = coord.views();
    poll(DEADLINE, "node registered", || {
        let views = views.clone();
        async move { node_epoch(&views, node).is_some_and(|e| e >= 1) }
    })
    .await;

    // The advertised NodeService address landed in replicated state.
    assert_eq!(
        views
            .latest()
            .state()
            .nodes
            .get(&node)
            .and_then(|n| n.node.service_addr.clone())
            .as_deref(),
        Some(service_addr.as_str()),
        "the agent's service_addr must be advertised through registration"
    );

    // -- Submit a retrying job; run its first attempt to Running. ----------
    let entity = QuotaEntityId::new();
    seed_quota(&coord, entity).await;
    let job = JobId::new();
    submit_retrying_job(&coord, job, entity).await;

    poll(DEADLINE, "attempt 1 Running", || {
        let views = views.clone();
        async move {
            current_attempt_id(&views, job).and_then(|a| attempt_state(&views, a))
                == Some(AttemptState::Running)
        }
    })
    .await;
    let attempt1 = current_attempt_id(&views, job).expect("attempt 1");
    let alloc1 = attempt_alloc(&views, attempt1).expect("alloc 1");

    // -- Force a second attempt: exit non-zero. `retry_user_errors` requeues
    //    the job, and the scheduler places attempt 2 back on the one node. ---
    executor.finish(
        alloc1,
        ExitInfo {
            code: 1,
            cause: ExitCause::Natural,
            runtime: CoreDuration::from_secs(1),
            finished_at: executor.now(),
        },
    );
    poll(DEADLINE, "attempt 2 Running", || {
        let views = views.clone();
        async move {
            let ids = attempt_ids(&views, job);
            ids.len() == 2
                && current_attempt_id(&views, job).and_then(|a| attempt_state(&views, a))
                    == Some(AttemptState::Running)
        }
    })
    .await;
    let ids = attempt_ids(&views, job);
    assert_eq!(ids.len(), 2, "the job carries exactly two attempts");
    assert_eq!(ids[0], attempt1, "attempt 1 leads the walk order");
    let attempt2 = ids[1];
    assert_ne!(attempt2, attempt1, "attempt 2 is a distinct attempt");
    let alloc2 = attempt_alloc(&views, attempt2).expect("alloc 2");

    // -- Seed PER_ATTEMPT ascending samples for each attempt, seq-unique
    //    across the whole job so a value proves identity and order. ---------
    let mut seq = 1u64;
    let seed_attempt = |attempt: AttemptId, alloc: AllocationId, seq: &mut u64| {
        let batch: Vec<MetricSample> = (0..PER_ATTEMPT)
            .map(|i| {
                let s = seed_sample(job, attempt, alloc, i, *seq);
                *seq += 1;
                s
            })
            .collect();
        batch
    };
    let batch1 = seed_attempt(attempt1, alloc1, &mut seq);
    let batch2 = seed_attempt(attempt2, alloc2, &mut seq);
    MetricsSink::append(&sink, &batch1).await;
    MetricsSink::append(&sink, &batch2).await;

    // The whole ascending series, in walk order: attempt 1 then attempt 2, each
    // ascending. The descending series is each attempt reversed, attempts
    // reversed — the exact reverse of the ascending walk.
    let expected_asc: Vec<Key> = batch1.iter().chain(&batch2).map(sample_key).collect();
    let expected_desc: Vec<Key> = batch2
        .iter()
        .rev()
        .chain(batch1.iter().rev())
        .map(sample_key)
        .collect();

    // -- Build the real read path: plane + node client (shared with logs). --
    let coord_leaf = ca.leaf();
    let node_client = Arc::new(NodeClient::new(
        &ca.pem,
        &coord_leaf.cert_pem,
        &coord_leaf.key_pem,
    ));
    let plane = Arc::new(
        CoordinatorControlPlane::new(coord.consensus(), coord.views(), cluster_id)
            .with_log_client(node_client),
    );
    let router = coppice_api::http::router(plane);

    // -- 1. Exact round-trip, ascending default order, two `available`. ----
    let (status, body) = get_usage(&router, job, "order=asc&limit=200").await;
    assert_eq!(status, StatusCode::OK);
    let got: Vec<Key> = body.samples.iter().map(point_key).collect();
    assert_eq!(
        got, expected_asc,
        "every field round-trips exactly, in ascending cross-attempt order"
    );
    assert_eq!(body.sources.len(), 2, "one source record per attempt");
    for src in &body.sources {
        assert_eq!(
            src.availability,
            UsageAvailability::Available,
            "both proven-ran attempts have data on the node"
        );
        assert!(!src.truncated, "no `from` bound, so nothing is truncated");
    }
    assert_eq!(body.sources[0].attempt, attempt1);
    assert_eq!(body.sources[0].node, Some(node));
    assert_eq!(body.sources[1].attempt, attempt2);
    assert_eq!(body.sources[1].node, Some(node));
    assert!(
        body.next_cursor.is_none(),
        "the whole series fit in one page"
    );

    // -- 2a. Ascending paging: limit=2 walks the six samples across the
    //        attempt boundary, reassembling the series once, no gaps/dupes. --
    let paged_asc = walk_pages(&router, job, "asc", 2).await;
    assert_eq!(
        paged_asc, expected_asc,
        "asc paged concatenation equals the full ascending series"
    );

    // -- 2b. Descending paging: same walk, reverse order. ------------------
    let paged_desc = walk_pages(&router, job, "desc", 2).await;
    assert_eq!(
        paged_desc, expected_desc,
        "desc paged concatenation equals the full descending series"
    );

    // -- 3. Window: half-open `[from, until)` over the shared per-attempt
    //       timeline keeps indices 1..PER_ATTEMPT-1 from each attempt. ------
    let from = Timestamp::from_micros(BASE_US + STEP_US).expect("from"); // index 1, inclusive
    let until = Timestamp::from_micros(BASE_US + 2 * STEP_US).expect("until"); // index 2, exclusive
    let query = format!(
        "order=asc&limit=200&from={}&to={}",
        from.to_rfc3339(),
        until.to_rfc3339()
    );
    let (status, body) = get_usage(&router, job, &query).await;
    assert_eq!(status, StatusCode::OK);
    let want_window: Vec<Key> = batch1
        .iter()
        .chain(&batch2)
        .filter(|s| {
            let at = s.at.as_micros();
            from.as_micros() <= at && at < until.as_micros()
        })
        .map(sample_key)
        .collect();
    let got_window: Vec<Key> = body.samples.iter().map(point_key).collect();
    assert_eq!(
        got_window, want_window,
        "only in-window samples returned, both attempts, ascending"
    );
    assert!(
        !got_window.is_empty(),
        "the window intersects the seeded timeline"
    );
    for src in &body.sources {
        assert_eq!(src.availability, UsageAvailability::Available);
        assert!(
            !src.truncated,
            "`from` sits after each attempt's earliest sample, so nothing is head-truncated"
        );
    }

    // -- 4. `expired`: delete both attempts' telemetry directories. --------
    // Retention deletes whole attempt directories; simulate that. The store
    // then answers UnknownAttempt, and the coordinator — still proving from
    // replicated state that the attempts ran here — reports `expired`.
    for attempt in [attempt1, attempt2] {
        let attempt_dir = sink_root.join(job.to_string()).join(attempt.to_string());
        std::fs::remove_dir_all(&attempt_dir).expect("delete attempt telemetry");
    }
    let (status, body) = get_usage(&router, job, "order=asc&limit=200").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an empty best-effort answer is still 200"
    );
    assert!(
        body.samples.is_empty(),
        "no samples once the telemetry is gone"
    );
    assert_eq!(body.sources.len(), 2);
    for src in &body.sources {
        assert_eq!(
            src.availability,
            UsageAvailability::Expired,
            "a proven-ran attempt whose store answers UnknownAttempt is `expired`"
        );
    }

    // -- 5. `unreachable`: stop the listener; the dial fails closed. -------
    // Aborting the accept task drops the `TcpIncoming`, closing the listening
    // socket. A fresh read path (empty channel cache) then redials that closed
    // port and fails closed — a stale cached channel would instead ride a
    // per-connection task that outlives the accept-loop abort, so we drive the
    // fetch through a fresh client to observe genuine unreachability.
    server.abort();
    let _ = (&mut server).await;
    let node_client2 = Arc::new(NodeClient::new(
        &ca.pem,
        &coord_leaf.cert_pem,
        &coord_leaf.key_pem,
    ));
    let router2 = coppice_api::http::router(Arc::new(
        CoordinatorControlPlane::new(coord.consensus(), coord.views(), cluster_id)
            .with_log_client(node_client2),
    ));
    let (status, body) = get_usage(&router2, job, "order=asc&limit=200").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.samples.is_empty());
    assert_eq!(body.sources.len(), 2);
    for src in &body.sources {
        assert_eq!(
            src.availability,
            UsageAvailability::Unreachable,
            "a node whose NodeService is down is `unreachable`"
        );
    }

    // -- Teardown. ---------------------------------------------------------
    agent.abort();
    let _ = agent.await;
    coord.shutdown().await;
    drop(agent_dir);
    drop(tel_dir);
}

/// Walk `GET .../usage` from the edge with `limit`, following `next_cursor`
/// until it is `None`, and return the concatenated sample keys. Asserts the
/// walk terminates and every page's sources stay `available`.
async fn walk_pages(router: &axum::Router, job: JobId, order: &str, limit: usize) -> Vec<Key> {
    use coppice_api::http::dto::UsageAvailability;

    let mut out: Vec<Key> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages <= 20, "paging did not terminate");
        let query = match &cursor {
            None => format!("order={order}&limit={limit}"),
            Some(c) => format!("order={order}&limit={limit}&cursor={c}"),
        };
        let (status, page) = get_usage(router, job, &query).await;
        assert_eq!(status, StatusCode::OK);
        for src in &page.sources {
            assert_eq!(
                src.availability,
                UsageAvailability::Available,
                "every page's source stays available"
            );
        }
        out.extend(page.samples.iter().map(point_key));
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    out
}
