//! End-to-end best-effort job-log retrieval (ADR 0034).
//!
//! The full read path, daemonless and over real mTLS:
//!
//! - a real [`FilesystemSink`] seeded with log chunks (no Docker — the sink is
//!   the same store the Docker collectors write, exercised directly);
//! - the agent-hosted [`NodeService`](coppice_agent::node_service) served over a
//!   real mTLS listener whose leaf carries the node id as a dNSName SAN;
//! - a real node registered through the live coordinator runtime, so its
//!   advertised `service_addr` lands in replicated state;
//! - `GET /api/v1/jobs/{job}/logs` driven through the real axum router, the real
//!   [`CoordinatorControlPlane`], and the real [`NodeClient`] dialing that
//!   listener id-pinned.
//!
//! One test shares the (expensive) bootstrap + register + run-to-Running setup
//! and then asserts, in sequence: entry content/order, `available` sources,
//! cursor paging across pages, `expired` after the attempt's telemetry is
//! deleted, and `unreachable` after the listener stops.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use coppice_agent::config::{CapacityConfig, Config, TlsConfig};
use coppice_agent::executor::FakeExecutor;
use coppice_agent::journal::Journal;
use coppice_agent::node_service::{serve, NodeServiceListener};
use coppice_agent::session::{run, Session};
use coppice_agent::telemetry::{
    FilesystemSink, FilesystemSinkOptions, LogChunk, LogSink, LogStream,
};
use coppice_consensus::fs::RealFs;
use coppice_consensus::{Consensus, ConsensusError, StateViews};
use coppice_coordinator::config::CliOverrides;
use coppice_coordinator::{CoordinatorControlPlane, NodeClient};
use coppice_core::attempt::AttemptState;
use coppice_core::bytes::ByteSize;
use coppice_core::id::{AllocationId, AttemptId, ClusterId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::{Job, RetryPolicy};
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::resource::Resources;
use coppice_core::time::Timestamp;
use coppice_state::command::{ConfigureQuotaEntity, SubmitJob};
use coppice_state::Command;

use common::{poll, Ca, Node, RunningCoordinator};

/// Generous per-wait deadline, matching `agent_protocol.rs`.
const DEADLINE: Duration = Duration::from_secs(20);

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

async fn submit_job(coord: &RunningCoordinator, job: JobId, entity: QuotaEntityId) {
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
                    max_retries: 0,
                    retry_user_errors: false,
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

// ---- HTTP driver ---------------------------------------------------------

/// Drive one `GET /api/v1/jobs/{job}/logs?{query}` through the router and decode
/// the JSON body.
async fn get_logs(
    router: &axum::Router,
    job: JobId,
    query: &str,
) -> (StatusCode, coppice_api::http::dto::GetJobLogsResponse) {
    let uri = format!("/api/v1/jobs/{job}/logs?{query}");
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

/// As [`get_logs`], but also returns the `Coppice-Applied-Index` response
/// header — the applied index of the exact view *this replica* served from.
async fn get_logs_with_applied_index(
    router: &axum::Router,
    job: JobId,
    query: &str,
) -> (StatusCode, u64, coppice_api::http::dto::GetJobLogsResponse) {
    let uri = format!("/api/v1/jobs/{job}/logs?{query}");
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
    let applied_index = response
        .headers()
        .get(coppice_api::http::COPPICE_APPLIED_INDEX)
        .expect("applied-index header present")
        .to_str()
        .expect("applied-index header is ASCII")
        .parse::<u64>()
        .expect("applied-index header is a u64");
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let decoded = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("decode body ({e}): {}", String::from_utf8_lossy(&bytes)));
    (status, applied_index, decoded)
}

/// Add `follower` to `leader`'s cluster as a voter: add it as a learner, then
/// promote once the ADR 0016 catch-up gate clears (it needs leader replication
/// metrics for the learner, so the promotion is retried until it does).
async fn add_voter(leader: &RunningCoordinator, follower: &Node, deadline: Duration) {
    leader
        .consensus()
        .add_learner(follower.raft_id(), follower.advertise.clone())
        .await
        .unwrap_or_else(|e| panic!("add-learner node {} failed: {e:?}", follower.id));
    let start = Instant::now();
    loop {
        match leader
            .consensus()
            .promote_voter(follower.raft_id(), None)
            .await
        {
            Ok(()) => return,
            Err(ConsensusError::LearnerNotCaughtUp { .. }) if start.elapsed() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("promote node {} failed: {e:?}", follower.id),
        }
    }
}

// ---- the tests -----------------------------------------------------------

/// One seeded log line at a fixed microsecond timestamp.
fn seed_chunk(
    job: JobId,
    attempt: AttemptId,
    alloc: AllocationId,
    at_us: i64,
    text: &str,
) -> LogChunk {
    LogChunk {
        allocation: alloc,
        attempt,
        job,
        at: Timestamp::from_micros(at_us).expect("valid timestamp"),
        stream: LogStream::Stdout,
        bytes: bytes::Bytes::copy_from_slice(text.as_bytes()),
    }
}

#[tokio::test]
async fn best_effort_job_logs_full_read_path() {
    use coppice_api::http::dto::LogAvailability;

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
    // coordinator's id-pinned dial validates (ADR 0034).
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

    // -- Submit a job and run it to Running (attempt gets started_at + node). --
    let entity = QuotaEntityId::new();
    seed_quota(&coord, entity).await;
    let job = JobId::new();
    submit_job(&coord, job, entity).await;

    poll(DEADLINE, "attempt Running", || {
        let views = views.clone();
        async move {
            current_attempt_id(&views, job).and_then(|a| attempt_state(&views, a))
                == Some(AttemptState::Running)
        }
    })
    .await;
    let attempt = current_attempt_id(&views, job).expect("attempt");
    let alloc = attempt_alloc(&views, attempt).expect("alloc");

    // -- Seed five ordered log lines for that exact (job, attempt). ---------
    const BASE: i64 = 1_700_000_000_000_000;
    let lines: Vec<String> = (0..5).map(|i| format!("line-{i}")).collect();
    let chunks: Vec<LogChunk> = lines
        .iter()
        .enumerate()
        .map(|(i, text)| seed_chunk(job, attempt, alloc, BASE + i as i64 * 1_000, text))
        .collect();
    LogSink::append(&sink, &chunks).await;

    // -- Build the real read path: plane + log client + router. ------------
    let coord_leaf = ca.leaf();
    let log_client = Arc::new(NodeClient::new(
        &ca.pem,
        &coord_leaf.cert_pem,
        &coord_leaf.key_pem,
    ));
    let plane = Arc::new(
        CoordinatorControlPlane::new(coord.consensus(), coord.views(), cluster_id)
            .with_log_client(log_client),
    );
    let router = coppice_api::http::router(plane);

    // -- 1. Content + order + `available`, in one ascending page. ----------
    let (status, body) = get_logs(&router, job, "order=asc&limit=200").await;
    assert_eq!(status, StatusCode::OK);
    let got: Vec<String> = body.entries.iter().map(|e| e.text.clone()).collect();
    assert_eq!(
        got, lines,
        "entries returned in ascending (at, insertion) order"
    );
    assert_eq!(
        body.sources.len(),
        1,
        "one source record for the single attempt"
    );
    let src = &body.sources[0];
    assert_eq!(src.attempt, attempt);
    assert_eq!(src.node, Some(node));
    assert_eq!(src.availability, LogAvailability::Available);
    assert!(
        !src.truncated,
        "no `from` bound was set, so nothing is truncated"
    );
    assert!(
        body.next_cursor.is_none(),
        "the whole attempt fit in one page"
    );

    // -- 2. Cursor paging: limit=2 walks the five lines across three pages. --
    let mut paged: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages <= 10, "paging did not terminate");
        let query = match &cursor {
            None => "order=asc&limit=2".to_string(),
            Some(c) => format!("order=asc&limit=2&cursor={c}"),
        };
        let (status, page) = get_logs(&router, job, &query).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            page.sources[0].availability,
            LogAvailability::Available,
            "every page's source stays available"
        );
        paged.extend(page.entries.iter().map(|e| e.text.clone()));
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(
        paged, lines,
        "paged concatenation equals the full ordered set"
    );
    assert_eq!(pages, 3, "five lines at limit=2 span exactly three pages");

    // -- 3. `expired`: the attempt's telemetry is deleted from the sink. ----
    // Retention deletes whole attempt directories; simulate that by removing
    // this attempt's directory. The store then answers UnknownAttempt, and the
    // coordinator — which still proves from replicated state that the attempt
    // ran here — reports `expired`.
    let attempt_dir = sink_root.join(job.to_string()).join(attempt.to_string());
    std::fs::remove_dir_all(&attempt_dir).expect("delete attempt telemetry");
    let (status, body) = get_logs(&router, job, "order=asc&limit=200").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an empty best-effort answer is still 200"
    );
    assert!(
        body.entries.is_empty(),
        "no entries once the telemetry is gone"
    );
    assert_eq!(body.sources.len(), 1);
    assert_eq!(
        body.sources[0].availability,
        LogAvailability::Expired,
        "a proven-ran attempt whose store answers UnknownAttempt is `expired`"
    );

    // -- 4. `unreachable`: stop the listener; the dial fails closed. --------
    // Aborting the accept task drops the `TcpIncoming`, closing the listening
    // socket. A fresh read path (empty channel cache) then redials that closed
    // port and fails closed — a stale cached channel would instead ride a
    // per-connection task that outlives the accept-loop abort, so we drive the
    // fetch through a fresh client to observe genuine unreachability.
    server.abort();
    let _ = (&mut server).await;
    let log_client2 = Arc::new(NodeClient::new(
        &ca.pem,
        &coord_leaf.cert_pem,
        &coord_leaf.key_pem,
    ));
    let router2 = coppice_api::http::router(Arc::new(
        CoordinatorControlPlane::new(coord.consensus(), coord.views(), cluster_id)
            .with_log_client(log_client2),
    ));
    let (status, body) = get_logs(&router2, job, "order=asc&limit=200").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.entries.is_empty());
    assert_eq!(
        body.sources[0].availability,
        LogAvailability::Unreachable,
        "a node whose NodeService is down is `unreachable`"
    );

    // -- Teardown. ---------------------------------------------------------
    agent.abort();
    let _ = agent.await;
    coord.shutdown().await;
    drop(agent_dir);
    drop(tel_dir);
}

/// A **follower** serves `GET /api/v1/jobs/{job}/logs` directly — the ADR 0034
/// promise that every replica answers from its own applied state, with no
/// `NOT_LEADER`/`421`, no redirect, and no leader round-trip on the read path.
///
/// One three-voter cluster boot: a `RunningCoordinator` leader (agent gateway +
/// scheduler) plus two `Node` followers that replicate its state. The agent
/// registers and the job runs through the **leader** (sessions are leader-only,
/// unchanged); then the read path is built against a **follower's** consensus +
/// views and asserts (a) 200 + expected entries, (b) the `Coppice-Applied-Index`
/// header comes from that follower's own view, and (c) the follower's own
/// `NodeClient` dialed the agent (the entries came back).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_serves_job_logs_directly() {
    use coppice_api::http::dto::LogAvailability;

    init_tracing();

    // -- Seed the sink and stand up the mTLS NodeService (as the full path). --
    let ca = Ca::new();
    let node = NodeId::new();

    let tel_dir = tempfile::tempdir().expect("telemetry tempdir");
    let sink_root = tel_dir.path().join("tel");
    let sink = FilesystemSink::new(FilesystemSinkOptions::new(sink_root.clone()))
        .await
        .expect("build filesystem sink");

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

    // -- Boot a three-voter cluster: leader + two followers. ----------------
    let cluster_id = ClusterId::new();
    let leader = RunningCoordinator::start(cluster_id, &ca).await;
    poll(DEADLINE, "leader elected", || {
        let leader = &leader;
        async move { leader.is_leader() }
    })
    .await;

    let mut follower_a = Node::new(2, cluster_id, &ca);
    let mut follower_b = Node::new(3, cluster_id, &ca);
    follower_a
        .boot(CliOverrides {
            bootstrap: false,
            join: true,
        })
        .await;
    follower_b
        .boot(CliOverrides {
            bootstrap: false,
            join: true,
        })
        .await;
    add_voter(&leader, &follower_a, DEADLINE).await;
    add_voter(&leader, &follower_b, DEADLINE).await;

    // The replica we will query is a genuine follower, not the leader.
    poll(DEADLINE, "follower_a is a non-leader voter", || {
        let follower_a = &follower_a;
        let leader = &leader;
        async move { !follower_a.is_leader() && leader.is_leader() }
    })
    .await;

    // -- Register the agent + run the job through the LEADER. ---------------
    let agent_dir = tempfile::tempdir().expect("agent tempdir");
    let executor = FakeExecutor::new();
    let config = agent_config(
        node,
        agent_dir.path().join("data"),
        &leader.agent_endpoint,
        &ca,
        agent_dir.path(),
    );
    let agent = spawn_agent(config, executor.clone(), service_addr.clone());

    let leader_views = leader.views();
    poll(DEADLINE, "node registered", || {
        let leader_views = leader_views.clone();
        async move { node_epoch(&leader_views, node).is_some_and(|e| e >= 1) }
    })
    .await;

    let entity = QuotaEntityId::new();
    seed_quota(&leader, entity).await;
    let job = JobId::new();
    submit_job(&leader, job, entity).await;

    poll(DEADLINE, "attempt Running", || {
        let leader_views = leader_views.clone();
        async move {
            current_attempt_id(&leader_views, job).and_then(|a| attempt_state(&leader_views, a))
                == Some(AttemptState::Running)
        }
    })
    .await;
    let attempt = current_attempt_id(&leader_views, job).expect("attempt");
    let alloc = attempt_alloc(&leader_views, attempt).expect("alloc");

    // Seed three ordered log lines for that exact (job, attempt).
    const BASE: i64 = 1_700_000_000_000_000;
    let lines: Vec<String> = (0..3).map(|i| format!("line-{i}")).collect();
    let chunks: Vec<LogChunk> = lines
        .iter()
        .enumerate()
        .map(|(i, text)| seed_chunk(job, attempt, alloc, BASE + i as i64 * 1_000, text))
        .collect();
    LogSink::append(&sink, &chunks).await;

    // -- Wait until the FOLLOWER has replicated the attempt + service_addr. --
    let follower_views = follower_a.views();
    poll(DEADLINE, "follower replicated the running attempt", || {
        let follower_views = follower_views.clone();
        async move {
            let running = current_attempt_id(&follower_views, job)
                .and_then(|a| attempt_state(&follower_views, a))
                == Some(AttemptState::Running);
            let advertised = follower_views
                .latest()
                .state()
                .nodes
                .get(&node)
                .and_then(|n| n.node.service_addr.clone())
                .is_some();
            running && advertised
        }
    })
    .await;

    // -- Build the read path against the FOLLOWER's own consensus + views. ---
    // Eventual reads (logs) serve from the local published view with no leader
    // involvement, and the fetch dials the agent with no leadership gating; the
    // leader's client listener is never touched by this request.
    let coord_leaf = ca.leaf();
    let log_client = Arc::new(NodeClient::new(
        &ca.pem,
        &coord_leaf.cert_pem,
        &coord_leaf.key_pem,
    ));
    let follower_plane = Arc::new(
        CoordinatorControlPlane::new(follower_a.consensus(), follower_a.views(), cluster_id)
            .with_log_client(log_client),
    );
    let follower_router = coppice_api::http::router(follower_plane);

    let applied_before = follower_a.views().latest().applied_index();
    let (status, served_applied_index, body) =
        get_logs_with_applied_index(&follower_router, job, "order=asc&limit=200").await;

    // (a) 200 with the expected entries — no NOT_LEADER/421, no redirect.
    assert_eq!(
        status,
        StatusCode::OK,
        "a follower must answer the log route directly, not redirect or 421"
    );
    let got: Vec<String> = body.entries.iter().map(|e| e.text.clone()).collect();
    assert_eq!(got, lines, "the follower returns the full ordered entries");

    // (b) It served from the follower's OWN applied state: the header reflects a
    // real index off that follower's monotonic view (>= what we sampled just
    // before), and the plane was built from the follower — not the leader.
    assert!(
        served_applied_index >= applied_before,
        "served applied index {served_applied_index} must come from the follower's view \
         (>= {applied_before} sampled just before the read)"
    );
    assert!(
        served_applied_index <= follower_a.views().latest().applied_index(),
        "served applied index must not exceed the follower's current view"
    );
    assert!(
        !follower_a.is_leader() && leader.is_leader(),
        "the replica that served is a follower; the leader stayed the leader"
    );

    // (c) The follower dialed the agent directly: the source is the node that
    // ran the attempt and its data came back Available — only the follower's
    // own NodeClient made that fetch.
    assert_eq!(body.sources.len(), 1, "one source for the single attempt");
    assert_eq!(body.sources[0].attempt, attempt);
    assert_eq!(body.sources[0].node, Some(node));
    assert_eq!(
        body.sources[0].availability,
        LogAvailability::Available,
        "the follower fetched the live logs from the agent it dialed"
    );

    // -- Teardown. ---------------------------------------------------------
    agent.abort();
    let _ = agent.await;
    server.abort();
    let _ = (&mut server).await;
    follower_a.graceful_stop().await;
    follower_b.graceful_stop().await;
    leader.shutdown().await;
    drop(agent_dir);
    drop(tel_dir);
}
