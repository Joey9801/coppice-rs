//! End-to-end protocol test for the agent↔coordinator reconciliation loop
//! (ADR 0009/0013, `docs/protocols/agent-coordinator.md`).
//!
//! The real node agent — the library `Session` runner, a `FakeExecutor`
//! container runtime, and a `RealFs` journal in a tempdir — is driven over real
//! mTLS against the real coordinator task runtime (ingestion, dispatch, the
//! scheduler driver, and the agent session server), booted through
//! `bootstrap::bootstrap` + `bootstrap::serve_runtime`. One test instead scripts
//! the coordinator side in-process to fence the agent against stale commands.
//!
//! Everything synchronizes through `common::poll` (or a bounded negative
//! window); no test blocks on a bare sleep. Each test allocates its own free
//! ports so the suite runs in parallel.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, Identity, Server as TonicServer, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use coppice_agent::config::{CapacityConfig, Config, TlsConfig};
use coppice_agent::executor::{ExitInfo, FakeExecutor};
use coppice_agent::journal::Journal;
use coppice_agent::session::{run, Session};
use coppice_consensus::fs::RealFs;
use coppice_consensus::{Consensus, StateViews};
use coppice_core::attempt::{AttemptOutcome, AttemptState};
use coppice_core::id::{AllocationId, AttemptId, ClusterId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::{Job, JobState, RetryPolicy};
use coppice_core::quota::{CostUnits, PriorityMultiplier};
use coppice_core::resource::Resources;
use coppice_net::session::{AgentService, Server as AgentServiceServer};
use coppice_proto::pb::agent::v1 as pb;
use coppice_state::command::{ConfigureQuotaEntity, DeclareNodeLost, SubmitJob};
use coppice_state::Command;

use common::{free_port, poll, Ca, RunningCoordinator};

/// Generous per-wait deadline: well above the coordinator's 300ms election
/// timeout and the agent's 300ms heartbeat, small enough that a genuine hang
/// fails the test rather than the harness timeout.
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

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// A small resource request that fits comfortably in the agent's advertised
/// capacity.
fn requested() -> Resources {
    Resources {
        cpu_millis: 500,
        memory_bytes: 1 << 20,
        disk_bytes: 0,
    }
}

// ---- agent harness -------------------------------------------------------

/// Build an agent config pointing at `endpoint`, writing its mTLS PKI into
/// `pki_dir`. The client leaf's subject CN is the node id's typed string form
/// (`node-<uuid>`), which the gateway binds to the claimed NodeId at session
/// accept (ADR 0011).
fn agent_config(
    node_id: NodeId,
    data_dir: PathBuf,
    endpoint: &str,
    ca: &Ca,
    pki_dir: &Path,
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
        // Generous, so a job's request always fits.
        capacity: CapacityConfig {
            cpu_millis: 16_000,
            memory_bytes: 1 << 34,
            disk_bytes: 1 << 40,
        },
        // Fast cadences for the test (short heartbeat + reconnect backoff).
        heartbeat_interval: Duration::from_millis(300),
        reconnect_backoff_min: Duration::from_millis(100),
        reconnect_backoff_max: Duration::from_millis(500),
        labels: BTreeMap::new(),
    }
}

/// Open the journal at the config's data dir (acquiring its `LOCK`) and build a
/// session over `executor`.
fn build_session(config: &Config, executor: FakeExecutor) -> Session<RealFs, FakeExecutor> {
    std::fs::create_dir_all(&config.data_dir).expect("create agent data dir");
    let fs = RealFs::new(config.data_dir.clone());
    let (journal, state) = Journal::open(fs).expect("open agent journal");
    Session::new(
        config.node(),
        config.capacity_resources(),
        Vec::new(),
        journal,
        state,
        executor,
    )
}

/// Spawn the real agent session runner; returns its task handle. The runner
/// loops forever (connect, serve, reconnect), so a test stops it with
/// [`stop_agent`].
fn spawn_agent(config: Config, executor: FakeExecutor) -> JoinHandle<()> {
    let session = build_session(&config, executor);
    tokio::spawn(async move {
        let _ = run(session, &config).await;
    })
}

/// Abort the agent and await its full drop, which releases the journal `LOCK`
/// so a fresh instance can reopen the same data dir (ADR 0009 restart).
async fn stop_agent(handle: JoinHandle<()>) {
    handle.abort();
    let _ = handle.await;
}

// ---- state readers (all over the coordinator's published views) ----------

fn node_epoch(views: &StateViews, node: NodeId) -> Option<u64> {
    views.latest().state().nodes.get(&node).map(|n| n.epoch)
}

fn job_state(views: &StateViews, job: JobId) -> Option<JobState> {
    views.latest().state().jobs.get(&job).map(|j| j.state)
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
            updated_at_us: now_us(),
        }))
        .await
        .expect("propose ConfigureQuotaEntity");
    assert!(
        applied.outcome.is_ok(),
        "ConfigureQuotaEntity rejected: {:?}",
        applied.outcome
    );
}

async fn submit_job(
    coord: &RunningCoordinator,
    job: JobId,
    entity: QuotaEntityId,
    max_retries: u32,
) {
    let applied = coord
        .consensus()
        .propose(Command::SubmitJob(SubmitJob {
            job: Job {
                id: job,
                image: "registry/img:latest".into(),
                requests: requested(),
                priority: 0,
                max_runtime_us: None,
                quota_entity: entity,
                retry: RetryPolicy {
                    max_retries,
                    retry_user_errors: false,
                },
                abort_requested: None,
            },
            multiplier: PriorityMultiplier::ONE,
            submitted_at_us: now_us(),
        }))
        .await
        .expect("propose SubmitJob");
    assert!(
        applied.outcome.is_ok(),
        "SubmitJob rejected: {:?}",
        applied.outcome
    );
}

/// Boot a coordinator, run a real agent against it, and drive the job to
/// `Running`. Returns the running world so each test continues from there.
struct RunningJob {
    coord: RunningCoordinator,
    ca: Ca,
    node: NodeId,
    job: JobId,
    attempt: AttemptId,
    alloc: AllocationId,
    executor: FakeExecutor,
    agent: JoinHandle<()>,
    agent_dir: tempfile::TempDir,
}

/// The shared prefix of tests 1, 2, and 4: boot + register + submit + reach
/// attempt `Running` with the container started exactly once.
async fn run_to_running() -> RunningJob {
    let ca = Ca::new();
    let coord = RunningCoordinator::start(ClusterId::new(), &ca).await;
    poll(DEADLINE, "coordinator leadership", || {
        let coord = &coord;
        async move { coord.is_leader() }
    })
    .await;

    let node = NodeId::new();
    let agent_dir = tempfile::tempdir().expect("agent tempdir");
    let executor = FakeExecutor::new();
    let config = agent_config(
        node,
        agent_dir.path().join("data"),
        &coord.agent_endpoint,
        &ca,
        agent_dir.path(),
    );
    let agent = spawn_agent(config, executor.clone());

    let views = coord.views();

    // Node registered: RegisterNode applied, epoch >= 1 (command-catalog.md
    // #registernode; the first registration seats epoch 1).
    poll(DEADLINE, "node registered (epoch >= 1)", || {
        let views = views.clone();
        async move { node_epoch(&views, node).is_some_and(|e| e >= 1) }
    })
    .await;

    // Seed the quota entity, then submit a schedulable job.
    let entity = QuotaEntityId::new();
    seed_quota(&coord, entity).await;
    let job = JobId::new();
    submit_job(&coord, job, entity, 0).await;

    // The scheduler places it, dispatch sends StartJob, the agent starts the
    // container and reports Running. The job stays `Attempting(id)` for the
    // whole window; the attempt's own state is where Running shows up (ADR
    // 0029 collapses the job-level Preparing/Running/Finalizing mirror).
    poll(DEADLINE, "attempt Running", || {
        let views = views.clone();
        async move {
            current_attempt_id(&views, job).and_then(|a| attempt_state(&views, a))
                == Some(AttemptState::Running)
        }
    })
    .await;

    let attempt = current_attempt_id(&views, job).expect("current attempt");
    let alloc = attempt_alloc(&views, attempt).expect("attempt allocation");

    // Agent reported started: the FakeExecutor has the container running, and
    // the job is Attempting this exact attempt with it Running (ADR 0029).
    assert!(executor.is_running(alloc), "container should be running");
    assert_eq!(job_state(&views, job), Some(JobState::Attempting(attempt)));
    assert_eq!(attempt_state(&views, attempt), Some(AttemptState::Running));
    assert_eq!(
        executor.start_count(alloc),
        1,
        "the allocation must have started exactly once"
    );

    RunningJob {
        coord,
        ca,
        node,
        job,
        attempt,
        alloc,
        executor,
        agent,
        agent_dir,
    }
}

// ---- Test 1 --------------------------------------------------------------

/// A job runs start-to-finish over the real protocol: register, place,
/// dispatch, start, then a clean exit resolves it Succeeded.
///
/// docs/lifecycle/job-lifecycle.md "Job machine": every attempt end funnels
/// through Finalizing; with the agent's single terminal report the resolution
/// happens in the `RecordAttemptOutcome` apply.
#[tokio::test]
async fn job_runs_end_to_end() {
    init_tracing();
    let world = run_to_running().await;
    let views = world.coord.views();

    // Finish the container with exit code 0.
    world.executor.finish(
        world.alloc,
        ExitInfo {
            code: 0,
            oom_killed: false,
            runtime_us: 1_000,
        },
    );

    // Job Succeeded, attempt Terminal(Exited{0}).
    poll(DEADLINE, "job Succeeded", || {
        let views = views.clone();
        let job = world.job;
        async move { job_state(&views, job) == Some(JobState::Succeeded) }
    })
    .await;
    assert_eq!(
        attempt_state(&views, world.attempt),
        Some(AttemptState::Terminal(AttemptOutcome::Exited { code: 0 })),
    );
    // Exactly one container start for the allocation across the whole run.
    assert_eq!(world.executor.start_count(world.alloc), 1);

    stop_agent(world.agent).await;
    world.coord.shutdown().await;
    drop(world.agent_dir);
}

// ---- Test 2 --------------------------------------------------------------

/// An agent restart mid-run converges without re-executing the container.
///
/// ADR 0009 "Restart reconciliation" + "Idempotency": StartJob is idempotent on
/// AllocationId, and the ObservedSet adopt path re-establishes the running
/// attempt without a state regression or a duplicate container.
#[tokio::test]
async fn agent_restart_mid_run_converges_without_duplicate_execution() {
    init_tracing();
    let world = run_to_running().await;
    let views = world.coord.views();
    let node = world.node;
    let job = world.job;
    let attempt = world.attempt;
    let alloc = world.alloc;

    let epoch_before = node_epoch(&views, node).expect("epoch before restart");

    // Stop the agent (its journal LOCK releases on drop); keep the FakeExecutor
    // (the container keeps running in it) and the journal tempdir.
    stop_agent(world.agent).await;

    // Restart a fresh agent over the SAME journal dir and the SAME container
    // state (a forked executor: shared containers, its own exit queue).
    let executor2 = world.executor.fork();
    let config2 = agent_config(
        world.node,
        world.agent_dir.path().join("data"),
        &world.coord.agent_endpoint,
        &world.ca,
        world.agent_dir.path(),
    );
    let agent2 = spawn_agent(config2, executor2.clone());

    // Re-registration bumps the node epoch (session re-established).
    poll(DEADLINE, "node epoch bumped on re-registration", || {
        let views = views.clone();
        async move { node_epoch(&views, node).is_some_and(|e| e > epoch_before) }
    })
    .await;

    // The attempt is STILL Running: the ObservedSet reported the surviving
    // container and the coordinator adopted it — no regression, no restart.
    assert_eq!(
        attempt_state(&views, attempt),
        Some(AttemptState::Running),
        "attempt must not regress across the agent restart"
    );
    // No duplicate container: the coordinator never re-dispatched.
    assert_eq!(
        executor2.start_count(alloc),
        1,
        "the allocation must not be started a second time"
    );

    // Finish the container -> the job resolves Succeeded through the live agent.
    executor2.finish(
        alloc,
        ExitInfo {
            code: 0,
            oom_killed: false,
            runtime_us: 2_000,
        },
    );
    poll(DEADLINE, "job Succeeded after restart", || {
        let views = views.clone();
        async move { job_state(&views, job) == Some(JobState::Succeeded) }
    })
    .await;
    assert_eq!(
        attempt_state(&views, attempt),
        Some(AttemptState::Terminal(AttemptOutcome::Exited { code: 0 })),
    );
    assert_eq!(executor2.start_count(alloc), 1);

    stop_agent(agent2).await;
    world.coord.shutdown().await;
    drop(world.agent_dir);
}

// ---- Test 3 --------------------------------------------------------------

/// The response half of a scripted session: the queue of commands the test
/// pushes down to the agent.
type CommandStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<pb::AgentCommand, Status>> + Send>>;

/// A scripted in-process coordinator: it forwards every inbound agent report to
/// the test and streams back exactly the commands the test enqueues. One
/// session only (the agent holds one long-lived stream).
struct ScriptedCoordinator {
    reports_tx: mpsc::Sender<pb::AgentReport>,
    commands_rx: Mutex<Option<mpsc::Receiver<pb::AgentCommand>>>,
}

#[tonic::async_trait]
impl AgentService for ScriptedCoordinator {
    type SessionStream = CommandStream;

    async fn session(
        &self,
        request: Request<Streaming<pb::AgentReport>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let mut inbound = request.into_inner();
        let reports_tx = self.reports_tx.clone();
        tokio::spawn(async move {
            while let Ok(Some(report)) = inbound.message().await {
                if reports_tx.send(report).await.is_err() {
                    break;
                }
            }
        });
        let rx = self
            .commands_rx
            .lock()
            .expect("commands lock")
            .take()
            .expect("scripted coordinator accepts a single session");
        let stream = ReceiverStream::new(rx).map(Ok::<pb::AgentCommand, Status>);
        Ok(Response::new(Box::pin(stream)))
    }
}

fn command(seq: u64, term: u64, epoch: u64, body: pb::agent_command::Body) -> pb::AgentCommand {
    pb::AgentCommand {
        header: Some(pb::CommandHeader {
            token: Some(pb::FencingToken {
                leader_term: term,
                node_epoch: epoch,
            }),
            command_seq: seq,
        }),
        body: Some(body),
    }
}

fn start_job_body(alloc: AllocationId, attempt: AttemptId, job: JobId) -> pb::agent_command::Body {
    pb::agent_command::Body::StartJob(pb::StartJob {
        allocation: Some(alloc.into()),
        attempt: Some(attempt.into()),
        job: Some(job.into()),
        image: "registry/img:latest".into(),
        limits: None,
        max_runtime_us: None,
    })
}

/// Drain reports until one matches `pred`, or panic after `DEADLINE`.
async fn wait_for_report<F>(
    rx: &mut mpsc::Receiver<pb::AgentReport>,
    label: &str,
    mut pred: F,
) -> pb::AgentReport
where
    F: FnMut(&pb::AgentReport) -> bool,
{
    let deadline = tokio::time::sleep(DEADLINE);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => panic!("timed out waiting for report: {label}"),
            report = rx.recv() => {
                let report = report.unwrap_or_else(|| panic!("report stream closed waiting for: {label}"));
                if pred(&report) {
                    return report;
                }
            }
        }
    }
}

fn is_register(report: &pb::AgentReport) -> bool {
    matches!(report.body, Some(pb::agent_report::Body::Register(_)))
}

fn is_observed_set(report: &pb::AgentReport) -> bool {
    matches!(report.body, Some(pb::agent_report::Body::ObservedSet(_)))
}

fn attempt_status_alloc(report: &pb::AgentReport) -> Option<AllocationId> {
    match &report.body {
        Some(pb::agent_report::Body::AttemptStatus(s)) => s
            .allocation
            .clone()
            .and_then(|a| AllocationId::try_from(a).ok()),
        _ => None,
    }
}

/// Assert `cond` never becomes true across a bounded window.
async fn assert_never<F: Fn() -> bool>(window: Duration, label: &str, cond: F) {
    let start = Instant::now();
    while start.elapsed() < window {
        assert!(!cond(), "condition became true but must not: {label}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A deposed leader's or superseded epoch's commands fail closed at the agent.
///
/// ADR 0009 "a deposed leader's commands fail closed at every agent (term
/// check)": the agent rejects a StartJob whose term is below its watermark or
/// whose epoch is stale, and treats a re-delivered seq as idempotent.
#[tokio::test]
async fn stale_fenced_command_is_rejected_by_the_agent() {
    init_tracing();
    let ca = Ca::new();
    let node_id = NodeId::new();
    let port = free_port();
    let endpoint = format!("localhost:{port}");

    // Scripted coordinator: mTLS server with the same CA.
    let (reports_tx, mut reports_rx) = mpsc::channel::<pb::AgentReport>(256);
    let (commands_tx, commands_rx) = mpsc::channel::<pb::AgentCommand>(64);
    let service = ScriptedCoordinator {
        reports_tx,
        commands_rx: Mutex::new(Some(commands_rx)),
    };
    let server_leaf = ca.leaf();
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(
            &server_leaf.cert_pem,
            &server_leaf.key_pem,
        ))
        .client_ca_root(Certificate::from_pem(&ca.pem))
        .client_auth_optional(false);
    let addr = format!("127.0.0.1:{port}").parse().expect("scripted addr");
    let incoming = TcpIncoming::new(addr, true, None).expect("bind scripted listener");
    let server = tokio::spawn(async move {
        TonicServer::builder()
            .tls_config(tls)
            .expect("scripted tls")
            .add_service(AgentServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
    });

    // Run the real agent against the scripted coordinator.
    let executor = FakeExecutor::new();
    let agent_dir = tempfile::tempdir().expect("agent tempdir");
    let config = agent_config(
        node_id,
        agent_dir.path().join("data"),
        &endpoint,
        &ca,
        agent_dir.path(),
    );
    let agent = spawn_agent(config, executor.clone());

    // 1. Registration: on the agent's Register, accept with token (term 5,
    //    epoch 3, seq 1).
    wait_for_report(&mut reports_rx, "Register", is_register).await;
    commands_tx
        .send(command(
            1,
            5,
            3,
            pb::agent_command::Body::RegisterAccepted(pb::RegisterAccepted {}),
        ))
        .await
        .expect("send RegisterAccepted");

    // 2. The agent adopts epoch 3 and reports its (empty) ObservedSet.
    wait_for_report(&mut reports_rx, "ObservedSet", is_observed_set).await;

    // 3. A valid StartJob for A (term 5, epoch 3, seq 2) -> A starts once.
    let a = AllocationId::new();
    let (a_at, a_job) = (AttemptId::new(), JobId::new());
    commands_tx
        .send(command(2, 5, 3, start_job_body(a, a_at, a_job)))
        .await
        .expect("send StartJob A");
    poll(DEADLINE, "A started", || {
        let executor = &executor;
        async move { executor.is_running(a) }
    })
    .await;
    // Consume A's Running report from the valid StartJob.
    wait_for_report(&mut reports_rx, "AttemptStatus for A", |r| {
        attempt_status_alloc(r) == Some(a)
    })
    .await;
    assert_eq!(executor.start_count(a), 1);

    // 4. A stale-term StartJob for B (term 4 < watermark 5) and a stale-epoch
    //    StartJob for C (epoch 2 < watermark 3) are both rejected: B and C
    //    never start.
    let b = AllocationId::new();
    let c = AllocationId::new();
    commands_tx
        .send(command(
            3,
            4,
            3,
            start_job_body(b, AttemptId::new(), JobId::new()),
        ))
        .await
        .expect("send stale StartJob B");
    commands_tx
        .send(command(
            4,
            5,
            2,
            start_job_body(c, AttemptId::new(), JobId::new()),
        ))
        .await
        .expect("send stale-epoch StartJob C");
    assert_never(Duration::from_millis(500), "B or C started", || {
        executor.is_running(b) || executor.is_running(c)
    })
    .await;

    // 5. A duplicate seq=2 StartJob for A: idempotent re-delivery — the agent
    //    re-reports A's status and never starts a second container.
    commands_tx
        .send(command(2, 5, 3, start_job_body(a, a_at, a_job)))
        .await
        .expect("send duplicate StartJob A");
    wait_for_report(&mut reports_rx, "re-reported AttemptStatus for A", |r| {
        attempt_status_alloc(r) == Some(a)
    })
    .await;
    assert_eq!(
        executor.start_count(a),
        1,
        "duplicate StartJob must not re-execute A"
    );
    assert!(!executor.is_running(b));
    assert!(!executor.is_running(c));

    stop_agent(agent).await;
    drop(commands_tx);
    server.abort();
    let _ = server.await;
    drop(agent_dir);
}

// ---- Test 4 --------------------------------------------------------------

/// A node declared lost terminates its work `NodeLost`; when the agent
/// reappears with the container still running, the coordinator stops it
/// directly and never rewrites the terminal truth.
///
/// command-catalog.md #declarenodelost (platform outcome — retry policy
/// applies) and "The agent-report ingestion boundary" (an orphan container gets
/// a direct `StopJob`, never a log command); ADR 0013 truth-wins-the-race.
#[tokio::test]
async fn node_lost_then_reappearing_container_is_stopped() {
    init_tracing();
    // Give the job one retry so the NodeLost requeue is observable.
    let ca = Ca::new();
    let coord = RunningCoordinator::start(ClusterId::new(), &ca).await;
    poll(DEADLINE, "coordinator leadership", || {
        let coord = &coord;
        async move { coord.is_leader() }
    })
    .await;

    let node = NodeId::new();
    let agent_dir = tempfile::tempdir().expect("agent tempdir");
    let executor = FakeExecutor::new();
    let config = agent_config(
        node,
        agent_dir.path().join("data"),
        &coord.agent_endpoint,
        &ca,
        agent_dir.path(),
    );
    let agent = spawn_agent(config, executor.clone());
    let views = coord.views();

    poll(DEADLINE, "node registered", || {
        let views = views.clone();
        async move { node_epoch(&views, node).is_some_and(|e| e >= 1) }
    })
    .await;
    let entity = QuotaEntityId::new();
    seed_quota(&coord, entity).await;
    let job = JobId::new();
    submit_job(&coord, job, entity, 1).await;

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
    assert!(executor.is_running(alloc));
    let epoch_before = node_epoch(&views, node).expect("epoch");

    // Kill the agent session; the container keeps "running" in the executor.
    stop_agent(agent).await;

    // Simulate the housekeeping liveness deadline firing (the real
    // AGENT_LIVENESS_DEADLINE is 90s wall-clock, unit-tested separately) by
    // proposing DeclareNodeLost directly.
    let applied = coord
        .consensus()
        .propose(Command::DeclareNodeLost(DeclareNodeLost {
            node,
            declared_at_us: now_us(),
        }))
        .await
        .expect("propose DeclareNodeLost");
    assert!(
        applied.outcome.is_ok(),
        "DeclareNodeLost rejected: {:?}",
        applied.outcome
    );

    // The lost attempt is Terminal(NodeLost); the node is unschedulable with a
    // bumped epoch; the job requeues per retry policy (returns to Queued, and
    // stays there — its only node is unschedulable).
    poll(DEADLINE, "attempt Terminal(NodeLost)", || {
        let views = views.clone();
        async move {
            attempt_state(&views, attempt) == Some(AttemptState::Terminal(AttemptOutcome::NodeLost))
        }
    })
    .await;
    assert!(node_epoch(&views, node).is_some_and(|e| e > epoch_before));
    assert!(
        !views
            .latest()
            .state()
            .nodes
            .get(&node)
            .unwrap()
            .node
            .schedulable,
        "a lost node is unschedulable"
    );
    poll(DEADLINE, "job requeued to Queued", || {
        let views = views.clone();
        async move { job_state(&views, job) == Some(JobState::Queued) }
    })
    .await;

    // Restart the agent (same journal + container state). The container for the
    // lost attempt is still running.
    let executor2 = executor.fork();
    assert!(executor2.is_running(alloc));
    let config2 = agent_config(
        node,
        agent_dir.path().join("data"),
        &coord.agent_endpoint,
        &ca,
        agent_dir.path(),
    );
    let agent2 = spawn_agent(config2, executor2.clone());

    // The agent re-registers and reports the running container; the coordinator
    // finds no live intent for it and sends StopJob directly (never a log
    // command), so the container gets stopped.
    poll(DEADLINE, "orphan container stopped", || {
        let executor2 = &executor2;
        async move { !executor2.is_running(alloc) }
    })
    .await;

    // Truth about what stopped the work is never rewritten (ADR 0013): the lost
    // attempt's terminal outcome is still NodeLost, not Aborted.
    assert_eq!(
        attempt_state(&views, attempt),
        Some(AttemptState::Terminal(AttemptOutcome::NodeLost)),
        "the terminal outcome must remain NodeLost"
    );

    stop_agent(agent2).await;
    coord.shutdown().await;
    drop(agent_dir);
}
