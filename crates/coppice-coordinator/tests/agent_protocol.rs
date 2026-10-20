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
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, Identity, Server as TonicServer, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use coppice_agent::config::{CapacityConfig, Config};
use coppice_agent::executor::{ExitCause, ExitInfo, FakeExecutor};
use coppice_agent::journal::Journal;
use coppice_agent::session::{run, Session};
use coppice_consensus::fs::RealFs;
use coppice_consensus::{Consensus, StateViews};
use coppice_core::attempt::{AttemptOutcome, AttemptState};
use coppice_core::bytes::ByteSize;
use coppice_core::id::{AllocationId, AttemptId, ClusterId, JobId, NodeId, QuotaEntityId};
use coppice_core::job::{Job, JobState, RetryPolicy};
use coppice_core::quota::{CostUnits, PriorityClass, PriorityMultiplier};
use coppice_core::resource::Resources;
use coppice_core::time::Timestamp;
use coppice_discovery::SeedConfig;
use coppice_net::session::{AgentService, Server as AgentServiceServer};
use coppice_proto::pb::agent::v1 as pb;
use coppice_state::command::{AbortJob, ConfigureQuotaEntity, DeclareNodeLost, SubmitJob};
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

/// A small resource request that fits comfortably in the agent's advertised
/// capacity.
fn requested() -> Resources {
    Resources {
        cpu_millis: 500,
        memory: ByteSize::from_mib(1),
        disk: ByteSize::ZERO,
    }
}

// ---- agent harness -------------------------------------------------------

/// Build an agent config pointing at `endpoint`, with its cluster-managed mTLS
/// material under `<data_dir>/pki`. The client leaf's subject CN is the node
/// id's typed string form (`node-<uuid>`), which the gateway binds to the
/// claimed NodeId at session accept (ADR 0011).
fn agent_config(node_id: NodeId, data_dir: PathBuf, endpoint: &str, ca: &Ca) -> Config {
    // The leaf and the identity land in the one layout an agent reads, the
    // same one enrollment would have written them to (issue #127).
    common::agent_material(&data_dir, ca, node_id);

    Config {
        data_dir,
        // One coordinator, this harness's gateway (ADR 0037 §2).
        discovery: SeedConfig::static_seeds(vec![endpoint.to_string()]),
        // Declared because every agent declares one, and never contacted:
        // the leaf above is already installed (issue #127).
        enrollment: common::unused_enrollment(),
        // Generous, so a job's request always fits.
        // Generous overrides on every dimension (deployment-story A3), so the
        // harness never depends on what the machine running the test reports.
        capacity: CapacityConfig {
            cpu_millis: Some(16_000),
            memory: Some(ByteSize::from_gib(16)),
            disk: Some(ByteSize::from_tib(1)),
        },
        reservation: Default::default(),
        // Fast cadences for the test (short heartbeat + reconnect backoff).
        heartbeat_interval: Duration::from_millis(300),
        reconnect_backoff_min: Duration::from_millis(100),
        reconnect_backoff_max: Duration::from_millis(500),
        // Long enough that only the tests that mean to hit the deadline do
        // (ADR 0041); those override it.
        shutdown_grace: Duration::from_secs(60),
        labels: BTreeMap::new(),
        executor: Default::default(),
        pressure: Default::default(),
        image_cache: Default::default(),
        telemetry: Default::default(),
        // This protocol test exercises the session plane only, not the
        // agent-hosted NodeService (ADR 0034), so no listener is configured.
        listen: None,
        metrics_addr: None,
        preemption: Default::default(),
    }
}

/// Open the journal at the config's data dir (acquiring its `LOCK`) and build a
/// session over `executor`.
fn build_session(config: &Config, executor: FakeExecutor) -> Session<RealFs, FakeExecutor> {
    std::fs::create_dir_all(&config.data_dir).expect("create agent data dir");
    let fs = RealFs::new(config.data_dir.clone());
    let (journal, state) = Journal::open(fs).expect("open agent journal");
    let detected = coppice_agent::capacity::detect(&config.data_dir);
    Session::new(
        node_identity(config),
        advertised(config),
        Vec::new(),
        journal,
        state,
        executor,
    )
    // The daemon describes its host on every registration; the harness does
    // the same so the facts path is the real one end to end.
    .with_host_facts(
        coppice_agent::hostinfo::collect(&detected),
        detected.as_resources(),
    )
}

/// A spawned agent: its task handle, the ADR 0041 shutdown watch that stands in
/// for the SIGTERM a test must never raise at the process running it, and the
/// operational listener serving `/healthz` and `/readyz` beside it.
struct RunningAgent {
    join: JoinHandle<anyhow::Result<()>>,
    shutdown: watch::Sender<bool>,
    /// `http://127.0.0.1:<port>` of this agent's operational listener.
    probe_base: String,
    /// The listener's own trigger — deliberately *not* `shutdown` (ADR 0041).
    /// The daemon flips this only after the session loop returns, so the probes
    /// keep answering for the whole drain; the harness mirrors that ordering
    /// because it is the property under test.
    listeners: watch::Sender<bool>,
    listener_join: JoinHandle<()>,
}

impl RunningAgent {
    /// `GET <probe_base><path>`, answering the status and decoded JSON body.
    async fn probe(&self, path: &str) -> (reqwest::StatusCode, serde_json::Value) {
        let response = coppice_tls::HttpTls::no_roots()
            .reqwest_client()
            .expect("http client")
            .get(format!("{}{path}", self.probe_base))
            .send()
            .await
            .expect("the agent's operational listener must answer");
        let status = response.status();
        let body = response.json().await.expect("a JSON probe body");
        (status, body)
    }

    /// Ask the agent to drain, exactly as the daemon's signal handler would.
    fn signal_shutdown(&self) {
        self.shutdown.send(true).expect("the agent loop is running");
    }

    /// Whether the run loop has already returned.
    fn has_exited(&self) -> bool {
        self.join.is_finished()
    }

    /// Wait for the run loop to return, asserting it returned `Ok` — the drain
    /// finished, rather than the loop falling out on an error.
    async fn expect_clean_exit(self, deadline: Duration, label: &str) {
        let outcome = tokio::time::timeout(deadline, self.join)
            .await
            .unwrap_or_else(|_| panic!("timed out after {deadline:?} waiting for: {label}"))
            .expect("the agent task must not panic");
        outcome.unwrap_or_else(|e| panic!("the agent must exit Ok ({label}): {e:#}"));
        // The daemon's step 4, in the same order: the session loop has returned,
        // so now — and only now — the operational listener is told to stop, and
        // joined to observe it actually down.
        let _ = self.listeners.send(true);
        tokio::time::timeout(DEADLINE, self.listener_join)
            .await
            .expect("the operational listener must stop once the drain has finished")
            .expect("the listener task must not panic");
    }
}

/// Spawn the real agent session runner; returns its handle and its shutdown
/// watch. The runner serves until it is drained
/// ([`RunningAgent::signal_shutdown`]) or aborted ([`stop_agent`]).
async fn spawn_agent(config: Config, executor: FakeExecutor) -> RunningAgent {
    // The shared `/readyz` snapshot, wired exactly as `run_daemon_with_shutdown`
    // wires it: the session writes it, the operational listener reads it, and
    // the two know nothing of each other (ADR 0041).
    let session = build_session(&config, executor);
    let health = coppice_agent::health::AgentHealth::new(node_identity(&config));
    let session = session.with_health(health.clone());

    // Two triggers, not one — the point of the fix this exercises. `shutdown`
    // is the SIGTERM seam and starts a drain that can run for `shutdown_grace`;
    // `listeners` is flipped only once the session loop has returned, so
    // `/readyz` is still answering `draining` with a live `running` count for
    // the whole of that window, which is what an ASG lifecycle hook polls.
    let (shutdown, shutdown_rx) = watch::channel(false);
    let (listeners, listeners_rx) = watch::channel(false);

    let listener = coppice_agent::metrics_server::prepare_listener(
        format!("127.0.0.1:{}", free_port()).parse().unwrap(),
    )
    .await
    .expect("bind the agent operational listener");
    let probe_base = format!(
        "http://{}",
        listener.local_addr().expect("listener local addr")
    );
    // A process-local recorder (never installed globally — the test process
    // hosts several agents), which is all `/metrics` needs; the probes below
    // read `health`.
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let listener_join = coppice_agent::metrics_server::serve_until(
        listener,
        recorder.handle(),
        coppice_agent::gather_metrics,
        coppice_agent::usage::render_exposition,
        health,
        listeners_rx,
    );

    let join = tokio::spawn(async move {
        let tls = coppice_agent::load_tls_store(&config).expect("load agent tls store");
        run(session, &config, tls, shutdown_rx).await
    });
    RunningAgent {
        join,
        shutdown,
        probe_base,
        listeners,
        listener_join,
    }
}

/// Abort the agent and await its full drop, which releases the journal `LOCK`
/// so a fresh instance can reopen the same data dir (ADR 0009 restart). The
/// ungraceful stop — a test that wants the drain uses
/// [`RunningAgent::signal_shutdown`] instead.
async fn stop_agent(agent: RunningAgent) {
    agent.join.abort();
    let _ = agent.join.await;
    let _ = agent.listeners.send(true);
    let _ = agent.listener_join.await;
}

// ---- state readers (all over the coordinator's published views) ----------

fn node_epoch(views: &StateViews, node: NodeId) -> Option<u64> {
    views.latest().state().nodes.get(&node).map(|n| n.epoch)
}

/// The node record's agent-announced drain flag (ADR 0041), as replicated.
fn node_draining(views: &StateViews, node: NodeId) -> Option<bool> {
    views.latest().state().nodes.get(&node).map(|n| n.draining)
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
            updated_at: Timestamp::now(),
            actor: None,
            preemptible: None,
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
                command: vec!["run".into()],
                entrypoint: None,
                env: Default::default(),
                requests: requested(),
                priority: PriorityClass(0),
                max_runtime: None,
                quota_entity: entity,
                retry: RetryPolicy {
                    max_retries,
                    retry_user_errors: false,
                },
                abort_requested: None,
                submitted_by: None,
                metadata: Default::default(),
                preemption: Default::default(),
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
    agent: RunningAgent,
    agent_dir: tempfile::TempDir,
}

/// The shared prefix of tests 1, 2, and 4: boot + register + submit + reach
/// attempt `Running` with the container started exactly once.
async fn run_to_running() -> RunningJob {
    run_to_running_with(|_| {}).await
}

/// [`run_to_running`] with a last look at the agent's config before it starts —
/// the drain tests turn `shutdown_grace` down to something a test can wait out.
async fn run_to_running_with(tweak: impl FnOnce(&mut Config)) -> RunningJob {
    run_to_running_under(None, tweak).await
}

/// [`run_to_running_with`], with the replicated policy edited first — before
/// the job is submitted, so its notice resolves against the edited floor and
/// its `StartJob` carries the edited grace and cleanup margin.
async fn run_to_running_under(
    policy: Option<fn(&mut coppice_state::PolicyConfig)>,
    tweak: impl FnOnce(&mut Config),
) -> RunningJob {
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
    let mut config = agent_config(
        node,
        agent_dir.path().join("data"),
        &coord.agent_endpoint,
        &ca,
    );
    tweak(&mut config);
    let agent = spawn_agent(config, executor.clone()).await;

    let views = coord.views();

    // Node registered: RegisterNode applied, epoch >= 1 (command-catalog.md
    // #registernode; the first registration seats epoch 1).
    poll(DEADLINE, "node registered (epoch >= 1)", || {
        let views = views.clone();
        async move { node_epoch(&views, node).is_some_and(|e| e >= 1) }
    })
    .await;

    if let Some(edit) = policy {
        let mut edited = views.latest().state().policy.clone();
        edit(&mut edited);
        let applied = coord
            .consensus()
            .propose(Command::UpdatePolicy(
                coppice_state::command::UpdatePolicy {
                    policy: edited,
                    updated_at: Timestamp::now(),
                    actor: None,
                },
            ))
            .await
            .expect("propose UpdatePolicy");
        assert!(
            applied.outcome.is_ok(),
            "UpdatePolicy rejected: {:?}",
            applied.outcome
        );
    }

    // Seed the quota entity, then submit a placeable job.
    let entity = QuotaEntityId::new();
    seed_quota(&coord, entity).await;
    let job = JobId::new();
    submit_job(&coord, job, entity, 0).await;

    // The scheduler places it, dispatch sends StartJob, the agent starts the
    // container and reports Running. The job stays `Attempting(id)` for the
    // whole window; the attempt's own state is where Running shows up (ADR
    // 0030 collapses the job-level Preparing/Running/Finalizing mirror).
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
    // the job is Attempting this exact attempt with it Running (ADR 0030).
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
            cause: ExitCause::Natural,
            runtime: coppice_core::time::Duration::from_micros(1_000),
            finished_at: coppice_core::time::Timestamp::now(),
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
    );
    let agent2 = spawn_agent(config2, executor2.clone()).await;

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
            cause: ExitCause::Natural,
            runtime: coppice_core::time::Duration::from_micros(2_000),
            finished_at: coppice_core::time::Timestamp::now(),
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

    /// The scripted coordinator serves sessions only; renewal (ADR 0037 §4)
    /// belongs to the real gateway and is exercised in the coordinator's own
    /// suite.
    async fn renew(
        &self,
        _request: Request<pb::RenewRequest>,
    ) -> Result<Response<pb::RenewResponse>, Status> {
        Err(Status::unimplemented(
            "the scripted test coordinator does not renew certificates",
        ))
    }

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
        command: vec!["run".into()],
        entrypoint: None,
        env: Vec::new(),
        limits: None,
        max_runtime_us: None,
        notice_signal: coppice_proto::pb::core::v1::NoticeSignal::Sigusr1 as i32,
        notice_us: 60_000_000,
        ..Default::default()
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
/// Host facts collected by the agent survive the whole registration path —
/// `hostinfo::collect` → `Register` → ingestion's normalizer → `RegisterNode`
/// → apply → replicated state, which is what the node-detail projection
/// behind `GET /api/v1/nodes/{node}` reads. (This harness stands up no HTTP
/// server; the projection's own half is covered by `project.rs`'s unit test.)
///
/// The assertions are deliberately shape-level: the values are whatever this
/// machine happens to report, and a CI box may legitimately answer "unknown"
/// for the OS release or the CPU model. What must hold is that the fields the
/// agent can *always* fill arrive intact and unswapped, and that the detected
/// vector rides alongside the advertised one — the harness overrides all three
/// capacity dimensions, so the two genuinely differ here, which is exactly the
/// confusion the host card exists to explain.
#[tokio::test]
async fn host_facts_survive_registration_to_the_node_detail() {
    let ca = Ca::new();
    let coord = RunningCoordinator::start(ClusterId::new(), &ca).await;
    poll(DEADLINE, "coordinator leadership", || {
        let coord = &coord;
        async move { coord.is_leader() }
    })
    .await;

    let node = NodeId::new();
    let agent_dir = tempfile::tempdir().expect("agent tempdir");
    let config = agent_config(
        node,
        agent_dir.path().join("data"),
        &coord.agent_endpoint,
        &ca,
    );
    let agent = spawn_agent(config, FakeExecutor::new()).await;

    let views = coord.views();
    poll(DEADLINE, "node registered (epoch >= 1)", || {
        let views = views.clone();
        async move { node_epoch(&views, node).is_some_and(|e| e >= 1) }
    })
    .await;

    let view = views.latest();
    let record = &view.state().nodes[&node];

    let host = record
        .node
        .host_facts
        .clone()
        .expect("the agent described its host");
    assert_eq!(host.os, std::env::consts::OS);
    assert_eq!(host.arch, std::env::consts::ARCH);
    assert!(
        !host.agent_version.is_empty(),
        "the agent version rides too"
    );
    assert!(host.logical_cores >= 1, "at least one hardware thread");

    // Advertised capacity is the harness's overrides minus the reservation;
    // the detected vector is what the machine actually reported. Linux must
    // provide the complete vector in CI. Other platforms may omit a reading
    // when the test runner restricts a system API, so check their invariants
    // only when the complete vector is available.
    #[cfg(target_os = "linux")]
    {
        let detected = record
            .node
            .detected_capacity
            .expect("every dimension detects on a Linux test host");
        assert_ne!(
            detected, record.node.capacity,
            "the harness overrides all three dimensions, so the two must differ"
        );
        assert!(detected.cpu_millis >= 1_000, "a real core count");
    }
    #[cfg(not(target_os = "linux"))]
    if let Some(detected) = record.node.detected_capacity {
        assert_ne!(
            detected, record.node.capacity,
            "the harness overrides all three dimensions, so the two must differ"
        );
        assert!(detected.cpu_millis >= 1_000, "a real core count");
    }

    stop_agent(agent).await;
}

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
    let incoming = TcpIncoming::bind(addr)
        .expect("bind scripted listener")
        .with_nodelay(Some(true));
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
    let config = agent_config(node_id, agent_dir.path().join("data"), &endpoint, &ca);
    let agent = spawn_agent(config, executor.clone()).await;

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
    );
    let agent = spawn_agent(config, executor.clone()).await;
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
            declared_at: Timestamp::now(),
        }))
        .await
        .expect("propose DeclareNodeLost");
    assert!(
        applied.outcome.is_ok(),
        "DeclareNodeLost rejected: {:?}",
        applied.outcome
    );

    // The lost attempt is Terminal(NodeLost); the node is drained with a
    // bumped epoch; the job requeues per retry policy (returns to Queued, and
    // stays there — its only node is drained).
    poll(DEADLINE, "attempt Terminal(NodeLost)", || {
        let views = views.clone();
        async move {
            attempt_state(&views, attempt) == Some(AttemptState::Terminal(AttemptOutcome::NodeLost))
        }
    })
    .await;
    assert!(node_epoch(&views, node).is_some_and(|e| e > epoch_before));
    assert!(
        views
            .latest()
            .state()
            .nodes
            .get(&node)
            .unwrap()
            .node
            .drain
            .is_some(),
        "a lost node is drained"
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
    );
    let agent2 = spawn_agent(config2, executor2.clone()).await;

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

// ---- Test 5 --------------------------------------------------------------

/// One HTTP GET against the coordinator's own client listener, split into its
/// status code and JSON body — both wanted at every assertion, since a bare
/// status comparison leaves a failure with no record of what was said.
async fn api_get(client: &reqwest::Client, url: &str) -> (u16, serde_json::Value) {
    let resp = client
        .get(url)
        .send()
        .await
        .unwrap_or_else(|e| panic!("GET {url}: {e}"));
    let status = resp.status().as_u16();
    let text = resp.text().await.expect("response body text");
    let body = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("GET {url} answered {status} with non-JSON ({e}): {text}"));
    (status, body)
}

/// The ADR 0039 pipeline end to end over the real protocol: the executor's
/// fold rides a heartbeat, ingestion peels it into the leader's sink, and the
/// read surfaces serve it as a measured `used` rather than a zero.
///
/// The *rolling* half is deliberately not asserted here. A bucket closes on
/// `limits::USAGE_BUCKET_INTERVAL` (30 s), so waiting for one would add half
/// a minute to the suite to re-observe what
/// `tasks::usage_history`'s virtual-time test already covers instantly. What
/// this test is uniquely able to check is the *live* path — the one that runs
/// on every dashboard poll — plus the shape of the utilization route now that
/// it is no longer a 501: a known node answers 200 with its capacity and a
/// well-formed (possibly still-empty) series, and an unknown one 404s.
#[tokio::test]
async fn node_usage_rides_the_heartbeat_to_the_read_surfaces() {
    init_tracing();
    let world = run_to_running().await;

    // What the agent's executor will fold and report on its next heartbeat.
    let used = coppice_core::resource::Resources {
        cpu_millis: 1_750,
        memory: coppice_core::bytes::ByteSize::from_mib(384),
        disk: coppice_core::bytes::ByteSize::from_mib(96),
    };
    world.executor.set_usage(Some(used));

    let client = coppice_tls::HttpTls::no_roots()
        .reqwest_client()
        .expect("http client");
    let node = world.node;
    let overview_url = world.coord.api("/api/v1/overview");

    // The next heartbeat carries it; the overview's cluster total is the sum
    // of the per-node readings that exist, so with one node it is exactly it.
    poll(DEADLINE, "overview reports measured usage", || {
        let (client, url) = (client.clone(), overview_url.clone());
        async move {
            let (status, body) = api_get(&client, &url).await;
            status == 200 && body["capacity"]["used"]["cpu_millis"] == 1_750
        }
    })
    .await;

    let (_, overview) = api_get(&client, &overview_url).await;
    assert_eq!(overview["capacity"]["used"]["memory_bytes"], 384 << 20);
    assert_eq!(overview["capacity"]["used"]["disk_bytes"], 96 << 20);

    // The same reading on the node's own summary — one source, so the two
    // surfaces can never disagree.
    let (status, detail) =
        api_get(&client, &world.coord.api(&format!("/api/v1/nodes/{node}"))).await;
    assert_eq!(status, 200);
    assert_eq!(detail["summary"]["used"]["cpu_millis"], 1_750);

    // The utilization route is real now (ADR 0039 amends ADR 0031's 501).
    let (status, util) = api_get(
        &client,
        &world
            .coord
            .api(&format!("/api/v1/nodes/{node}/utilization")),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        util["capacity"]["cpu_millis"],
        advertised_cpu_millis(&world),
        "utilization reports the node's registered capacity"
    );
    assert!(
        util["samples"].is_array(),
        "a known node answers with a series, empty or not: {util}"
    );

    // An unknown node is still a 404 — distinct from a known node the leader
    // has collected nothing for.
    let (status, _) = api_get(
        &client,
        &world
            .coord
            .api(&format!("/api/v1/nodes/{}/utilization", NodeId::new())),
    )
    .await;
    assert_eq!(status, 404);

    stop_agent(world.agent).await;
    world.coord.shutdown().await;
    drop(world.agent_dir);
}

/// The node's registered cpu capacity, read back from the replicated state
/// the coordinator actually applied (rather than recomputed from config).
fn advertised_cpu_millis(world: &RunningJob) -> u64 {
    world
        .coord
        .views()
        .latest()
        .state()
        .nodes
        .get(&world.node)
        .expect("the node registered")
        .node
        .capacity
        .cpu_millis
}

/// The node id the agent settles on: read back from `<data_dir>/node-identity`
/// exactly as the daemon does (deployment-story A1).
fn node_identity(config: &Config) -> NodeId {
    coppice_agent::identity::load_or_mint_node_identity(&config.data_dir)
        .expect("the harness seeded the agent node identity")
}

/// The resource vector the agent advertises: detection, the config's overrides
/// (all three set by [`agent_config`]), then the system reservation
/// (deployment-story A3).
fn advertised(config: &Config) -> coppice_core::resource::Resources {
    config
        .effective_capacity(&coppice_agent::capacity::detect(&config.data_dir))
        .expect("every capacity dimension is overridden by the harness")
        .advertised
}

// ---- Test 6: the ADR 0041 shutdown drain ---------------------------------

/// Assert `cond` holds continuously for `window` — the bounded *negative*
/// check: "the agent does not exit", "the job is not placed". A `poll` proves
/// something eventually happens; this proves something does not.
async fn holds_for<F, Fut>(window: Duration, label: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < window {
        assert!(cond().await, "expected to hold for {window:?}: {label}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// An idle agent stopped with SIGTERM announces its drain and exits promptly,
/// and the node it left behind takes no more work.
///
/// ADR 0041 "Agent shutdown is a drain, then a bounded wait": with nothing to
/// wait for, the whole drain is the announcement — and the announcement is the
/// point, since it is what stops the node receiving placements from that log
/// position rather than 90 s later down the liveness path.
#[tokio::test]
async fn shutdown_while_idle_announces_the_drain_and_exits() {
    init_tracing();
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
    );
    let agent = spawn_agent(config, executor.clone()).await;
    let views = coord.views();

    poll(DEADLINE, "node registered", || {
        let views = views.clone();
        async move { node_epoch(&views, node).is_some_and(|e| e >= 1) }
    })
    .await;
    assert_eq!(
        node_draining(&views, node),
        Some(false),
        "a running agent announces nothing"
    );

    // SIGTERM, as the daemon's handler would deliver it.
    agent.signal_shutdown();

    // The announcement rides the immediate heartbeat and the leader turns it
    // into SetNodeDraining.
    poll(DEADLINE, "node record draining", || {
        let views = views.clone();
        async move { node_draining(&views, node) == Some(true) }
    })
    .await;

    // Nothing to wait for, so the loop returns almost at once.
    agent
        .expect_clean_exit(Duration::from_secs(5), "an idle agent's drain")
        .await;

    // And the node takes no more work: an announced node admits nothing while
    // draining, so a fresh job has nowhere to go and stays queued.
    let entity = QuotaEntityId::new();
    seed_quota(&coord, entity).await;
    let job = JobId::new();
    submit_job(&coord, job, entity, 0).await;
    poll(DEADLINE, "the submitted job is applied", || {
        let views = views.clone();
        async move { job_state(&views, job).is_some() }
    })
    .await;
    holds_for(Duration::from_secs(2), "the job stays queued", || {
        let views = views.clone();
        async move { job_state(&views, job) == Some(JobState::Queued) }
    })
    .await;

    coord.shutdown().await;
    drop(agent_dir);
}

/// An agent stopped mid-job waits for the container, and the attempt it was
/// running ends as the clean outcome it earned — not the `NodeLost` a killed
/// agent would have produced.
#[tokio::test]
async fn shutdown_waits_for_running_work_then_exits() {
    init_tracing();
    let world = run_to_running().await;
    let views = world.coord.views();
    let node = world.node;

    world.agent.signal_shutdown();

    // The drain is announced while the work continues.
    poll(DEADLINE, "node record draining", || {
        let views = views.clone();
        async move { node_draining(&views, node) == Some(true) }
    })
    .await;
    holds_for(
        Duration::from_secs(2),
        "the agent keeps serving while its container runs",
        || {
            let agent = &world.agent;
            let executor = &world.executor;
            let alloc = world.alloc;
            async move { !agent.has_exited() && executor.is_running(alloc) }
        },
    )
    .await;

    // The job finishes inside the window: the drain got what it was waiting
    // for, and the coordinator sees the real outcome.
    world.executor.finish(
        world.alloc,
        ExitInfo {
            code: 0,
            cause: ExitCause::Natural,
            runtime: coppice_core::time::Duration::from_micros(1_000),
            finished_at: coppice_core::time::Timestamp::now(),
        },
    );
    poll(DEADLINE, "job Succeeded", || {
        let views = views.clone();
        let job = world.job;
        async move { job_state(&views, job) == Some(JobState::Succeeded) }
    })
    .await;
    assert_eq!(
        attempt_state(&views, world.attempt),
        Some(AttemptState::Terminal(AttemptOutcome::Exited { code: 0 })),
        "a drained agent's finished work is its own outcome, never NodeLost",
    );

    world
        .agent
        .expect_clean_exit(DEADLINE, "the drain after the container finished")
        .await;
    world.coord.shutdown().await;
    drop(world.agent_dir);
}

/// The probes keep answering for the whole drain, and answer `draining` with a
/// live `running` count while they do (ADR 0041).
///
/// This is the contract an ASG lifecycle hook (or a `systemd` unit's stop
/// handler) is written against: it polls `/readyz` and waits for `running` to
/// reach zero or for the process to go. A listener that shared the session's
/// shutdown watch would close on the signal — in the first milliseconds of a
/// window that can legitimately run for minutes — and every one of those polls
/// would be a connection refused instead of an answer. So the operational
/// listener has a trigger of its own, flipped only once the session loop has
/// returned; the harness wires the two in that same order.
#[tokio::test]
async fn readyz_reports_the_drain_while_it_is_still_draining() {
    init_tracing();
    let world = run_to_running().await;
    let node = world.node;

    // Before the signal: registered, serving, and ready.
    let (status, body) = world.agent.probe("/readyz").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body["phase"], "ready");
    assert_eq!(body["node_id"], node.to_string());
    assert_eq!(body["draining"], false);

    world.agent.signal_shutdown();

    // Mid-drain — the container is still running, so the agent is still here.
    // `draining` wins the phase precedence over a session that is otherwise
    // perfectly healthy, and `running` is the work being waited for.
    poll(DEADLINE, "/readyz reports the drain", || {
        let agent = &world.agent;
        async move {
            let (status, body) = agent.probe("/readyz").await;
            status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                && body["phase"] == "draining"
                && body["draining"] == true
                && body["running"] == 1
        }
    })
    .await;

    // Liveness is unconditional throughout: a draining agent is doing exactly
    // what it was told to, and a `/healthz` that failed here would have systemd
    // restart it mid-drain.
    let (status, body) = world.agent.probe("/healthz").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body["status"], "ok");

    // The listener outlives the drain, not the signal: it is still answering
    // some seconds in, with the session loop still running.
    holds_for(
        Duration::from_secs(2),
        "the probes stay up for the whole drain",
        || {
            let agent = &world.agent;
            async move {
                if agent.has_exited() {
                    return false;
                }
                let (status, body) = agent.probe("/readyz").await;
                status == reqwest::StatusCode::SERVICE_UNAVAILABLE && body["phase"] == "draining"
            }
        },
    )
    .await;

    // Let the work finish; `running` falls to zero and the drain completes.
    world.executor.finish(
        world.alloc,
        ExitInfo {
            code: 0,
            cause: ExitCause::Natural,
            runtime: coppice_core::time::Duration::from_micros(1_000),
            finished_at: coppice_core::time::Timestamp::now(),
        },
    );
    // `expect_clean_exit` asserts the other half of the ordering: the listener
    // does come down, once the session loop has returned.
    world
        .agent
        .expect_clean_exit(DEADLINE, "the drain after the container finished")
        .await;

    world.coord.shutdown().await;
    drop(world.agent_dir);
}

/// `shutdown_grace` bounds the process, not the gaps between events: a branch
/// body parked in a slow executor call is cut short at the deadline.
///
/// The real `heartbeat_report()` awaits `observe()`, which on the Docker
/// executor is a list/inspect that can sit there for the daemon's own 120 s
/// request timeout. Checking the deadline only between `select!` iterations
/// would let a 2 s grace window become a two-minute stop — and a flip that
/// arrived during such a call would not even be *observed* until it returned.
/// The runner races one grace deadline, built from a cloned shutdown receiver,
/// against every await it makes: the clock starts at the flip, and the
/// in-flight future is dropped when the window closes.
#[tokio::test]
async fn shutdown_grace_preempts_an_executor_call_in_flight() {
    init_tracing();
    let world = run_to_running_with(|config| {
        config.shutdown_grace = Duration::from_secs(2);
    })
    .await;

    // Far longer than the grace window, and longer than `DEADLINE`: if the
    // deadline did not preempt, the agent would still be inside `observe()`
    // when the test gave up.
    world.executor.set_observe_delay(Duration::from_secs(120));

    // Wait until a heartbeat has actually entered the slow `observe()` — the
    // next tick is at most one `heartbeat_interval` away, and the call parks
    // for two minutes once it starts, so the flip below lands mid-await.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let flipped_at = Instant::now();
    world.agent.signal_shutdown();

    world
        .agent
        .expect_clean_exit(DEADLINE, "the drain preempting an in-flight executor call")
        .await;
    let waited = flipped_at.elapsed();
    assert!(
        waited < Duration::from_secs(2) + Duration::from_secs(8),
        "shutdown_grace must bound the whole stop, not just the gaps between \
         events; waited {waited:?} on a 2s window with a 120s executor call in flight"
    );

    // Abandoned, never killed — the same rule as the ordinary deadline path.
    assert!(
        world.executor.is_running(world.alloc),
        "an abandoned container is left running, never stopped"
    );

    world.coord.shutdown().await;
    drop(world.agent_dir);
}

/// Work that outlives `shutdown_grace` is abandoned, not killed: the agent
/// exits at the deadline with the container still running.
///
/// ADR 0041: killing it locally would report an exit code the coordinator
/// classifies as the job's own failure, where a node that falls silent has its
/// work classified `NodeLost` and retried elsewhere. The backstop that follows
/// is the coordinator's business and is tested there.
#[tokio::test]
async fn shutdown_past_the_grace_window_leaves_the_container_running() {
    init_tracing();
    let world = run_to_running_with(|config| {
        config.shutdown_grace = Duration::from_secs(2);
    })
    .await;
    let views = world.coord.views();

    let flipped_at = Instant::now();
    world.agent.signal_shutdown();

    // Never finished: the only thing that ends this drain is the deadline.
    world
        .agent
        .expect_clean_exit(DEADLINE, "the drain hitting shutdown_grace")
        .await;
    let waited = flipped_at.elapsed();
    assert!(
        waited >= Duration::from_secs(2),
        "the drain must wait out the whole window, waited {waited:?}"
    );

    // The container was left alone, and the attempt is still Running as far as
    // the cluster knows — nothing reported a fabricated failure for it.
    assert!(
        world.executor.is_running(world.alloc),
        "an abandoned container is left running, never stopped"
    );
    assert_eq!(
        attempt_state(&views, world.attempt),
        Some(AttemptState::Running),
        "the attempt is still Running at exit; the backstop is the coordinator's",
    );
    assert_eq!(
        node_draining(&views, world.node),
        Some(true),
        "the drain was still announced",
    );

    world.coord.shutdown().await;
    drop(world.agent_dir);
}

/// A drain that begins while the agent is reconnecting survives the gap and
/// lands on the next registration.
///
/// ADR 0041: the intent is agent-local state, not stream state. The agent has
/// nothing to announce on while the coordinator is down, so it keeps
/// reconnecting rather than slipping away silently, and its re-registration
/// carries `draining` — which `RegisterNode` writes at the same log position
/// that bumps the epoch.
#[tokio::test]
async fn shutdown_while_reconnecting_lands_on_re_registration() {
    init_tracing();
    let ca = Ca::new();
    // Both coordinators serve the agent gateway on this one port, so the
    // agent's reconnect loop finds the second one at the address it already
    // has (its discovery list is static, as a real agent's would be).
    let agent_port = free_port();
    let coord = RunningCoordinator::start_on_agent_port(ClusterId::new(), &ca, agent_port).await;
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
    );
    let agent = spawn_agent(config, executor.clone()).await;

    poll(DEADLINE, "node registered", || {
        let views = coord.views();
        async move { node_epoch(&views, node).is_some_and(|e| e >= 1) }
    })
    .await;

    // Sever the session by taking the whole coordinator away; the agent falls
    // into its reconnect loop against an address nothing answers on.
    coord.shutdown().await;

    // SIGTERM with nowhere to say it. An idle agent that exited here would
    // leave the cluster to discover its absence by timeout — the exact path
    // the drain exists to replace — so it must still be running when the
    // coordinator comes back.
    agent.signal_shutdown();
    holds_for(
        Duration::from_secs(2),
        "the agent keeps reconnecting until it can announce",
        || {
            let agent = &agent;
            async move { !agent.has_exited() }
        },
    )
    .await;

    // A coordinator at the same address again. The agent re-registers, and the
    // registration itself carries the drain.
    let coord2 = RunningCoordinator::start_on_agent_port(ClusterId::new(), &ca, agent_port).await;
    poll(DEADLINE, "second coordinator leadership", || {
        let coord2 = &coord2;
        async move { coord2.is_leader() }
    })
    .await;
    let views = coord2.views();
    poll(DEADLINE, "re-registered, announcing the drain", || {
        let views = views.clone();
        async move {
            node_epoch(&views, node).is_some_and(|e| e >= 1)
                && node_draining(&views, node) == Some(true)
        }
    })
    .await;

    // Announced at last, and with nothing running, the drain is done.
    agent
        .expect_clean_exit(DEADLINE, "the drain after re-registration")
        .await;

    coord2.shutdown().await;
    drop(agent_dir);
}

// ---- Issue #167 ----------------------------------------------------------

/// An abort applied while the agent is disconnected is re-delivered when the
/// agent reconnects: `StopJob` is routed once at `StopRequested`, the gateway
/// drops it for a node with no session, and the reconnect `ObservedSet` diff
/// must re-derive it from the job's `abort_requested` (no leader change).
#[tokio::test]
async fn abort_while_disconnected_is_stopped_on_reconnect() {
    init_tracing();
    let world = run_to_running().await;
    let views = world.coord.views();
    let (node, job, attempt, alloc) = (world.node, world.job, world.attempt, world.alloc);

    // Take the session down; the container stays alive in the executor.
    stop_agent(world.agent).await;
    assert!(world.executor.is_running(alloc));

    // Abort while nobody is listening. Nothing can have stopped the container.
    let applied = world
        .coord
        .consensus()
        .propose(Command::AbortJob(AbortJob {
            job,
            reason: Some("disconnected abort".into()),
            actor: None,
            requested_at: Timestamp::now(),
        }))
        .await
        .expect("propose AbortJob");
    assert!(
        applied.outcome.is_ok(),
        "AbortJob rejected: {:?}",
        applied.outcome
    );
    assert!(world.executor.is_running(alloc), "no agent to stop it yet");
    assert_eq!(job_state(&views, job), Some(JobState::Attempting(attempt)));

    let epoch_before = node_epoch(&views, node).expect("epoch");
    let executor2 = world.executor.fork();
    let config2 = agent_config(
        node,
        world.agent_dir.path().join("data"),
        &world.coord.agent_endpoint,
        &world.ca,
    );
    let agent2 = spawn_agent(config2, executor2.clone()).await;

    poll(DEADLINE, "re-registration", || {
        let views = views.clone();
        async move { node_epoch(&views, node).is_some_and(|e| e > epoch_before) }
    })
    .await;
    poll(DEADLINE, "job Aborted after reconnect", || {
        let views = views.clone();
        async move { job_state(&views, job) == Some(JobState::Aborted) }
    })
    .await;
    assert!(!executor2.is_running(alloc), "the container was stopped");
    assert_eq!(executor2.start_count(alloc), 1, "never restarted");

    stop_agent(agent2).await;
    world.coord.shutdown().await;
    drop(world.agent_dir);
}

// ---- Issue #166 ----------------------------------------------------------

/// A TCP forwarder between the agent and the coordinator's gateway. The mTLS
/// session runs end to end through it untouched; [`SeverableProxy::sever`]
/// drops every live connection while the listener keeps accepting, so the agent
/// reconnects (and re-registers) exactly as after a network break or a leader
/// change, without the coordinator or its state going anywhere.
struct SeverableProxy {
    port: u16,
    /// Bumped by `sever`; each forwarded connection watches it.
    generation: watch::Sender<u64>,
    accept: JoinHandle<()>,
}

impl SeverableProxy {
    async fn start(upstream: String) -> SeverableProxy {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind proxy");
        let port = listener.local_addr().expect("proxy addr").port();
        let (generation, rx) = watch::channel(0u64);
        let accept = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    return;
                };
                let upstream = upstream.clone();
                // A clone inherits the accept loop's stale view, so mark the
                // current generation seen: only a *later* sever may fire.
                let mut rx = rx.clone();
                rx.mark_unchanged();
                tokio::spawn(async move {
                    let Ok(mut outbound) = tokio::net::TcpStream::connect(&upstream).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _ = rx.changed() => {}
                    }
                });
            }
        });
        SeverableProxy {
            port,
            generation,
            accept,
        }
    }

    fn endpoint(&self) -> String {
        format!("localhost:{}", self.port)
    }

    fn sever(&self) {
        self.generation.send_modify(|g| *g += 1);
    }
}

impl Drop for SeverableProxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// Boot a coordinator and an agent dialing it through `endpoint_via`, submit a
/// job bounded by `max_runtime`, and drive it to `Running`.
async fn run_bounded_to_running(max_runtime: coppice_core::time::Duration) -> BoundedWorld {
    let ca = Ca::new();
    let coord = RunningCoordinator::start(ClusterId::new(), &ca).await;
    poll(DEADLINE, "coordinator leadership", || {
        let coord = &coord;
        async move { coord.is_leader() }
    })
    .await;
    let gateway = coord
        .agent_endpoint
        .rsplit(':')
        .next()
        .expect("gateway port")
        .to_string();
    let proxy = SeverableProxy::start(format!("127.0.0.1:{gateway}")).await;

    let node = NodeId::new();
    let agent_dir = tempfile::tempdir().expect("agent tempdir");
    let executor = FakeExecutor::new();
    let config = agent_config(node, agent_dir.path().join("data"), &proxy.endpoint(), &ca);
    let agent = spawn_agent(config, executor.clone()).await;
    let views = coord.views();
    poll(DEADLINE, "node registered", || {
        let views = views.clone();
        async move { node_epoch(&views, node).is_some_and(|e| e >= 1) }
    })
    .await;

    let entity = QuotaEntityId::new();
    seed_quota(&coord, entity).await;
    let job = JobId::new();
    let applied = coord
        .consensus()
        .propose(Command::SubmitJob(SubmitJob {
            job: Job {
                id: job,
                image: "registry/img:latest".into(),
                command: vec!["run".into()],
                entrypoint: None,
                env: Default::default(),
                requests: requested(),
                priority: PriorityClass(0),
                max_runtime: Some(max_runtime),
                quota_entity: entity,
                retry: RetryPolicy {
                    max_retries: 0,
                    retry_user_errors: false,
                },
                abort_requested: None,
                submitted_by: None,
                metadata: Default::default(),
                preemption: Default::default(),
            },
            multiplier: PriorityMultiplier::ONE,
            submitted_at: Timestamp::now(),
            actor: None,
        }))
        .await
        .expect("propose SubmitJob");
    assert!(applied.outcome.is_ok(), "SubmitJob rejected");
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
    assert!(executor.is_running(alloc));

    BoundedWorld {
        coord,
        ca,
        proxy,
        node,
        attempt,
        alloc,
        executor,
        agent,
        agent_dir,
    }
}

struct BoundedWorld {
    coord: RunningCoordinator,
    ca: Ca,
    proxy: SeverableProxy,
    node: NodeId,
    attempt: AttemptId,
    alloc: AllocationId,
    executor: FakeExecutor,
    agent: RunningAgent,
    agent_dir: tempfile::TempDir,
}

/// The attempt resolves `RuntimeLimitExceeded` once the container has run its
/// bound, and the container is actually stopped.
async fn expect_runtime_limit(
    coord: &RunningCoordinator,
    attempt: AttemptId,
    alloc: AllocationId,
    executor: &FakeExecutor,
    label: &str,
) {
    let views = coord.views();
    poll(DEADLINE, label, || {
        let views = views.clone();
        async move {
            attempt_state(&views, attempt)
                == Some(AttemptState::Terminal(AttemptOutcome::RuntimeLimitExceeded))
        }
    })
    .await;
    assert!(!executor.is_running(alloc), "the container was stopped");
}

/// The watchdog survives a session reconnect: re-registration re-arms it from
/// the journaled bound and the container's observed runtime.
///
/// The fake clock is advanced so 1s of the 3s budget remains at the break; with
/// the pre-fix agent the deadline died with the connection and the attempt
/// stayed `Running` forever.
#[tokio::test]
async fn max_runtime_watchdog_survives_a_session_reconnect() {
    init_tracing();
    let world = run_bounded_to_running(coppice_core::time::Duration::from_secs(3)).await;
    let views = world.coord.views();
    let epoch_before = node_epoch(&views, world.node).expect("epoch");

    world
        .executor
        .advance(coppice_core::time::Duration::from_secs(2));
    world.proxy.sever();
    poll(DEADLINE, "re-registration after the break", || {
        let views = views.clone();
        let node = world.node;
        async move { node_epoch(&views, node).is_some_and(|e| e > epoch_before) }
    })
    .await;

    expect_runtime_limit(
        &world.coord,
        world.attempt,
        world.alloc,
        &world.executor,
        "RuntimeLimitExceeded after reconnect",
    )
    .await;
    assert_eq!(world.executor.start_count(world.alloc), 1);

    stop_agent(world.agent).await;
    world.coord.shutdown().await;
    drop(world.agent_dir);
}

/// The watchdog survives an agent restart: recovery re-arms it from the
/// journaled `StartIntent.max_runtime_us` and the surviving container's runtime.
#[tokio::test]
async fn max_runtime_watchdog_survives_an_agent_restart() {
    init_tracing();
    let world = run_bounded_to_running(coppice_core::time::Duration::from_secs(3)).await;
    let views = world.coord.views();
    let epoch_before = node_epoch(&views, world.node).expect("epoch");

    stop_agent(world.agent).await;
    world
        .executor
        .advance(coppice_core::time::Duration::from_secs(2));

    let executor2 = world.executor.fork();
    let config2 = agent_config(
        world.node,
        world.agent_dir.path().join("data"),
        &world.proxy.endpoint(),
        &world.ca,
    );
    let agent2 = spawn_agent(config2, executor2.clone()).await;
    poll(DEADLINE, "re-registration after the restart", || {
        let views = views.clone();
        let node = world.node;
        async move { node_epoch(&views, node).is_some_and(|e| e > epoch_before) }
    })
    .await;

    expect_runtime_limit(
        &world.coord,
        world.attempt,
        world.alloc,
        &executor2,
        "RuntimeLimitExceeded after restart",
    )
    .await;
    assert_eq!(executor2.start_count(world.alloc), 1);

    stop_agent(agent2).await;
    world.coord.shutdown().await;
    drop(world.agent_dir);
}

// ---- spot interruption (ADR 0044 §5) --------------------------------------

/// A spot reclaim end to end, from instance metadata to the next attempt.
///
/// The local IMDSv2 stand-in's deadline leaves no notice window (`T_stop` is
/// already past), so signal, mark, stop and `Preempted { Interruption }` run
/// back to back, with the full notice recorded as shortfall. After `D` the
/// restarted agent retires the dead plan, ignores the stale notice still in
/// metadata, and takes the next attempt.
#[tokio::test]
async fn a_spot_reclaim_preempts_requeues_free_and_tells_the_next_attempt() {
    use coppice_agent::config::InterruptionSource;
    use coppice_agent::interruption::FakeImds;
    use coppice_core::attempt::PreemptReason;
    use coppice_core::preemption::NoticeSignal;

    init_tracing();
    let imds = FakeImds::start().await.expect("start the fake IMDS");
    let endpoint = imds.endpoint();
    let mut agent_config_used = None;
    let world = run_to_running_with(|config| {
        config.preemption.interruption_source = InterruptionSource::AwsImds;
        config.preemption.imds_endpoint = Some(endpoint.clone());
        config.preemption.imds_poll_interval = Duration::from_millis(50);
        agent_config_used = Some(config.clone());
    })
    .await;
    let config = agent_config_used.expect("the tweak ran");
    let views = world.coord.views();

    let deadline = Timestamp::now() + coppice_core::time::Duration::from_secs(2);
    imds.set_action("terminate", deadline);

    let preempted = AttemptState::Terminal(AttemptOutcome::Preempted {
        reason: PreemptReason::Interruption,
    });
    poll(DEADLINE, "attempt Preempted { Interruption }", || {
        let views = views.clone();
        let attempt = world.attempt;
        let preempted = preempted.clone();
        async move { attempt_state(&views, attempt) == Some(preempted) }
    })
    .await;

    // The notice protocol ran: one SIGUSR1 (the default notice signal), and
    // the coordinator acknowledged the mark before the outcome landed.
    assert_eq!(
        world.executor.signals_delivered(world.alloc),
        vec![NoticeSignal::Sigusr1]
    );
    let state = views.latest().state().clone();
    let record = state.attempts.get(&world.attempt).expect("attempt record");
    let preemption = record.preemption.as_ref().expect("a preemption record");
    assert_eq!(preemption.reason, PreemptReason::Interruption);
    let acknowledged = preemption
        .acknowledged
        .as_ref()
        .expect("the reported mark acknowledged it");
    assert!(
        acknowledged.notice_shortfall.is_positive(),
        "no window at all: the whole notice is short"
    );
    let node = state.nodes.get(&world.node).expect("node record");
    assert!(node.draining);
    assert_eq!(
        node.interruption_deadline
            .map(|d| d.as_micros() / 1_000_000),
        Some(deadline.as_micros() / 1_000_000)
    );

    // Free requeue: Queued again, no retry consumed, nothing placed on the
    // interrupted node.
    poll(DEADLINE, "job requeued", || {
        let views = views.clone();
        let job = world.job;
        async move { job_state(&views, job) == Some(JobState::Queued) }
    })
    .await;
    assert_eq!(
        views
            .latest()
            .state()
            .jobs
            .get(&world.job)
            .unwrap()
            .retries_used,
        0
    );

    // The host comes back after D (an EC2 `stop` reclaim): the restarted
    // agent finds a plan whose deadline has passed, retires it, and
    // re-registers undrained, while metadata still shows the old notice.
    stop_agent(world.agent).await;
    while Timestamp::now() <= deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let agent = spawn_agent(config, world.executor.fork()).await;

    poll(DEADLINE, "next attempt Running", || {
        let views = views.clone();
        let job = world.job;
        let first = world.attempt;
        async move {
            current_attempt_id(&views, job)
                .filter(|a| *a != first)
                .and_then(|a| attempt_state(&views, a))
                == Some(AttemptState::Running)
        }
    })
    .await;
    let next = current_attempt_id(&views, world.job).expect("next attempt");
    let next_alloc = attempt_alloc(&views, next).expect("next allocation");
    let env = world
        .executor
        .started_env(next_alloc)
        .expect("the next attempt started here");
    assert_eq!(
        env.get("COPPICE_PREVIOUS_ATTEMPT_OUTCOME")
            .map(String::as_str),
        Some("preempted")
    );
    assert_eq!(
        env.get("COPPICE_PREVIOUS_ATTEMPT_ID"),
        Some(&world.attempt.to_string())
    );
    let state = views.latest().state().clone();
    let node = state.nodes.get(&world.node).expect("node record");
    assert!(!node.draining, "the retired plan no longer drains the node");
    assert_eq!(node.interruption_deadline, None);

    stop_agent(agent).await;
    world.coord.shutdown().await;
    drop(world.agent_dir);
}

// ---- Scheduler priority preemption (ADR 0044 §4) -------------------------

/// Propose a submission of `job` on node-filling `requests`, with an explicit
/// class, multiplier and `[preemption]` table.
async fn submit_classed(
    coord: &RunningCoordinator,
    job: JobId,
    entity: QuotaEntityId,
    requests: Resources,
    priority: i32,
    multiplier: PriorityMultiplier,
    preemption: coppice_core::preemption::PreemptionSpec,
) {
    let applied = coord
        .consensus()
        .propose(Command::SubmitJob(SubmitJob {
            job: Job {
                id: job,
                image: "registry/img:latest".into(),
                command: vec!["run".into()],
                entrypoint: None,
                env: Default::default(),
                requests,
                priority: PriorityClass(priority),
                max_runtime: None,
                quota_entity: entity,
                retry: RetryPolicy::default(),
                abort_requested: None,
                submitted_by: None,
                metadata: Default::default(),
                preemption,
            },
            multiplier,
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
}

/// A scheduler preemption end to end: a low-class preemptible job fills the
/// node, a higher-class job arrives with no room, and the pass evicts the
/// first for the second. The victim is signalled, its mark is reported and
/// acknowledged — which is what gives the waiting beneficiary a finite
/// `projected_ready` — and at the end of its notice it is stopped,
/// `Preempted { Priority }`, and requeued free while the beneficiary runs.
#[tokio::test]
async fn a_higher_class_job_preempts_running_work_and_runs_once_it_is_gone() {
    use coppice_core::attempt::PreemptReason;
    use coppice_core::preemption::{NoticeSignal, PreemptionSpec};

    init_tracing();
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
    );
    let agent = spawn_agent(config, executor.clone()).await;
    let views = coord.views();
    poll(DEADLINE, "node registered", || {
        let views = views.clone();
        async move { node_epoch(&views, node).is_some_and(|e| e >= 1) }
    })
    .await;
    let capacity = views.latest().state().nodes[&node].node.capacity;

    // A short notice and grace, so the test waits seconds, not minutes.
    let notice = coppice_core::time::Duration::from_secs(5);
    let mut policy = views.latest().state().policy.clone();
    policy.preemption_notice_floor = coppice_core::time::Duration::from_secs(1);
    policy.abort_grace = coppice_core::time::Duration::from_secs(1);
    let applied = coord
        .consensus()
        .propose(Command::UpdatePolicy(
            coppice_state::command::UpdatePolicy {
                policy,
                updated_at: Timestamp::now(),
                actor: None,
            },
        ))
        .await
        .expect("propose UpdatePolicy");
    assert!(applied.outcome.is_ok(), "{:?}", applied.outcome);

    let entity = QuotaEntityId::new();
    seed_quota(&coord, entity).await;
    let victim = JobId::new();
    submit_classed(
        &coord,
        victim,
        entity,
        capacity,
        0,
        PriorityMultiplier::ONE,
        PreemptionSpec {
            preemptible: true,
            notice: Some(notice),
            notice_signal: NoticeSignal::Sigusr1,
        },
    )
    .await;
    poll(DEADLINE, "victim Running", || {
        let views = views.clone();
        async move {
            current_attempt_id(&views, victim).and_then(|a| attempt_state(&views, a))
                == Some(AttemptState::Running)
        }
    })
    .await;
    let victim_attempt = current_attempt_id(&views, victim).expect("victim attempt");
    let victim_alloc = attempt_alloc(&views, victim_attempt).expect("victim allocation");

    // The beneficiary: higher class, and a higher multiplier so its score
    // beats the victim's head start in age (the score rule).
    let beneficiary = JobId::new();
    submit_classed(
        &coord,
        beneficiary,
        entity,
        capacity,
        5,
        PriorityMultiplier(2 << 32),
        PreemptionSpec::default(),
    )
    .await;

    // Chosen, signalled, and the mark acknowledged: the release event that
    // mark manufactures gives the beneficiary's accrual a finite bound.
    let reason = PreemptReason::Priority {
        for_job: beneficiary,
    };
    poll(DEADLINE, "victim notified and acknowledged", || {
        let views = views.clone();
        async move {
            views
                .latest()
                .state()
                .attempts
                .get(&victim_attempt)
                .and_then(|a| a.preemption)
                .is_some_and(|p| p.reason == reason && p.acknowledged.is_some())
        }
    })
    .await;
    // The notice window is seconds long, so the beneficiary is still
    // waiting in its accrual here.
    let state = views.latest().state().clone();
    let Some(JobState::Attempting(attempt)) = state.jobs.get(&beneficiary).map(|j| j.state) else {
        panic!("the beneficiary is committed accruing alongside the eviction");
    };
    let alloc = state.attempts[&attempt].attempt.allocation;
    assert!(
        state.accrual_queue.values().any(|a| *a == alloc),
        "still accruing while its victim's notice runs"
    );
    let deadline = state.attempts[&victim_attempt]
        .preemption
        .and_then(|p| p.acknowledged)
        .expect("acknowledged")
        .deadline;
    assert_eq!(
        coppice_scheduler::projected_starts(&state)
            .get(&alloc)
            .copied()
            .flatten(),
        Some(deadline + state.policy.abort_grace),
        "the acknowledged mark is the beneficiary's release event"
    );
    assert_eq!(
        executor.signals_delivered(victim_alloc),
        vec![NoticeSignal::Sigusr1]
    );

    // At the end of its notice the victim is stopped and resolves as a
    // priority preemption, requeued without spending a retry.
    let preempted = AttemptState::Terminal(AttemptOutcome::Preempted { reason });
    poll(DEADLINE, "victim Preempted { Priority }", || {
        let views = views.clone();
        let preempted = preempted.clone();
        async move { attempt_state(&views, victim_attempt) == Some(preempted) }
    })
    .await;
    let victim_record = views.latest().state().jobs[&victim].clone();
    assert_eq!(victim_record.retries_used, 0);
    // Requeued: queued again, or already waiting in a fresh accrual of its
    // own behind the beneficiary.
    assert!(
        match victim_record.state {
            JobState::Queued => true,
            JobState::Attempting(next) => next != victim_attempt,
            _ => false,
        },
        "the victim requeues: {:?}",
        victim_record.state
    );

    // The released capacity funds the beneficiary, which runs.
    poll(DEADLINE, "beneficiary Running", || {
        let views = views.clone();
        async move {
            current_attempt_id(&views, beneficiary).and_then(|a| attempt_state(&views, a))
                == Some(AttemptState::Running)
        }
    })
    .await;

    stop_agent(agent).await;
    coord.shutdown().await;
    drop(agent_dir);
}

// ---- operator deadline drain (ADR 0044 §5) ---------------------------------

/// Short policy values so a deadline drain plays out in seconds: a 2 s notice
/// (floor = cap, so the job declares exactly the common window), 1 s grace
/// and 1 s cleanup margin — a deadline needs 4 s of lead, `T_stop = D − 2 s`
/// and `T_notice = D − 4 s`.
fn short_drain_policy(policy: &mut coppice_state::PolicyConfig) {
    policy.preemption_notice_floor = coppice_core::time::Duration::from_secs(2);
    policy.preemption_notice_cap = coppice_core::time::Duration::from_secs(2);
    policy.abort_grace = coppice_core::time::Duration::from_secs(1);
    policy.drain_cleanup_margin = coppice_core::time::Duration::from_secs(1);
}

/// Propose the operator's drain of `node` — with `deadline`, or none for an
/// undrain when `drain` is false — stamped now, as the HTTP handler would.
async fn set_drain(
    coord: &RunningCoordinator,
    node: NodeId,
    drain: bool,
    deadline: Option<Timestamp>,
) {
    let now = Timestamp::now();
    let applied = coord
        .consensus()
        .propose(Command::SetNodeDrain(
            coppice_state::command::SetNodeDrain {
                node,
                drain: drain.then_some(coppice_core::node::Drain {
                    requested_at: now,
                    deadline,
                }),
                updated_at: now,
                actor: None,
            },
        ))
        .await
        .expect("propose SetNodeDrain");
    assert!(
        applied.outcome.is_ok(),
        "SetNodeDrain rejected: {:?}",
        applied.outcome
    );
}

fn preemption_of(
    views: &StateViews,
    attempt: AttemptId,
) -> Option<coppice_core::attempt::Preemption> {
    views
        .latest()
        .state()
        .attempts
        .get(&attempt)
        .and_then(|a| a.preemption)
}

async fn wait_preempted_by_drain(views: &StateViews, attempt: AttemptId) {
    let preempted = AttemptState::Terminal(AttemptOutcome::Preempted {
        reason: coppice_core::attempt::PreemptReason::Drain,
    });
    poll(DEADLINE, "attempt Preempted { Drain }", || {
        let views = views.clone();
        let preempted = preempted.clone();
        async move { attempt_state(&views, attempt) == Some(preempted) }
    })
    .await;
}

async fn wait_requeued_free(views: &StateViews, job: JobId) {
    poll(DEADLINE, "job requeued", || {
        let views = views.clone();
        async move { job_state(&views, job) == Some(JobState::Queued) }
    })
    .await;
    assert_eq!(
        views.latest().state().jobs.get(&job).unwrap().retries_used,
        0,
        "a drain preemption is a free retry"
    );
}

/// A deadline drain with a non-preemptible running job: the leader delivers
/// the deadline on the next heartbeat, the agent notifies at `T_notice` with
/// the full window, stops at `T_stop`, and the attempt resolves
/// `Preempted { Drain }` with a free retry. The job stays queued: the node
/// admits only bounded backfill, and it has no `max_runtime`.
#[tokio::test]
async fn a_deadline_drain_notifies_at_its_notice_time_and_requeues_free() {
    use coppice_core::preemption::NoticeSignal;

    init_tracing();
    let world = run_to_running_under(Some(short_drain_policy), |_| {}).await;
    let views = world.coord.views();

    let deadline = Timestamp::now() + coppice_core::time::Duration::from_secs(6);
    let t_stop = deadline - coppice_core::time::Duration::from_secs(2);
    let t_notice = t_stop - coppice_core::time::Duration::from_secs(2);
    set_drain(&world.coord, world.node, true, Some(deadline)).await;

    // Nothing is said before T_notice.
    let now = Timestamp::now();
    if now < t_notice - coppice_core::time::Duration::from_millis(500) {
        assert!(world.executor.signals_delivered(world.alloc).is_empty());
    }

    wait_preempted_by_drain(&views, world.attempt).await;
    assert!(
        Timestamp::now() >= t_stop,
        "the stop ran no earlier than T_stop"
    );
    assert_eq!(
        world.executor.signals_delivered(world.alloc),
        vec![NoticeSignal::Sigusr1]
    );
    let preemption = preemption_of(&views, world.attempt).expect("a preemption record");
    assert_eq!(
        preemption.reason,
        coppice_core::attempt::PreemptReason::Drain
    );
    let acknowledged = preemption.acknowledged.expect("the mark was acknowledged");
    // Notified at T_notice, so the window is the declared notice less only
    // the timer's wake-up latency.
    assert!(
        acknowledged.notice_shortfall < coppice_core::time::Duration::from_millis(500),
        "notified on time, with (nearly) the full declared window: {}",
        acknowledged.notice_shortfall
    );
    wait_requeued_free(&views, world.job).await;

    // The node record is the operator's; the agent never announced draining.
    let state = views.latest().state().clone();
    let node = state.nodes.get(&world.node).expect("node record");
    assert_eq!(node.node.drain.and_then(|d| d.deadline), Some(deadline));
    assert!(!node.draining);
    // Nothing more is placed there: the job has no bound to backfill with.
    holds_for(
        Duration::from_millis(800),
        "the requeued job stays queued on the deadline-drained node",
        || {
            let views = views.clone();
            let job = world.job;
            async move { job_state(&views, job) == Some(JobState::Queued) }
        },
    )
    .await;

    stop_agent(world.agent).await;
    world.coord.shutdown().await;
    drop(world.agent_dir);
}

/// A drain that reaches the agent after `T_notice` (here: the agent was down
/// when it was recorded, and reconnects late) notifies at once, still stops
/// at `T_stop`, and reports the notice it could not give as shortfall.
#[tokio::test]
async fn a_deadline_drain_reaching_the_agent_late_reports_a_shortfall() {
    init_tracing();
    let mut agent_config_used = None;
    let world = run_to_running_under(Some(short_drain_policy), |config| {
        agent_config_used = Some(config.clone());
    })
    .await;
    let config = agent_config_used.expect("the tweak ran");
    let views = world.coord.views();

    stop_agent(world.agent).await;
    let deadline = Timestamp::now() + coppice_core::time::Duration::from_secs(6);
    let t_notice = deadline - coppice_core::time::Duration::from_secs(4);
    set_drain(&world.coord, world.node, true, Some(deadline)).await;
    // Past T_notice by a clear second before the agent hears of it.
    while Timestamp::now() <= t_notice + coppice_core::time::Duration::from_secs(1) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let agent = spawn_agent(config, world.executor.fork()).await;

    wait_preempted_by_drain(&views, world.attempt).await;
    let acknowledged = preemption_of(&views, world.attempt)
        .and_then(|p| p.acknowledged)
        .expect("the late notice was acknowledged");
    assert!(
        acknowledged.notice_shortfall >= coppice_core::time::Duration::from_secs(1),
        "at least the second it arrived late is short, got {}",
        acknowledged.notice_shortfall
    );
    wait_requeued_free(&views, world.job).await;

    stop_agent(agent).await;
    world.coord.shutdown().await;
    drop(world.agent_dir);
}

/// Undraining mid-notice withdraws the plan but not the notice: the notified
/// attempt still stops at its `T_stop` and resolves `Preempted { Drain }`, and
/// the reopened node takes the requeued job again.
#[tokio::test]
async fn an_undrain_mid_notice_leaves_the_notified_attempt_stopping() {
    init_tracing();
    let world = run_to_running_under(Some(short_drain_policy), |_| {}).await;
    let views = world.coord.views();

    let deadline = Timestamp::now() + coppice_core::time::Duration::from_secs(6);
    let t_stop = deadline - coppice_core::time::Duration::from_secs(2);
    set_drain(&world.coord, world.node, true, Some(deadline)).await;
    poll(DEADLINE, "notice acknowledged", || {
        let views = views.clone();
        let attempt = world.attempt;
        async move { preemption_of(&views, attempt).is_some_and(|p| p.acknowledged.is_some()) }
    })
    .await;
    assert!(
        Timestamp::now() < t_stop,
        "the undrain must land inside the notice window for this test to mean anything"
    );
    set_drain(&world.coord, world.node, false, None).await;

    wait_preempted_by_drain(&views, world.attempt).await;
    assert!(Timestamp::now() >= t_stop, "the issued stop held");
    wait_requeued_free(&views, world.job).await;

    // The node is open again: the job runs a fresh attempt there.
    poll(DEADLINE, "next attempt Running", || {
        let views = views.clone();
        let job = world.job;
        let first = world.attempt;
        async move {
            current_attempt_id(&views, job)
                .filter(|a| *a != first)
                .and_then(|a| attempt_state(&views, a))
                == Some(AttemptState::Running)
        }
    })
    .await;

    stop_agent(world.agent).await;
    world.coord.shutdown().await;
    drop(world.agent_dir);
}
