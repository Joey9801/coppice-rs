//! The agent session: fencing, command handling, reporting, and the live
//! bidirectional stream to the coordinator (ADR 0009,
//! `docs/protocols/agent-coordinator.md`).
//!
//! The [`Session`] core holds every correctness-bearing decision — fencing
//! acceptance, sequence dedup, StartJob idempotency and the tombstone rule,
//! truth-wins classification — as plain `async` methods over the journal and
//! the executor, with no transport in sight. That is what makes fencing,
//! dedup, and idempotency unit-testable without a live server; [`run`] wraps
//! the core in the tonic stream and the reconnect/backoff loop.
//!
//! # Reporting model (ADR 0013, command catalog)
//!
//! The agent has no finalization phase in v1: it reports `Running` when a
//! container is observed started and `Terminal{outcome}` directly when it
//! ends. Skipping `Finalizing` is explicitly legal — `RecordAttemptExited` is
//! a skippable command. Delivery is at-least-once by construction: the stream
//! is reliable, and every reconnect re-registers and re-sends the full
//! ObservedSet, so an outcome observed while the coordinator was unreachable
//! survives to the next resync (it is durable in the journal until then).

use coppice_consensus::fs::Fs;
use coppice_core::attempt::{AttemptOutcome, AttemptState};
use coppice_core::id::{AllocationId, AttemptId, JobId, NodeId};
use coppice_core::resource::Resources;
use coppice_core::time::{Duration, Timestamp};
use coppice_proto::pb::agent::v1 as pb;
use coppice_proto::pb::core::v1 as pbcore;

use crate::executor::{classify_exit, Executor, ExitCause, ExitInfo, StartSpec, StopOutcome};
use crate::journal::{ExitRec, IntentRec, Journal, JournalState, Watermark};
use crate::observed::{build_observed_set, ObservedAllocation};

/// The outcome of the fencing check for one inbound command (ADR 0009).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admit {
    /// Stale term or epoch: silently dropped (logged), no nack on the wire.
    Reject,
    /// An already-processed `command_seq`: acknowledged without re-acting.
    Duplicate,
    /// A fresh command to process.
    Fresh,
}

/// A runtime watchdog armed by a successful start: the allocation to kill and
/// the runtime bound it may not exceed.
///
/// `max_runtime` is the enforced bound copied from `StartJob.max_runtime_us`
/// (ADR 0014), not a wall-clock deadline — the live loop converts it to a timer
/// when it drains these via [`Session::take_armed_watchdogs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArmedWatchdog {
    /// The allocation whose container the timer will stop.
    pub allocation: AllocationId,
    /// The maximum runtime before the watchdog fires.
    pub max_runtime: Duration,
}

/// The transport-free session core. Generic over the filesystem seam (for the
/// journal) and the executor.
pub struct Session<F: Fs, E: Executor> {
    node: NodeId,
    capacity: Resources,
    labels: Vec<pbcore::Label>,
    journal: Journal<F>,
    state: JournalState,
    executor: E,
    /// The node epoch we are currently operating under; echoed on every
    /// report. Recovered from the journal watermark, raised by each accepted
    /// fencing update.
    epoch: u64,
    /// Highest processed `command_seq` this session; `None` until the first
    /// command or after an epoch raise (which retires the sequence space).
    last_seq: Option<u64>,
    /// Whether registration completed and the ObservedSet was sent — the gate
    /// before any non-`RegisterAccepted` command is honored (fail closed).
    registered: bool,
    /// Advisory drain flag (ADR 0013): placement enforcement lives in apply,
    /// so a StartJob that arrives drained is still executed (committed intent
    /// predating the drain) — this only records the request.
    drained: bool,
    /// Watchdogs armed by successful starts with a `max_runtime`, drained
    /// by the live loop which owns the timers.
    armed_watchdogs: Vec<ArmedWatchdog>,
    /// The advertised `NodeService` address (ADR 0034), echoed in every
    /// `Register` so coordinators can dial this node for job logs. `None` when
    /// no `[listen]` listener is configured — the node hosts no service.
    service_addr: Option<String>,
}

impl<F: Fs, E: Executor> Session<F, E> {
    /// Build a session core over a freshly recovered journal.
    pub fn new(
        node: NodeId,
        capacity: Resources,
        labels: Vec<pbcore::Label>,
        journal: Journal<F>,
        state: JournalState,
        executor: E,
    ) -> Session<F, E> {
        let epoch = state.watermark.node_epoch;
        Session {
            node,
            capacity,
            labels,
            journal,
            state,
            executor,
            epoch,
            last_seq: None,
            registered: false,
            drained: false,
            armed_watchdogs: Vec::new(),
            service_addr: None,
        }
    }

    /// Set the advertised `NodeService` address (ADR 0034) echoed in every
    /// `Register`. A builder setter so the many call sites that host no service
    /// need not thread a `None` through [`Session::new`].
    pub fn with_service_addr(mut self, service_addr: Option<String>) -> Session<F, E> {
        self.service_addr = service_addr;
        self
    }

    // ---- test / loop accessors ----

    pub fn state(&self) -> &JournalState {
        &self.state
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn is_registered(&self) -> bool {
        self.registered
    }
    pub fn is_drained(&self) -> bool {
        self.drained
    }
    pub fn executor(&self) -> &E {
        &self.executor
    }

    /// Clear session-scoped state on reconnect: registration must be redone
    /// and the sequence space is re-established under the fresh epoch. The
    /// durable watermark and epoch persist across the gap.
    pub fn reset_session(&mut self) {
        self.registered = false;
        self.last_seq = None;
    }

    /// Drain the watchdogs armed since the last call — the [`ArmedWatchdog`]s
    /// the loop must set timers for.
    pub fn take_armed_watchdogs(&mut self) -> Vec<ArmedWatchdog> {
        std::mem::take(&mut self.armed_watchdogs)
    }

    // ---- fencing (ADR 0009) ----

    /// Run one command header through the fencing check, journaling a raised
    /// watermark (fsynced) *before* the command is acted on.
    fn admit(&mut self, header: &pb::CommandHeader) -> std::io::Result<Admit> {
        let token = header.token.unwrap_or_default();
        let wm = self.state.watermark;
        if token.leader_term < wm.leader_term || token.node_epoch < wm.node_epoch {
            tracing::warn!(
                node = %self.node,
                token_term = token.leader_term,
                token_epoch = token.node_epoch,
                wm_term = wm.leader_term,
                wm_epoch = wm.node_epoch,
                "rejecting stale command (deposed leader or superseded epoch)"
            );
            return Ok(Admit::Reject);
        }
        if token.leader_term > wm.leader_term || token.node_epoch > wm.node_epoch {
            let raised = Watermark {
                leader_term: token.leader_term.max(wm.leader_term),
                node_epoch: token.node_epoch.max(wm.node_epoch),
            };
            // Durable BEFORE acting: a restarted agent never regresses to
            // obeying a deposed leader.
            self.journal.journal_fencing(raised)?;
            self.state.watermark = raised;
            self.epoch = raised.node_epoch;
            self.last_seq = None;
        }
        if let Some(last) = self.last_seq {
            if header.command_seq <= last {
                return Ok(Admit::Duplicate);
            }
        }
        self.last_seq = Some(header.command_seq);
        Ok(Admit::Fresh)
    }

    // ---- command dispatch ----

    /// Handle one inbound command, returning the reports to send. Fencing,
    /// dedup, and idempotency are all applied here.
    pub async fn handle_command(
        &mut self,
        cmd: pb::AgentCommand,
    ) -> std::io::Result<Vec<pb::AgentReport>> {
        let Some(header) = cmd.header else {
            tracing::warn!(node = %self.node, "dropping command without a header");
            return Ok(Vec::new());
        };
        let Some(body) = cmd.body else {
            tracing::warn!(node = %self.node, "dropping command without a body");
            return Ok(Vec::new());
        };

        // RegisterAccepted establishes registration; it must pass fencing (it
        // raises the epoch) but is not gated on `registered`.
        if let pb::agent_command::Body::RegisterAccepted(_) = &body {
            return match self.admit(&header)? {
                Admit::Reject => Ok(Vec::new()),
                Admit::Duplicate | Admit::Fresh => self.on_register_accepted().await,
            };
        }

        // Every other command requires an established registration. An inbound
        // non-RegisterAccepted command before the ObservedSet was sent can't
        // happen under the coordinator's seq ordering, but we fail closed.
        if !self.registered {
            tracing::warn!(node = %self.node, "dropping command received before ObservedSet was sent");
            return Ok(Vec::new());
        }

        match self.admit(&header)? {
            Admit::Reject => Ok(Vec::new()),
            Admit::Duplicate => self.handle_duplicate(body),
            Admit::Fresh => self.handle_fresh(body).await,
        }
    }

    /// Registration applied: build and send the full ObservedSet from journal
    /// + runtime *before* accepting any new work (ADR 0009 restart step 3).
    async fn on_register_accepted(&mut self) -> std::io::Result<Vec<pb::AgentReport>> {
        self.registered = true;
        let runtime = match self.executor.observe().await {
            Ok(runtime) => runtime,
            Err(e) => {
                tracing::warn!(node = %self.node, error = %e, "executor.observe failed; ObservedSet reports journal only");
                Vec::new()
            }
        };
        let observed = build_observed_set(&self.state, &runtime);
        let allocations = observed.iter().map(observed_to_pb).collect();

        // Recovery reap (§5): an exited container whose exit is already
        // journaled is spent evidence — remove it. Exited containers with no
        // journaled exit are left intact; their evidence is still needed.
        use crate::executor::ContainerState;
        for c in &runtime {
            if matches!(c.state, ContainerState::Exited(_))
                && self.state.exits.contains_key(&c.allocation)
            {
                self.reap_journaled(c.allocation).await;
            }
        }

        Ok(vec![self.report(pb::agent_report::Body::ObservedSet(
            pb::ObservedSet { allocations },
        ))])
    }

    /// A duplicate (already-seen seq): acknowledge without acting. A duplicate
    /// StartJob re-reports the attempt's current status instead of
    /// re-executing (ADR 0009); other commands are simply idempotent no-ops.
    fn handle_duplicate(
        &mut self,
        body: pb::agent_command::Body,
    ) -> std::io::Result<Vec<pb::AgentReport>> {
        if let pb::agent_command::Body::StartJob(sj) = body {
            if let Some(StartIds {
                allocation: alloc,
                attempt,
                job,
            }) = start_ids(&sj)
            {
                return Ok(self.report_current_status(alloc, attempt, job));
            }
        }
        Ok(Vec::new())
    }

    async fn handle_fresh(
        &mut self,
        body: pb::agent_command::Body,
    ) -> std::io::Result<Vec<pb::AgentReport>> {
        use pb::agent_command::Body;
        match body {
            Body::StartJob(sj) => self.start_job(sj).await,
            Body::StopJob(sp) => self.stop_job(sp).await,
            Body::Drain(_) => {
                self.drained = true;
                tracing::info!(node = %self.node, "drain requested (advisory; enforcement is in apply)");
                Ok(Vec::new())
            }
            Body::PrepareCache(pc) => {
                // Forwarded to the cache manager as an advisory warm pull (ADR
                // 0010, §7): still freely ignorable — dropped under high disk
                // pressure, and the fire-and-forget fetch may just fail.
                tracing::debug!(node = %self.node, image = %pc.image, "forwarding prepare-cache hint to the executor (ADR 0010)");
                self.executor.prepare_cache(pc.image);
                Ok(Vec::new())
            }
            Body::EvictImageHint(e) => {
                // Forwarded as an advisory evict-if-unpinned (ADR 0010, §7):
                // ignored when the digest is pinned or unknown.
                tracing::debug!(node = %self.node, digest = %e.image_digest, "forwarding evict-image hint to the executor (ADR 0010)");
                self.executor.evict_image(e.image_digest);
                Ok(Vec::new())
            }
            // Handled before fencing dispatch; unreachable here.
            Body::RegisterAccepted(_) => Ok(Vec::new()),
        }
    }

    // ---- StartJob ----

    async fn start_job(&mut self, sj: pb::StartJob) -> std::io::Result<Vec<pb::AgentReport>> {
        let Some(StartIds {
            allocation: alloc,
            attempt,
            job,
        }) = start_ids(&sj)
        else {
            tracing::warn!(node = %self.node, "dropping malformed StartJob (missing ids)");
            return Ok(Vec::new());
        };

        // Tombstone → refuse and report, unless a journaled exit records the
        // honest outcome (truth wins over the tombstone-abort, ADR 0013).
        if self.state.tombstones.contains(&alloc) {
            if let Some(exit) = self.state.exits.get(&alloc) {
                return Ok(vec![self.terminal_status(
                    alloc,
                    exit.attempt,
                    exit.job,
                    exit.outcome.clone(),
                    exit.runtime,
                )]);
            }
            return Ok(vec![self.terminal_status(
                alloc,
                attempt,
                job,
                AttemptOutcome::Aborted,
                Duration::ZERO,
            )]);
        }

        // Already journaled → idempotent: re-report current status, never
        // re-execute (ADR 0009).
        if self.state.intents.contains_key(&alloc) {
            return Ok(self.report_current_status(alloc, attempt, job));
        }

        if self.drained {
            tracing::info!(node = %self.node, %alloc, "starting committed StartJob while drained (intent predates the drain)");
        }

        // Journal the intent and fsync BEFORE starting the container: a
        // running container always has durable intent behind it (ADR 0009).
        let intent = IntentRec {
            allocation: alloc,
            attempt,
            job,
            node_epoch: self.epoch,
        };
        self.journal.journal_intent(&intent)?;
        self.state.intents.insert(alloc, intent);

        let max_runtime = sj.max_runtime_us.map(runtime_bound);
        let spec = StartSpec {
            allocation: alloc,
            attempt,
            job,
            image: sj.image,
            command: sj.command,
            entrypoint: sj.entrypoint.map(|e| e.argv),
            limits: sj
                .limits
                .and_then(|r| r.try_into().ok())
                .unwrap_or(Resources::ZERO),
            max_runtime,
        };
        match self.executor.start(spec).await {
            Ok(()) => {
                if let Some(max) = max_runtime {
                    self.armed_watchdogs.push(ArmedWatchdog {
                        allocation: alloc,
                        max_runtime: max,
                    });
                }
                Ok(vec![self.running_status(alloc, attempt, job)])
            }
            Err(e) => {
                let outcome = e.outcome();
                tracing::warn!(node = %self.node, %alloc, error = %e, "container start failed");
                self.record_exit(alloc, attempt, job, outcome.clone(), Duration::ZERO)?;
                Ok(vec![self.terminal_status(
                    alloc,
                    attempt,
                    job,
                    outcome,
                    Duration::ZERO,
                )])
            }
        }
    }

    // ---- StopJob ----

    async fn stop_job(&mut self, sp: pb::StopJob) -> std::io::Result<Vec<pb::AgentReport>> {
        let Some(alloc) = sp.allocation.and_then(|a| a.try_into().ok()) else {
            tracing::warn!(node = %self.node, "dropping malformed StopJob (missing allocation)");
            return Ok(Vec::new());
        };
        let grace = Duration::from_micros(sp.grace_us).max(Duration::ZERO);

        // Tombstone first, fsynced, valid even for an unknown allocation: a
        // racing or re-delivered StartJob is refused even across a restart
        // (ADR 0013).
        self.journal.journal_tombstone(alloc)?;
        self.state.tombstones.insert(alloc);

        // Already ended → report the honest outcome (truth wins).
        if let Some(exit) = self.state.exits.get(&alloc) {
            return Ok(vec![self.terminal_status(
                alloc,
                exit.attempt,
                exit.job,
                exit.outcome.clone(),
                exit.runtime,
            )]);
        }

        // We believe it is running (intent, no exit) → stop and classify.
        if let Some(intent) = self.state.intents.get(&alloc).copied() {
            return self
                .stop_and_classify(
                    alloc,
                    intent.attempt,
                    intent.job,
                    grace,
                    AttemptOutcome::Aborted,
                )
                .await;
        }

        tracing::info!(node = %self.node, %alloc, "stop for unknown allocation; tombstone journaled, nothing running");
        Ok(Vec::new())
    }

    /// Stop a container and classify per truth-wins-the-race (ADR 0013):
    /// `kill_outcome` (Aborted or RuntimeLimitExceeded) applies only if *our*
    /// stop terminated it; if it had already exited, the natural outcome wins.
    async fn stop_and_classify(
        &mut self,
        alloc: AllocationId,
        attempt: AttemptId,
        job: JobId,
        grace: Duration,
        kill_outcome: AttemptOutcome,
    ) -> std::io::Result<Vec<pb::AgentReport>> {
        match self.executor.stop(alloc, grace).await {
            Ok(StopOutcome::Stopped(exit)) => {
                // Carve-out on the stopped path (docker-executor.md §4): if the
                // evidence shows a limit kill (kernel OOM, disk enforcer) landed
                // as our stop took effect, that kill wins over `kill_outcome` —
                // the terminal state must never claim our stop terminated a
                // container that policy had already killed.
                let outcome = match exit.cause {
                    ExitCause::Natural => kill_outcome,
                    ExitCause::OomKilled | ExitCause::DiskKilled => classify_exit(&exit),
                };
                self.record_exit(alloc, attempt, job, outcome.clone(), exit.runtime)?;
                self.reap_journaled(alloc).await;
                Ok(vec![self.terminal_status(
                    alloc,
                    attempt,
                    job,
                    outcome,
                    exit.runtime,
                )])
            }
            Ok(StopOutcome::AlreadyExited(exit)) => {
                let outcome = classify_exit(&exit);
                self.record_exit(alloc, attempt, job, outcome.clone(), exit.runtime)?;
                self.reap_journaled(alloc).await;
                Ok(vec![self.terminal_status(
                    alloc,
                    attempt,
                    job,
                    outcome,
                    exit.runtime,
                )])
            }
            Ok(StopOutcome::Unknown) => {
                tracing::warn!(node = %self.node, %alloc, "stop: executor has no record of the container");
                Ok(Vec::new())
            }
            Err(e) => {
                tracing::warn!(node = %self.node, %alloc, error = %e, "stop failed");
                Ok(Vec::new())
            }
        }
    }

    // ---- watchdog and natural exits (driven by the live loop) ----

    /// The max-runtime watchdog fired for `alloc`: stop it and classify
    /// `RuntimeLimitExceeded` (kill-reason tracking distinguishes it from an
    /// abort). A no-op if the container already exited.
    pub async fn trigger_max_runtime(
        &mut self,
        alloc: AllocationId,
    ) -> std::io::Result<Vec<pb::AgentReport>> {
        if self.state.exits.contains_key(&alloc) {
            return Ok(Vec::new());
        }
        let Some(intent) = self.state.intents.get(&alloc).copied() else {
            return Ok(Vec::new());
        };
        self.stop_and_classify(
            alloc,
            intent.attempt,
            intent.job,
            Duration::ZERO,
            AttemptOutcome::RuntimeLimitExceeded,
        )
        .await
    }

    /// A natural exit was observed by the executor's watcher: journal it (if
    /// not already recorded) and report the terminal status.
    pub async fn handle_observed_exit(
        &mut self,
        alloc: AllocationId,
        exit: ExitInfo,
    ) -> std::io::Result<Vec<pb::AgentReport>> {
        if self.state.exits.contains_key(&alloc) {
            return Ok(Vec::new()); // already recorded (e.g. via a stop)
        }
        let Some(intent) = self.state.intents.get(&alloc).copied() else {
            tracing::warn!(node = %self.node, %alloc, "observed exit for an allocation with no intent; ignoring");
            return Ok(Vec::new());
        };
        let outcome = classify_exit(&exit);
        self.record_exit(
            alloc,
            intent.attempt,
            intent.job,
            outcome.clone(),
            exit.runtime,
        )?;
        // Exit is durable → the container is now just debris; reap it (§5).
        self.reap_journaled(alloc).await;
        Ok(vec![self.terminal_status(
            alloc,
            intent.attempt,
            intent.job,
            outcome,
            exit.runtime,
        )])
    }

    /// Backstop reap sweep (§5): diff `observe()` against the journaled exits
    /// and reap every exited container whose exit is journaled and whose age
    /// (`now − finished_at`) exceeds `bound`. Running containers, unjournaled
    /// exits, and young exits are all left untouched. Non-fatal throughout —
    /// an observe or reap failure is logged and the next tick retries.
    pub async fn janitor_sweep(&mut self, now: Timestamp, bound: Duration) -> std::io::Result<()> {
        use crate::executor::ContainerState;
        let containers = match self.executor.observe().await {
            Ok(containers) => containers,
            Err(e) => {
                tracing::warn!(node = %self.node, error = %e, "executor.observe failed during janitor sweep; skipping");
                return Ok(());
            }
        };
        for c in containers {
            let ContainerState::Exited(info) = c.state else {
                continue; // never reap a running container
            };
            if !self.state.exits.contains_key(&c.allocation) {
                continue; // exit not journaled: evidence still needed
            }
            if now.duration_since(info.finished_at) > bound {
                self.reap_journaled(c.allocation).await;
            }
        }
        Ok(())
    }

    // ---- reports ----

    /// The registration report: capacity and labels, `node_epoch = 0` (zero
    /// before (re)registration, ADR 0009).
    pub fn register_report(&self) -> pb::AgentReport {
        pb::AgentReport {
            node: Some(self.node.into()),
            node_epoch: 0,
            body: Some(pb::agent_report::Body::Register(pb::Register {
                capacity: Some((&self.capacity).into()),
                labels: self.labels.clone(),
                // The advertised NodeService endpoint (ADR 0034), from the agent
                // `[listen]` config; `None` when the node hosts no service.
                service_addr: self.service_addr.clone(),
            })),
        }
    }

    /// A periodic heartbeat: capacity, the currently-running allocation set,
    /// and the (v1-empty) image-cache inventory.
    pub async fn heartbeat_report(&self) -> pb::AgentReport {
        use crate::executor::ContainerState;
        let running = match self.executor.observe().await {
            Ok(containers) => containers
                .into_iter()
                .filter(|c| matches!(c.state, ContainerState::Running { .. }))
                .map(|c| c.allocation.into())
                .collect(),
            Err(e) => {
                tracing::warn!(node = %self.node, error = %e, "executor.observe failed for heartbeat; running set empty");
                Vec::new()
            }
        };
        self.report(pb::agent_report::Body::Heartbeat(pb::Heartbeat {
            capacity: Some((&self.capacity).into()),
            running,
            image_cache: Some(self.executor.cache_inventory()),
        }))
    }

    /// Re-report the current known status of an allocation without acting.
    fn report_current_status(
        &self,
        alloc: AllocationId,
        attempt: AttemptId,
        job: JobId,
    ) -> Vec<pb::AgentReport> {
        if let Some(exit) = self.state.exits.get(&alloc) {
            return vec![self.terminal_status(
                alloc,
                exit.attempt,
                exit.job,
                exit.outcome.clone(),
                exit.runtime,
            )];
        }
        if self.state.tombstones.contains(&alloc) {
            return vec![self.terminal_status(
                alloc,
                attempt,
                job,
                AttemptOutcome::Aborted,
                Duration::ZERO,
            )];
        }
        // Intent present (or best-effort): believed running.
        vec![self.running_status(alloc, attempt, job)]
    }

    /// Reap an exited container whose exit is already durably journaled (§5).
    /// Non-fatal: a reap failure is logged and swallowed — the janitor sweep is
    /// the backstop, and the exited container is harmless evidence until then.
    async fn reap_journaled(&self, alloc: AllocationId) {
        if let Err(e) = self.executor.reap(alloc).await {
            tracing::warn!(node = %self.node, %alloc, error = %e, "reap failed after journaled exit; janitor will retry");
        }
    }

    fn record_exit(
        &mut self,
        alloc: AllocationId,
        attempt: AttemptId,
        job: JobId,
        outcome: AttemptOutcome,
        runtime: Duration,
    ) -> std::io::Result<()> {
        let exit = ExitRec {
            allocation: alloc,
            attempt,
            job,
            outcome,
            runtime,
        };
        self.journal.journal_exit(&exit)?;
        self.state.exits.insert(alloc, exit);
        Ok(())
    }

    fn report(&self, body: pb::agent_report::Body) -> pb::AgentReport {
        pb::AgentReport {
            node: Some(self.node.into()),
            node_epoch: self.epoch,
            body: Some(body),
        }
    }

    fn running_status(
        &self,
        alloc: AllocationId,
        attempt: AttemptId,
        job: JobId,
    ) -> pb::AgentReport {
        self.report(pb::agent_report::Body::AttemptStatus(pb::AttemptStatus {
            allocation: Some(alloc.into()),
            attempt: Some(attempt.into()),
            job: Some(job.into()),
            observed: Some((&AttemptState::Running).into()),
            runtime_us: 0,
        }))
    }

    fn terminal_status(
        &self,
        alloc: AllocationId,
        attempt: AttemptId,
        job: JobId,
        outcome: AttemptOutcome,
        runtime: Duration,
    ) -> pb::AgentReport {
        self.report(pb::agent_report::Body::AttemptStatus(pb::AttemptStatus {
            allocation: Some(alloc.into()),
            attempt: Some(attempt.into()),
            job: Some(job.into()),
            observed: Some((&AttemptState::Terminal(outcome)).into()),
            runtime_us: runtime.as_micros() as u64,
        }))
    }
}

/// The three identifiers naming the work in a `StartJob`, parsed from its proto
/// ids. `None` if any is missing or malformed (a StartJob we must drop).
struct StartIds {
    /// The allocation the executor runs.
    allocation: AllocationId,
    /// The attempt this run records against.
    attempt: AttemptId,
    /// The job the attempt belongs to.
    job: JobId,
}

/// The runtime bound a `StartJob` carries, as a span. The wire field is
/// unsigned and so reaches past [`Duration`]'s range; a bound that far out
/// saturates, which is indistinguishable from the unbounded case it means.
fn runtime_bound(micros: u64) -> Duration {
    Duration::from_micros(i64::try_from(micros).unwrap_or(i64::MAX))
}

fn start_ids(sj: &pb::StartJob) -> Option<StartIds> {
    let allocation = sj.allocation.clone()?.try_into().ok()?;
    let attempt = sj.attempt.clone()?.try_into().ok()?;
    let job = sj.job.clone()?.try_into().ok()?;
    Some(StartIds {
        allocation,
        attempt,
        job,
    })
}

fn observed_to_pb(o: &ObservedAllocation) -> pb::ObservedAllocation {
    pb::ObservedAllocation {
        allocation: Some(o.allocation.into()),
        attempt: Some(o.attempt.into()),
        job: Some(o.job.into()),
        running: o.running,
        outcome: o.outcome.as_ref().map(|oc| oc.into()),
        runtime_us: o.runtime.as_micros() as u64,
    }
}

#[cfg(test)]
mod tests;

mod runner;
pub use runner::run;
