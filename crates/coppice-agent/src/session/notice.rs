//! The agent's termination mechanism (ADR 0044 §3) and the drain-plan
//! executor it runs under (§5): notice intents, signals, marks, the
//! preemption stop path, and their recovery.
//!
//! Every decision is a plain method on [`Session`]; the live loop
//! ([`super::runner`]) only feeds it notices and the clock and spawns the
//! stops it hands back. Nothing here needs a connected coordinator: a report
//! produced while the stream is down is simply not sent, because the
//! post-reconnect `ObservedSet` repeats every live mark and journaled exit.

use std::io;

use coppice_consensus::fs::Fs;
use coppice_core::attempt::PreemptReason;
use coppice_core::id::AllocationId;
use coppice_core::time::{Duration, Timestamp};
use coppice_proto::pb::agent::v1 as pb;

use super::{classify_stop, Session};
use crate::executor::{Executor, ExecutorError, SignalOutcome, StopOutcome};
use crate::journal::{DrainPlanRec, NoticeMarkRec, TombstoneReason};

/// ADR 0044 §5's cleanup margin. An agent constant until slice 5 introduces
/// the replicated `drain_cleanup_margin` policy.
pub const DRAIN_CLEANUP_MARGIN: Duration = Duration::from_secs(30);

/// EC2's spot interruption window; the sizing warning's yardstick.
const EC2_INTERRUPTION_WINDOW: Duration = Duration::from_secs(120);

/// How long a failed preemption stop waits before the loop tries it again.
const STOP_RETRY: Duration = Duration::from_secs(5);

/// A stop (preemption or abort) for the live loop to run on its own task;
/// its result comes back through [`Session::finish_stop`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopRequest {
    pub allocation: AllocationId,
    pub grace: Duration,
}

impl<F: Fs, E: Executor> Session<F, E> {
    pub fn active_plan(&self) -> Option<DrainPlanRec> {
        self.state.plan
    }

    /// Announced as `interruption_deadline_us`: the active plan's deadline
    /// when an interruption started it.
    pub fn interruption_deadline(&self) -> Option<Timestamp> {
        self.state
            .plan
            .filter(|p| p.reason == PreemptReason::Interruption)
            .map(|p| p.deadline)
    }

    /// The `draining` flag on `Register`/`Heartbeat`: the agent's own
    /// shutdown (ADR 0041) or an active plan (ADR 0044 §5).
    pub(super) fn announces_draining(&self) -> bool {
        self.draining || self.state.plan.is_some()
    }

    // ---- interruption (ADR 0044 §5) ----

    /// Starts (or shortens) the drain plan, journaled before anything else,
    /// then notifies every running attempt.
    ///
    /// With no plan, a deadline already past is a stale record of a reclaim
    /// the host survived; a plan from it would stop every new attempt on a
    /// healthy node, so it is ignored. A deadline no earlier than the active
    /// plan's is a no-op: it would postpone published stops.
    pub async fn on_interruption(
        &mut self,
        deadline: Timestamp,
        now: Timestamp,
    ) -> io::Result<Vec<pb::AgentReport>> {
        match self.state.plan {
            Some(plan) if deadline >= plan.deadline => return Ok(Vec::new()),
            None if deadline <= now => {
                tracing::warn!(
                    node = %self.node,
                    %deadline,
                    "ignoring an interruption notice whose deadline has already passed \
                     (a stale record of an earlier reclaim)"
                );
                return Ok(Vec::new());
            }
            _ => {}
        }
        let plan = DrainPlanRec {
            deadline,
            reason: PreemptReason::Interruption,
            accepted_target: None,
        };
        let replaced = self.state.plan.replace(plan);
        // Durable before any notice goes out: a restarted agent still runs it.
        if let Err(e) = self.journal.journal_drain_plan(&plan) {
            self.state.plan = replaced;
            return Err(e);
        }
        if let Some(health) = &self.health {
            health.set_interrupted(true);
        }
        let live = self.outstanding_live_work();
        tracing::warn!(
            node = %self.node,
            %deadline,
            replaces = ?replaced.map(|p| p.deadline),
            attempts = live.len(),
            "host interruption: draining and notifying every running attempt (ADR 0044 §5)"
        );
        let mut reports = Vec::new();
        for alloc in live {
            reports.extend(self.notify_under_plan(alloc, now).await?);
        }
        Ok(reports)
    }

    /// Bring one live allocation under the active plan (ADR 0044 §3,
    /// "external interruption"): notify it, or shorten a later mark.
    pub(super) async fn notify_under_plan(
        &mut self,
        alloc: AllocationId,
        now: Timestamp,
    ) -> io::Result<Vec<pb::AgentReport>> {
        let Some(plan) = self.state.plan else {
            return Ok(Vec::new());
        };
        let Some(intent) = self.state.intents.get(&alloc).copied() else {
            return Ok(Vec::new());
        };
        // A start still in flight is notified once it has a container
        // (`finish_start`): a signal now would reach nothing.
        if self.state.exits.contains_key(&alloc) || self.starting.contains(&alloc) {
            return Ok(Vec::new());
        }
        let stop_at = plan_stop_at(&plan, intent.abort_grace);
        match (
            self.state.notice_intents.get(&alloc).copied(),
            self.state.marks.get(&alloc).copied(),
        ) {
            (None, _) => self.notify(alloc, plan.reason, stop_at, now).await,
            // Intent without mark: the signal may never have gone out.
            (Some(reason), None) => self.signal_and_mark(alloc, reason, stop_at, now).await,
            (Some(_), Some(mark)) if stop_at < mark.stop_at => {
                // The window still opened at the original signal; only its
                // end moves, so the shortfall is recomputed against it.
                let replacement = NoticeMarkRec {
                    stop_at,
                    shortfall: shortfall(intent.notice, mark.notified_at, stop_at),
                    notified_at: mark.notified_at,
                };
                self.journal.journal_notice_mark(alloc, &replacement)?;
                self.state.marks.insert(alloc, replacement);
                tracing::info!(
                    node = %self.node,
                    %alloc,
                    from = %mark.stop_at,
                    to = %stop_at,
                    "interruption shortens a notified attempt's stop (replacement mark)"
                );
                Ok(self.notice_report(alloc, now).into_iter().collect())
            }
            (Some(_), Some(_)) => Ok(Vec::new()),
        }
    }

    async fn notify(
        &mut self,
        alloc: AllocationId,
        reason: PreemptReason,
        stop_at: Timestamp,
        now: Timestamp,
    ) -> io::Result<Vec<pb::AgentReport>> {
        // Durable before the signal: the intent is the classifier, and an exit
        // the signal provokes must find it (ADR 0044 §3).
        self.journal.journal_notice_intent(alloc, reason)?;
        self.state.notice_intents.insert(alloc, reason);
        self.signal_and_mark(alloc, reason, stop_at, now).await
    }

    async fn signal_and_mark(
        &mut self,
        alloc: AllocationId,
        reason: PreemptReason,
        stop_at: Timestamp,
        now: Timestamp,
    ) -> io::Result<Vec<pb::AgentReport>> {
        let Some(intent) = self.state.intents.get(&alloc).copied() else {
            return Ok(Vec::new());
        };
        match self.executor.signal(alloc, intent.notice_signal).await {
            Ok(SignalOutcome::Delivered) => {}
            // Not worth failing the protocol for: the mark still bounds the
            // attempt, and the notice intent classifies whatever exit it had.
            Ok(SignalOutcome::NotRunning) => tracing::info!(
                node = %self.node,
                %alloc,
                "notice signal found the container not running; marking anyway"
            ),
            Err(e) => tracing::warn!(
                node = %self.node,
                %alloc,
                error = %e,
                "notice signal delivery failed; marking anyway"
            ),
        }
        let mark = NoticeMarkRec {
            stop_at,
            shortfall: shortfall(intent.notice, now, stop_at),
            notified_at: now,
        };
        self.journal.journal_notice_mark(alloc, &mark)?;
        self.state.marks.insert(alloc, mark);
        tracing::info!(
            node = %self.node,
            %alloc,
            ?reason,
            signal = intent.notice_signal.name(),
            %stop_at,
            shortfall = %mark.shortfall,
            "notice delivered"
        );
        Ok(self.notice_report(alloc, now).into_iter().collect())
    }

    /// `(reason, remaining_us, shortfall_us)` of the mark in force.
    pub(super) fn notice_payload(
        &self,
        alloc: AllocationId,
        now: Timestamp,
    ) -> Option<(PreemptReason, u64, u64)> {
        let reason = *self.state.notice_intents.get(&alloc)?;
        let mark = self.state.marks.get(&alloc)?;
        let remaining = mark.stop_at.duration_since(now).max(Duration::ZERO);
        Some((
            reason,
            u64::try_from(remaining.as_micros()).unwrap_or(0),
            u64::try_from(mark.shortfall.as_micros()).unwrap_or(0),
        ))
    }

    fn notice_report(&self, alloc: AllocationId, now: Timestamp) -> Option<pb::AgentReport> {
        let intent = self.state.intents.get(&alloc)?;
        let (reason, remaining_us, shortfall_us) = self.notice_payload(alloc, now)?;
        Some(
            self.report(pb::agent_report::Body::AttemptNotice(pb::AttemptNotice {
                allocation: Some(alloc.into()),
                attempt: Some(intent.attempt.into()),
                job: Some(intent.job.into()),
                reason: Some(reason.into()),
                remaining_us,
                shortfall_us,
            })),
        )
    }

    // ---- PreemptJob (ADR 0044 §3, §4) ----

    /// The coordinator's scheduler preemption of a running attempt.
    ///
    /// Idempotent on the journaled notice intent, because the coordinator
    /// re-sends it until the mark is acknowledged: an attempt already
    /// notified — by an earlier `PreemptJob`, or by a drain plan or
    /// interruption — keeps its one stop time and only has its mark
    /// re-reported (ADR 0044 §3's precedence table: "no effect, one record
    /// per attempt"). One already ended, or already being stopped by an
    /// abort, is past warning. Otherwise: intent → signal → mark → report,
    /// with stop = delivery + the declared notice — never later than an
    /// active plan's stop, since an interruption may shorten any window.
    pub(super) async fn preempt_job(
        &mut self,
        pj: pb::PreemptJob,
        now: Timestamp,
    ) -> io::Result<Vec<pb::AgentReport>> {
        let Some(alloc) = pj.allocation.and_then(|a| AllocationId::try_from(a).ok()) else {
            tracing::warn!(node = %self.node, "dropping malformed PreemptJob (missing allocation)");
            return Ok(Vec::new());
        };
        let Some(reason) = pj.reason.and_then(|r| PreemptReason::try_from(r).ok()) else {
            tracing::warn!(node = %self.node, %alloc, "dropping malformed PreemptJob (missing reason)");
            return Ok(Vec::new());
        };
        let Some(intent) = self.state.intents.get(&alloc).copied() else {
            tracing::info!(node = %self.node, %alloc, "PreemptJob for an allocation this agent never started; ignoring");
            return Ok(Vec::new());
        };
        if self.state.exits.contains_key(&alloc) || self.state.tombstones.contains_key(&alloc) {
            return Ok(Vec::new());
        }
        // The coordinator preempts only attempts it has seen running, so a
        // start still in flight here means this agent restarted since; the
        // next session open re-derives the command once it runs.
        if self.starting.contains(&alloc) {
            tracing::debug!(node = %self.node, %alloc, "PreemptJob for a start still in flight; ignoring");
            return Ok(Vec::new());
        }
        let notice = Duration::from_micros(i64::try_from(pj.notice_us).unwrap_or(i64::MAX));
        let mut stop_at = now.saturating_add(notice);
        if let Some(plan) = self.state.plan {
            stop_at = stop_at.min(plan_stop_at(&plan, intent.abort_grace));
        }
        match (
            self.state.notice_intents.get(&alloc).copied(),
            self.state.marks.contains_key(&alloc),
        ) {
            // Already notified: one stop time per attempt.
            (Some(_), true) => Ok(self.notice_report(alloc, now).into_iter().collect()),
            // An intent whose signal may never have gone out (a mark that
            // failed to journal): signal again, under the intent's reason.
            (Some(journaled), false) => self.signal_and_mark(alloc, journaled, stop_at, now).await,
            (None, _) => {
                tracing::info!(
                    node = %self.node,
                    %alloc,
                    ?reason,
                    %notice,
                    "scheduler preemption: notifying the attempt"
                );
                self.notify(alloc, reason, stop_at, now).await
            }
        }
    }

    // ---- recovery (ADR 0044 §3, "Recovery of an intent …") ----

    /// Re-establish every notice from the recovered journal, once, before the
    /// loop starts: the session may not connect for a while, and the stops
    /// must not wait for it.
    ///
    /// Under a plan, [`Session::notify_under_plan`] repairs whichever step a
    /// crash interrupted. With no plan, an intent without a mark (only a
    /// `PreemptJob` can leave one) gets a fresh countdown of the declared
    /// notice. A plan whose
    /// deadline has passed means the host survived the reclaim: its stops all
    /// fire at once, then the plan is retired so the node is not draining
    /// forever.
    pub async fn resume_notices(&mut self, now: Timestamp) -> io::Result<Vec<pb::AgentReport>> {
        let plan = self.state.plan;
        let mut reports = Vec::new();
        for alloc in self.outstanding_live_work() {
            if plan.is_some() {
                reports.extend(self.notify_under_plan(alloc, now).await?);
                continue;
            }
            let intent = self.state.intents[&alloc];
            if let (Some(reason), false) = (
                self.state.notice_intents.get(&alloc).copied(),
                self.state.marks.contains_key(&alloc),
            ) {
                tracing::info!(node = %self.node, %alloc, "recovering a notice intent without a mark: signalling again");
                let stop_at = now.saturating_add(intent.notice);
                reports.extend(self.signal_and_mark(alloc, reason, stop_at, now).await?);
            }
        }
        if let Some(plan) = plan.filter(|p| p.deadline <= now) {
            self.journal.journal_drain_plan_retired()?;
            self.state.plan = None;
            if let Some(health) = &self.health {
                health.set_interrupted(false);
            }
            tracing::warn!(
                node = %self.node,
                deadline = %plan.deadline,
                "recovered a drain plan whose deadline has passed: the host came back; \
                 stopping what is left under it and retiring the plan"
            );
        }
        Ok(reports)
    }

    // ---- the stop path ----

    /// When the live loop should next call [`Session::begin_due_stops`]. A
    /// tombstoned allocation whose stop never finished (restart, or failed
    /// and due for retry) is due at once.
    pub fn next_stop_at(&self) -> Option<Timestamp> {
        self.stop_candidates().map(|(_, at)| at).min()
    }

    /// Live marked or tombstoned allocations with nothing in flight, and when
    /// each stop is due (after any retry backoff).
    fn stop_candidates(&self) -> impl Iterator<Item = (AllocationId, Timestamp)> + '_ {
        self.state.intents.keys().filter_map(|alloc| {
            if self.state.exits.contains_key(alloc)
                || self.stopping.contains(alloc)
                || self.starting.contains(alloc)
                || self.stop_abandoned.contains(alloc)
            {
                return None;
            }
            let due = if self.state.tombstones.contains_key(alloc) {
                Timestamp::min_value()
            } else {
                self.state.marks.get(alloc)?.stop_at
            };
            let due = match self.stop_retry_at.get(alloc) {
                Some(retry) => due.max(*retry),
                None => due,
            };
            Some((*alloc, due))
        })
    }

    /// Journal a preemption tombstone for every stop due by `now` and hand
    /// the stops to the loop. An existing tombstone (a `StopJob`'s abort, or
    /// this stop's own from before a restart) is kept: the command that wrote
    /// it decides the kill's attribution.
    pub fn begin_due_stops(&mut self, now: Timestamp) -> io::Result<Vec<StopRequest>> {
        let due: Vec<AllocationId> = self
            .stop_candidates()
            .filter(|(_, at)| *at <= now)
            .map(|(alloc, _)| alloc)
            .collect();
        for alloc in due {
            if !self.state.tombstones.contains_key(&alloc) {
                let reason = self
                    .state
                    .notice_intents
                    .get(&alloc)
                    .copied()
                    .or(self.state.plan.map(|p| p.reason))
                    .unwrap_or(PreemptReason::Interruption);
                let tombstone = TombstoneReason::Preempt { reason };
                self.journal.journal_tombstone(alloc, tombstone)?;
                self.state.tombstones.insert(alloc, tombstone);
                tracing::info!(node = %self.node, %alloc, "notice window over: stopping");
            }
            let grace = self.state.intents[&alloc].abort_grace;
            self.queue_stop(alloc, grace);
        }
        Ok(self.take_pending_stops())
    }

    pub(super) fn queue_stop(&mut self, alloc: AllocationId, grace: Duration) {
        self.stop_retry_at.remove(&alloc);
        self.stopping.insert(alloc);
        self.pending_stops.push(StopRequest {
            allocation: alloc,
            grace,
        });
    }

    /// A spawned stop finished: classify, journal and report the exit. The
    /// kill is attributed to the tombstone's command; a container that had
    /// already exited keeps its evidence's classification.
    ///
    /// An exit the watcher saw while this stop was in flight was held for it
    /// (ADR 0013: an exit during our SIGTERM is the stop's). If the stop
    /// cannot answer, that held exit is classified on its own evidence
    /// instead. Otherwise a failed stop is retried after a backoff, and one
    /// that found no container is abandoned to reconciliation as lost.
    pub fn finish_stop(
        &mut self,
        alloc: AllocationId,
        result: Result<StopOutcome, ExecutorError>,
        now: Timestamp,
    ) -> io::Result<Vec<pb::AgentReport>> {
        self.stopping.remove(&alloc);
        let deferred = self.deferred_exits.remove(&alloc);
        // Already journaled (the watcher before this stop began, or a start
        // failure): truth landed first, and was reported then.
        if self.state.exits.contains_key(&alloc) {
            return Ok(Vec::new());
        }
        let Some(intent) = self.state.intents.get(&alloc).copied() else {
            return Ok(Vec::new());
        };
        let classified = match &result {
            Ok(stopped @ (StopOutcome::Stopped(_) | StopOutcome::AlreadyExited(_))) => {
                let kill_outcome = self
                    .state
                    .tombstones
                    .get(&alloc)
                    .copied()
                    .unwrap_or(TombstoneReason::Abort)
                    .outcome();
                classify_stop(stopped, kill_outcome, self.state.attribution(&alloc))
            }
            Ok(StopOutcome::Unknown) | Err(_) => deferred.map(|exit| {
                (
                    crate::executor::classify_exit(&exit, &self.state.attribution(&alloc)),
                    exit.runtime,
                )
            }),
        };
        let Some((outcome, runtime)) = classified else {
            match result {
                Err(e) => {
                    tracing::warn!(node = %self.node, %alloc, error = %e, "stop failed; retrying");
                    self.stop_retry_at
                        .insert(alloc, now.saturating_add(STOP_RETRY));
                }
                _ => {
                    tracing::warn!(node = %self.node, %alloc, "stop: executor has no record of the container");
                    self.stop_abandoned.insert(alloc);
                }
            }
            return Ok(Vec::new());
        };
        self.record_exit(alloc, intent.attempt, intent.job, outcome.clone(), runtime)?;
        self.pending_reaps.push(alloc);
        Ok(vec![self.terminal_status(
            alloc,
            intent.attempt,
            intent.job,
            outcome,
            runtime,
        )])
    }

    // ---- sizing ----

    /// Warn, once per distinct `(notice, abort_grace)`, when a declared
    /// notice cannot fit EC2's window (ADR 0044 §5). Checked per StartJob
    /// because the agent does not know the replicated policy at startup.
    pub(super) fn warn_notice_sizing(&mut self, intent: &crate::journal::IntentRec) {
        if !self.interruption_source {
            return;
        }
        let needed = intent
            .notice
            .saturating_add(intent.abort_grace)
            .saturating_add(DRAIN_CLEANUP_MARGIN);
        if needed <= EC2_INTERRUPTION_WINDOW {
            return;
        }
        if self
            .sizing_warned
            .insert((intent.notice, intent.abort_grace))
        {
            tracing::warn!(
                node = %self.node,
                notice = %intent.notice,
                abort_grace = %intent.abort_grace,
                cleanup_margin = %DRAIN_CLEANUP_MARGIN,
                window = %EC2_INTERRUPTION_WINDOW,
                "declared notice + abort_grace + cleanup margin exceeds the provider's \
                 interruption window: an EC2 spot reclaim cannot honour this notice in full"
            );
        }
    }
}

/// How much of `notice` the window from `notified_at` to `stop_at` misses.
fn shortfall(notice: Duration, notified_at: Timestamp, stop_at: Timestamp) -> Duration {
    let window = stop_at.duration_since(notified_at).max(Duration::ZERO);
    notice.saturating_sub(window).max(Duration::ZERO)
}

/// An attempt's stop time under `plan` (ADR 0044 §5).
fn plan_stop_at(plan: &DrainPlanRec, abort_grace: Duration) -> Timestamp {
    plan.deadline
        .saturating_sub(DRAIN_CLEANUP_MARGIN)
        .saturating_sub(abort_grace)
}
