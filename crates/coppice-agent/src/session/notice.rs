//! The agent's termination mechanism (ADR 0044 §3) and the drain-plan
//! executor it runs under (§5): notice intents, signals, marks, the
//! preemption stop path, and their recovery.
//!
//! The node's plan has two independent components, journaled together as one
//! record (the latest in force):
//!
//! - an **operator** deadline drain, from the coordinator's `Drain` command —
//!   its deadline is the target the heartbeat echoes, and a withdrawal clears
//!   it;
//! - an external **interruption**, from the configured source — the
//!   provider's reclaim time.
//!
//! Each active component gives an attempt a stop time, `T_stop = deadline −
//! cleanup_margin − abort_grace` with the margin and grace the attempt was
//! dispatched under, and a notice time: an interruption notifies at once, an
//! operator drain at `T_stop − notice_window`. A running attempt is due for
//! notice at the earliest notice time of the active components; once
//! notified its stop is the earliest of their stop times, and only an
//! interruption may ever shorten it afterwards (§3's precedence table):
//! nothing the operator does retracts, postpones or advances an issued
//! notice.
//!
//! Every decision is a plain method on [`Session`]; the live loop
//! ([`super::runner`]) only feeds it notices, commands and the clock and
//! spawns the stops it hands back. Nothing here needs a connected
//! coordinator: a report produced while the stream is down is simply not
//! sent, because the post-reconnect `ObservedSet` repeats every live mark and
//! journaled exit.

use std::io;

use coppice_consensus::fs::Fs;
use coppice_core::attempt::PreemptReason;
use coppice_core::id::AllocationId;
use coppice_core::time::{Duration, Timestamp};
use coppice_proto::pb::agent::v1 as pb;

use super::{classify_stop, Session};
use crate::executor::{
    Executor, ExecutorError, LaunchBound, LaunchBoundSetter, SignalOutcome, StartError, StopOutcome,
};
use crate::journal::{DrainPlanRec, IntentRec, NoticeMarkRec, OperatorDrain, TombstoneReason};

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

/// The session's handle on one start in flight's [`LaunchBound`].
#[derive(Debug)]
pub(super) struct StartBound {
    setter: LaunchBoundSetter,
    /// The StartJob's own `latest_start` (drain backfill), if any.
    latest_start: Option<Timestamp>,
    /// The reason of the plan that last set the bound below `latest_start`:
    /// what a refusal at a bound other than `latest_start` resolves as. Kept
    /// when a later plan change hands the bound back, so a refusal that raced
    /// that change is still classified by the bound that actually passed.
    plan_reason: Option<PreemptReason>,
}

impl StartBound {
    /// The `Preempted` reason a refused launch resolves as when the bound
    /// that passed was the plan's; `None` for any other start failure, and
    /// for a drain backfill's own `latest_start` (which answers `Revoked`).
    pub(super) fn plan_refusal(&self, error: &StartError) -> Option<PreemptReason> {
        let StartError::LaunchBoundPassed {
            latest_start: passed,
        } = error
        else {
            return None;
        };
        // Any bound but the StartJob's own was set by a plan.
        (self.latest_start != Some(*passed))
            .then(|| self.plan_reason.unwrap_or(PreemptReason::Drain))
    }
}

/// What the plan in force says about one attempt (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlanTerms {
    /// When the attempt is due for notice, if not notified yet.
    notice_at: Timestamp,
    /// Its stop time once notified: the earliest component's `T_stop`.
    stop_at: Timestamp,
    /// `Interruption` while that component is active, else `Drain`.
    reason: PreemptReason,
}

impl<F: Fs, E: Executor> Session<F, E> {
    /// The drain plan in force; empty when there is none.
    pub fn active_plan(&self) -> DrainPlanRec {
        self.state.plan
    }

    /// Announced as `interruption_deadline_us`: the interruption component's
    /// deadline.
    pub fn interruption_deadline(&self) -> Option<Timestamp> {
        self.state.plan.interruption
    }

    /// Echoed as `Heartbeat.drain_target_us`: the operator deadline this agent
    /// last accepted (ADR 0044 §5).
    pub fn drain_target(&self) -> Option<Timestamp> {
        self.state.plan.operator.map(|o| o.deadline)
    }

    /// The `draining` flag on `Register`/`Heartbeat`: the agent's own
    /// shutdown (ADR 0041) or an interruption (ADR 0044 §5). An operator
    /// deadline plan alone does not announce draining: that would close the
    /// coordinator's admission before `T_notice`, which the drain record
    /// already governs.
    pub(super) fn announces_draining(&self) -> bool {
        self.draining || self.state.plan.interruption.is_some()
    }

    /// What the heartbeat announces of the plan — the interruption deadline
    /// and the operator target — so the loop can re-announce at once when
    /// either changes.
    pub fn plan_announcement(&self) -> super::PlanAnnouncement {
        (self.interruption_deadline(), self.drain_target())
    }

    /// When the live loop's timer should next fire: the earlier of the next
    /// notice time and the next stop time.
    pub fn next_timer_at(&self) -> Option<Timestamp> {
        match (self.next_notice_at(), self.next_stop_at()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Journal `plan` (fsynced) and only then make it the plan in force, then
    /// re-bound every start in flight under it.
    fn set_plan(&mut self, plan: DrainPlanRec, now: Timestamp) -> io::Result<()> {
        self.journal.journal_drain_plan(&plan)?;
        self.state.plan = plan;
        self.refresh_launch_bounds(now);
        Ok(())
    }

    // ---- launch bounds of starts in flight (ADR 0044 §5) ----

    /// The launch bound for a start being queued: the earlier of its own
    /// `latest_start` and its stop under the plan in force, if any.
    pub(super) fn open_launch_bound(
        &mut self,
        intent: &IntentRec,
        latest_start: Option<Timestamp>,
        now: Timestamp,
    ) -> LaunchBound {
        let (setter, bound) = LaunchBound::channel(latest_start);
        self.launch_bounds.insert(
            intent.allocation,
            StartBound {
                setter,
                latest_start,
                plan_reason: None,
            },
        );
        self.refresh_launch_bound(intent, now);
        bound
    }

    /// Re-bound every start in flight under the plan now in force: a
    /// container still starting at its `T_stop` must not start at all ("at
    /// `T_stop` it stops what remains under this plan, including containers
    /// still starting"). A start in flight has been given no notice, so the
    /// bound is recomputed, not only tightened: a withdrawal or a later
    /// replacement hands it back, as §3's cancellation and replacement rules
    /// do for any attempt not yet notified.
    fn refresh_launch_bounds(&mut self, now: Timestamp) {
        let starting: Vec<IntentRec> = self
            .starting
            .iter()
            .filter_map(|alloc| self.state.intents.get(alloc).copied())
            .collect();
        for intent in starting {
            self.refresh_launch_bound(&intent, now);
        }
    }

    /// Set one start's bound to the earlier of its own `latest_start` and its
    /// stop under the plan now in force (none without either).
    fn refresh_launch_bound(&mut self, intent: &IntentRec, now: Timestamp) {
        let terms = self.plan_terms(intent, now);
        let Some(bound) = self.launch_bounds.get_mut(&intent.allocation) else {
            return;
        };
        let (at, plan_reason) = match (bound.latest_start, terms) {
            (own, Some(terms)) if !own.is_some_and(|own| terms.stop_at >= own) => {
                (Some(terms.stop_at), Some(terms.reason))
            }
            (own, _) => (own, None),
        };
        if let Some(reason) = plan_reason {
            bound.plan_reason = Some(reason);
        }
        if bound.setter.set(at) {
            tracing::info!(
                node = %self.node,
                alloc = %intent.allocation,
                bound = ?at,
                reason = ?plan_reason,
                "re-bounded a start in flight under the drain plan in force"
            );
        }
    }

    /// When an attempt not yet notified is due for notice under the plan in
    /// force: at once under an interruption, else the operator's
    /// `T_stop − notice_window`. `None` with no plan.
    fn plan_notice_at(&self, intent: &IntentRec) -> Option<Timestamp> {
        let plan = self.state.plan;
        if plan.interruption.is_some() {
            return Some(Timestamp::min_value());
        }
        plan.operator
            .map(|o| stop_time(o.deadline, intent).saturating_sub(o.notice_window))
    }

    /// An attempt's terms under the plan in force at `now`, or `None` with no
    /// plan.
    ///
    /// With both components active, an interruption notifies at once with
    /// the provider's `T_stop` (ADR 0044 §3's table), and that stop is kept
    /// through anything the operator does later. Only when the operator's
    /// own notice time has already come — the attempt was due under the
    /// operator plan anyway, as when both land in one tick or on recovery —
    /// is the stop the earlier of the two.
    fn plan_terms(&self, intent: &IntentRec, now: Timestamp) -> Option<PlanTerms> {
        let plan = self.state.plan;
        let interruption = plan.interruption.map(|d| stop_time(d, intent));
        let operator = plan.operator.map(|o| {
            let stop = stop_time(o.deadline, intent);
            (stop.saturating_sub(o.notice_window), stop)
        });
        match (interruption, operator) {
            (None, None) => None,
            (Some(stop_at), None) => Some(PlanTerms {
                notice_at: Timestamp::min_value(),
                stop_at,
                reason: PreemptReason::Interruption,
            }),
            (None, Some((notice_at, stop_at))) => Some(PlanTerms {
                notice_at,
                stop_at,
                reason: PreemptReason::Drain,
            }),
            (Some(interrupted), Some((operator_notice, drained))) => Some(PlanTerms {
                notice_at: Timestamp::min_value(),
                stop_at: if now < operator_notice {
                    interrupted
                } else {
                    interrupted.min(drained)
                },
                reason: PreemptReason::Interruption,
            }),
        }
    }

    /// The operator plan's notice time for a start dispatched with
    /// `abort_grace` and `cleanup_margin` — when its admission closed at the
    /// agent's door. `None` with no operator plan.
    pub(super) fn operator_notice_at(
        &self,
        abort_grace: Duration,
        cleanup_margin: Duration,
    ) -> Option<Timestamp> {
        self.state.plan.operator.map(|o| {
            o.deadline
                .saturating_sub(cleanup_margin)
                .saturating_sub(abort_grace)
                .saturating_sub(o.notice_window)
        })
    }

    // ---- the operator's deadline drain (ADR 0044 §5) ----

    /// The coordinator's `Drain` command: the operator's deadline, or its
    /// withdrawal. The deadline already held is a no-op, journaling nothing.
    /// Otherwise the new plan is journaled before anything acts on it —
    /// a replacement being a withdrawal followed by a new plan: attempts
    /// already notified keep their marks, the rest are governed by the new
    /// deadline. Anything already due is notified at once, so a command that
    /// reaches the agent after `T_notice` notifies with a shortfall; a
    /// deadline already past notifies and stops at once (the API's lead rule
    /// makes that reachable only through delay).
    pub async fn on_drain(
        &mut self,
        drain: pb::Drain,
        now: Timestamp,
    ) -> io::Result<Vec<pb::AgentReport>> {
        let target = match drain.deadline_us {
            None => None,
            Some(us) => match Timestamp::from_micros(us) {
                Some(deadline) => Some(deadline),
                None => {
                    tracing::warn!(node = %self.node, "dropping malformed Drain (deadline out of range)");
                    return Ok(Vec::new());
                }
            },
        };
        if self.drain_target() == target {
            return Ok(Vec::new());
        }
        let notice_window =
            Duration::from_micros(i64::try_from(drain.notice_window_us).unwrap_or(i64::MAX));
        let previous = self.drain_target();
        let mut plan = self.state.plan;
        plan.operator = target.map(|deadline| OperatorDrain {
            deadline,
            notice_window,
        });
        self.set_plan(plan, now)?;
        match target {
            Some(deadline) => tracing::warn!(
                node = %self.node,
                %deadline,
                %notice_window,
                replaces = ?previous,
                "operator deadline drain accepted (ADR 0044 §5)"
            ),
            None => tracing::info!(
                node = %self.node,
                withdrawn = ?previous,
                "operator deadline drain withdrawn; notices already issued keep their stops"
            ),
        }
        self.begin_due_notices(now).await
    }

    // ---- interruption (ADR 0044 §5) ----

    /// Starts (or shortens) the interruption component, journaled before
    /// anything else, then notifies every running attempt.
    ///
    /// With no interruption in force, a deadline already past is a stale
    /// record of a reclaim the host survived; acting on it would stop every
    /// new attempt on a healthy node, so it is ignored. A deadline no earlier
    /// than the one in force is a no-op: it would postpone published stops.
    pub async fn on_interruption(
        &mut self,
        deadline: Timestamp,
        now: Timestamp,
    ) -> io::Result<Vec<pb::AgentReport>> {
        let current = self.state.plan.interruption;
        match current {
            Some(current) if deadline >= current => return Ok(Vec::new()),
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
        let mut plan = self.state.plan;
        plan.interruption = Some(deadline);
        // Durable before any notice goes out: a restarted agent still runs it.
        self.set_plan(plan, now)?;
        if let Some(health) = &self.health {
            health.set_interrupted(true);
        }
        let live = self.outstanding_live_work();
        tracing::warn!(
            node = %self.node,
            %deadline,
            replaces = ?current,
            attempts = live.len(),
            "host interruption: draining and notifying every running attempt (ADR 0044 §5)"
        );
        let mut reports = Vec::new();
        for alloc in live {
            reports.extend(self.notify_under_plan(alloc, now).await?);
        }
        Ok(reports)
    }

    // ---- notices under the plan ----

    /// Live allocations the plan could still notify: started, not ended, not
    /// already being stopped by a tombstone, and not yet notified.
    fn notice_candidates(&self) -> impl Iterator<Item = (AllocationId, Timestamp)> + '_ {
        self.state.intents.values().filter_map(|intent| {
            let alloc = intent.allocation;
            if self.state.exits.contains_key(&alloc)
                || self.starting.contains(&alloc)
                || self.state.tombstones.contains_key(&alloc)
                || self.state.notice_intents.contains_key(&alloc)
            {
                return None;
            }
            Some((alloc, self.plan_notice_at(intent)?))
        })
    }

    /// When the live loop should next call [`Session::begin_due_notices`]:
    /// the earliest notice time among attempts not yet notified.
    pub fn next_notice_at(&self) -> Option<Timestamp> {
        self.notice_candidates().map(|(_, at)| at).min()
    }

    /// Notify every attempt due for notice by `now`.
    pub async fn begin_due_notices(&mut self, now: Timestamp) -> io::Result<Vec<pb::AgentReport>> {
        let due: Vec<AllocationId> = self
            .notice_candidates()
            .filter(|(_, at)| *at <= now)
            .map(|(alloc, _)| alloc)
            .collect();
        let mut reports = Vec::new();
        for alloc in due {
            reports.extend(self.notify_under_plan(alloc, now).await?);
        }
        Ok(reports)
    }

    /// Bring one live allocation under the plan in force (ADR 0044 §3's
    /// precedence table): notify it if it is due, finish a notice a crash
    /// interrupted, or let an interruption shorten a later mark. A notified
    /// attempt's stop is otherwise left exactly as issued.
    pub(super) async fn notify_under_plan(
        &mut self,
        alloc: AllocationId,
        now: Timestamp,
    ) -> io::Result<Vec<pb::AgentReport>> {
        let Some(intent) = self.state.intents.get(&alloc).copied() else {
            return Ok(Vec::new());
        };
        // A start still in flight is notified once it has a container
        // (`finish_start`): a signal now would reach nothing.
        if self.state.exits.contains_key(&alloc) || self.starting.contains(&alloc) {
            return Ok(Vec::new());
        }
        let Some(terms) = self.plan_terms(&intent, now) else {
            return Ok(Vec::new());
        };
        match (
            self.state.notice_intents.get(&alloc).copied(),
            self.state.marks.get(&alloc).copied(),
        ) {
            // Not yet notified: only once due, and never one a tombstone is
            // already stopping.
            (None, _) => {
                if terms.notice_at > now || self.state.tombstones.contains_key(&alloc) {
                    return Ok(Vec::new());
                }
                self.notify(alloc, terms.reason, terms.stop_at, now).await
            }
            // Intent without mark: the signal may never have gone out (§3,
            // recovery). A drain or interruption notice is covered by the
            // plan, so its stop is the plan's; a scheduler preemption gets a
            // fresh countdown of its declared notice, never later than the
            // plan's stop.
            (Some(reason), None) => {
                let stop_at = match reason {
                    PreemptReason::Drain | PreemptReason::Interruption => terms.stop_at,
                    _ => now.saturating_add(intent.notice).min(terms.stop_at),
                };
                self.signal_and_mark(alloc, reason, stop_at, now).await
            }
            // Notified: only an interruption may shorten the window.
            (Some(_), Some(mark)) => {
                let Some(interrupted) = self.state.plan.interruption.map(|d| stop_time(d, &intent))
                else {
                    return Ok(Vec::new());
                };
                if interrupted >= mark.stop_at {
                    return Ok(Vec::new());
                }
                // The window still opened at the original signal; only its
                // end moves, so the shortfall is recomputed against it. The
                // replacement mark is durable before it is re-reported, so a
                // restart can never restore a later stop.
                let replacement = NoticeMarkRec {
                    stop_at: interrupted,
                    shortfall: shortfall(intent.notice, mark.notified_at, interrupted),
                    notified_at: mark.notified_at,
                };
                self.journal.journal_notice_mark(alloc, &replacement)?;
                self.state.marks.insert(alloc, replacement);
                tracing::info!(
                    node = %self.node,
                    %alloc,
                    from = %mark.stop_at,
                    to = %interrupted,
                    "interruption shortens a notified attempt's stop (replacement mark)"
                );
                Ok(self.notice_report(alloc, now).into_iter().collect())
            }
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
        // Never later than the plan in force would stop it: a notice issued
        // now is the attempt's one stop time (ADR 0044 §3).
        if let Some(terms) = self.plan_terms(&intent, now) {
            stop_at = stop_at.min(terms.stop_at);
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
    /// Under a plan, [`Session::notify_under_plan`] notifies what is due and
    /// repairs whichever step a crash interrupted. With no plan, an intent
    /// without a mark (a scheduler preemption, or an attempt whose drain was
    /// withdrawn) gets a fresh countdown of its declared notice. An
    /// interruption whose deadline has passed means the host survived the
    /// reclaim: its stops all fire at once, then the component is retired so
    /// the node is not draining forever. The operator component is never
    /// retired here — only a withdrawal ends it.
    pub async fn resume_notices(&mut self, now: Timestamp) -> io::Result<Vec<pb::AgentReport>> {
        let planned = !self.state.plan.is_empty();
        let mut reports = Vec::new();
        for alloc in self.outstanding_live_work() {
            if planned {
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
        if let Some(deadline) = self.state.plan.interruption.filter(|d| *d <= now) {
            let mut plan = self.state.plan;
            plan.interruption = None;
            self.set_plan(plan, now)?;
            if let Some(health) = &self.health {
                health.set_interrupted(false);
            }
            tracing::warn!(
                node = %self.node,
                %deadline,
                "recovered an interruption whose deadline has passed: the host came back; \
                 stopping what is left under it and retiring it"
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
                let reason = self.state.notice_intents.get(&alloc).copied().unwrap_or(
                    if self.state.plan.operator.is_some() && self.state.plan.interruption.is_none()
                    {
                        PreemptReason::Drain
                    } else {
                        PreemptReason::Interruption
                    },
                );
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

    /// Warn, once per distinct `(notice, abort_grace, cleanup_margin)`, when
    /// a declared notice cannot fit EC2's window (ADR 0044 §5). Checked per
    /// StartJob because the agent does not know the replicated policy at
    /// startup: the grace and the margin arrive with each dispatch.
    pub(super) fn warn_notice_sizing(&mut self, intent: &IntentRec) {
        if !self.interruption_source {
            return;
        }
        let needed = intent
            .notice
            .saturating_add(intent.abort_grace)
            .saturating_add(intent.cleanup_margin);
        if needed <= EC2_INTERRUPTION_WINDOW {
            return;
        }
        if self
            .sizing_warned
            .insert((intent.notice, intent.abort_grace, intent.cleanup_margin))
        {
            tracing::warn!(
                node = %self.node,
                notice = %intent.notice,
                abort_grace = %intent.abort_grace,
                cleanup_margin = %intent.cleanup_margin,
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

/// An attempt's `T_stop` under a plan component with `deadline` (ADR 0044
/// §5), with the margin and grace it was dispatched under.
fn stop_time(deadline: Timestamp, intent: &IntentRec) -> Timestamp {
    deadline
        .saturating_sub(intent.cleanup_margin)
        .saturating_sub(intent.abort_grace)
}
