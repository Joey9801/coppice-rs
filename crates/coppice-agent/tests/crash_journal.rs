//! Crash-injection sweep over the agent journal (ADR 0009), mirroring the
//! storage engine's directed sweeps (`coppice-consensus/tests/crash_storage.rs`)
//! but with the agent's own vocabulary.
//!
//! The workload drives a scripted mix of agent ops — accept a fencing bump,
//! journal a start intent, "start" a container in a deterministic
//! [`RuntimeModel`], observe its exit, journal the exit, journal a stop
//! tombstone — where the `RuntimeModel` is *not* subject to `fs.crash`:
//! containers outlive the agent process, exactly as real ones do. A container
//! is only marked started in the model *after* the journal intent's
//! `sync_data` returned `Ok`, so the model can never contain a survivor whose
//! intent is not durable — which is precisely the fsync-before-start invariant
//! (ADR 0009) the sweep asserts.
//!
//! At every crash point `k` and several adversary seeds we recover the journal
//! and rebuild the ObservedSet, then assert:
//!
//! - (a) every `RuntimeModel` container appears with its truthful state (a
//!   survivor is never forgotten);
//! - (b) no entry claims `running` without `RuntimeModel` evidence (the agent
//!   never claims what it cannot prove);
//! - (c) every `RuntimeModel` container has a durable intent in the recovered
//!   journal (fsync-before-start);
//! - (d) recovery is idempotent (double-open yields identical state);
//! - (e) a notice signal is only ever delivered behind a durable notice intent
//!   (ADR 0044 §3);
//! - (f) the recovered notice mark is never later than a stop time already
//!   reported — the mark, and every replacement, is durable before the report;
//! - (g) the recovered drain plan is the one in force: never later than the
//!   last plan acted on, and gone once its retirement was durable.
//!
//! A second test crashes *inside recovery* (the compaction rewrite) and
//! re-recovers, checking the same invariants.

use std::collections::BTreeMap;
use std::io;

use coppice_agent::executor::{
    classify_exit, ContainerState, ExitCause, ExitInfo, ObservedContainer,
};
use coppice_agent::journal::{
    DrainPlanRec, ExitRec, IntentRec, Journal, JournalState, NoticeMarkRec, TombstoneReason,
    Watermark,
};
use coppice_agent::observed::{build_observed_set, ObservedAllocation};
use coppice_core::attempt::{AttemptOutcome, PreemptReason};
use coppice_core::id::{AllocationId, AttemptId, JobId};
use coppice_core::time::{Duration, Timestamp};
use coppice_testkit::simfs::{is_sim_crash, SimConfig, SimFs};
use uuid::Uuid;

/// Fixed ids so every replay of the workload is byte-identical — a stable
/// seam-op count is what makes crash point `k` reproducible.
struct Ids {
    allocation: AllocationId,
    attempt: AttemptId,
    job: JobId,
}

fn fixed_ids() -> Vec<Ids> {
    (0u128..4)
        .map(|i| Ids {
            allocation: AllocationId(Uuid::from_u128(0x1000 + i)),
            attempt: AttemptId(Uuid::from_u128(0x2000 + i)),
            job: JobId(Uuid::from_u128(0x3000 + i)),
        })
        .collect()
}

/// The deterministic stand-in for the container runtime. Plain state, never
/// perturbed by a simulated crash — containers survive the agent.
#[derive(Default)]
struct RuntimeModel {
    containers: BTreeMap<AllocationId, ObservedContainer>,
    /// Containers that have been sent a notice signal. Like the containers
    /// themselves, a delivered signal survives the agent's crash.
    signaled: std::collections::BTreeSet<AllocationId>,
    /// The latest stop time reported to the coordinator per allocation — the
    /// published bound a restart must never postpone.
    reported_stops: BTreeMap<AllocationId, Timestamp>,
    /// The deadline of the last drain plan acted on, and whether its
    /// retirement has been acted on.
    plan: Option<Timestamp>,
    plan_retired: bool,
}

impl RuntimeModel {
    fn start(&mut self, ids: &Ids) {
        self.containers.insert(
            ids.allocation,
            ObservedContainer {
                allocation: ids.allocation,
                attempt: ids.attempt,
                job: ids.job,
                state: ContainerState::Running {
                    runtime: Duration::ZERO,
                },
            },
        );
    }

    fn exit(&mut self, ids: &Ids, info: ExitInfo) {
        self.containers.insert(
            ids.allocation,
            ObservedContainer {
                allocation: ids.allocation,
                attempt: ids.attempt,
                job: ids.job,
                state: ContainerState::Exited(info),
            },
        );
    }

    fn observe(&self) -> Vec<ObservedContainer> {
        self.containers.values().copied().collect()
    }
}

/// Drive the scripted workload against the journal on `fs`, mutating `runtime`
/// in lockstep. Every journal op may crash (propagated via `?`); a container is
/// only started in the model after its intent append+fsync returned `Ok`.
fn drive(fs: SimFs, runtime: &mut RuntimeModel, ids: &[Ids]) -> io::Result<()> {
    let (mut journal, _state) = Journal::open(fs)?;

    journal.journal_fencing(Watermark {
        leader_term: 1,
        node_epoch: 1,
    })?;

    // Start allocation 0 and 1 under epoch 1.
    for i in [0usize, 1] {
        let intent = IntentRec {
            allocation: ids[i].allocation,
            attempt: ids[i].attempt,
            job: ids[i].job,
            node_epoch: 1,
            max_runtime: None,
            notice_signal: Default::default(),
            notice: Default::default(),
            abort_grace: Default::default(),
        };
        journal.journal_intent(&intent)?;
        runtime.start(&ids[i]);
    }

    // Allocation 0 exits naturally; the runtime observes it, then we journal
    // the classified exit (crash here ⇒ runtime evidence still wins on
    // recovery).
    let exit0 = ExitInfo {
        code: 0,
        cause: ExitCause::Natural,
        runtime: Duration::from_micros(111),
        finished_at: Timestamp::UNIX_EPOCH,
    };
    runtime.exit(&ids[0], exit0);
    journal.journal_exit(&ExitRec {
        allocation: ids[0].allocation,
        attempt: ids[0].attempt,
        job: ids[0].job,
        outcome: AttemptOutcome::Exited { code: 0 },
        runtime: exit0.runtime,
    })?;

    // Epoch bump, then start allocation 2 under the new epoch.
    journal.journal_fencing(Watermark {
        leader_term: 1,
        node_epoch: 2,
    })?;
    let intent2 = IntentRec {
        allocation: ids[2].allocation,
        attempt: ids[2].attempt,
        job: ids[2].job,
        node_epoch: 2,
        max_runtime: None,
        notice_signal: Default::default(),
        notice: Default::default(),
        abort_grace: Default::default(),
    };
    journal.journal_intent(&intent2)?;
    runtime.start(&ids[2]);

    // Stop allocation 1: tombstone journaled before the stop takes effect,
    // then the stop's kill outcome journaled as `Aborted` — deliberately
    // disagreeing with the container's bare 137, so recovery must prefer the
    // journaled classification (a crash before this journal_exit ⇒ runtime
    // evidence still wins on recovery).
    journal.journal_tombstone(ids[1].allocation, TombstoneReason::Abort)?;
    let exit1 = ExitInfo {
        code: 137,
        cause: ExitCause::Natural,
        runtime: Duration::from_micros(222),
        finished_at: Timestamp::UNIX_EPOCH,
    };
    runtime.exit(&ids[1], exit1);
    journal.journal_exit(&ExitRec {
        allocation: ids[1].allocation,
        attempt: ids[1].attempt,
        job: ids[1].job,
        outcome: AttemptOutcome::Aborted,
        runtime: exit1.runtime,
    })?;

    // Allocation 2 is OOM-killed; runtime observes it, then we journal it.
    let exit2 = ExitInfo {
        code: 137,
        cause: ExitCause::OomKilled,
        runtime: Duration::from_micros(333),
        finished_at: Timestamp::UNIX_EPOCH,
    };
    runtime.exit(&ids[2], exit2);
    journal.journal_exit(&ExitRec {
        allocation: ids[2].allocation,
        attempt: ids[2].attempt,
        job: ids[2].job,
        outcome: AttemptOutcome::MemoryLimitExceeded,
        runtime: exit2.runtime,
    })?;

    // Allocation 3 runs the full interruption sequence (ADR 0044 §3, §5),
    // including a replacement plan and mark, through to the plan's retirement.
    let notice = Duration::from_secs(60);
    let grace = Duration::from_secs(10);
    journal.journal_intent(&IntentRec {
        allocation: ids[3].allocation,
        attempt: ids[3].attempt,
        job: ids[3].job,
        node_epoch: 2,
        max_runtime: None,
        notice_signal: Default::default(),
        notice,
        abort_grace: grace,
    })?;
    runtime.start(&ids[3]);

    let interruption = PreemptReason::Interruption;
    let notified_at = Timestamp::from_micros(1_800_000_000_000_000).unwrap();
    let d1 = notified_at + Duration::from_secs(120);
    journal.journal_drain_plan(&DrainPlanRec {
        deadline: d1,
        reason: interruption,
        accepted_target: None,
    })?;
    runtime.plan = Some(d1);
    journal.journal_notice_intent(ids[3].allocation, interruption)?;
    runtime.signaled.insert(ids[3].allocation);
    let first = NoticeMarkRec {
        stop_at: d1 - Duration::from_secs(40),
        shortfall: Duration::ZERO,
        notified_at,
    };
    journal.journal_notice_mark(ids[3].allocation, &first)?;
    runtime
        .reported_stops
        .insert(ids[3].allocation, first.stop_at);

    let d2 = d1 - Duration::from_secs(40);
    journal.journal_drain_plan(&DrainPlanRec {
        deadline: d2,
        reason: interruption,
        accepted_target: None,
    })?;
    runtime.plan = Some(d2);
    let replacement = NoticeMarkRec {
        stop_at: d2 - Duration::from_secs(40),
        shortfall: Duration::from_secs(20),
        notified_at,
    };
    journal.journal_notice_mark(ids[3].allocation, &replacement)?;
    runtime
        .reported_stops
        .insert(ids[3].allocation, replacement.stop_at);

    journal.journal_tombstone(
        ids[3].allocation,
        TombstoneReason::Preempt {
            reason: interruption,
        },
    )?;
    // The workload checkpointed on the signal and exits 143 to the stop's
    // SIGTERM: a non-zero exit after a notice (crash before the journaled exit
    // ⇒ the notice intent classifies it `Preempted` on recovery).
    let exit3 = ExitInfo {
        code: 143,
        cause: ExitCause::Natural,
        runtime: Duration::from_micros(444),
        finished_at: Timestamp::UNIX_EPOCH,
    };
    runtime.exit(&ids[3], exit3);
    journal.journal_exit(&ExitRec {
        allocation: ids[3].allocation,
        attempt: ids[3].attempt,
        job: ids[3].job,
        outcome: AttemptOutcome::Preempted {
            reason: interruption,
        },
        runtime: exit3.runtime,
    })?;
    journal.journal_drain_plan_retired()?;
    runtime.plan_retired = true;

    Ok(())
}

/// Count the seam ops a clean (crash-free) run of the workload consumes.
fn clean_op_count(ids: &[Ids]) -> u64 {
    let fs = SimFs::new(SimConfig::default());
    let mut runtime = RuntimeModel::default();
    drive(fs.clone(), &mut runtime, ids).expect("clean drive must not fail");
    fs.op_count()
}

/// The four invariants (a)–(c); (d) is checked by the callers.
fn check_invariants(
    recovered: &JournalState,
    runtime: &RuntimeModel,
    observed: &[ObservedAllocation],
    ctx: &str,
) {
    let by_alloc: BTreeMap<AllocationId, &ObservedAllocation> =
        observed.iter().map(|o| (o.allocation, o)).collect();

    // (a) + (c): every runtime container appears truthfully and has a durable
    // intent behind it.
    for container in runtime.containers.values() {
        let entry = by_alloc.get(&container.allocation).unwrap_or_else(|| {
            panic!(
                "{ctx}: runtime container {} was forgotten",
                container.allocation
            )
        });
        match container.state {
            ContainerState::Running { .. } => {
                assert!(
                    entry.running,
                    "{ctx}: running container {} reported not running",
                    container.allocation
                );
            }
            ContainerState::Exited(info) => {
                assert!(
                    !entry.running,
                    "{ctx}: exited container {} reported running",
                    container.allocation
                );
                // The journaled classification is the truth when the exit
                // reached the journal (it carries kill attribution the bare
                // exit code cannot); the runtime's classification stands only
                // for unjournaled exits.
                let expected = recovered
                    .exits
                    .get(&container.allocation)
                    .map(|rec| rec.outcome.clone())
                    .unwrap_or_else(|| {
                        classify_exit(&info, &recovered.attribution(&container.allocation))
                    });
                assert_eq!(
                    entry.outcome,
                    Some(expected),
                    "{ctx}: exited container {} misclassified",
                    container.allocation
                );
            }
        }
        assert!(
            recovered.intents.contains_key(&container.allocation),
            "{ctx}: runtime container {} has no durable intent (fsync-before-start violated)",
            container.allocation
        );
    }

    // (e): every delivered signal has a durable notice intent behind it.
    for alloc in &runtime.signaled {
        assert!(
            recovered.notice_intents.contains_key(alloc),
            "{ctx}: {alloc} was signalled with no durable notice intent"
        );
    }

    // (f): no recovered stop time is later than one already reported.
    for (alloc, reported) in &runtime.reported_stops {
        let mark = recovered
            .marks
            .get(alloc)
            .unwrap_or_else(|| panic!("{ctx}: {alloc} reported a mark the journal lost"));
        assert!(
            mark.stop_at <= *reported,
            "{ctx}: {alloc} recovered stop {} is later than the reported {}",
            mark.stop_at,
            reported
        );
    }

    // (g): the plan in force is the latest one, and a retirement sticks. (A
    // record can land even when its append reported the crash, so recovery
    // may be one step ahead of the model — never behind it.)
    if runtime.plan_retired {
        assert!(recovered.plan.is_none(), "{ctx}: a retired plan came back");
    } else if let Some(acted_on) = runtime.plan {
        if let Some(plan) = recovered.plan {
            assert!(
                plan.deadline <= acted_on,
                "{ctx}: recovered plan deadline {} is later than the one acted on {}",
                plan.deadline,
                acted_on
            );
        } else {
            // Only the retirement, which follows every other step, can have
            // removed a plan that was acted on.
            assert!(
                recovered.exits.contains_key(&ids_of_noticed(runtime)),
                "{ctx}: a plan that was acted on is missing"
            );
        }
    }

    // (b): no entry claims running without a running runtime container.
    for entry in observed {
        if entry.running {
            let container = runtime
                .containers
                .get(&entry.allocation)
                .unwrap_or_else(|| {
                    panic!(
                        "{ctx}: {} claimed running with no runtime evidence",
                        entry.allocation
                    )
                });
            assert!(
                matches!(container.state, ContainerState::Running { .. }),
                "{ctx}: {} claimed running but the runtime shows it exited",
                entry.allocation
            );
        }
    }
}

/// The allocation the workload notifies — the only one that is ever signalled.
fn ids_of_noticed(runtime: &RuntimeModel) -> AllocationId {
    *runtime
        .signaled
        .iter()
        .next()
        .expect("a plan is only acted on alongside a notice")
}

fn recover(fs: &SimFs, ctx: &str) -> JournalState {
    Journal::open(fs.clone())
        .unwrap_or_else(|e| panic!("{ctx}: recovery failed to open: {e}"))
        .1
}

const SEEDS_PER_POINT: u64 = 4;

#[test]
fn crash_journal_sweep() {
    let ids = fixed_ids();
    let total = clean_op_count(&ids);
    assert!(total > 0, "the workload must touch the seam");

    for k in 0..total {
        for s in 0..SEEDS_PER_POINT {
            let seed = 0x0A9E_5EED ^ (k << 8) ^ s;

            // Drive to a crash at op k, then settle the disk with the seed.
            let fs = SimFs::new(SimConfig::default());
            fs.set_crash_at(k);
            let mut runtime = RuntimeModel::default();
            if let Err(e) = drive(fs.clone(), &mut runtime, &ids) {
                assert!(
                    is_sim_crash(&e),
                    "k={k} seed={seed}: unexpected drive error: {e}"
                );
            }
            fs.crash(seed);
            fs.disarm();

            let ctx = format!("k={k} seed={seed:#x}");

            // Recover and check the invariants.
            let recovered = recover(&fs, &ctx);
            let observed = build_observed_set(&recovered, &runtime.observe());
            check_invariants(&recovered, &runtime, &observed, &ctx);

            // (d) double-recovery idempotence.
            let again = recover(&fs, &ctx);
            assert_eq!(recovered, again, "{ctx}: recovery is not idempotent");
        }
    }
}

#[test]
fn crash_during_recovery() {
    let ids = fixed_ids();

    // Produce a settled disk deterministically: crash the drive partway
    // through an append so the journal has a torn tail, then settle it. Same
    // (drive_k, seed) always yields the same disk, so we can rebuild it for
    // every recovery crash point.
    let drive_k = clean_op_count(&ids) / 2;
    let settle_seed = 0xBEEF_u64;

    let settled = |drive_k: u64| -> (SimFs, RuntimeModel) {
        let fs = SimFs::new(SimConfig::default());
        fs.set_crash_at(drive_k);
        let mut runtime = RuntimeModel::default();
        let _ = drive(fs.clone(), &mut runtime, &ids);
        fs.crash(settle_seed);
        fs.disarm();
        (fs, runtime)
    };

    // Measure the seam-op range of recovery on this disk.
    let (probe_fs, _) = settled(drive_k);
    let pre = probe_fs.op_count();
    recover(&probe_fs, "probe");
    let rec_ops = probe_fs.op_count() - pre;
    assert!(rec_ops > 0, "recovery must touch the seam");

    for j in 0..rec_ops {
        for rs in 0..3u64 {
            let (fs, runtime) = settled(drive_k);
            let recovery_seed = 0x0DEC_0DE1 ^ (j << 8) ^ rs;

            let pre = fs.op_count();
            fs.set_crash_at(pre + j);
            if let Ok((_journal, _state)) = Journal::open(fs.clone()) {
                // Recovery completed before the armed point; nothing to crash
                // through, but still settle and re-check below.
            }
            fs.crash(recovery_seed);
            fs.disarm();

            let ctx = format!("recovery-crash j={j} seed={recovery_seed:#x}");
            let recovered = recover(&fs, &ctx);
            let observed = build_observed_set(&recovered, &runtime.observe());
            check_invariants(&recovered, &runtime, &observed, &ctx);

            let again = recover(&fs, &ctx);
            assert_eq!(recovered, again, "{ctx}: recovery is not idempotent");
        }
    }
}

/// Every notice-era record survives the compaction rewrite field for field
/// (`canonical_records` round-trip), and a superseded mark or plan is dropped
/// down to the one in force.
#[test]
fn notice_records_round_trip_through_compaction() {
    let ids = fixed_ids();
    let fs = SimFs::new(SimConfig::default());
    let interruption = PreemptReason::Interruption;
    let at = |s: i64| Timestamp::from_micros(1_800_000_000_000_000 + s * 1_000_000).unwrap();
    let (mut journal, _) = Journal::open(fs.clone()).unwrap();
    journal
        .journal_intent(&IntentRec {
            allocation: ids[0].allocation,
            attempt: ids[0].attempt,
            job: ids[0].job,
            node_epoch: 1,
            max_runtime: None,
            notice_signal: coppice_core::preemption::NoticeSignal::Sigusr2,
            notice: Duration::from_secs(45),
            abort_grace: Duration::from_secs(7),
        })
        .unwrap();
    journal
        .journal_drain_plan(&DrainPlanRec {
            deadline: at(120),
            reason: interruption,
            accepted_target: Some(at(30)),
        })
        .unwrap();
    journal
        .journal_drain_plan(&DrainPlanRec {
            deadline: at(100),
            reason: interruption,
            accepted_target: None,
        })
        .unwrap();
    journal
        .journal_notice_intent(ids[0].allocation, interruption)
        .unwrap();
    for (stop, shortfall) in [(80, 0), (60, 5)] {
        journal
            .journal_notice_mark(
                ids[0].allocation,
                &NoticeMarkRec {
                    stop_at: at(stop),
                    shortfall: Duration::from_secs(shortfall),
                    notified_at: at(0),
                },
            )
            .unwrap();
    }
    journal
        .journal_tombstone(
            ids[1].allocation,
            TombstoneReason::Preempt {
                reason: PreemptReason::Priority {
                    for_job: ids[2].job,
                },
            },
        )
        .unwrap();
    drop(journal);

    let first = recover(&fs, "first");
    let intent = first.intents[&ids[0].allocation];
    assert_eq!(
        intent.notice_signal,
        coppice_core::preemption::NoticeSignal::Sigusr2
    );
    assert_eq!(intent.notice, Duration::from_secs(45));
    assert_eq!(intent.abort_grace, Duration::from_secs(7));
    assert_eq!(
        first.plan,
        Some(DrainPlanRec {
            deadline: at(100),
            reason: interruption,
            accepted_target: None,
        })
    );
    assert_eq!(first.notice_intents[&ids[0].allocation], interruption);
    assert_eq!(
        first.marks[&ids[0].allocation],
        NoticeMarkRec {
            stop_at: at(60),
            shortfall: Duration::from_secs(5),
            notified_at: at(0),
        }
    );
    assert_eq!(
        first.tombstones[&ids[1].allocation],
        TombstoneReason::Preempt {
            reason: PreemptReason::Priority {
                for_job: ids[2].job
            }
        }
    );
    // The compacted file reproduces the state exactly.
    assert_eq!(recover(&fs, "second"), first);

    // A retirement drops the plan for good, through compaction too.
    let (mut journal, _) = Journal::open(fs.clone()).unwrap();
    journal.journal_drain_plan_retired().unwrap();
    drop(journal);
    let retired = recover(&fs, "retired");
    assert!(retired.plan.is_none());
    assert_eq!(retired.marks, first.marks);
    assert_eq!(recover(&fs, "retired again"), retired);
}
