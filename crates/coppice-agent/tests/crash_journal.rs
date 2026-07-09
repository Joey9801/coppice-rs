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
//! - (d) recovery is idempotent (double-open yields identical state).
//!
//! A second test crashes *inside recovery* (the compaction rewrite) and
//! re-recovers, checking the same invariants.

use std::collections::BTreeMap;
use std::io;

use coppice_agent::executor::{classify_exit, ContainerState, ExitInfo, ObservedContainer};
use coppice_agent::journal::{ExitRec, IntentRec, Journal, JournalState, Watermark};
use coppice_agent::observed::{build_observed_set, ObservedAllocation};
use coppice_core::attempt::AttemptOutcome;
use coppice_core::id::{AllocationId, AttemptId, JobId};
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
    (0u128..3)
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
}

impl RuntimeModel {
    fn start(&mut self, ids: &Ids) {
        self.containers.insert(
            ids.allocation,
            ObservedContainer {
                allocation: ids.allocation,
                attempt: ids.attempt,
                job: ids.job,
                state: ContainerState::Running { runtime_us: 0 },
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
        };
        journal.journal_intent(&intent)?;
        runtime.start(&ids[i]);
    }

    // Allocation 0 exits naturally; the runtime observes it, then we journal
    // the classified exit (crash here ⇒ runtime evidence still wins on
    // recovery).
    let exit0 = ExitInfo {
        code: 0,
        oom_killed: false,
        runtime_us: 111,
    };
    runtime.exit(&ids[0], exit0);
    journal.journal_exit(&ExitRec {
        allocation: ids[0].allocation,
        attempt: ids[0].attempt,
        job: ids[0].job,
        outcome: AttemptOutcome::Exited { code: 0 },
        runtime_us: exit0.runtime_us,
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
    };
    journal.journal_intent(&intent2)?;
    runtime.start(&ids[2]);

    // Stop allocation 1: tombstone journaled before the stop takes effect.
    journal.journal_tombstone(ids[1].allocation)?;
    runtime.exit(
        &ids[1],
        ExitInfo {
            code: 137,
            oom_killed: false,
            runtime_us: 222,
        },
    );

    // Allocation 2 is OOM-killed; runtime observes it, then we journal it.
    let exit2 = ExitInfo {
        code: 137,
        oom_killed: true,
        runtime_us: 333,
    };
    runtime.exit(&ids[2], exit2);
    journal.journal_exit(&ExitRec {
        allocation: ids[2].allocation,
        attempt: ids[2].attempt,
        job: ids[2].job,
        outcome: AttemptOutcome::OomKilled,
        runtime_us: exit2.runtime_us,
    })?;

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
                assert_eq!(
                    entry.outcome,
                    Some(classify_exit(&info)),
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
