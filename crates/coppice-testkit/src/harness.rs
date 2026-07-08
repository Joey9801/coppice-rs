//! The crash-injection driver.
//!
//! This is the executable form of the contract in
//! `docs/architecture/storage-testing.md` §"The crash harness". It drives a
//! storage implementation (any [`CrashSubject`]) through a workload against
//! [`SimFs`], kills the simulated process at every seam-operation index in
//! turn, runs recovery on each disk the seeded adversary can produce, and
//! asserts the six durability invariants. It knows nothing about any concrete
//! engine — the toy in [`crate::toy`] and the real segment engine both plug in
//! through the same trait, so the suite that guards storage crash-safety is
//! written once and cannot be edited to accommodate a broken engine.
//!
//! # The model of acknowledged state
//!
//! The driver is single-threaded, so at most one operation is ever in flight
//! at a crash. The harness keeps a [`Model`] of *acknowledged* state — an
//! operation enters the model only when [`CrashSubject::apply`] returned `Ok`
//! (the durability barrier the engine promises). The recovered state must then
//! be explainable as *the acknowledged model, plus at most the single
//! in-flight operation*: fully applied for atomic operations, or **any prefix**
//! of the batch for an in-flight append (the group commit had not fsynced, so
//! the adversary may keep, drop, or tear each frame — recovery's torn-tail rule
//! collapses whatever survived to a contiguous prefix).
//!
//! # Reproducing a failure
//!
//! Every failure panics with the full reproduction triple —
//! `scenario`, `crash_at` (the seam-op index), `adversary_seed` — plus a diff
//! of expected-vs-observed and the invariant that fired. Named scenarios are
//! exhaustive and deterministic, so re-running the test reproduces the
//! failure; the randomized sweep is pinned by `COPPICE_CRASH_SEED`.

use std::collections::BTreeMap;

use crate::rng::Rng;
use crate::simfs::{is_sim_crash, SimConfig, SimFs};

/// The abstract operation vocabulary of the log-storage layer. Payloads are
/// opaque bytes — the harness never interprets them, so the same driver serves
/// the toy and the real protobuf engine unchanged.
#[derive(Debug, Clone)]
pub enum StorageOp {
    /// Entries at contiguous indices starting at the current end+1; payloads
    /// opaque.
    AppendBatch { payloads: Vec<Vec<u8>> },
    /// Raft vote: (term, voted_for). Callers only issue non-regressing votes.
    SetVote { term: u64, voted_for: u64 },
    /// Discard indices >= from (Raft conflict resolution).
    TruncateSuffix { from: u64 },
    /// Drop indices <= upto (snapshot covered them).
    Purge { upto: u64 },
    /// Make a snapshot current: covers indices <= upto_index, opaque payload.
    InstallSnapshot { upto_index: u64, payload: Vec<u8> },
    /// Force a segment rotation (new active segment + manifest write).
    Rotate,
}

/// What a recovered store reports. Entry map is index -> payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observed {
    pub entries: BTreeMap<u64, Vec<u8>>,
    pub vote: Option<(u64, u64)>,
    /// (upto_index, payload) of the current snapshot, if any.
    pub snapshot: Option<(u64, Vec<u8>)>,
    /// Last purged index (0 = nothing purged).
    pub purge_floor: u64,
}

/// A storage engine the harness can crash-test. The methods are the whole of
/// what the harness needs: bring a fresh directory into existence, open it
/// through the full recovery procedure, apply one abstract operation
/// (returning `Ok` only once it is acknowledged durable), and report observed
/// state.
pub trait CrashSubject {
    type Store;
    /// Initialize a fresh data directory (identity stamps etc.). Runs once per
    /// fs, always un-armed, so it need not itself be crash-safe.
    fn init(&self, fs: &SimFs) -> std::io::Result<()>;
    /// Open through the full recovery procedure.
    fn open(&self, fs: &SimFs) -> std::io::Result<Self::Store>;
    /// Ok(()) = the operation is acknowledged durable.
    fn apply(&self, store: &mut Self::Store, op: &StorageOp) -> std::io::Result<()>;
    fn observe(&self, store: &Self::Store) -> Observed;
}

/// The acknowledged-state model: exactly the operations `apply` accepted.
#[derive(Debug, Clone)]
struct Model {
    entries: BTreeMap<u64, Vec<u8>>,
    /// One past the highest live index (the index the next append would take).
    next_index: u64,
    vote: Option<(u64, u64)>,
    snapshot: Option<(u64, Vec<u8>)>,
    purge_floor: u64,
}

impl Model {
    fn new() -> Model {
        Model {
            entries: BTreeMap::new(),
            next_index: 1,
            vote: None,
            snapshot: None,
            purge_floor: 0,
        }
    }

    fn observed(&self) -> Observed {
        Observed {
            entries: self.entries.clone(),
            vote: self.vote,
            snapshot: self.snapshot.clone(),
            purge_floor: self.purge_floor,
        }
    }

    fn append_one(&mut self, payload: Vec<u8>) {
        self.entries.insert(self.next_index, payload);
        self.next_index += 1;
    }

    fn apply(&mut self, op: &StorageOp) {
        match op {
            StorageOp::AppendBatch { payloads } => {
                for p in payloads {
                    self.append_one(p.clone());
                }
            }
            StorageOp::SetVote { term, voted_for } => self.vote = Some((*term, *voted_for)),
            StorageOp::TruncateSuffix { from } => {
                self.entries.retain(|&i, _| i < *from);
                self.next_index = (*from).max(self.purge_floor + 1);
            }
            StorageOp::Purge { upto } => {
                self.entries.retain(|&i, _| i > *upto);
                self.purge_floor = self.purge_floor.max(*upto);
                self.next_index = self.next_index.max(self.purge_floor + 1);
            }
            StorageOp::InstallSnapshot { upto_index, payload } => {
                self.snapshot = Some((*upto_index, payload.clone()));
            }
            StorageOp::Rotate => {}
        }
    }
}

/// Configuration for [`crash_sweep`].
#[derive(Debug, Clone)]
pub struct SweepConfig {
    /// How many independent adversary seeds to settle the disk with at each
    /// crash point. More seeds explore more surviving-disk shapes per point;
    /// three is a good default for the exhaustive named scenarios.
    pub adversary_seeds_per_point: u64,
    /// Whether to also crash *during recovery* at each point (invariant 6).
    pub recovery_crash: bool,
}

impl Default for SweepConfig {
    fn default() -> SweepConfig {
        SweepConfig { adversary_seeds_per_point: 3, recovery_crash: true }
    }
}

/// Exhaustively crash-test `subject` under `workload`: crash at every seam-op
/// index the workload reaches, under several adversary seeds, and check the
/// invariants after each recovery. This is what a named scenario calls.
pub fn crash_sweep<S: CrashSubject>(
    scenario: &str,
    subject: &S,
    workload: &[StorageOp],
    cfg: &SweepConfig,
) {
    // Step 1: a clean pass must acknowledge every operation, and reopening the
    // result must observe exactly the model (clean-restart idempotence).
    let (base, total, model) = clean_pass(scenario, subject, workload);
    let _ = model;

    // Step 2 + 3: every crash point in the post-init range.
    for k in base..total {
        for s in 0..cfg.adversary_seeds_per_point {
            let adv = adversary_seed(scenario, k, s);
            run_crash_point(scenario, subject, workload, k, adv, cfg.recovery_crash && s == 0);
        }
    }
}

/// Drive the workload with no faults, asserting every op acks and that a clean
/// reopen is idempotent. Returns `(base, total, model)` where `base` is the
/// seam-op count consumed by `init` alone (crash points below it live inside
/// initialization, which is out of scope) and `total` is the count after the
/// whole clean run.
fn clean_pass<S: CrashSubject>(
    scenario: &str,
    subject: &S,
    workload: &[StorageOp],
) -> (u64, u64, Model) {
    let fs = SimFs::new(SimConfig::default());
    subject.init(&fs).expect("clean init");
    let mut store = subject.open(&fs).expect("clean open");
    let mut model = Model::new();
    for (i, op) in workload.iter().enumerate() {
        subject
            .apply(&mut store, op)
            .unwrap_or_else(|e| panic!("scenario={scenario}: clean apply of op {i} ({}) failed: {e}", op_kind(op)));
        model.apply(op);
    }
    let total = fs.op_count();
    assert_eq!(
        subject.observe(&store),
        model.observed(),
        "scenario={scenario}: clean observe disagrees with the model"
    );
    drop(store);
    let reopened = subject.open(&fs).expect("clean reopen");
    assert_eq!(
        subject.observe(&reopened),
        model.observed(),
        "scenario={scenario}: clean reopen is not idempotent"
    );
    drop(reopened);

    // `base` is init's own seam-op count, measured in isolation so crash points
    // inside init (un-armed, out of scope) are excluded from the sweep.
    let init_fs = SimFs::new(SimConfig::default());
    subject.init(&init_fs).expect("init for base measurement");
    let base = init_fs.op_count();
    assert!(base <= total, "scenario={scenario}: init consumed more ops than the whole run");
    (base, total, model)
}

/// One crash point: drive to a crash at seam-op `k`, settle the disk with
/// `adv`, recover, and check the invariants — including the double-open and
/// (optionally) the crash-during-recovery checks of invariant 6.
fn run_crash_point<S: CrashSubject>(
    scenario: &str,
    subject: &S,
    workload: &[StorageOp],
    k: u64,
    adv: u64,
    recovery_crash: bool,
) {
    let (fs, model, inflight) = drive_and_crash(subject, workload, k, adv);

    let pre = fs.op_count();
    let store = subject.open(&fs).unwrap_or_else(|e| {
        panic!(
            "crash harness failure: scenario={scenario} crash_at={k} adversary_seed={adv:#x}: \
             recovery refused to open after an ordinary crash: {e}\n  disk: {}",
            summarize_disk(&fs)
        )
    });
    let rec_ops = fs.op_count() - pre;
    let obs = subject.observe(&store);
    if let Err(msg) = check(&model, inflight.as_ref(), &obs) {
        panic!(
            "crash harness failure: scenario={scenario} crash_at={k} adversary_seed={adv:#x}: {msg}\n  disk: {}",
            summarize_disk(&fs)
        );
    }

    // Invariant 6, part 1: opening an already-recovered directory again
    // observes identical state.
    drop(store);
    let store_b = subject.open(&fs).expect("second open of a recovered directory");
    let obs_b = subject.observe(&store_b);
    assert_eq!(
        obs, obs_b,
        "crash harness failure: scenario={scenario} crash_at={k} adversary_seed={adv:#x}: \
         invariant 6 (recovery idempotence): double-open observed differently"
    );
    drop(store_b);

    // Invariant 6, part 2: recovery may itself crash and must still converge.
    if recovery_crash && rec_ops > 0 {
        crash_during_recovery(scenario, subject, workload, k, adv, rec_ops, &model, inflight.as_ref());
    }
}

/// Rebuild the identical post-crash disk, then kill the process again at a
/// seeded point *inside recovery's own seam-op range*, settle with a second
/// adversary, and recover once more. The final state must satisfy the same
/// invariants — recovery's own writes (torn-tail truncation, orphan deletion)
/// have to be crash-safe too.
#[allow(clippy::too_many_arguments)]
fn crash_during_recovery<S: CrashSubject>(
    scenario: &str,
    subject: &S,
    workload: &[StorageOp],
    k: u64,
    adv: u64,
    rec_ops: u64,
    model: &Model,
    inflight: Option<&StorageOp>,
) {
    let (fs, _m, _i) = drive_and_crash(subject, workload, k, adv);
    let mut rng = Rng::new(adv ^ k.rotate_left(23) ^ 0x5EED_0BAD_C0DE);
    let pre = fs.op_count();
    let point = pre + rng.below(rec_ops);
    let seed2 = rng.next_u64();

    fs.set_crash_at(point);
    match subject.open(&fs) {
        Ok(_) => {
            // The armed point lay inside recovery's range, so recovery should
            // have died before returning a store.
            panic!(
                "crash harness failure: scenario={scenario} crash_at={k} adversary_seed={adv:#x}: \
                 invariant 6: recovery unexpectedly completed with a crash armed at op {point}"
            );
        }
        Err(e) => assert!(
            is_sim_crash(&e),
            "crash harness failure: scenario={scenario} crash_at={k}: recovery crash at op {point} \
             raised a non-crash error: {e}"
        ),
    }
    fs.crash(seed2);
    fs.disarm();

    let store = subject.open(&fs).unwrap_or_else(|e| {
        panic!(
            "crash harness failure: scenario={scenario} crash_at={k} adversary_seed={adv:#x} \
             recovery_crash_at={point} recovery_seed={seed2:#x}: \
             re-recovery refused to open: {e}\n  disk: {}",
            summarize_disk(&fs)
        )
    });
    let obs = subject.observe(&store);
    if let Err(msg) = check(model, inflight, &obs) {
        panic!(
            "crash harness failure: scenario={scenario} crash_at={k} adversary_seed={adv:#x} \
             recovery_crash_at={point} recovery_seed={seed2:#x}: {msg}\n  disk: {}",
            summarize_disk(&fs)
        );
    }
}

/// Build a fresh fs, initialize it un-armed, arm a crash at seam-op `k`, drive
/// the workload recording acknowledgements, and settle the disk with `adv`.
/// Returns the crashed (unpoisoned, fully durable) fs, the acknowledged model,
/// and the single in-flight operation (if the crash landed inside one rather
/// than inside recovery's own `open`).
fn drive_and_crash<S: CrashSubject>(
    subject: &S,
    workload: &[StorageOp],
    k: u64,
    adv: u64,
) -> (SimFs, Model, Option<StorageOp>) {
    let fs = SimFs::new(SimConfig::default());
    subject.init(&fs).expect("init runs un-armed and must not fail");
    fs.set_crash_at(k);

    let mut model = Model::new();
    let mut inflight = None;
    match subject.open(&fs) {
        // The crash landed inside recovery's open of the freshly-initialized
        // directory: nothing acknowledged, no storage op in flight.
        Err(e) => assert!(
            is_sim_crash(&e),
            "armed open at k={k} raised a non-crash error (subject bug): {e}"
        ),
        Ok(mut store) => {
            for op in workload {
                match subject.apply(&mut store, op) {
                    Ok(()) => model.apply(op),
                    Err(e) => {
                        assert!(
                            is_sim_crash(&e),
                            "armed apply of {} at k={k} raised a non-crash error (subject bug): {e}",
                            op_kind(op)
                        );
                        inflight = Some(op.clone());
                        break;
                    }
                }
            }
        }
    }
    fs.crash(adv);
    fs.disarm();
    (fs, model, inflight)
}

/// Check the recovered `obs` against the acknowledged `model` and the single
/// in-flight op. Standing invariants are tested first, most-specific-first, so
/// the panic names the invariant that broke; the final membership test covers
/// durability + at-most-one-in-flight with a value diff.
fn check(model: &Model, inflight: Option<&StorageOp>, obs: &Observed) -> Result<(), String> {
    // Invariant 3: the recovered log is a contiguous index range.
    let idxs: Vec<u64> = obs.entries.keys().copied().collect();
    for w in idxs.windows(2) {
        if w[1] != w[0] + 1 {
            return Err(format!(
                "invariant 3 (contiguity): observed log has a gap between index {} and {}",
                w[0], w[1]
            ));
        }
    }
    // Invariant 3 / 1: nothing at or below the purge floor may remain.
    if let Some(&min) = idxs.first() {
        if min <= obs.purge_floor {
            return Err(format!(
                "invariant 3 (pessimistic purge floor): entry {min} survives at or below purge_floor {}",
                obs.purge_floor
            ));
        }
    }
    // Invariant 4: the recovered vote is never older than the acknowledged one.
    if let Some((mt, mv)) = model.vote {
        let ok = matches!(obs.vote, Some((ot, _)) if ot >= mt);
        if !ok {
            return Err(format!(
                "invariant 4 (vote monotonicity): acknowledged vote (term {mt}, for {mv}) lost; \
                 observed {:?}",
                obs.vote
            ));
        }
    }
    // Invariant 5: the snapshot is the acknowledged one or the in-flight one,
    // and nothing else (a torn/footer-less snapshot must never be adopted).
    if !allowed_snapshots(model, inflight).iter().any(|s| s == &obs.snapshot) {
        return Err(format!(
            "invariant 5 (snapshot integrity): observed snapshot {:?} is neither the acknowledged \
             snapshot {:?} nor the in-flight one",
            obs.snapshot.as_ref().map(|(u, p)| (*u, p.len())),
            model.snapshot.as_ref().map(|(u, p)| (*u, p.len())),
        ));
    }
    // Invariants 1 + 2: the whole observed value must equal the model, or the
    // model with the single in-flight op applied (any prefix, for an append).
    if candidates(model, inflight).iter().any(|c| c == obs) {
        Ok(())
    } else {
        Err(format!(
            "invariants 1/2 (durability + at-most-one-in-flight): observed [{}] is not explained by \
             the acknowledged model [{}] plus the in-flight op {}",
            summarize(obs),
            summarize(&model.observed()),
            inflight.map_or("<none>", op_kind),
        ))
    }
}

/// The observed states the recovery is allowed to produce: the acknowledged
/// model, plus — for the single in-flight op — the model with it applied (every
/// prefix, for an append batch).
fn candidates(model: &Model, inflight: Option<&StorageOp>) -> Vec<Observed> {
    let mut out = vec![model.observed()];
    if let Some(op) = inflight {
        match op {
            StorageOp::AppendBatch { payloads } => {
                let mut m = model.clone();
                for p in payloads {
                    m.append_one(p.clone());
                    out.push(m.observed());
                }
            }
            other => {
                let mut m = model.clone();
                m.apply(other);
                out.push(m.observed());
            }
        }
    }
    out
}

/// The snapshots recovery may present: the acknowledged one, plus the in-flight
/// one if the interrupted op was a snapshot install.
fn allowed_snapshots(model: &Model, inflight: Option<&StorageOp>) -> Vec<Option<(u64, Vec<u8>)>> {
    let mut out = vec![model.snapshot.clone()];
    if let Some(StorageOp::InstallSnapshot { upto_index, payload }) = inflight {
        out.push(Some((*upto_index, payload.clone())));
    }
    out
}

/// Read `COPPICE_CRASH_SEED` (decimal or `0x`-hex), or a fixed constant. The
/// constant keeps CI runs identical run-to-run; the env var pins a reproduction.
pub fn master_seed() -> u64 {
    const DEFAULT: u64 = 0x_C0FF_EE15_DEAD_5EED;
    match std::env::var("COPPICE_CRASH_SEED") {
        Ok(s) => {
            let s = s.trim();
            let parsed = s
                .strip_prefix("0x")
                .or_else(|| s.strip_prefix("0X"))
                .map(|hex| u64::from_str_radix(hex, 16))
                .unwrap_or_else(|| s.parse::<u64>());
            parsed.unwrap_or_else(|_| panic!("COPPICE_CRASH_SEED is not a u64: {s:?}"))
        }
        Err(_) => DEFAULT,
    }
}

/// A seeded randomized sweep: generate `iterations` arbitrary workloads and, for
/// each, sample a handful of crash points and adversary seeds. Non-exhaustive by
/// design — exhaustiveness is the named scenarios' job; this layers arbitrary
/// op mixes and sizes on top to find orderings the named tests did not think of.
pub fn random_sweep<S: CrashSubject>(subject: &S, iterations: u64) {
    let master = master_seed();
    let mut rng = Rng::new(master ^ 0x_A5A5_5A5A_1234_9876);
    for it in 0..iterations {
        let mut wrng = rng.fork();
        let len = wrng.range(8, 40);
        let workload = random_workload(&mut wrng, len);

        // Bounds for this workload: init-only op count and full-run op count.
        let (base, total) = bounds(subject, &workload);
        if total <= base {
            continue;
        }
        let scenario = format!("random#{it}(master={master:#x})");

        // A handful of seeded crash points, two adversary seeds each.
        let points = 6.min(total - base);
        for _ in 0..points {
            let k = wrng.range(base, total);
            for s in 0..2 {
                let adv = wrng.next_u64() ^ (s as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                // Crash-during-recovery on one of the two seeds keeps runtime sane.
                run_crash_point(&scenario, subject, &workload, k, adv, s == 0);
            }
        }
    }
}

/// Init-only and full-run seam-op counts for `workload`, matching `clean_pass`.
fn bounds<S: CrashSubject>(subject: &S, workload: &[StorageOp]) -> (u64, u64) {
    let init_fs = SimFs::new(SimConfig::default());
    subject.init(&init_fs).expect("init for bounds");
    let base = init_fs.op_count();

    let fs = SimFs::new(SimConfig::default());
    subject.init(&fs).expect("init for bounds");
    let mut store = subject.open(&fs).expect("open for bounds");
    for op in workload {
        // A generated workload must be applyable cleanly; a failure here is a
        // generator bug, not a crash-safety finding.
        subject.apply(&mut store, op).expect("clean apply of generated op");
    }
    (base, fs.op_count())
}

/// Generate a valid arbitrary workload against a shadow model, so truncate /
/// purge / snapshot only ever name indices that exist. Payload sizes straddle
/// the 4 KiB tear boundary (16..2000 bytes) with occasional 10 KiB entries, so
/// multi-entry batches cross page boundaries the adversary can tear at.
fn random_workload(rng: &mut Rng, len: u64) -> Vec<StorageOp> {
    let mut model = Model::new();
    let mut term = 0u64;
    let mut snap_upto = 0u64;
    let mut ops = Vec::new();

    for _ in 0..len {
        let choice = rng.below(100);
        let op = if choice < 46 {
            let n = rng.range(1, 5);
            let payloads = (0..n).map(|_| random_payload(rng)).collect();
            StorageOp::AppendBatch { payloads }
        } else if choice < 60 {
            // Non-regressing vote: term stays or advances.
            term += rng.below(2);
            StorageOp::SetVote { term, voted_for: rng.below(5) }
        } else if choice < 70 {
            StorageOp::Rotate
        } else if choice < 80 {
            // Truncate a suffix that keeps at least the snapshot-covered prefix.
            let lo = model.purge_floor.max(snap_upto) + 1;
            if lo < model.next_index {
                StorageOp::TruncateSuffix { from: rng.range(lo, model.next_index) }
            } else {
                StorageOp::Rotate
            }
        } else if choice < 90 {
            // Install a snapshot covering some existing prefix, never regressing.
            let last = model.next_index.saturating_sub(1);
            let lo = snap_upto.max(model.purge_floor).max(1);
            if last >= lo {
                let upto = rng.range(lo, last + 1);
                snap_upto = upto;
                StorageOp::InstallSnapshot { upto_index: upto, payload: random_snapshot(rng) }
            } else {
                StorageOp::Rotate
            }
        } else {
            // Purge up to a durable snapshot's coverage.
            if snap_upto > model.purge_floor {
                StorageOp::Purge { upto: rng.range(model.purge_floor + 1, snap_upto + 1) }
            } else {
                StorageOp::Rotate
            }
        };
        model.apply(&op);
        ops.push(op);
    }
    ops
}

fn random_payload(rng: &mut Rng) -> Vec<u8> {
    let len = if rng.chance(1, 12) {
        rng.range(9000, 11_000)
    } else {
        rng.range(16, 2000)
    };
    fill(rng, len)
}

fn random_snapshot(rng: &mut Rng) -> Vec<u8> {
    let len = rng.range(8, 300);
    fill(rng, len)
}

/// Deterministic pseudo-random bytes. The exact contents do not matter to the
/// harness — only that they round-trip and differ between entries.
fn fill(rng: &mut Rng, len: u64) -> Vec<u8> {
    (0..len).map(|_| (rng.next_u64() & 0xff) as u8).collect()
}

/// A stable per-(scenario, crash point, seed index) adversary seed, so a named
/// scenario reproduces exactly and the panic's `adversary_seed` re-runs it.
fn adversary_seed(scenario: &str, k: u64, s: u64) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in scenario.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut rng = Rng::new(h ^ k.rotate_left(17) ^ s.rotate_left(41));
    rng.next_u64()
}

fn op_kind(op: &StorageOp) -> &'static str {
    match op {
        StorageOp::AppendBatch { .. } => "AppendBatch",
        StorageOp::SetVote { .. } => "SetVote",
        StorageOp::TruncateSuffix { .. } => "TruncateSuffix",
        StorageOp::Purge { .. } => "Purge",
        StorageOp::InstallSnapshot { .. } => "InstallSnapshot",
        StorageOp::Rotate => "Rotate",
    }
}

/// A compact one-line rendering of an [`Observed`] for panic diffs.
fn summarize(obs: &Observed) -> String {
    let entries = match (obs.entries.keys().next(), obs.entries.keys().next_back()) {
        (Some(lo), Some(hi)) => format!("entries {lo}..={hi} ({})", obs.entries.len()),
        _ => "entries <empty>".to_string(),
    };
    format!(
        "{entries}, vote {:?}, snapshot {:?}, purge_floor {}",
        obs.vote,
        obs.snapshot.as_ref().map(|(u, p)| format!("upto={u},len={}", p.len())),
        obs.purge_floor,
    )
}

/// File names and sizes on the crashed disk, for the panic's reproduction dump.
fn summarize_disk(fs: &SimFs) -> String {
    let mut parts: Vec<String> = fs.dump().iter().map(|(p, b)| format!("{p}({})", b.len())).collect();
    parts.sort();
    parts.join(" ")
}
