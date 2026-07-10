//! The mandated crash scenarios (ADR 0002 + ADR 0017), run against the toy
//! reference engine.
//!
//! Each named test drives one structural transition through an exhaustive
//! crash-point sweep: the harness kills the simulated process at *every*
//! filesystem operation the scenario reaches, settles each disk with several
//! seeded adversaries, recovers, and checks the durability invariants of
//! `docs/architecture/storage-testing.md`. The randomized sweep layers
//! arbitrary op mixes on top; pin it with `COPPICE_CRASH_SEED` to reproduce a
//! failure.
//!
//! When the real segment engine lands it implements `CrashSubject` and runs
//! these identical scenarios; the toy stays as the harness's own regression
//! subject.

use coppice_testkit::harness::{crash_sweep, random_sweep, StorageOp, SweepConfig};
use coppice_testkit::rng::Rng;
use coppice_testkit::toy::ToyConfig;

/// Deterministic payload bytes; sizes chosen per scenario to straddle the
/// 4 KiB tear granularity where it matters.
fn batch(rng: &mut Rng, sizes: &[u64]) -> StorageOp {
    StorageOp::AppendBatch {
        payloads: sizes
            .iter()
            .map(|&n| (0..n).map(|_| (rng.next_u64() & 0xff) as u8).collect())
            .collect(),
    }
}

fn sweep(scenario: &str, workload: &[StorageOp]) {
    crash_sweep(
        scenario,
        &ToyConfig::default(),
        workload,
        &SweepConfig::default(),
    );
}

#[test]
fn crash_during_append() {
    let mut rng = Rng::new(0xA99E);
    // Group-committed batches: single entries, a batch straddling the 4 KiB
    // tear boundary (6000 B), and a multi-entry batch whose frames can
    // survive partially.
    let workload = [
        batch(&mut rng, &[100]),
        batch(&mut rng, &[6000]),
        batch(&mut rng, &[300, 4500, 16, 900]),
        batch(&mut rng, &[40, 40]),
    ];
    sweep("append", &workload);
}

#[test]
fn crash_during_rotation() {
    let mut rng = Rng::new(0x0707);
    // Explicit rotations between appends, plus one automatic rotation forced
    // by a small threshold.
    let small = ToyConfig {
        rotation_threshold: 2048,
        ..ToyConfig::default()
    };
    let workload = [
        batch(&mut rng, &[500, 500]),
        StorageOp::Rotate,
        batch(&mut rng, &[3000]), // crosses the small threshold
        batch(&mut rng, &[100]),  // triggers the automatic rotation
        StorageOp::Rotate,        // rotating a fresh segment is a no-op
        StorageOp::Rotate,
        batch(&mut rng, &[64]),
    ];
    crash_sweep("rotation", &small, &workload, &SweepConfig::default());
}

#[test]
fn crash_during_suffix_truncation() {
    let mut rng = Rng::new(0x7A7A);
    let workload = [
        batch(&mut rng, &[200, 200, 200]),
        StorageOp::Rotate,
        batch(&mut rng, &[200, 200, 200]), // entries 4..=6 in segment 4
        // Mid-segment: drops 5..=6, leaves stale bytes in segment 4.
        StorageOp::TruncateSuffix { from: 5 },
        batch(&mut rng, &[900]), // entry 5 in a fresh segment
        // Segment-boundary: drops the fresh segment 5 entirely.
        StorageOp::TruncateSuffix { from: 5 },
        batch(&mut rng, &[31, 32]),
    ];
    sweep("suffix-truncation", &workload);
}

#[test]
fn crash_during_purge() {
    let mut rng = Rng::new(0x9096);
    let workload = [
        batch(&mut rng, &[300, 300]),
        StorageOp::Rotate,
        batch(&mut rng, &[300, 300]),
        StorageOp::Rotate,
        batch(&mut rng, &[300]),
        StorageOp::InstallSnapshot {
            upto_index: 4,
            payload: vec![0x51; 128],
        },
        // Floor mid-segment-2: only segment 1 is fully covered and deleted.
        StorageOp::Purge { upto: 3 },
        // Floor at the segment-2 boundary: segment 2 goes too.
        StorageOp::Purge { upto: 4 },
        batch(&mut rng, &[77]),
    ];
    sweep("purge", &workload);
}

#[test]
fn crash_during_snapshot_install() {
    let mut rng = Rng::new(0x5A9);
    let workload = [
        batch(&mut rng, &[100, 100, 100, 100]),
        // First install, then a second superseding it: exercises
        // previous-retained-until-the-new-one-is-durable at every crash point.
        StorageOp::InstallSnapshot {
            upto_index: 2,
            payload: vec![0xAA; 5000],
        },
        batch(&mut rng, &[100]),
        StorageOp::InstallSnapshot {
            upto_index: 4,
            payload: vec![0xBB; 900],
        },
    ];
    sweep("snapshot-install", &workload);
}

#[test]
fn crash_during_manifest_swap() {
    let mut rng = Rng::new(0x3A17);
    // Every structural transition in one workload — each op below performs a
    // manifest swap, so every write/fsync/rename/dir-fsync step of the swap
    // discipline is a crash point somewhere in this sweep.
    let workload = [
        batch(&mut rng, &[600, 600]),
        StorageOp::Rotate,
        batch(&mut rng, &[600, 600]),
        StorageOp::InstallSnapshot {
            upto_index: 3,
            payload: vec![0x11; 64],
        },
        StorageOp::Purge { upto: 2 },
        StorageOp::TruncateSuffix { from: 4 },
        batch(&mut rng, &[600]),
    ];
    sweep("manifest-swap", &workload);
}

#[test]
fn crash_during_vote_write() {
    let mut rng = Rng::new(0x707E);
    let workload = [
        StorageOp::SetVote {
            term: 1,
            voted_for: 1,
        },
        batch(&mut rng, &[100]),
        StorageOp::SetVote {
            term: 2,
            voted_for: 3,
        },
        StorageOp::SetVote {
            term: 2,
            voted_for: 3,
        },
        batch(&mut rng, &[100]),
        StorageOp::SetVote {
            term: 5,
            voted_for: 1,
        },
    ];
    sweep("vote", &workload);
}

#[test]
fn randomized_sweep() {
    random_sweep(&ToyConfig::default(), 30);
}
