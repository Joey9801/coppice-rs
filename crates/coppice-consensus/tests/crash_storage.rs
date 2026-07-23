//! The crash-injection suite over the real segment engine (ADR 0002 +
//! ADR 0017; `docs/architecture/storage-testing.md`).
//!
//! The engine plugs into `coppice_testkit::harness` through [`RealEngine`],
//! driving the synchronous [`StorageCore`] directly over `SimFs` — no
//! executor in the loop, so a crash at seam-operation *k* is exactly
//! reproducible from a seed. The eight mandated scenario sweeps mirror
//! `coppice-testkit/tests/crash_scenarios.rs` (where the toy reference
//! engine runs them); the directed tests at the bottom cover the orderings
//! the harness vocabulary cannot express, most importantly the ADR 0016
//! install path where one manifest swap flips the snapshot pointer *and*
//! advances the purge floor.

use std::cell::RefCell;
use std::io;

use coppice_consensus::storage::{raw, EncodedEntry, FrameLogId, StorageCore, StorageOptions};
use coppice_core::bytes::ByteSize;
use coppice_proto::pb::raft::v1 as pbraft;
use coppice_proto::pb::storage::v1 as pbstorage;
use coppice_testkit::harness::{
    crash_sweep, random_sweep, CrashSubject, Observed, StorageOp, SweepConfig,
};
use coppice_testkit::rng::Rng;
use coppice_testkit::simfs::{is_sim_crash, SimConfig, SimFs};

const CLUSTER_UUID: [u8; 16] = *b"crash-suite-clst";
const INSTANCE_UUID: [u8; 16] = [0x42; 16];
const NODE_ID: u64 = 1;

/// The real engine as a harness subject.
///
/// Entry payloads stay opaque bytes: the container layer frames them
/// without protobuf (by design, ADR 0018), so the harness's byte-equality
/// checks exercise exactly what the engine persists.
struct RealEngine {
    segment_max: ByteSize,
}

impl Default for RealEngine {
    fn default() -> Self {
        RealEngine {
            segment_max: ByteSize::from_mib(64),
        }
    }
}

impl RealEngine {
    fn options(&self) -> StorageOptions {
        let mut options = StorageOptions::new(CLUSTER_UUID);
        options.segment_max = self.segment_max;
        options
    }
}

/// Build the one-section opaque container the harness's `InstallSnapshot`
/// payload rides in. Container-level (header, CRCs, footer) it is a real
/// ADR 0018 snapshot; its records are deliberately not decodable, which the
/// durable install path never needs.
fn opaque_snapshot(core: &mut StorageCore<SimFs>, upto_index: u64, payload: &[u8]) -> Vec<u8> {
    let meta = pbstorage::SnapshotMeta {
        cluster_uuid: CLUSTER_UUID.to_vec(),
        snapshot_id: core.mint_snapshot_id(),
        last_applied: Some(pbraft::LogId {
            leader_id: Some(pbraft::LeaderId {
                term: 1,
                node_id: NODE_ID,
            }),
            index: upto_index,
        }),
        membership: None,
        cluster_version: 1,
        shard_count: 1,
    };
    raw::assemble_container(
        &meta,
        vec![raw::RawSection {
            kind: pbstorage::SectionKind::Job,
            shard: 0,
            encoding: "opaque-test".into(),
            record_count: 0,
            bytes: payload.to_vec(),
        }],
    )
}

fn apply_op(core: &mut StorageCore<SimFs>, op: &StorageOp) -> io::Result<()> {
    match op {
        StorageOp::AppendBatch { payloads } => {
            let (floor, last) = core.log_state();
            // Harness entries are 1-based (the toy's convention): the next
            // index continues the log, or restarts just above the floor.
            let next = last.or(floor).map(|id| id.index + 1).unwrap_or(1);
            let entries: Vec<EncodedEntry> = payloads
                .iter()
                .enumerate()
                .map(|(i, payload)| EncodedEntry {
                    id: FrameLogId {
                        index: next + i as u64,
                        term: 1,
                        node_id: NODE_ID,
                    },
                    payload: payload.clone(),
                })
                .collect();
            core.append_batch(&entries)
        }
        StorageOp::SetVote { term, voted_for } => core.save_vote(&pbraft::Vote {
            leader_id: Some(pbraft::LeaderId {
                term: *term,
                node_id: *voted_for,
            }),
            committed: false,
        }),
        StorageOp::TruncateSuffix { from } => core.truncate(*from),
        StorageOp::Purge { upto } => core.purge(pbraft::LogId {
            leader_id: Some(pbraft::LeaderId {
                term: 1,
                node_id: NODE_ID,
            }),
            index: *upto,
        }),
        StorageOp::InstallSnapshot {
            upto_index,
            payload,
        } => {
            let container = opaque_snapshot(core, *upto_index, payload);
            // `advance_floor: false`: the harness models install and purge
            // as separate ops, exactly as openraft drives them; the fused
            // install+floor manifest swap of ADR 0016 is crash-tested by
            // the directed sweep below.
            core.install_snapshot(&container, false).map(|_| ())
        }
        StorageOp::Rotate => core.rotate(),
    }
}

fn observe_core(core: &RefCell<StorageCore<SimFs>>) -> Observed {
    let mut core = core.borrow_mut();
    let entries = core
        .read_payloads(0, u64::MAX)
        .expect("post-recovery read must succeed")
        .into_iter()
        .collect();
    let vote = core.vote().map(|v| {
        let leader = v.leader_id.as_ref().expect("vote carries a leader id");
        (leader.term, leader.node_id)
    });
    let snapshot = core
        .current_snapshot()
        .expect("claimed snapshot must validate")
        .map(|(meta, bytes)| {
            let (_, index) =
                raw::validate_container(std::path::Path::new("snap"), &bytes).expect("validated");
            let upto = meta.last_applied.map(|id| id.index).unwrap_or(0);
            (
                upto,
                raw::section_bytes(&bytes, &index.sections[0]).to_vec(),
            )
        });
    let (floor, _) = core.log_state();
    Observed {
        entries,
        vote,
        snapshot,
        purge_floor: floor.map(|f| f.index).unwrap_or(0),
    }
}

impl CrashSubject for RealEngine {
    type Store = RefCell<StorageCore<SimFs>>;

    fn init(&self, fs: &SimFs) -> io::Result<()> {
        StorageCore::init(fs, &self.options(), NODE_ID, INSTANCE_UUID)
    }

    fn open(&self, fs: &SimFs) -> io::Result<Self::Store> {
        StorageCore::open(fs.clone(), self.options()).map(RefCell::new)
    }

    fn apply(&self, store: &mut Self::Store, op: &StorageOp) -> io::Result<()> {
        apply_op(&mut store.borrow_mut(), op)
    }

    fn observe(&self, store: &Self::Store) -> Observed {
        observe_core(store)
    }
}

/// Deterministic payload bytes; sizes straddle the 4 KiB tear granularity
/// where it matters (mirrors `crash_scenarios.rs`).
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
        &RealEngine::default(),
        workload,
        &SweepConfig::default(),
    );
}

// ---- the eight mandated scenarios, identical workloads to the toy's ------

#[test]
fn crash_during_append() {
    let mut rng = Rng::new(0xA99E);
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
    let small = RealEngine {
        segment_max: ByteSize::from_bytes(2048),
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
        StorageOp::TruncateSuffix { from: 5 }, // mid-segment: stale bytes stay in segment 4
        batch(&mut rng, &[900]),           // entry 5 in a fresh segment
        StorageOp::TruncateSuffix { from: 5 }, // segment-boundary: drops segment 5 entirely
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
        StorageOp::Purge { upto: 3 }, // mid-segment: only segment 1 fully covered
        StorageOp::Purge { upto: 4 }, // segment boundary: segment 3 goes too
        batch(&mut rng, &[77]),
    ];
    sweep("purge", &workload);
}

#[test]
fn crash_during_snapshot_install() {
    let mut rng = Rng::new(0x5A9);
    let workload = [
        batch(&mut rng, &[100, 100, 100, 100]),
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
    random_sweep(&RealEngine::default(), 30);
}

// ---- directed sweeps for orderings the harness vocabulary cannot say -----

/// A dense mix of the nastiest adjacencies at a small rotation threshold:
/// seal-then-rotate, truncation landing inside a sealed segment, purge right
/// behind a fresh install, append into a segment index that was truncated
/// away moments before.
#[test]
fn crash_during_tight_structural_sequence() {
    let mut rng = Rng::new(0xD1CE);
    let small = RealEngine {
        segment_max: ByteSize::from_bytes(1024),
    };
    let workload = [
        batch(&mut rng, &[400, 400, 400]), // crosses the threshold mid-workload
        batch(&mut rng, &[400]),           // forces the automatic seal+rotate
        StorageOp::Rotate,
        StorageOp::TruncateSuffix { from: 3 }, // back into the sealed segment
        batch(&mut rng, &[2000]),
        StorageOp::InstallSnapshot {
            upto_index: 3,
            payload: vec![0xCD; 300],
        },
        StorageOp::Purge { upto: 3 },
        StorageOp::TruncateSuffix { from: 4 }, // empties the log to the floor
        batch(&mut rng, &[50, 50]),
    ];
    crash_sweep(
        "tight-structural",
        &small,
        &workload,
        &SweepConfig::default(),
    );
}

/// The ADR 0016 learner-install path: one manifest swap flips the snapshot
/// pointer, advances the purge floor, and drops every covered segment claim.
/// The harness models install and purge separately, so this ordering gets a
/// bespoke exhaustive sweep: at every crash point inside the fused install,
/// recovery must observe the flip all-or-nothing.
#[test]
fn crash_during_install_with_floor_advance() {
    let engine = RealEngine::default();

    // The deterministic pre-install phase: entries 1..=6 across two
    // segments, plus one older snapshot so delete-after-flip is on the path.
    let setup = |fs: &SimFs| -> StorageCore<SimFs> {
        engine.init(fs).expect("init");
        let mut core = StorageCore::open(fs.clone(), engine.options()).expect("open");
        let mut rng = Rng::new(0xF10);
        for op in [
            batch(&mut rng, &[300, 300, 300]),
            StorageOp::Rotate,
            batch(&mut rng, &[300, 300, 300]),
            StorageOp::InstallSnapshot {
                upto_index: 2,
                payload: vec![0xEE; 200],
            },
        ] {
            apply_op(&mut core, &op).expect("setup op");
        }
        core
    };

    // Measure the crash-point range of the fused install.
    let fs = SimFs::new(SimConfig::default());
    let mut core = setup(&fs);
    let base = fs.op_count();
    let container = opaque_snapshot(&mut core, 4, &[0xFF; 500]);
    core.install_snapshot(&container, true)
        .expect("clean fused install");
    let total = fs.op_count();
    drop(core);
    assert!(total > base, "the fused install must touch the seam");

    for crash_at in base..total {
        for seed in 0..3u64 {
            let adversary = 0x0115_7a11 ^ (crash_at << 8) ^ seed;
            let fs = SimFs::new(SimConfig::default());
            let mut core = setup(&fs);
            fs.set_crash_at(crash_at);
            let container = opaque_snapshot(&mut core, 4, &[0xFF; 500]);
            let err = core
                .install_snapshot(&container, true)
                .expect_err("armed install must crash");
            assert!(
                is_sim_crash(&err),
                "crash_at={crash_at}: engine wrapped the crash: {err}"
            );
            drop(core);
            fs.crash(adversary);
            fs.disarm();

            let reopened = StorageCore::open(fs.clone(), engine.options()).unwrap_or_else(|e| {
                panic!("crash_at={crash_at} seed={seed}: recovery failed: {e}")
            });
            let observed = observe_core(&RefCell::new(reopened));

            // All-or-nothing: the pointer flip and the floor advance live in
            // one manifest swap, so they must land together.
            let ctx = format!("crash_at={crash_at} seed={seed}: {observed:?}");
            match &observed.snapshot {
                Some((4, payload)) => {
                    assert_eq!(payload, &vec![0xFF; 500], "{ctx}");
                    assert_eq!(
                        observed.purge_floor, 4,
                        "new snapshot without new floor: {ctx}"
                    );
                    assert_eq!(
                        observed.entries.keys().copied().collect::<Vec<_>>(),
                        vec![5, 6],
                        "{ctx}"
                    );
                }
                Some((2, payload)) => {
                    assert_eq!(payload, &vec![0xEE; 200], "{ctx}");
                    assert_eq!(
                        observed.purge_floor, 0,
                        "old snapshot with new floor: {ctx}"
                    );
                    assert_eq!(
                        observed.entries.keys().copied().collect::<Vec<_>>(),
                        (1..=6).collect::<Vec<_>>(),
                        "{ctx}"
                    );
                }
                other => panic!("unexpected snapshot state {other:?}: {ctx}"),
            }

            // Recovery idempotence: a second open observes the same state.
            let again = StorageCore::open(fs.clone(), engine.options())
                .unwrap_or_else(|e| panic!("{ctx}: second recovery failed: {e}"));
            assert_eq!(
                observe_core(&RefCell::new(again)),
                observed,
                "double-open diverged"
            );
        }
    }
}

/// The durable formation token (ADR 0037 §3) survives a reopen: a daemon that
/// crashes after recording the token but before `raft.initialize` reads the
/// same token back on restart, so it completes formation itself with the same
/// operator intent (never a fresh, conflicting one).
#[test]
fn formation_token_persists_across_reopen() {
    let engine = RealEngine::default();
    let fs = SimFs::new(SimConfig::default());

    // A freshly-stamped directory carries no token yet.
    StorageCore::init(&fs, &engine.options(), NODE_ID, INSTANCE_UUID).expect("init");
    {
        let core = StorageCore::open(fs.clone(), engine.options()).expect("open fresh");
        assert_eq!(core.formation_token(), None, "fresh directory has no token");
    }

    // Record the operator's formation intent durably (the pre-initialize stamp).
    {
        let mut core = StorageCore::open(fs.clone(), engine.options()).expect("reopen to record");
        core.record_formation_token("stack-42")
            .expect("record formation token");
        assert_eq!(core.formation_token(), Some("stack-42"));
    }

    // "Crash" and reopen: the token is read back, so restart resumes the SAME
    // formation rather than manufacturing a conflict.
    {
        let core = StorageCore::open(fs.clone(), engine.options()).expect("reopen after crash");
        assert_eq!(core.formation_token(), Some("stack-42"));
        assert_eq!(
            core.node_id(),
            NODE_ID,
            "identity is unchanged across the crash"
        );
    }

    // Recording the same token again is an idempotent no-op that still persists.
    {
        let mut core = StorageCore::open(fs.clone(), engine.options()).expect("reopen idempotent");
        core.record_formation_token("stack-42").expect("re-record");
        assert_eq!(core.formation_token(), Some("stack-42"));
    }
}
