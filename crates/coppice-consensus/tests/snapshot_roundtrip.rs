//! The storage-engine task's "round-trip at scale" deliverable: snapshot
//! build -> install -> rebuild equality against synthetic states.
//!
//! ADR 0018 defines the sharded-section snapshot container and gates it on
//! scale (this file's 1M-job test); ADR 0016 requires that a learner
//! rebuilding from an installed snapshot end up in exactly the state the
//! leader snapshotted — not merely "no decode error", but structural
//! equality of the rebuilt [`coppice_state::StateMachine`]. Two layers are
//! exercised:
//!
//! - `codec_roundtrip_medium` / `codec_roundtrip_million_jobs` drive the
//!   container codec directly (`state_to_records` -> `encode_state` ->
//!   `decode_state` -> `state_from_records`), the same path
//!   `snapshot_codec` benches.
//! - `engine_roundtrip_through_build_and_install` drives the whole openraft
//!   surface across two independent stores, mirroring how a learner catches
//!   up: [`openraft::storage::RaftStateMachine::get_snapshot_builder`] on one
//!   store, [`openraft::storage::RaftStateMachine::install_snapshot`] on
//!   another, then both the freshly-installed in-memory state and a from-disk
//!   reopen are checked against the original synthetic state.

use std::path::Path;

use openraft::storage::RaftStateMachine;
use openraft::RaftSnapshotBuilder;
use tokio::sync::{mpsc, oneshot};

use coppice_consensus::fs::RealFs;
use coppice_consensus::storage::raw::{decode_state, encode_state};
use coppice_consensus::storage::{self, run_apply_task, StorageOptions};
use coppice_consensus::{ApplyRequest, APPLY_CHANNEL_CAPACITY};
use coppice_proto::convert::{state_from_records, state_to_records};
use coppice_proto::pb::storage::v1::SnapshotMeta;
use coppice_state::StateMachine;
use coppice_testkit::synth::{check_consistency, synth_state, SynthConfig};

/// A `SnapshotMeta` good enough for the codec-only tests: no real Raft
/// coordinates (this path never touches openraft), just the identity fields
/// the container header carries and validates on decode.
fn bare_meta(snapshot_id: &str, shards: u32) -> SnapshotMeta {
    SnapshotMeta {
        cluster_uuid: vec![7; 16],
        snapshot_id: snapshot_id.to_string(),
        last_applied: None,
        membership: None,
        cluster_version: 1,
        shard_count: shards,
    }
}

/// `state_to_records` -> `encode_state` -> `decode_state` ->
/// `state_from_records` must reproduce the original state exactly, at a
/// scale that exercises every section and every shard.
#[test]
fn codec_roundtrip_medium() {
    let cfg = SynthConfig::with_jobs(20_000);
    let original = synth_state(&cfg);

    let records = state_to_records(&original);
    let meta = bare_meta("roundtrip", 4);
    let bytes = encode_state(&meta, &records, 4);

    let (_decoded_meta, decoded_records) =
        decode_state(Path::new("snap"), &bytes).expect("decode_state must accept its own output");
    let rebuilt = state_from_records(decoded_records).expect("records must rebuild a StateMachine");

    assert_eq!(
        rebuilt, original,
        "rebuilt state must equal the original bit-for-bit"
    );
    check_consistency(&rebuilt);
}

/// The full openraft path across two stores: build a snapshot on store A
/// (seeded with a synthetic state), install it on store B, and check B's
/// state both immediately after install (in memory, via the apply-task
/// protocol) and after a from-disk reopen (durable). Both stores share one
/// cluster identity — the engine refuses cross-cluster snapshots (ADR 0016).
#[tokio::test(flavor = "multi_thread")]
async fn engine_roundtrip_through_build_and_install() {
    const CLUSTER: [u8; 16] = [0xC0; 16];

    let cfg = SynthConfig::with_jobs(5_000);
    let synth = synth_state(&cfg);
    check_consistency(&synth);

    // --- Store A: seeded with the synthetic state, builds a snapshot. ---
    let dir_a = tempfile::tempdir().expect("tempdir for store A");
    let options_a = StorageOptions::new(CLUSTER);
    storage::init(&RealFs::new(dir_a.path()), &options_a).expect("init A");
    let recovered_a = storage::open(RealFs::new(dir_a.path()), options_a).expect("open A");

    let (tx_a, rx_a) = mpsc::channel(APPLY_CHANNEL_CAPACITY);
    tokio::spawn(run_apply_task(synth.clone(), rx_a));
    let (_log_a, mut sm_a) = recovered_a.into_stores(tx_a);

    let mut builder = sm_a.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.expect("build_snapshot on A");
    assert!(
        snapshot.meta.last_log_id.is_none(),
        "A never applied a log entry through openraft, only through the seeded apply task"
    );

    // --- Store B: separate directory, same cluster identity. ---
    let dir_b = tempfile::tempdir().expect("tempdir for store B");
    let options_b = StorageOptions::new(CLUSTER);
    storage::init(&RealFs::new(dir_b.path()), &options_b).expect("init B");
    let recovered_b = storage::open(RealFs::new(dir_b.path()), options_b).expect("open B");

    let (tx_b, rx_b) = mpsc::channel(APPLY_CHANNEL_CAPACITY);
    tokio::spawn(run_apply_task(StateMachine::default(), rx_b));
    let tx_b_query = tx_b.clone();
    let (log_b, mut sm_b) = recovered_b.into_stores(tx_b);

    sm_b.install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .expect("install_snapshot on B");

    // B's in-memory state, reached through the apply-task protocol exactly
    // as a snapshot build would, must equal the original synthetic state.
    let (reply, rx) = oneshot::channel();
    tx_b_query
        .send(ApplyRequest::Snapshot { reply })
        .await
        .expect("B's apply task is alive");
    let (installed_state, _applied_index) = rx.await.expect("apply task replied");
    assert_eq!(
        *installed_state, synth,
        "B's installed state must equal A's snapshotted state"
    );
    check_consistency(&installed_state);

    // B's durable snapshot pointer must carry the installed snapshot's id.
    let current = sm_b
        .get_current_snapshot()
        .await
        .expect("get_current_snapshot on B")
        .expect("B must have a durable snapshot after install");
    assert_eq!(current.meta.snapshot_id, snapshot.meta.snapshot_id);

    // Reopen B from scratch: both `log_b` and `sm_b` hold an `Arc` onto the
    // shared engine, so both must drop before the `LOCK` file releases.
    drop(log_b);
    drop(sm_b);
    drop(tx_b_query);

    let reopened = storage::open(RealFs::new(dir_b.path()), StorageOptions::new(CLUSTER))
        .expect("reopen B through recovery");
    assert_eq!(
        reopened.state, synth,
        "a from-disk reopen of B must rebuild the same state"
    );
}

/// The 1M-live-job scale round-trip ADR 0018's benchmark suite is gated on.
/// Expensive in debug builds; run explicitly in release:
/// `cargo test -p coppice-consensus --test snapshot_roundtrip --release -- --ignored`.
#[test]
#[ignore = "1M-job scale round-trip; run explicitly with --release: cargo test -p coppice-consensus --test snapshot_roundtrip --release -- --ignored"]
fn codec_roundtrip_million_jobs() {
    let cfg = SynthConfig::with_jobs(1_000_000);

    let start = std::time::Instant::now();
    let original = synth_state(&cfg);
    eprintln!("synth_state(1_000_000 jobs) took {:?}", start.elapsed());

    let records = state_to_records(&original);
    let meta = bare_meta("roundtrip-1m", 4);

    let encode_start = std::time::Instant::now();
    let bytes = encode_state(&meta, &records, 4);
    eprintln!(
        "encode_state took {:?} ({} bytes)",
        encode_start.elapsed(),
        bytes.len()
    );

    let decode_start = std::time::Instant::now();
    let (_decoded_meta, decoded_records) =
        decode_state(Path::new("snap"), &bytes).expect("decode_state must accept its own output");
    let rebuilt = state_from_records(decoded_records).expect("records must rebuild a StateMachine");
    eprintln!(
        "decode_state + state_from_records took {:?}",
        decode_start.elapsed()
    );

    assert_eq!(
        rebuilt, original,
        "rebuilt state must equal the original bit-for-bit"
    );

    let check_start = std::time::Instant::now();
    check_consistency(&rebuilt);
    eprintln!("check_consistency took {:?}", check_start.elapsed());
}
