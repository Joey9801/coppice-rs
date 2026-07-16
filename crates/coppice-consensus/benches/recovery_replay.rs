//! ADR 0018 storage benchmark suite, family 3/3: cold-recovery replay
//! against the real engine.
//!
//! Mandate ([ADR 0018](../../../docs/decisions/0018-protobuf-records-in-parallel-containers.md)):
//! recovery replay is pipelined — segment read-ahead and entry decode run
//! ahead of the serial apply loop — so apply, not parsing, must be the
//! limiter. This bench builds a 100k-entry data directory once (real
//! `RegisterNode` commands, real `LogEntry` protobuf framing, real
//! `StorageCore::append_batch`), then measures the three legs of the
//! pipeline through the `Fs`/`FsFile` seam on `RealFs`:
//!
//! - `open/recovery`: `StorageCore::open` cold — manifest read, orphan
//!   sweep, and the full tail-segment scan + CRC32C verify of every entry
//!   (ADR 0017 recovery step 4). This is what a replica pays once, before
//!   anything is replayed.
//! - `scan_decode/entries`: `read_payloads` over the whole live range, then
//!   protobuf-decoding each payload back to a `LogEntry` and converting its
//!   `Command`. The scan+decode half of the pipeline.
//! - `apply/entries`: applying the already-decoded commands to a fresh
//!   `StateMachine`, one `sm.apply` per entry. The serial half of the
//!   pipeline — the one ADR 0018 says must be the bottleneck.
//!
//! Regressions here gate storage merges exactly like the crash suite
//! (see docs/architecture/storage-testing.md, "The benchmark suite").

use std::collections::BTreeMap;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::Instant;

use prost::Message;

use coppice_consensus::fs::{Fs, FsFile, RealFs};
use coppice_consensus::storage::{EncodedEntry, FrameLogId, StorageCore, StorageOptions};
use coppice_core::id::NodeId;
use coppice_core::resource::Resources;
use coppice_core::time::Timestamp;
use coppice_proto::convert::{command_from_pb, command_to_pb};
use coppice_proto::pb::raft::v1 as pbraft;
use coppice_state::command::RegisterNode;
use coppice_state::{Command, StateMachine};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};

const CLUSTER_UUID: [u8; 16] = *b"replay-bench-clu";
const INSTANCE_UUID: [u8; 16] = [0x77; 16];
const NODE_ID: u64 = 1;

/// Entry count in the replay-floor fixture.
///
/// Large enough that read-ahead and per-chunk overhead average out, small
/// enough that `--quick` runs finish in seconds.
const ENTRY_COUNT: usize = 100_000;

/// Batch size the fixture is appended in (matches a realistic group-commit
/// size, not a single-entry-per-fsync fixture build).
const APPEND_BATCH: usize = 1024;

/// One realistic small command: register a distinct compute node. Cheap to
/// construct, decodes through the real `Command::RegisterNode` apply path
/// (unconditional accept, ADR 0009 epoch bump), so `apply/entries` never
/// spends its measured time on rejection bookkeeping.
fn register_node_command() -> Command {
    Command::RegisterNode(RegisterNode {
        node: NodeId::new(),
        capacity: Resources {
            cpu_millis: 4_000,
            memory_bytes: 8 << 30,
            disk_bytes: 100 << 30,
        },
        labels: BTreeMap::new(),
        registered_at: Timestamp::from_micros(1_700_000_000_000_000).expect("in range"),
    })
}

/// Encode one fixture entry as the real durable `coppice.raft.v1.LogEntry`
/// payload (the same bytes `SegmentLogStorage::append` produces), wrapped in
/// its frame-level log id.
fn encode_fixture_entry(index: u64) -> EncodedEntry {
    let command = register_node_command();
    let entry = pbraft::LogEntry {
        log_id: Some(pbraft::LogId {
            leader_id: Some(pbraft::LeaderId {
                term: 1,
                node_id: NODE_ID,
            }),
            index,
        }),
        payload: Some(pbraft::log_entry::Payload::Normal(command_to_pb(
            &command, 1,
        ))),
    };
    EncodedEntry {
        id: FrameLogId {
            index,
            term: 1,
            node_id: NODE_ID,
        },
        payload: entry.encode_to_vec(),
    }
}

/// Decode one fixture payload back to its domain `Command` — the
/// `scan_decode` half of the pipeline this module measures.
fn decode_fixture_entry(bytes: &[u8]) -> Command {
    let entry = pbraft::LogEntry::decode(bytes).expect("decode LogEntry");
    let Some(pbraft::log_entry::Payload::Normal(pb_command)) = entry.payload else {
        panic!("recovery_replay fixture only ever writes Command::Normal entries");
    };
    command_from_pb(pb_command).expect("command_from_pb").1
}

/// Build the fixture directory once, through the seam, appending
/// `ENTRY_COUNT` real `RegisterNode` entries in `APPEND_BATCH`-sized group
/// commits.
///
/// Not timed: this is the "cold recovery finds this on disk" precondition,
/// not the replay itself. The engine is dropped at the end of this
/// function, releasing the data directory's `LOCK` for the benches.
fn build_fixture(fs_root: &Path, options: &StorageOptions) {
    let fs = RealFs::new(fs_root);
    StorageCore::init(&fs, options, NODE_ID, INSTANCE_UUID).expect("init");
    let mut core = StorageCore::open(fs, options.clone()).expect("open");

    let mut index = 0u64;
    while (index as usize) < ENTRY_COUNT {
        let batch_len = APPEND_BATCH.min(ENTRY_COUNT - index as usize) as u64;
        let batch: Vec<EncodedEntry> = (index..index + batch_len)
            .map(encode_fixture_entry)
            .collect();
        core.append_batch(&batch).expect("append_batch");
        index += batch_len;
    }
}

fn bench_recovery_replay(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs_root: PathBuf = dir.path().to_path_buf();
    let options = StorageOptions::new(CLUSTER_UUID);

    build_fixture(&fs_root, &options);

    // 100k small RegisterNode entries stay well under the 64 MiB default
    // rotation threshold, so every entry lands in the one segment starting
    // at index 0 — this is the byte count `open/recovery` scans and
    // CRC-verifies in full.
    let segment_bytes = {
        let fs = RealFs::new(&fs_root);
        let file = fs.open_read(Path::new("log/0.seg")).expect("open segment");
        file.len().expect("len")
    };

    let mut group = c.benchmark_group("recovery_replay");

    // (a) The cold-open cost: manifest + orphan sweep + full tail-segment
    // scan/CRC-verify of all 100k entries (ADR 0017 recovery step 4). Every
    // iteration opens a fresh `StorageCore` and drops it (releasing `LOCK`)
    // before the next.
    group.throughput(Throughput::Bytes(segment_bytes));
    group.bench_function("open/recovery", |b| {
        b.iter(|| {
            let core = StorageCore::open(RealFs::new(&fs_root), options.clone()).expect("open");
            black_box(&core);
        });
    });

    // One core, held open for both remaining groups.
    let mut core = StorageCore::open(RealFs::new(&fs_root), options.clone()).expect("open");

    // (b) scan_decode/entries: read_payloads over the whole live range, then
    // decode every payload's LogEntry + Command. Freshly re-reads and
    // re-decodes every sample, matching what a real recovery's scan+decode
    // stage does once per cold start.
    group.throughput(Throughput::Elements(ENTRY_COUNT as u64));
    group.bench_function("scan_decode/entries", |b| {
        b.iter(|| {
            let payloads = core.read_payloads(0, u64::MAX).expect("read_payloads");
            for (_, bytes) in &payloads {
                black_box(decode_fixture_entry(bytes));
            }
        });
    });

    // Pre-decode once, outside the timed apply loop — apply/entries
    // measures only `sm.apply`, not the decode that feeds it. This pass
    // also doubles as the untimed data source for the eprintln comparison
    // below.
    let scan_decode_start = Instant::now();
    let payloads = core.read_payloads(0, u64::MAX).expect("read_payloads");
    let commands: Vec<Command> = payloads
        .iter()
        .map(|(_, bytes)| decode_fixture_entry(bytes))
        .collect();
    let scan_decode_elapsed = scan_decode_start.elapsed();
    assert_eq!(
        commands.len(),
        ENTRY_COUNT,
        "fixture must hold exactly ENTRY_COUNT live entries"
    );

    // (c) apply/entries: the serial apply loop ADR 0018 says must be the
    // bottleneck. A fresh StateMachine per iteration so state size stays
    // constant across samples.
    group.throughput(Throughput::Elements(ENTRY_COUNT as u64));
    group.bench_function("apply/entries", |b| {
        b.iter(|| {
            let mut sm = StateMachine::default();
            for command in &commands {
                black_box(sm.apply(command).ok());
            }
            black_box(sm);
        });
    });

    group.finish();

    // A one-shot, unstatistical comparison (like snapshot_codec's eprintln)
    // of the two pipeline legs ADR 0018's thesis is about: scan+decode must
    // clear the apply rate, or recovery replay becomes bound by parsing
    // instead of application.
    let apply_start = Instant::now();
    let mut sm = StateMachine::default();
    for command in &commands {
        let _ = sm.apply(command);
    }
    let apply_elapsed = apply_start.elapsed();

    let scan_decode_rate = ENTRY_COUNT as f64 / scan_decode_elapsed.as_secs_f64();
    let apply_rate = ENTRY_COUNT as f64 / apply_elapsed.as_secs_f64();
    eprintln!(
        "recovery_replay: scan+decode {scan_decode_rate:.0} entries/s vs apply {apply_rate:.0} \
         entries/s (ratio {:.2}x; ADR 0018 requires scan+decode to clear apply so replay \
         pipelines ahead of it)",
        scan_decode_rate / apply_rate
    );
}

criterion_group!(benches, bench_recovery_replay);
criterion_main!(benches);
