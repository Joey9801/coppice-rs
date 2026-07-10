//! ADR 0018 storage benchmark suite, family 1/3: append throughput and
//! latency under group commit.
//!
//! Mandate ([ADR 0018](../../../docs/decisions/0018-protobuf-records-in-parallel-containers.md)):
//! commit latency is fsync-bound, not encoding-bound — encoding a small
//! command is microseconds against a millisecond-class fsync — and group
//! commit amortizes that fsync across a batch. This bench drives real
//! appends through the `Fs`/`FsFile` seam (`RealFs`, over a
//! `tempfile::tempdir()`), never raw `std::fs`: anything else is invisible
//! to the crash-injection suite this ADR is checked against.
//!
//! Regressions here gate storage merges exactly like the crash suite
//! (see docs/architecture/storage-testing.md, "The benchmark suite").
//!
//! Measurement target: per-entry commit latency (with fsync) should fall
//! roughly linearly with group size — one fsync amortized over more
//! entries — until the segment write itself becomes bandwidth-bound. The
//! no-fsync variant is the encoding-only ceiling: it isolates framing +
//! append cost from the fsync, so a regression there means the framing got
//! slower, not the disk.
//!
//! Three groups:
//!
//! - `append_commit/group_commit_fsync` and `append_commit/no_fsync_ceiling`
//!   are the "raw seam" groups: they write pre-framed group bytes straight
//!   through `Fs`/`FsFile` to a fresh file per iteration, isolating the
//!   append+fsync cost from every other engine concern (manifest, offset
//!   tables, rotation). The framing is the real one — `storage::raw::frame_entry`,
//!   the exact bytes `StorageCore::append_batch` writes — so a regression
//!   here is either a disk regression or a framing regression, never a
//!   fixture drift from the real format.
//! - `append_commit/engine` drives `StorageCore::append_batch` end to end:
//!   one engine instance over one data directory, appending with
//!   monotonically increasing indices across the whole sweep (indices are
//!   never reset between iterations). That means the active segment rotates
//!   at the configured `segment_max_bytes` partway through a long sample —
//!   sustained append behavior, rotation included, rather than a fiction
//!   where every iteration starts a fresh segment. The first-ever append
//!   also creates and claims the first segment (extra fsyncs plus a
//!   manifest swap); that one-time rare-path cost is amortized into
//!   `iter_batched`'s first sample and is not what this group isolates.

use std::hint::black_box;
use std::path::Path;

use coppice_consensus::fs::{Fs, FsFile, RealFs};
use coppice_consensus::storage::raw::{self, ENTRY_OVERHEAD};
use coppice_consensus::storage::{EncodedEntry, FrameLogId, StorageCore, StorageOptions};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};

/// Representative size of one log entry's command payload.
///
/// Small enough to be dominated by fsync latency at every group size in
/// `GROUP_SIZES`, which is exactly the property ADR 0018's thesis depends
/// on.
const ENTRY_PAYLOAD_LEN: usize = 256;

/// Group sizes spanning "no batching" to "deep batching under load".
const GROUP_SIZES: [usize; 4] = [1, 8, 64, 256];

/// Identity for the raw-seam groups' frame ids.
///
/// These groups never go through `StorageCore`, so the id only has to be
/// well-formed, not contiguous with anything.
const RAW_TERM: u64 = 1;
const RAW_NODE: u64 = 1;

/// Pre-frame one group's worth of entries once, using the engine's real
/// framing (`storage::raw::frame_entry`: length, index/term/node, CRC32C,
/// payload — ADR 0002/0018). The bench routine only appends and (maybe)
/// syncs the resulting bytes, so measurement isolates commit cost from
/// framing cost.
fn make_group(group_size: usize) -> Vec<u8> {
    let payload = vec![0xABu8; ENTRY_PAYLOAD_LEN];
    let mut buf = Vec::with_capacity(group_size * (ENTRY_OVERHEAD + ENTRY_PAYLOAD_LEN));
    for i in 0..group_size {
        let id = FrameLogId {
            index: i as u64,
            term: RAW_TERM,
            node_id: RAW_NODE,
        };
        raw::frame_entry(id, &payload, &mut buf);
    }
    buf
}

/// A fresh, empty segment file for one measured iteration.
///
/// Every iteration gets its own file (unique name from `counter`) so the
/// file never grows across the sample and `sync_data` cost stays
/// representative of a single group commit rather than of an ever-larger
/// segment.
fn fresh_segment(fs: &RealFs, counter: &mut u64) -> <RealFs as Fs>::File {
    let name = format!("seg-{:010}", *counter);
    *counter += 1;
    fs.create_new(Path::new(&name)).expect("create segment")
}

/// Group commit: append the group, then one `sync_data` — the durability
/// barrier nothing is acknowledged past (ADR 0002/0017). This is the number
/// the ADR 0018 thesis lives or dies on.
fn bench_group_commit_with_fsync(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs = RealFs::new(dir.path());
    let mut counter = 0u64;

    let mut group = c.benchmark_group("append_commit/group_commit_fsync");
    for &n in &GROUP_SIZES {
        let bytes = make_group(n);

        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(BenchmarkId::new("bytes", n), &n, |b, _| {
            b.iter_batched(
                || fresh_segment(&fs, &mut counter),
                |mut file| {
                    file.append(black_box(&bytes)).expect("append");
                    file.sync_data().expect("sync_data");
                },
                BatchSize::PerIteration,
            );
        });

        // Same measurement, reported as entries/s so per-entry commit
        // latency at each group size is directly comparable (the number
        // the "falls roughly linearly with group size" target is about).
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("entries", n), &n, |b, _| {
            b.iter_batched(
                || fresh_segment(&fs, &mut counter),
                |mut file| {
                    file.append(black_box(&bytes)).expect("append");
                    file.sync_data().expect("sync_data");
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// The encoding-only ceiling: append made visible, but never synced.
///
/// ADR 0018 claims this is never the limiter; this bench is how that claim
/// gets checked rather than assumed. A regression here is a framing/append
/// regression, not a disk one — the no-fsync numbers should be orders of
/// magnitude faster than the fsync'd numbers above at every group size.
fn bench_encoding_only_ceiling(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs = RealFs::new(dir.path());
    let mut counter = 0u64;

    let mut group = c.benchmark_group("append_commit/no_fsync_ceiling");
    for &n in &GROUP_SIZES {
        let bytes = make_group(n);

        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(BenchmarkId::new("bytes", n), &n, |b, _| {
            b.iter_batched(
                || fresh_segment(&fs, &mut counter),
                |mut file| {
                    file.append(black_box(&bytes)).expect("append");
                },
                BatchSize::PerIteration,
            );
        });

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("entries", n), &n, |b, _| {
            b.iter_batched(
                || fresh_segment(&fs, &mut counter),
                |mut file| {
                    file.append(black_box(&bytes)).expect("append");
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// End-to-end group commit through the real engine: `StorageCore::append_batch`
/// over one data directory, indices monotonically increasing across the
/// whole sweep (see the module doc — segment rotation at the default 64 MiB
/// threshold is left to happen honestly rather than reset away). This is
/// the number `append_commit/group_commit_fsync` promises holds once the
/// manifest-free append path (offset-table bookkeeping, contiguity checks,
/// segment-full detection) is added on top of the raw seam.
fn bench_engine_append(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs = RealFs::new(dir.path());
    let options = StorageOptions::new([0x13; 16]);
    StorageCore::init(&fs, &options, 1, [0x14; 16]).expect("init");
    let mut core = StorageCore::open(fs, options).expect("open");

    // Shared across every group size and every sample: the index counter
    // never resets, so appends stay contiguous the way `append_batch`
    // requires and the segment genuinely rotates under sustained load.
    let mut next_index = 0u64;

    let mut group = c.benchmark_group("append_commit/engine");
    for &n in &GROUP_SIZES {
        let payload_template = vec![0xCDu8; ENTRY_PAYLOAD_LEN];

        group.throughput(Throughput::Bytes((n * ENTRY_PAYLOAD_LEN) as u64));
        group.bench_with_input(BenchmarkId::new("bytes", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let start = next_index;
                    next_index += n as u64;
                    (start..start + n as u64)
                        .map(|index| EncodedEntry {
                            id: FrameLogId {
                                index,
                                term: 1,
                                node_id: 1,
                            },
                            payload: payload_template.clone(),
                        })
                        .collect::<Vec<_>>()
                },
                |batch| {
                    core.append_batch(black_box(&batch)).expect("append_batch");
                },
                BatchSize::PerIteration,
            );
        });

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("entries", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let start = next_index;
                    next_index += n as u64;
                    (start..start + n as u64)
                        .map(|index| EncodedEntry {
                            id: FrameLogId {
                                index,
                                term: 1,
                                node_id: 1,
                            },
                            payload: payload_template.clone(),
                        })
                        .collect::<Vec<_>>()
                },
                |batch| {
                    core.append_batch(black_box(&batch)).expect("append_batch");
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_group_commit_with_fsync,
    bench_encoding_only_ceiling,
    bench_engine_append
);
criterion_main!(benches);
