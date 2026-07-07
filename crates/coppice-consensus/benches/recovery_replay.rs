//! ADR 0018 storage benchmark suite, family 3/3: cold-recovery replay
//! floor.
//!
//! Mandate ([ADR 0018](../../../docs/decisions/0018-protobuf-records-in-parallel-containers.md)):
//! recovery replay is pipelined — segment read-ahead and entry decode run
//! ahead of the serial apply loop — so apply, not parsing, must be the
//! limiter. This bench measures the scan+verify half of that pipeline
//! (sequential read + frame-split + CRC32C) through the `Fs`/`FsFile` seam
//! on a real segment file, giving the entries/s floor recovery can promise
//! before a single applied command exists to bound it from the other side.
//!
//! Regressions here gate storage merges exactly like the crash suite
//! (see docs/architecture/storage-testing.md, "The benchmark suite").
//!
// TODO(storage-engine): once the engine exists, extend this bench to
// decode each entry's `Command` protobuf and feed it to the apply loop's
// input side, then compare directly against the apply loop's own rate.
// ADR 0018's thesis is only checked once that comparison exists: replay
// scan+decode must exceed the serial apply loop's rate, or recovery becomes
// the bottleneck ADR 0018 promises it isn't.

use std::path::Path;

use coppice_consensus::fs::{Fs, FsFile, RealFs};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};

/// Representative log entry payload size, matching `append_commit`'s.
const ENTRY_PAYLOAD_LEN: usize = 256;

/// Entry count in the replay-floor segment. Large enough that read-ahead
/// and per-chunk overhead average out.
const ENTRY_COUNT: usize = 100_000;

/// 1 MiB sequential read-ahead buffer for the scan.
const READ_CHUNK: usize = 1 << 20;

/// Frame one entry as length-delimited + CRC32C'd — the same shape
/// `append_commit` writes, matching the real ADR 0002/0018 segment framing.
fn frame_entry(payload: &[u8], out: &mut Vec<u8>) {
    let len = payload.len() as u32;
    let crc = crc32c::crc32c(payload);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
}

/// Build the fixed replay-floor segment once, through the seam, and return
/// an open read handle plus its length. Not timed: this is the "cold
/// recovery finds this segment on disk" precondition, not the replay
/// itself.
fn build_segment(fs: &RealFs) -> (<RealFs as Fs>::File, u64) {
    let payload = vec![0xEFu8; ENTRY_PAYLOAD_LEN];
    let mut buf = Vec::with_capacity(ENTRY_COUNT * (8 + ENTRY_PAYLOAD_LEN));
    for _ in 0..ENTRY_COUNT {
        frame_entry(&payload, &mut buf);
    }

    let path = Path::new("replay.seg");
    {
        let mut file = fs.create_new(path).expect("create segment");
        file.append(&buf).expect("append");
        file.sync_data().expect("sync_data");
    }
    let file = fs.open_read(path).expect("open_read");
    let len = file.len().expect("len");
    (file, len)
}

/// Sequential scan: chunked read-ahead, frame-split, CRC-verify every
/// entry. Returns (entries verified, bytes verified) so the caller can
/// report both throughput axes. A carry buffer holds any partial frame
/// spanning a chunk boundary — reading a fixed-size window can split a
/// frame anywhere.
fn replay_scan<F: FsFile>(file: &F, file_len: u64) -> (u64, u64) {
    let mut chunk = vec![0u8; READ_CHUNK];
    let mut carry: Vec<u8> = Vec::new();
    let mut offset = 0u64;
    let mut entries = 0u64;
    let mut bytes_verified = 0u64;

    while offset < file_len {
        let want = READ_CHUNK.min((file_len - offset) as usize);
        let n = file.read_at(offset, &mut chunk[..want]).expect("read_at");
        assert!(n > 0, "short read before expected end of segment");
        offset += n as u64;
        carry.extend_from_slice(&chunk[..n]);

        let mut pos = 0usize;
        loop {
            if carry.len() - pos < 8 {
                break;
            }
            let len = u32::from_le_bytes(carry[pos..pos + 4].try_into().unwrap()) as usize;
            if carry.len() - pos < 8 + len {
                break;
            }
            let crc_expected = u32::from_le_bytes(carry[pos + 4..pos + 8].try_into().unwrap());
            let payload = &carry[pos + 8..pos + 8 + len];
            let crc_actual = crc32c::crc32c(payload);
            assert_eq!(crc_actual, crc_expected, "corrupt entry during replay scan");
            entries += 1;
            bytes_verified += (8 + len) as u64;
            pos += 8 + len;
        }
        carry.drain(..pos);
    }

    (entries, bytes_verified)
}

/// The replay-rate floor: entries/s a cold-recovery scan can sustain before
/// any apply-side decode is added. Read-only against a fixed segment, so no
/// `iter_batched` setup is needed per iteration.
fn bench_recovery_replay(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs = RealFs::new(dir.path());
    let (file, file_len) = build_segment(&fs);

    let mut group = c.benchmark_group("recovery_replay");

    group.throughput(Throughput::Elements(ENTRY_COUNT as u64));
    group.bench_function("scan_and_verify/entries", |b| {
        b.iter(|| {
            let (entries, bytes) = replay_scan(&file, file_len);
            std::hint::black_box((entries, bytes));
        });
    });

    group.throughput(Throughput::Bytes(file_len));
    group.bench_function("scan_and_verify/bytes", |b| {
        b.iter(|| {
            let (entries, bytes) = replay_scan(&file, file_len);
            std::hint::black_box((entries, bytes));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_recovery_replay);
criterion_main!(benches);
