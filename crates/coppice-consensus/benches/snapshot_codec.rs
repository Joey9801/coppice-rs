//! ADR 0018 storage benchmark suite, family 2/3: snapshot payload
//! encode/decode over synthetic states.
//!
//! Mandate ([ADR 0018](../../../docs/decisions/0018-protobuf-records-in-parallel-containers.md)):
//! snapshot cost must scale with cores, not with one core. This bench
//! measures the per-section, single-thread payload work every section does
//! regardless of how the container shards it — the record-encode and
//! record-decode cost the container multiplies by shard count. States come
//! from `coppice_testkit::synth` (ADR 0012's 1M-live-job retention target
//! sets the scale); this is exactly the payload shape a real snapshot
//! section carries.
//!
//! Regressions here gate storage merges exactly like the crash suite
//! (see docs/architecture/storage-testing.md, "The benchmark suite").
//!
//! Scales: 10_000 and 100_000 jobs always; 1_000_000 only with
//! `COPPICE_BENCH_1M` set (generation is sub-second at that scale, but
//! encode/decode can run tens of seconds per iteration, so it is opt-in and
//! uses a longer measurement window).

use std::env;
use std::time::Duration;

use coppice_proto::convert::{state_from_records, state_to_records, StateRecords};
use coppice_proto::pb::storage::v1 as pb;
use coppice_testkit::synth::{synth_state, SynthConfig};
use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion, Throughput};
use prost::Message;

/// One buffer per snapshot section (ADR 0018: jobs / attempts / allocations
/// / nodes / quota entities / cluster, each an independent length-delimited
/// protobuf stream). Sharding those streams across cores, optional zstd,
/// and the per-section CRC32C are the storage engine's job.
// TODO(storage-engine): wire real sections (sharding + compression + CRC)
// around this payload path and re-measure end to end; this bench isolates
// exactly the part that does not change when that lands.
#[derive(Default)]
struct SectionBuffers {
    jobs: Vec<u8>,
    attempts: Vec<u8>,
    allocations: Vec<u8>,
    nodes: Vec<u8>,
    quota_entities: Vec<u8>,
    cluster: Vec<u8>,
}

/// Encode every record in one section as length-delimited protobuf into a
/// reusable buffer — the per-section payload work of the ADR 0018
/// container.
fn encode_section<M: Message>(records: &[M], buf: &mut Vec<u8>) {
    buf.clear();
    for record in records {
        record.encode_length_delimited(buf).expect("encode_length_delimited");
    }
}

/// Encode the whole state's records into `bufs`, returning total encoded
/// bytes across all sections.
fn encode_all(records: &StateRecords, bufs: &mut SectionBuffers) -> u64 {
    encode_section(&records.jobs, &mut bufs.jobs);
    encode_section(&records.attempts, &mut bufs.attempts);
    encode_section(&records.allocations, &mut bufs.allocations);
    encode_section(&records.nodes, &mut bufs.nodes);
    encode_section(&records.quota_entities, &mut bufs.quota_entities);
    bufs.cluster.clear();
    if let Some(cluster) = &records.cluster {
        cluster.encode_length_delimited(&mut bufs.cluster).expect("encode_length_delimited");
    }
    (bufs.jobs.len()
        + bufs.attempts.len()
        + bufs.allocations.len()
        + bufs.nodes.len()
        + bufs.quota_entities.len()
        + bufs.cluster.len()) as u64
}

/// Decode one section's length-delimited protobuf stream back into owned
/// records — the per-shard work a learner join's rebuild does N times in
/// parallel.
fn decode_section<M: Message + Default>(buf: &[u8]) -> Vec<M> {
    let mut out = Vec::new();
    let mut cursor = buf;
    while !cursor.is_empty() {
        out.push(M::decode_length_delimited(&mut cursor).expect("decode_length_delimited"));
    }
    out
}

fn decode_all(bufs: &SectionBuffers) -> StateRecords {
    StateRecords {
        jobs: decode_section(&bufs.jobs),
        attempts: decode_section(&bufs.attempts),
        allocations: decode_section(&bufs.allocations),
        nodes: decode_section(&bufs.nodes),
        quota_entities: decode_section(&bufs.quota_entities),
        cluster: if bufs.cluster.is_empty() {
            None
        } else {
            Some(
                pb::ClusterStateRecord::decode_length_delimited(&mut &bufs.cluster[..])
                    .expect("decode_length_delimited"),
            )
        },
    }
}

/// Job scales to sweep. 1M (ADR 0012's retention target) is opt-in — see
/// module docs.
fn scales() -> Vec<usize> {
    let mut scales = vec![10_000, 100_000];
    if env::var_os("COPPICE_BENCH_1M").is_some() {
        scales.push(1_000_000);
    }
    scales
}

/// A benchmark group tuned for whole-state work at `jobs` scale: the
/// criterion-minimum sample size (and a longer window at 1M) keeps
/// wall-clock sane once a single iteration takes seconds.
fn group_for<'a>(c: &'a mut Criterion, name: &str, jobs: usize) -> BenchmarkGroup<'a, WallTime> {
    let mut group = c.benchmark_group(name);
    if jobs >= 100_000 {
        group.sample_size(10);
    }
    if jobs >= 1_000_000 {
        group.measurement_time(Duration::from_secs(120));
    }
    group
}

/// Encode never dominates snapshot production: production time is bounded
/// by section-parallel write bandwidth, and encode must stay a small slice
/// of that budget at every scale up to the 1M-job target.
fn bench_encode(c: &mut Criterion) {
    for jobs in scales() {
        let state = synth_state(&SynthConfig::with_jobs(jobs));
        let records = state_to_records(&state);
        let mut bufs = SectionBuffers::default();
        let total_bytes = encode_all(&records, &mut bufs);
        eprintln!("snapshot_codec/encode: jobs={jobs} total_encoded_bytes={total_bytes}");

        let mut group = group_for(c, "snapshot_codec/encode", jobs);
        // Elements, not bytes: the number that matters is jobs/s, so it
        // reads directly against the 1M-job retention target.
        group.throughput(Throughput::Elements(jobs as u64));
        group.bench_with_input(BenchmarkId::new("jobs", jobs), &jobs, |b, _| {
            b.iter(|| {
                let bytes = encode_all(&records, &mut bufs);
                std::hint::black_box(bytes);
            });
        });
        group.finish();
    }
}

/// The rebuild path a learner join takes (ADR 0016): decode every section's
/// records and reassemble a `StateMachine`. The check: single-thread decode
/// rate here, multiplied by the section-shard count the real container
/// will have, must comfortably clear install-snapshot streaming rates —
/// decode must never be the reason a rebuilding node falls behind.
fn bench_decode(c: &mut Criterion) {
    for jobs in scales() {
        let state = synth_state(&SynthConfig::with_jobs(jobs));
        let records = state_to_records(&state);
        let mut bufs = SectionBuffers::default();
        // Pre-encode once, outside the timed loop — only decode +
        // state_from_records is measured.
        encode_all(&records, &mut bufs);

        let mut group = group_for(c, "snapshot_codec/decode", jobs);
        group.throughput(Throughput::Elements(jobs as u64));
        group.bench_with_input(BenchmarkId::new("jobs", jobs), &jobs, |b, _| {
            b.iter(|| {
                let records = decode_all(&bufs);
                let state = state_from_records(records).expect("state_from_records");
                std::hint::black_box(state);
            });
        });
        group.finish();
    }
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
