# 18. Protobuf records inside parallel-decodable containers

- **Status:** Accepted
- **Date:** 2026-07-07
- **Extends:** [ADR 0003](0003-protobuf-serialization-and-cluster-version-gates.md),
  [ADR 0015](0015-durable-format-versioning.md)

## Context

ADR 0003 mandates protobuf for all durable formats, chosen for its evolution
rules. Reading and writing the log and snapshots sits on paths with real
serial-bottleneck potential — commit latency, recovery replay, snapshot
production, and rebuild-node resync — so before building the storage layer we
re-examined whether protobuf's known limitations disqualify it there:

- **No random access, no zero-copy.** Varint-heavy encoding must be decoded
  field-by-field into owned structs; you cannot mmap a message and read one
  field.
- **Whole-message decode with a practical ~2 GB ceiling.** A snapshot of the
  1M-live-job target state ([ADR 0012](0012-data-retention.md)) as one
  monolithic message is at or beyond the limit and cannot be streamed.
- **Single-threaded decode** of any one message; parallelism must come from
  having many independent messages.

Alternatives considered:

| Format | Bulk decode | Evolution | Verdict for durable state |
| --- | --- | --- | --- |
| **rkyv** | Fastest available: archived data is the in-memory layout, access is ~free | Manual, weak; format is coupled to Rust type layout and rkyv's own versioning | Rejected: our durability horizon is years of rolling upgrades; ADR 0003's evolution guarantees are the whole point. Rust-only also breaks the shared-schema story |
| **Cap'n Proto / FlatBuffers** | Zero-copy reads; write side is builder-based and awkward in Rust | Sound (additive, like protobuf) | Rejected: zero-copy pays off when you read *in place*, but we hydrate everything into an owned in-memory state machine anyway — the win shrinks to decode CPU, and it costs a second schema universe beside ADR 0003's protobuf |
| **bincode / postcard** | Fast | None — exactly the instability ADR 0003 was written to eliminate | Rejected |
| **Protobuf (prost)** | Good, not best-in-class; parallel across messages | The rules we already committed to | Accepted, with the container doing the performance work |

Two observations decided it. First, **log entries are not free to diverge**:
the bytes on disk are the same `Command` protobufs replicated over the wire
and shared with agents, CLI, and tooling. A different disk encoding means
either transcoding every entry on the hot path or maintaining parallel
schemas for the same types — both worse than protobuf's decode cost. Second,
**the serial chokepoints are not serialization**. Commit latency is
fsync-bound (group commit amortizes it; encoding a small command is
microseconds against a millisecond-class fsync). Apply is serial *by design*
([high-availability](../architecture/high-availability.md)) — no encoding
makes it faster. What remains are the bulk paths — snapshot write, snapshot
read/rebuild, recovery replay — and those parallelize at the **container**
level regardless of record encoding.

## Decision

Record payloads stay protobuf per ADR 0003. Performance is delivered by the
container formats, which are ours to shape:

### Snapshot container

Not one message. A snapshot file is:

- A **header** (magic + container version per ADR 0015) carrying snapshot
  metadata: last-applied log id, membership, `ClusterVersion`, shard count.
- **Sections**, one per entity type × hash-shard of primary key. Each section
  is an independent stream of length-delimited protobuf records with its own
  record count, optional zstd compression, and CRC32C. Sections are the unit
  of parallelism: N shards encode on N cores at write and decode on N cores
  at read, feeding the (serial) state rebuild through a channel.
- A **footer**: section index (type, shard, offset, length), total CRC,
  closing magic. The footer is written last, so a truncated snapshot is
  detectable by construction; readers locate sections via the footer, giving
  random access per section without decoding the world.

The same layout streams over install-snapshot in file order — a rebuilding
node ([ADR 0016](0016-coordinator-rebuild-learner-join.md)) can begin
decoding sections as chunks arrive rather than after full transfer.

### Log segments

Per ADR 0002: length-delimited protobuf entries, each framed with its length
and CRC32C. The entry envelope covers **openraft's payload kinds** — Normal
(our `Command`), Membership, and Blank — and we define our own protobuf
messages for openraft's `Vote`, `LogId`, and membership types rather than
relying on openraft's serde representations, so an openraft upgrade cannot
silently change our durable format. Recovery replay is pipelined: segment
read-ahead and entry decode run ahead of the serial apply loop (decode of
*distinct* entries parallelizes freely), so apply — not parsing — is the
limiter.

### Escape hatch and proof obligations

Every snapshot section header carries an **encoding id**; today the only
value is `protobuf-ld` (± zstd). If profiling ever shows record decode
dominating rebuild, a hot section type can move to a denser encoding behind a
`ClusterVersion` gate (ADR 0015) — a targeted swap, not a rewrite.

The claim "serialization is not the bottleneck" is checked, not assumed: the
storage crate carries a benchmark suite beside the crash-injection suite,
covering append throughput/latency under group commit, snapshot encode and
decode at the 1M-job scale, and cold-recovery replay rate. Regressions gate
changes exactly as the crash suite does.

## Consequences

- One schema universe, end to end: disk, wire, agents, CLI, and the
  inspection tooling all read the same protobufs. No transcoding on any hot
  path.
- Snapshot cost scales with cores, not with one core: shard-parallel encode
  and decode, plus per-section random access for tooling (inspect one entity
  type without reading the file).
- We accept protobuf's decode cost on the bulk paths in exchange for
  evolution guarantees, betting — with benchmarks as the check and the
  encoding id as the exit — that container parallelism keeps storage ahead of
  the serial apply loop, which no format choice can speed up.
- Snapshot rebuild order is per-shard, not global; the state machine's
  rebuild path must not depend on iteration order across sections (already
  required by determinism rules).
- Compression policy (zstd on/off, level) is a per-section writer choice
  readable from the section header — tunable without a format change.
