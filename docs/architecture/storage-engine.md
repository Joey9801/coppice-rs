# Storage Engine

This document specifies the segment storage engine: the on-disk layout, the
byte-level file formats, the recovery algorithm, and the crash-ordering
argument for the manifest protocol. It is the implementation contract for
`crates/coppice-consensus/src/storage/`.

It builds directly on decisions made elsewhere and does not restate them:
the segment/vote/manifest layout and recovery procedure
([ADR 0017](../decisions/0017-log-manifest-truncation-and-purge.md)),
container versioning and disk identity
([ADR 0015](../decisions/0015-durable-format-versioning.md),
[ADR 0016](../decisions/0016-coordinator-rebuild-learner-join.md)), and the
protobuf-in-parallel-containers design for logs and snapshots
([ADR 0018](../decisions/0018-protobuf-records-in-parallel-containers.md)).
How this engine is tested — the `Fs` seam, the SimFs crash model, the
crash-injection harness, and the benchmark/compliance gates — is
[storage-testing.md](storage-testing.md). The apply task this engine's
state-machine store is a client of is
[coordinator-runtime.md](coordinator-runtime.md#task-inventory).

## Directory layout

```text
<data-dir>/
  LOCK                    # flock'd for the process lifetime; second opener fails
  manifest                # the pessimistic structural truth, atomic-swap
  vote                    # atomic-swap Raft vote
  log/<start-index>.seg   # append-only segments, named by their first entry's index
  snap/<snapshot-id>.snap # ADR 0018 sharded-section snapshot containers
```

Module ownership, `crates/coppice-consensus/src/storage/`:

| Module | Owns |
| --- | --- |
| `container.rs` | The container header, the two record frame shapes, and the four (plus one) magics. Payload-agnostic — never decodes protobuf. |
| `engine.rs` (`StorageCore`) | The directory itself: the manifest, all structural transitions (segment create, rotation, truncation, purge, snapshot install), recovery, and the `LOCK`. |
| `snapshot.rs` | The ADR 0018 snapshot container codec: assembly, validation, sharded encode/decode. |
| `log.rs` | `SegmentLogStorage` / `SegmentLogReader` — the async `RaftLogStorage` bridge over `StorageCore`. |
| `sm.rs` | `StateMachineStore` — the async `RaftStateMachine` bridge, the apply-task client protocol, and the canonical `run_apply_task` loop. |
| `raftpb.rs` | Conversions between openraft's in-memory types and `coppice.raft.v1` protobuf. |

`engine.rs` is the only module that touches the `Fs` seam for structural
state; `log.rs` and `sm.rs` only convert types and dispatch onto the blocking
pool.

## File formats

### The container header

Every durable file (manifest, vote, segment, snapshot) opens with the same
fixed 16-byte header (`container.rs`, `HEADER_LEN = 16`):

```text
[8B magic][u32 LE container version][u32 LE header CRC32C]
```

The CRC32C covers the first 12 bytes (magic + version). `CONTAINER_VERSION`
is currently `1`; an unknown or out-of-range version fails stop naming the
readable range, before any protobuf is touched (ADR 0015 — "no best-effort
parse of an unknown container").

Four header magics, one per file kind, plus a fifth used only in the
snapshot trailer:

| Constant | Bytes | File |
| --- | --- | --- |
| `SEGMENT_MAGIC` | `b"CPC_SEG\0"` | `log/<start>.seg` |
| `VOTE_MAGIC` | `b"CPC_VOTE"` | `vote` |
| `MANIFEST_MAGIC` | `b"CPC_MANI"` | `manifest` |
| `SNAPSHOT_MAGIC` | `b"CPC_SNAP"` | `snap/<id>.snap` (header) |
| `SNAPSHOT_FOOTER_MAGIC` | `b"CPC_SNPE"` | `snap/<id>.snap` (closing magic, written last) |

### Record framing

Two frame shapes, both payload-agnostic — the container layer never decodes
the protobuf it carries (ADR 0018: recovery, truncation, and
`get_log_state` must never need a payload decode).

**Plain record frame** (`RECORD_OVERHEAD = 8`):

```text
[len u32 LE][crc32c u32 LE][payload]
```

Used for the manifest payload, the vote payload, and the snapshot's meta and
section-index records. The CRC32C covers the payload only.

**Log-entry frame** (`ENTRY_OVERHEAD = 32`), segments only:

```text
[len u32 LE][index u64 LE][term u64 LE][node u64 LE][crc32c u32 LE][payload]
```

The `LogId` (index, term, leader node) rides in the frame itself, not inside
the payload. The CRC32C covers index + term + node + payload (not the
length prefix). This is why recovery scans, suffix truncation, and
`get_log_state` never protobuf-decode an entry: `container::parse_entry`
reads the frame and returns a `FrameLogId` without touching `payload`'s
bytes as anything but an opaque slice.

### The manifest

A `container.rs` header (`MANIFEST_MAGIC`) followed by one plain record
frame carrying `coppice.storage.v1.Manifest` (protobuf, evolves under
ADR 0003 rules). It records:

- **Identity stamps** (ADR 0016): cluster UUID, node ID, instance UUID.
- **The segment list**: ascending start indices, `log/<start>.seg`.
- **The purge floor**: the last purged `LogId`, or absent if nothing has
  been purged.
- **The logical end of log**: an optional override of the physical tail,
  set by a suffix truncation and cleared by the next segment creation.
- **The snapshot pointer**: an optional snapshot id.
- **The best-effort committed index**: recorded opportunistically, never
  load-bearing for correctness (ADR 0017).

It is written **only** on structural events — segment create/rotate, suffix
truncation, purge, snapshot install (plain or learner-rebuild) — never on
the append path. Every write goes through the single `write_manifest`
helper in `engine.rs`, which is `fs::write_atomic`'s four-step discipline:
write-new → `sync_data` → rename → `sync_dir` of the parent. `save_vote`
uses the same `write_atomic` primitive against the `vote` file (not the
manifest) with the same discipline.

### Segments

A segment is `[16B header]` followed by zero or more log-entry frames,
append-only. Segments are named by the index of their first entry
(`log/<start>.seg`) and, once superseded by rotation, are never rewritten —
only deleted wholesale (by purge or truncation) or read.

### The snapshot container

`snapshot.rs`, one file, in order:

```text
[16B header]              magic CPC_SNAP, version, header CRC32C
[meta record]              SnapshotMeta, plain-record framed
[section bytes...]         per (entity kind, hash shard), contiguous, opaque to the container
[index record]              SectionIndex, plain-record framed
[index record length  u32 LE]
[total CRC32C          u32 LE]   covers header..sections only (NOT the index record)
[8B closing magic]         CPC_SNPE, written last
```

The trailer (`TRAILER_LEN = 16`) is `index_len(4) + total_crc(4) +
closing_magic(8)`. Two independent integrity checks compose: the index
record has its own record-frame CRC32C (protecting itself), and the total
CRC32C protects everything before it (header, meta, sections) — the index
record is deliberately outside the total CRC's span since it is the last
thing written before the trailer and already self-checks.

The footer is written last **by construction**: a snapshot whose write was
interrupted at any point cannot carry a valid closing magic, so
`validate_container` treats "closing magic present and correct" as the
adoption test, before it even reaches the total CRC or any section CRC.
Sections carry their own `(offset, length, record_count, encoding,
compression, crc32c)` entries in the index, giving random per-section access
without decoding the world (ADR 0018).

## Recovery, step by step

`StorageCore::open` (`engine.rs`) implements the five ADR 0017 steps exactly:

1. **Lock, then the manifest.** `fs.lock("LOCK")` — a second opener of the
   same directory fails here. Read the manifest, validate its header and
   record CRC, decode it, and check its identity stamps (cluster UUID, node
   ID) against `StorageOptions`; a mismatch fails stop naming the mismatch
   (ADR 0016: wrong volume or cross-cluster mixup).

2. **Orphan sweep.** Delete anything in `log/`, `snap/`, and the directory
   root that the manifest does not claim (segments not in its list, a
   snapshot file whose id doesn't match its pointer, any leftover `.tmp`
   file). Idempotent: an un-synced delete from a prior crash may have
   resurrected a file a previous recovery already removed, and sweeping
   again is harmless.

3. **Open claimed segments; verify headers and chain.** For every segment
   but the tail, open it and check its container header. (Ascending start
   indices were already validated when the manifest decoded.) The tail
   segment's header is checked as part of step 4's scan, not here.

4. **Scan the tail segment.** Read entry frames from the header forward,
   stopping at the manifest's logical end if one is set. This is where the
   **torn-tail self-heal rule** lives:
   - A frame at or before the manifest's claim (`expected <= claim_end`,
     when a logical end is set) that fails to parse is corruption of
     possibly-committed state: **fail stop**.
   - A frame past every claim (there is no logical end, or `expected` has
     advanced past it) that fails to parse is the un-acknowledged tail of an
     append that never completed its `fsync`: **truncate to the last good
     frame boundary**, durably, and stop scanning.
   - Damage inside any **sealed** (non-tail) segment is never self-healed
     either — but the engine doesn't discover it at open. Sealed-segment
     scanning is **lazy**: `ensure_scanned` builds a segment's offset table
     only the first time something reads from it (a `read_payloads` or
     `resolve_index` call), verifying the whole claimed range and failing
     stop on any damage found. This keeps cold-start latency bound by what
     openraft actually replays, not by the full log (ADR 0018: apply, not
     parsing, is the limiter).

5. **Derive and report.** `last_log_id` is derived in priority order:
   1. the manifest's `logical_end`, if set (a prior truncation's override
      always wins);
   2. otherwise the tail scan's last entry, if the tail held any;
   3. otherwise, if there are at least two claimed segments, the entry at
      `tail_start - 1` in the **predecessor** segment (scanned lazily) —
      rotation names segments contiguously, so that entry must exist there;
   4. otherwise the manifest's purge floor (possibly `None`, meaning nothing
      was ever appended).

   The result is then clamped up to the purge floor if the floor is higher
   (a purge can advance past the last locally-visible entry). The vote file
   is read the same way as the manifest (header, record, decode) if present.
   The best-effort committed index is resolved to a full `LogId` when the
   entry is still readable — never required for correctness. The tail
   segment is reopened for appending unless a truncation sealed it (logical
   end set); the snapshot-id counter is advanced past the current pointer so
   newly minted ids never collide with a stamped one.

## The crash-ordering argument

The manifest is the pessimistic truth: **it may claim less log or fewer
snapshots than physically exist on disk, never more.** Every structural
transition durably completes the manifest write before anything is
irreversible, and durably completes any file it points at before the
manifest claims it. A crash at any point either lands before the durable
handoff (recovery sees the old, still-valid state, with the new bytes as
harmless orphans the sweep removes) or after it (recovery sees the new
state as fully formed). There is no window where recovery must guess.

| Transition | Durable order | Why a crash recovers correctly |
| --- | --- | --- |
| **Append** | frame entries → `file.append` → `file.sync_data()` (ack) → **then** update in-memory offset table / `last_log_id`. Manifest untouched. | Only fsynced bytes are acknowledged. A crash mid-append leaves at most one unacknowledged, possibly torn frame at the tail, past every manifest claim (no `logical_end` bounds it) — the step-4 self-heal rule truncates it. Already-fsynced entries parse cleanly and are never touched. |
| **Segment create** (rotation's next append, or the first append ever) | create file → append header → `sync_data` → `sync_dir(log/)` → **then** push the start index onto `manifest.segments`, clear `logical_end`, stamp `committed_index` → `write_manifest`. | Crash before the manifest write: the file exists but is unclaimed — the orphan sweep deletes it at next open, and `open_fresh_segment` overwrites any such leftover unconditionally on retry. Crash after: the segment is claimed and picked up as the (possibly empty) tail. |
| **Rotation** | `rotate()` itself writes nothing; it only clears the in-memory `active` handle (a no-op if the current segment has no entries, so rotation can never mint two segments at one start index). The actual durable claim happens on the **next** append, via segment create above. | Identical crash story to segment create — rotation has no separate durability window of its own. |
| **Suffix truncation** | `write_manifest` (drop segments ≥ `from`, set `logical_end`, fsync) → **then** drop in-memory tables / clear `active` → delete the dropped segment files → `sync_dir(log/)`. | Crash before the manifest fsync: nothing changed: the truncation never took effect, and openraft can retry it. Crash after: the dropped segments are already unclaimed, so the orphan sweep deletes whatever's left of them at next open regardless of how far the explicit delete loop got — the delete loop is an optimization, not the correctness mechanism. Sealed bytes past `logical_end` are never rewritten, only ignored. |
| **Purge** | `write_manifest` (advance `purge_floor`, drop fully-covered segments, fsync) → **then** in-memory update → delete segment files → `sync_dir(log/)`. | Same shape as truncation: manifest-first means a crash mid-delete just leaves orphans the next open's sweep clears. |
| **Snapshot build-flip** (`install_snapshot(container, advance_floor=false)`) | validate the container → write to `snap/<id>.snap.tmp` → `sync_data` → rename to `snap/<id>.snap` → `sync_dir(snap/)` (**durable before anything points at it**) → **then** flip `manifest.snapshot_id`, stamp `committed_index` → `write_manifest` → **then** delete the previous snapshot file → `sync_dir(snap/)`. | Crash before the rename+dir-sync: the new file is a `.tmp` (or an un-synced rename), swept as an orphan; the manifest still points at the old snapshot (or none). Crash after the file is durable but before the manifest fsync: the new snapshot exists but is unclaimed, swept at next open. Crash after the manifest fsync: the previous snapshot is now unclaimed even if the explicit delete step never ran — the orphan sweep removes it, so "delete-after-flip" is enforced by the sweep, not by the delete call completing. |
| **Learner install** (`install_snapshot(container, advance_floor=true)`) | Identical snapshot-file durability as above, but the **same single `write_manifest` call** both flips `snapshot_id` **and** advances `purge_floor` / drops now-covered log segments — one atomic swap carries both facts. Log segment deletion and old-snapshot deletion both happen only after that one manifest write is durable. | A crash anywhere before the manifest write leaves the pre-install state fully intact (new snapshot file orphaned, swept). A crash anywhere after leaves both the pointer flip and the floor advance already committed together — there is no state where one took effect without the other, because they share one `write_manifest` call. Superseded log segments and the old snapshot are orphans either way and are swept if the explicit deletes didn't finish. |
| **Vote write** | `write_atomic("vote", ...)`: write-new → `sync_data` → rename → `sync_dir(parent)` — **only after this returns** does `save_vote` update `self.vote` and the call is acknowledged to openraft. | A crash during the write_atomic sequence is, by definition, before acknowledgment — SimFs may drop, apply, or tear the un-synced write, but Raft never learns the vote happened, so no correctness property (vote monotonicity) is violated. Once acknowledged, the vote is fully durable by construction — there is no post-ack crash window. |

## The async bridge

`StorageCore` is synchronous, built directly against the `Fs` seam
(`storage-testing.md`'s seam-is-synchronous rule). openraft's storage traits
are async; `log.rs` and `sm.rs` bridge every call through
`tokio::task::spawn_blocking` over one `Arc<Mutex<StorageCore<F>>>` shared
between the log store and the state-machine store (they share it because the
manifest is the single durable home of both the segment list and the
snapshot pointer).

A dedicated writer thread for the group-commit loop was considered and
deliberately not built. openraft 0.9 serializes all storage write IO through
its own core loop, so no batch of appends is ever handed to the storage
layer concurrently — cross-call group commit never materializes regardless
of threading. "Group commit" here just means the one batch openraft hands to
a single `append` call is written and fsynced together; a mutex plus
`spawn_blocking` gets the identical fsync schedule with less machinery than
a dedicated thread would add.

## Apply-task reconciliation

`StateMachineStore` (`sm.rs`) does not own the applied `coppice_state::StateMachine`
— the apply task does, by value, per
[coordinator-runtime.md](coordinator-runtime.md#state-ownership-and-views).
`StateMachineStore` is a **client** of that task's `ApplyRequest` protocol:

- `apply` batches the entries' `Normal` commands into one
  `ApplyRequest::Apply` and awaits the replies, so openraft's replication
  feels the apply task's backpressure directly. `Blank` and `Membership`
  entries never reach the apply task at all — they carry no state-machine
  command, so the store just records `(last_applied, membership)` locally
  and answers `Ok(Applied::default())`.
- `build_snapshot` asks for a coherent state via `ApplyRequest::Snapshot`;
  serialization and the durable write happen off the apply task, on the
  blocking pool.
- `install_snapshot` decodes and validates the incoming container fully
  (every section CRC checked, every record required to convert) **before**
  anything durable changes, then swaps state via `ApplyRequest::Install`.

An `Arc<tokio::sync::Mutex<AppliedState>>` — the **coherence lock** — is
held across every `apply` round-trip and every snapshot-state capture, so a
concurrent snapshot build can never observe a `(state, last_applied)` pair
that is torn between two different apply rounds.

Recovery of applied state is two composed paths, not one ad hoc one:
**snapshot restore at open** (`storage::open` decodes the manifest's current
snapshot, if any, into a fresh `StateMachine` before the caller ever sees
`Recovered`) **plus openraft-driven replay** from the manifest's best-effort
committed index — openraft's own startup path re-applies committed log
entries above the snapshot through this same `apply` method, so replay and
live application are one code path.

## Testing

The crash-injection suite now drives the real engine directly: `RealEngine`
in `crates/coppice-consensus/tests/crash_storage.rs` implements
`CrashSubject` over `StorageCore<SimFs>`, running the mandated scenario
sweeps (mirroring `coppice-testkit/tests/crash_scenarios.rs`, where the toy
engine runs the same set) plus directed tests for orderings the harness's
abstract vocabulary can't express (notably the learner-install case above,
where one manifest swap carries two facts). The compliance suite
(`tests/openraft_compliance.rs`) runs unconditionally against
`SegmentLogStorage` / `StateMachineStore` on a real filesystem — no feature
gate. See [storage-testing.md](storage-testing.md) for the harness's
invariants and the toy engine that remains its self-test.
