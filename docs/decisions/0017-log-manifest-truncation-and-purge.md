# 17. Log manifest with logical truncation and ordered purge

- **Status:** Accepted
- **Date:** 2026-07-07
- **Amends:** [ADR 0002](0002-openraft-with-custom-segment-storage.md)

## Context

ADR 0002 declared sealed segments immutable and handled the torn *tail* entry
by CRC-and-truncate. That is not the only mutation Raft demands of the log.
On leader change, a follower whose log diverges is told to **truncate a
suffix**: discard everything from some index N onward, where N can land in
the middle of a sealed segment. "Sealed segments are immutable" as written is
in conflict with Raft itself.

Three more bookkeeping facts need a durable home that ADR 0002 never
assigned: which segments exist and where the log logically starts (the purge
floor advances every time a snapshot covers old segments), where the log
logically *ends* when that differs from the last physical byte (after a
suffix truncation), and optionally how far apply had progressed (openraft
lets storage persist the committed index to speed recovery, but does not
require it). Recovery also needs a defined discovery procedure and protection
against two processes opening the same directory.

## Decision

Sealed segments become **physically immutable, logically truncatable**. All
structural facts about the log live in a single small **manifest** file.

### Directory layout

```
<data-dir>/
  LOCK                    # flock'd for the process lifetime; second opener fails
  manifest                # this ADR
  vote                    # unchanged from ADR 0002: atomic write + rename
  log/<start-index>.seg   # segments named by the index of their first entry
  snap/<snapshot-id>.snap # ADR 0018
```

### The manifest

A small protobuf file (container-versioned per
[ADR 0015](0015-durable-format-versioning.md)) holding: the cluster UUID,
node ID, and instance UUID ([ADR 0016](0016-coordinator-rebuild-learner-join.md));
the segment list with start indices; the **purge floor** (last purged log id);
an optional **logical end of log** overriding the physical tail; a pointer to
the current snapshot; and a best-effort committed index. It is updated by
atomic write-new + fsync + rename + parent-dir fsync — the same discipline as
the vote file.

The manifest is written only on rare structural events — segment rotation,
suffix truncation, purge, snapshot completion, clean shutdown — never on the
append path. Appends and their fsyncs touch only the active segment.

### Suffix truncation

`truncate(N)` (conflict resolution on leader change, rare):

1. Write the manifest recording logical end = N−1 and dropping segments whose
   start index ≥ N; fsync before acknowledging to openraft.
2. Delete dropped segment files (order irrelevant once the manifest is
   durable).
3. The segment containing N−1 keeps its stale physical bytes; recovery reads
   it only up to the manifest's logical end. Subsequent appends open a *new*
   segment starting at N — sealed bytes are never overwritten.
4. The next rotation clears the logical-end override.

### Purge

When a durable snapshot covers a prefix (ADR 0002's deletion rule):

1. Write the manifest advancing the purge floor and dropping covered
   segments; fsync.
2. Delete the segment files.

A crash between the steps leaves orphan files below the purge floor; startup
deletes anything the manifest does not claim. The manifest is ordered first in
both truncation and purge so the manifest is always the pessimistic truth:
it may claim less log than physically exists, never more.

### Committed index

Best-effort only: recorded in the manifest at rotation, snapshot, and clean
shutdown. Recovery may replay entries that were already applied — apply is
deterministic and idempotent from a snapshot, so correctness never depends on
this field; it exists to shorten replay after clean restarts.

### Recovery procedure

1. Take the `LOCK`; read and validate the manifest (magic, version, CRC,
   identity stamps per ADR 0016).
2. Delete orphan segment/snapshot files not claimed by the manifest.
3. Open claimed segments; verify headers and chain of start indices.
4. Scan the last segment (up to logical end, if set) entry-by-entry; a CRC
   failure at the tail truncates the torn entry (ADR 0002). A CRC failure
   *before* the tail — or in any sealed segment when it is read — is
   corruption of possibly-committed state: **fail stop**, never truncate. The
   remedy is `coordinator replace` (ADR 0016), with the ADR 0002 inspection
   tooling for forensics.
5. Report vote, purge floor, log range, and snapshot to openraft.

## Consequences

- ADR 0002's immutability claim is amended to something Raft-compatible, and
  every structural transition (rotate, truncate, purge, snapshot) is a single
  atomic manifest swap with a defined crash story — exactly what the
  crash-injection suite will exercise.
- The append hot path is untouched: manifest writes happen only at rare
  structural events, so the extra fsyncs cost nothing at steady state.
- Truncation never rewrites sealed bytes, so segment files stay append-only
  artifacts that tooling can checksum and diff.
- Corruption policy is now explicit: torn tails self-heal, everything else
  fail-stops into the node-replace path rather than guessing about committed
  state.
- The manifest is a single point of parse failure at startup — mitigated by
  its small size, CRC, atomic-swap update, and the fact that it can be
  regenerated by tooling from segment headers in the worst case (all its
  contents except the purge floor and identity stamps are derivable).
