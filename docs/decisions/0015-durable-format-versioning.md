# 15. Two-layer versioning for durable Raft storage files

- **Status:** Accepted
- **Date:** 2026-07-07
- **Extends:** [ADR 0002](0002-openraft-with-custom-segment-storage.md),
  [ADR 0003](0003-protobuf-serialization-and-cluster-version-gates.md)

## Context

ADR 0003 versions the *payloads* of durable data: protobuf messages evolved
under additive rules, with semantic changes gated by the Raft-replicated
`ClusterVersion`. It does not version the *containers* those payloads live in:
segment-file headers, entry framing (length-prefix width, checksum algorithm,
field layout), the vote file, the manifest, and the snapshot file structure.
The container layer is what tells a reader how to find the protobuf in the
first place, so it cannot be evolved by protobuf rules — it needs its own
explicit version.

The two container classes have different blast radii:

- **Log segments, the vote file, and the manifest are node-local.** They are
  written and read only by the coordinator that owns the disk.
- **Snapshot files cross node boundaries.** openraft streams them to lagging
  or rebuilding replicas via install-snapshot, so a snapshot container version
  is a wire-compatibility surface, not a local detail.

Node-local files are still not free to change on binary rollout alone:
rollback safety ([versioning](../architecture/versioning.md)) requires that if
binary N+1 has written a file, binary N can still read it — otherwise rolling
back one coordinator makes it unable to open its own log.

## Decision

Every durable file begins with a fixed header: an 8-byte file-type magic, a
`u32` **container format version**, and a `u32` header CRC. One header layout,
four magics (`segment`, `vote`, `manifest`, `snapshot`).

Version handling follows one rule for both container classes, reusing the
ADR 0003 machinery rather than inventing a second mechanism:

- **Readers** support every container version from a documented floor up to
  current. Support is dropped only by a later ADR.
- **Writers** produce a new container version only when `ClusterVersion`
  permits it. A container-format bump ships as: deploy binaries that read
  V(n+1) but write V(n); confirm cluster-wide; bump `ClusterVersion`; binaries
  begin writing V(n+1) *for newly created files only* — existing segments and
  snapshots are never rewritten in place.
- **Unknown or above-range versions fail stop.** A reader that encounters a
  container version it does not support refuses to start and names the binary
  range that can read the file. There is no best-effort parse of an unknown
  container.
- Each bump documents its **downgrade limit** exactly as ADR 0003 requires:
  once V(n+1) files exist on a node, rollback is bounded by the oldest binary
  that reads V(n+1).

Gating node-local formats on `ClusterVersion` is deliberate overkill: a
node-local flip could technically happen per-node once the local binary is
final, but a single gate means one upgrade choreography to operate and reason
about, and `ClusterVersion` already encodes "every replica runs a binary that
reads the new format."

Payload evolution inside the containers remains exactly ADR 0003: entries and
snapshot records are protobuf under additive rules; semantic changes gate on
`ClusterVersion`. The two layers version independently — a container bump does
not imply a schema change, and vice versa.

## Consequences

- "Can this binary open this disk?" has a checkable answer at file-open time,
  before any protobuf is decoded, with a precise error instead of a garbage
  parse.
- Container changes (checksum algorithm, framing, compression, section
  layout) get the same read-both / confirm / flip choreography we already
  planned for schemas — one mechanism, tested once.
- Mixed-version files coexist on one disk indefinitely (old segments in V1,
  new in V2). Readers carry old-version support until an ADR retires it; the
  inspection tooling promised in ADR 0002 must handle every supported version.
- The snapshot container version effectively becomes part of the
  install-snapshot wire protocol; the ClusterVersion gate is what guarantees a
  receiving replica can decode what the leader streams
  (see [ADR 0018](0018-protobuf-records-in-parallel-containers.md) for the
  container itself).
