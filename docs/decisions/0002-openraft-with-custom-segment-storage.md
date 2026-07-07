# 2. Use openraft with custom segment-file storage

- **Status:** Accepted
- **Date:** 2026-07-07
- **Resolves:** [OD-1](../roadmap/open-decisions.md#od-1-raft-library-and-persistence-layer)

## Context

The coordinator control plane is built around a Raft-replicated state machine
(see [high-availability](../architecture/high-availability.md)). The choice of
Raft implementation shapes the `coppice-consensus` API, the snapshot model, the
apply loop, and the operational story, and is expensive to change once built
against. Separately, the Raft log, vote/term metadata, and snapshots need
durable storage whose fsync and recovery semantics we trust.

Candidates considered: `openraft` (async, pluggable storage/network, built-in
snapshot support), `tikv/raft-rs` (proven core, but log storage, transport, and
the driving loop are all left to the integrator), and a hand-rolled
implementation. For persistence: RocksDB, redb, or purpose-built files.

## Decision

Build `coppice-consensus` on **openraft**, with a **custom segment-file storage
layer** implementing openraft's log-storage and snapshot traits:

- **Log**: append-only segment files of bounded size. Entries are
  length-prefixed protobuf (see [ADR 0003](0003-protobuf-serialization-and-cluster-version-gates.md))
  with a per-entry CRC. Appends are batched and fsynced before acknowledgement;
  a torn tail entry is detected by CRC and truncated on recovery. Sealed
  segments are immutable and deleted only after a snapshot covers them.
- **Vote/term metadata**: a small separate file, written atomically
  (write-new + fsync + rename), since Raft correctness depends on votes being
  durable before responding.
- **Snapshots**: a single protobuf snapshot file written to a temp path and
  atomically renamed; the previous snapshot is retained until the new one is
  durable.

The storage implementation must pass openraft's storage test suite, plus our
own crash-injection tests (kill during append, during snapshot install, during
segment rotation).

## Consequences

- openraft owns election, replication, and membership-change correctness; our
  correctness burden concentrates in the storage layer and the deterministic
  state machine — both testable in isolation.
- We own fsync and recovery semantics. This is the deliberately accepted cost
  of choosing purpose-built files over RocksDB: no C++ dependency, storage
  exactly fitted to Raft's append/scan/truncate access pattern, and a format we
  can inspect and repair with our own tooling.
- Crash-safety testing is not optional; the crash-injection suite gates any
  change to the storage layer.
- The `Consensus` trait in `coppice-consensus` becomes a thin adapter over
  openraft rather than an abstraction intended to swap libraries.
