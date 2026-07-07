# 3. Protobuf serialization with cluster-version feature gates

- **Status:** Accepted
- **Date:** 2026-07-07
- **Resolves:** [OD-2](../roadmap/open-decisions.md#od-2-command-and-snapshot-schema-versioning)

## Context

Old log entries and snapshots must be readable by newer binaries, and rolling
upgrades briefly run mixed versions (see
[versioning](../architecture/versioning.md)). The serialization format and its
evolution rules determine whether upgrades and rollbacks are safe. The current
skeleton uses serde derives, which give no stable wire contract: reordering
fields or inserting enum variants silently changes the encoding.

## Decision

All durable and cross-process formats — Raft log commands, snapshots, the
agent–coordinator protocol, and internal coordinator RPC — are **protobuf,
generated via `prost`**, evolved under the standard rules:

- Field tags are never reused or renumbered; removal is by reservation.
- All changes are additive (new optional fields, new message types, new enum
  variants with explicit unknown-handling).
- Commands are wrapped in a versioned envelope (`Command { version, oneof body }`).

Semantic evolution is governed by a **`ClusterVersion`** stored in replicated
state:

- Each binary advertises the range of cluster versions it supports.
- New command types, new fields with semantic weight, and behavior changes are
  gated: they may be *read* by any supporting binary but are not *written*
  until `ClusterVersion` is bumped by an explicit administrative command
  committed through Raft.
- The leader refuses to bump past the minimum version supported by current
  voting members.
- Rollback to an older binary is supported precisely up to the last
  `ClusterVersion` bump it supports; each bump documents its downgrade limit.

The public HTTP API is JSON at the edge, mapped onto the same protobuf types.

## Consequences

- Upgrade choreography from [versioning](../architecture/versioning.md)
  (read-new/write-old, confirm, flip) gets a concrete mechanism, and
  "can we roll back?" has a checkable answer: the current `ClusterVersion`.
- One schema is shared by coordinator, agent, CLI, and any future non-Rust
  tooling.
- The hand-written serde types in `coppice-core`/`coppice-state`/`coppice-proto`
  will be regenerated from `.proto` definitions; a `coppice-proto` build step
  (prost-build) is added. Rich Rust enums must be modelled as protobuf
  `oneof`s, which is mild but persistent friction — accepted for the evolution
  guarantees.
