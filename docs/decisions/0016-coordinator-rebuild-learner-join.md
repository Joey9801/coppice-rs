# 16. Coordinator rebuild via learner join with stamped disk identity

- **Status:** Accepted
- **Date:** 2026-07-07
- **Extends:** [ADR 0002](0002-openraft-with-custom-segment-storage.md)

## Context

Coordinators are expected to be rebuilt from scratch in a rolling fashion —
replaced one at a time, with the new replica resyncing Raft state from the
survivors — including during routine upgrades. This scenario contains a known
Raft safety trap that none of our docs addressed: **a voter that loses its
disk and rejoins under the same node ID has amnesia.** It may have voted in a
term it no longer remembers, or acknowledged log entries it no longer holds;
rejoining as "itself" with empty state can hand out a second vote in the same
term or un-acknowledge committed entries — both split-brain enablers. Raft's
correctness proof assumes a voter's durable state is never rolled back, and
openraft inherits that assumption.

[failure-handling](../operations/failure-handling.md) covered leader failover
and agent restarts, but not coordinator disk loss. Separately, an empty data
directory is ambiguous at startup: is this a deliberate fresh join, a failed
disk mount, or the wrong volume attached? Starting "helpfully" in any of those
cases converts an infrastructure mistake into a consensus-safety incident.

## Decision

**Coordinator node IDs are instance identities, not stable slots.** A node ID
is used by exactly one data directory for its entire life; a rebuilt machine
always joins with a fresh node ID and an empty directory, and the departed
node ID is removed from membership. No node ID is ever reused.

### Disk identity

The manifest ([ADR 0017](0017-log-manifest-truncation-and-purge.md)) is
stamped at initialization with:

- **Cluster UUID** — generated once at `coppice-cli cluster init`, identical
  on every replica, carried in snapshots.
- **Node ID** — this instance's Raft identity.
- **Instance UUID** — generated at directory initialization, distinguishing
  "same node ID, different life" in logs and forensics.

At startup the coordinator refuses to run — fail-stop with a specific error —
if any of these holds:

- The data directory is **non-empty** and its cluster UUID does not match the
  configured cluster (wrong volume / cross-cluster mixup).
- The data directory is **non-empty** and its node ID does not match the
  configured node ID (volume attached to the wrong instance).
- The data directory is **empty** and the process was not started with an
  explicit intent flag (`--bootstrap` for first-cluster-ever,
  `--join` for a fresh replica). An unexpectedly empty directory is treated as
  a failed mount, never as permission to start clean.

### Rebuild procedure

Replacing a coordinator (dead, or healthy-but-being-recycled during an
upgrade) is one operator verb, `coppice-cli coordinator replace`, which drives:

1. Provision the new machine with an empty data directory; start with
   `--join` and a **new node ID**.
2. The leader adds it as a **learner** (non-voting). It catches up via
   snapshot install plus log replay — no availability or quorum impact while
   it syncs.
3. Once caught up within a threshold, promote learner → voter and remove the
   departed node ID in a single joint-consensus membership change (openraft
   owns the mechanics per ADR 0002).
4. The CLI verifies the new voter is healthy and the membership change
   committed before reporting success.

Rolling rebuild of the whole fleet is this procedure repeated serially:
replace one, wait for promotion and health, proceed. Quorum among the
survivors holds throughout because at most one voter is in flight; the cluster
is never asked to trust a resurrected identity.

## Consequences

- The amnesiac-voter hazard is excluded by construction: an empty disk can
  only ever enter the cluster as a new learner, which has no voting history
  to forget. Correctness does not depend on operators remembering the rule —
  the startup refusals enforce it.
- Coordinator addressing must map logical replicas to (current) node IDs
  dynamically — discovery/config cannot hard-code "node 1/2/3" forever. Node
  IDs become allocate-once integers recorded in membership, not slot names.
- A wrong or unmounted volume produces a loud startup failure instead of a
  quietly forked cluster. The cost is one more operator concept (intent
  flags) and the `replace` verb to build in `coppice-cli`.
- Snapshot install becomes the primary resync path for rebuilds and must be
  sized accordingly — reinforcing the streaming/chunked snapshot container
  decided in [ADR 0018](0018-protobuf-records-in-parallel-containers.md).
- [failure-handling](../operations/failure-handling.md) gains its missing
  section: coordinator disk loss is handled by replace, not repair.
