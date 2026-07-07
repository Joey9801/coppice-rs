# 9. Agent fencing tokens and restart reconciliation

- **Status:** Accepted
- **Date:** 2026-07-07
- **Resolves:** [OD-8](../roadmap/open-decisions.md#od-8-agent-fencing-and-reconciliation-protocol)

## Context

Fencing is the safety boundary against split-brain and duplicate execution: a
deposed leader must not be able to start work, and an agent restart must not
lose track of running containers. The protocol needed concrete tokens, reject
rules, and a reconciliation procedure (see
[agent-coordinator](../protocols/agent-coordinator.md),
[failure-handling](../operations/failure-handling.md)).

## Decision

**Fencing token = (leader_term, node_epoch).**

- `leader_term` is the Raft term of the coordinator leader sending the
  command.
- `node_epoch` is replicated per node, bumped whenever the node registers,
  re-registers, or is declared lost. A bump invalidates all commands issued
  under earlier epochs for that node.

Every coordinator→agent command carries `(leader_term, node_epoch)` plus a
per-node monotonically increasing `command_seq`. The agent durably records the
highest `(leader_term, node_epoch)` it has accepted and **rejects** any
command with a lower term, a mismatched epoch, or a `command_seq` it has
already processed (idempotent re-delivery). Agent reports echo the epoch they
were observed under; the coordinator treats reports carrying a stale epoch as
reconciliation input only, never as authoritative attempt progress.

**Idempotency.** `StartJob` is idempotent on `AllocationId`: an agent that has
already journaled the allocation acknowledges without acting. Observed status
reports are idempotent on `(AttemptId, attempt_state)`: the attempt state
machine ([ADR 0004](0004-job-lifecycle-and-attempts.md)) is monotonic, so the
coordinator's apply naturally drops stale or duplicate reports. The dedup
window is the lifetime of the allocation in replicated state — no timed
windows.

**Agent-side durability.** Before starting a container the agent journals the
intent `(allocation_id, attempt_id, job_id, epoch)` locally; containers are
labeled with allocation and attempt IDs.

**Restart reconciliation.** On agent (re)start:

1. The agent inspects its journal and the container runtime (by label) to
   build the actual running/exited set.
2. It registers, receives its new `node_epoch`, and sends the full
   **ObservedSet** report.
3. The coordinator diffs ObservedSet against replicated intent and commits the
   outcome per allocation: *adopt* (intended and running), *stop* (running but
   no longer intended — unknown or superseded allocation), or *lost* (intended
   but absent → attempt failure, retry policy applies).

The same diff runs periodically against heartbeat-reported running sets as a
background invariant check, not just at restart.

## Consequences

- A deposed leader's commands fail closed at every agent (term check); a
  double-started node cannot act on stale intent (epoch check); redelivered
  messages are harmless (seq/allocation/attempt idempotency).
- Duplicate execution is prevented by construction: an allocation can only be
  adopted or stopped, never re-created, and retries always mint a new
  allocation.
- Agents need a small durable journal with the same crash-safety care as the
  coordinator's storage (fsync before container start).
- The full ObservedSet report keeps reconciliation simple at the cost of
  message size (bounded by ~100 containers/node — acceptable).
