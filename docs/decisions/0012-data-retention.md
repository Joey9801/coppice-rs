# 12. Per-store data retention with terminal-job eviction from Raft

- **Status:** Accepted
- **Date:** 2026-07-07
- **Resolves:** [OD-11](../roadmap/open-decisions.md#od-11-data-retention-policy)

## Context

Retention drives storage cost, debuggability, and snapshot size. The
[storage boundaries](../architecture/data-storage-boundaries.md) already
separate the stores; each needs a policy, and the replicated state in
particular must not accumulate history indefinitely (1M queued jobs is the
*live* target — terminal jobs must leave).

## Decision

Retention is configured per store; the defaults are:

| Store | Contents | Default retention |
| --- | --- | --- |
| Raft state machine | Live jobs, attempts, allocations, reservations, nodes, quota state | Terminal jobs evicted **72 h** after reaching a terminal state |
| Job-history store (SQL, ClickHouse by default; `none` is a supported explicit configuration) | Full job + attempt history, final status, usage summaries, audit trail | **90 days** |
| Event log (per coordinator, derived) | Reconnection buffer ([ADR 0008](0008-event-delivery-guarantees.md)) | 1 h / 1M events |
| Metrics (Prometheus) | Telemetry per [observability](../operations/observability.md) | 30 days |
| Container logs | On-node rotated; shipped to a log store when configured | 14 days in the log store; on-node best-effort |

**Eviction is commanded, not clock-driven.** When a history store is
configured, terminal jobs are written to it first, then removed from
replicated state by an explicit `EvictTerminalJobs` command proposed by the
leader's housekeeping loop — timestamps ride in the command, keeping apply
deterministic. A job evicted from Raft state remains queryable through the
history store (the API stitches this seam; eventual-consistency class per
[ADR 0007](0007-per-endpoint-read-consistency.md)).

The history store is a **sink, not a source**: nothing in scheduling reads
from it, so its loss degrades history, never correctness.

**Running without a history store is supported, and explicitly lossy.**
Deployments that don't need durable history (dev clusters, ephemeral CI
fleets, cost-sensitive installs) declare the `none` history mode:

```toml
[history]
mode = "none"
```

The mode is never inferred from a missing backend, and in it the system
never reports a history write as durable. Terminal-job data — jobs,
attempts, allocations, usage summaries, logs — is retained **best effort**
until a configurable TTL, which replaces the durable-receipt gate on
`EvictTerminalJobs`. Best effort means:

- Data stored opportunistically on the agents that ran the jobs (container
  logs, usage detail) rather than in replicated state is not persisted or
  migrated elsewhere on planned agent destruction (drain, decommission,
  scale-down); it is lost with the agent.
- Agent-local data is a valid candidate for reclamation under disk pressure
  before the TTL.

API reads surface what is still available and degrade gracefully — partial or
absent history is a normal response, not an error.

## Consequences

- Snapshot size and apply-loop working set stay proportional to live work, not
  cluster age.
- Users get 90 days of "what happened to my job" without bloating consensus
  state; compliance-driven deployments change one knob per store.
- Lossy deployments trade durability for zero external dependencies without
  lying about it: `[history] mode = "none"` is a visible configuration
  choice, and the TTL bounds how long best-effort data lingers.
- The coordinator needs the housekeeping loop and the history-store write path
  before terminal-job volume matters — early enough to schedule in the first
  milestone after core lifecycle works.
