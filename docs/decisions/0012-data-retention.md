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
| Job-history store (SQL, Postgres by default) | Full job + attempt history, final status, usage summaries, audit trail | **90 days** |
| Event log (per coordinator, derived) | Reconnection buffer ([ADR 0008](0008-event-delivery-guarantees.md)) | 1 h / 1M events |
| Metrics (Prometheus) | Telemetry per [observability](../operations/observability.md) | 30 days |
| Container logs | On-node rotated; shipped to a log store when configured | 14 days in the log store; on-node best-effort |

**Eviction is commanded, not clock-driven.** Terminal jobs are written to the
job-history store first, then removed from replicated state by an explicit
`EvictTerminalJobs` command proposed by the leader's housekeeping loop —
timestamps ride in the command, keeping apply deterministic. A job evicted
from Raft state remains queryable through the history store (the API stitches
this seam; eventual-consistency class per
[ADR 0007](0007-per-endpoint-read-consistency.md)).

The history store is a **sink, not a source**: nothing in scheduling reads
from it, so its loss degrades history, never correctness.

## Consequences

- Snapshot size and apply-loop working set stay proportional to live work, not
  cluster age.
- Users get 90 days of "what happened to my job" without bloating consensus
  state; compliance-driven deployments change one knob per store.
- The coordinator needs the housekeeping loop and the history-store write path
  before terminal-job volume matters — early enough to schedule in the first
  milestone after core lifecycle works.
