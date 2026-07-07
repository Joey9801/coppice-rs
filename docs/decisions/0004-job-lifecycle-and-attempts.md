# 4. Job lifecycle transition table with first-class attempts

- **Status:** Superseded by [ADR 0013](0013-job-attempt-allocation-state-machines.md)
- **Date:** 2026-07-07
- **Resolves:** [OD-3](../roadmap/open-decisions.md#od-3-job-lifecycle-state-machine)

## Context

The job lifecycle is referenced by the API, scheduler, agent, reconciler, and
event system. The `JobState` enum existed but the legal transitions, their
owners, retry identity, and cancellation semantics were undefined — the classic
source of correctness bugs and inconsistent observability.

## Decision

### Attempts are first-class

A **job** is the durable unit of user intent and keeps its `JobId` forever. An
**attempt** is one execution of the job: it has an `AttemptId`, an
`AllocationId`, a node, and its own small state machine. Retry never reuses an
attempt — it creates a new one and returns the *job* to `Queued`. Everything an
agent reports is scoped to an attempt, which is what makes duplicate and stale
reports safe to dedupe.

### Job states and legal transitions

| From | To | Owner (who commits) | Trigger |
| --- | --- | --- | --- |
| — | Submitted | Coordinator apply of `SubmitJob` | API validates, leader proposes |
| Submitted | Accepted | Coordinator apply | Admission checks pass (well-formed, quota entity exists). In v1 this happens in the same apply as submission; the state exists for observability and future async admission |
| Accepted | Queued | Coordinator apply | Enqueued under its quota entity |
| Queued | Reserved | Coordinator apply of scheduler proposal | Reservation earmarked for a whale job ([ADR 0006](0006-reservations-and-strict-backfill.md)) |
| Queued, Reserved | Assigned | Coordinator apply of `CommitPlacements` | New attempt + allocation created, fenced by node epoch |
| Assigned | Dispatching | Coordinator (dispatch loop) | `StartJob` sent to the agent |
| Dispatching | Running | Coordinator apply of agent report | Agent observed container start |
| Running | Completing | Coordinator apply of agent report | Agent observed container exit; finalization (log flush, usage summary) |
| Completing | Succeeded / Failed | Coordinator apply of agent report | Exit status recorded; attempt terminal |
| Assigned / Dispatching / Running / Completing | Retrying | Coordinator apply (retry policy) | Attempt failed (pull failure, start failure, nonzero exit, node lost, dispatch timeout) and policy allows another attempt |
| Retrying | Queued | Coordinator apply | Re-enqueued; next attempt gets a fresh `AttemptId` |
| any attempt-failure point above | Failed | Coordinator apply (retry policy) | Policy exhausted or failure classified non-retryable |
| Submitted / Accepted / Queued / Reserved | Cancelled | Coordinator apply of `CancelJob` | No attempt in flight — cancel is immediate |
| Assigned / Dispatching / Running / Completing | Cancelled | Coordinator apply of agent confirmation (or node-lost) | See cancellation below |

`Succeeded`, `Failed`, and `Cancelled` are terminal. No other edges are legal;
the state machine rejects (and logs) anything else.

### Cancellation

`CancelJob` commits a `cancel_requested` flag valid in any non-terminal state —
there is no separate `Cancelling` enum state; UIs render
`Running + cancel_requested` as "Cancelling". If no attempt is in flight the
apply moves the job straight to `Cancelled`. Otherwise the coordinator issues
`StopJob`, and when the agent confirms termination (or the node is declared
lost) the attempt ends and the job transitions to `Cancelled` instead of
entering retry. A cancel always wins over a concurrent retry decision.

### Ownership summary

- **API/user** requests transitions (submit, cancel, manual retry); it never
  commits them.
- **Scheduler** proposes `Queued → Reserved/Assigned`; proposals are validated
  and committed by the coordinator, and rejected proposals are normal.
- **Coordinator apply loop** commits every transition deterministically,
  including retry-policy and cancellation resolution.
- **Agent** is the source of observed attempt transitions; reports become
  commands committed by the leader.
- **Reconciler** turns desired/observed discrepancies into commands (node lost
  → attempt lost; orphan container → `StopJob`).

## Consequences

- Every component can validate transitions against one table; illegal edges
  become bugs at the point of proposal, not downstream inconsistencies.
- Attempt-scoped reporting gives natural idempotency for the agent protocol
  ([ADR 0009](0009-fencing-and-reconciliation.md)).
- `JobState` in `coppice-core` gains no `Cancelling` variant but the job record
  gains `cancel_requested`; attempts get their own state enum. Code follow-up
  required.
