# 13. Job, attempt, and allocation state machines with abort semantics

- **Status:** Accepted; job machine amended by
  [ADR 0029](0029-structural-job-attempt-link.md)
- **Date:** 2026-07-07
- **Supersedes:** [ADR 0004](0004-job-lifecycle-and-attempts.md)

## Context

ADR 0004 fixed the transition table but mirrored attempt phases in the
user-visible job enum and treated cancellation as one flag with immediate/
confirmed variants. Three requirements broke that shape:

1. Jobs must be abortable at any stage, and users must be able to tell
   explicitly that a job ended *because of that mechanism* rather than OOM,
   `max_runtime` breach, natural exit, or platform failure.
2. Whale jobs can be **partially scheduled** onto a node that doesn't yet have
   space to run them, accumulating capacity as it frees.
3. Groups of jobs may one day be scheduled together (distributed multi-node
   jobs that communicate at runtime), and the lifecycle must leave room for
   that without a redesign.

## Decision

Three machines, joined at an explicit readiness barrier.

### Job machine (user-visible, replicated, deliberately coarse)

`Submitted → Accepted → Queued → Preparing → Running → Finalizing →
{Succeeded, Failed, Aborted}`

| From | To | Owner (who commits) | Trigger |
| --- | --- | --- | --- |
| — | Submitted | Coordinator apply of `SubmitJob` | API validated, leader proposed |
| Submitted | Accepted | Coordinator apply | Admission checks pass (synchronous in v1) |
| Accepted | Queued | Coordinator apply | Enqueued under its quota entity |
| Queued | Preparing | Coordinator apply of `CommitPlacements` | Attempt + allocation(s) committed, possibly accruing |
| Preparing | Running | Coordinator apply of agent report | Container observed started |
| Preparing | Finalizing | Coordinator apply | Attempt ended before running (abort, revocation, pull/start failure) |
| Running | Finalizing | Coordinator apply of agent report | Exit observed, or attempt lost |
| Finalizing | Succeeded / Failed / Aborted | Coordinator apply (resolution) | Outcome recorded; retry policy and abort resolved here |
| Finalizing | Queued | Coordinator apply (resolution) | Retry: fresh `AttemptId`; `Revoked` outcomes requeue without consuming retry budget |
| Submitted / Accepted / Queued | Aborted | Coordinator apply of `AbortJob` | No live attempt — abort is immediate |

Every attempt end funnels through `Finalizing`, where outcome, retry, and
abort are resolved in one deterministic place. All execution detail lives on
the attempt, so this enum stays stable as the attempt machine evolves (the
change from ADR 0004's mirrored `Assigned/Dispatching/...` job states).
`Reserved` is gone as a job state: a whale waiting for capacity is `Preparing`
with an accruing allocation ([ADR 0014](0014-accruing-allocations-replace-reservations.md)).

### Attempt machine

`Accruing → Ready → Dispatching → Running → Finalizing → Terminal{outcome}`,
plus a direct edge from every non-terminal state to `Terminal` for early
endings (abort before start, revocation, pull/start failure, node lost).

- **Accruing**: allocations committed but not all funded. Skipped when
  capacity is immediately available (the common case).
- **Ready** is the barrier: all allocations funded. It is defined over a
  *placement group* from day one; in v1 every group is a singleton
  (`GroupId` = `JobId`), so the barrier is trivially per-job. Gang scheduling
  later means evaluating the same barrier across several jobs' attempts — no
  new states. (How long a `Funded` allocation may wait on slow group peers is
  deliberately deferred to the gang-scheduling ADR.)
- **Dispatching → Running → Finalizing** as observed by the agent.

### Terminal outcomes

Every terminal attempt records an **outcome** and a **classification**:

| Outcome | Classification | Retried by default? |
| --- | --- | --- |
| `Exited { code: 0 }` | Success | — |
| `Exited { code ≠ 0 }` | User error | No (job retry policy may opt in) |
| `OomKilled` | User error | No |
| `MaxRuntimeExceeded` | User error (policy kill) | No — deterministic recurrence |
| `Aborted` | User request | Never |
| `Revoked` | Platform (scheduler re-plan) | Always requeued, free — doesn't consume retry budget |
| `PullFailed` / `StartFailed` | User or platform per error detail | Platform: yes; user (bad image ref): no |
| `NodeLost` | Platform | Yes |
| `AgentError` | Platform | Yes |

The job's terminal state derives from the final attempt's outcome, so "did it
end because I aborted it, vs. OOM, vs. `max_runtime`, vs. exiting on its own"
is always answerable from recorded state, never inferred.

### Abort semantics

- The user command is **`AbortJob`** (renaming `CancelJob`; "abort" is the
  vocabulary everywhere). It commits `abort_requested` (who, when, optional
  message), legal in every non-terminal state.
- No live attempt → `Aborted` immediately. Attempt in `Accruing`/`Ready` →
  allocations released, no agent interaction, outcome `Aborted`. In
  `Dispatching` → `StopJob` is sent; the agent journals a **tombstone** for
  the allocation so a racing `StartJob` is refused. In `Running` → SIGTERM,
  configurable grace (default 30 s), SIGKILL; the agent reports outcome
  `Aborted`.
- Abort always wins over retry: once `abort_requested` is set, `Finalizing`
  never resolves to `Queued`.
- **Truth wins the race**: the job ends `Aborted` only if the abort mechanism
  actually terminated the attempt. If the container exited naturally first,
  the real outcome is recorded and `abort_requested` remains visible in job
  history — the terminal state never lies about what stopped the work.

### Allocation machine (per node)

`Accruing → Funded → Active → Released`, with `Accruing/Funded → Released` on
abort and `Accruing → Released` on scheduler revocation (revocation is legal
*only* while accruing; a funded allocation is stable). Funding is
deterministic replicated bookkeeping: when capacity frees on a node, the apply
loop pledges it to that node's accruing allocations in commit order — the
agent enforces but never decides funding.

## Consequences

- The user-visible job enum is small and stable; clients that switch on it
  survive attempt-machine evolution (accrual now, gangs later). UIs show
  job state plus current attempt state for detail.
- Abort is explicit, honest, and race-free by construction; the tombstone
  rule closes the dispatch race without coordination.
- `Revoked` gives the scheduler a safe re-planning primitive that never
  punishes the job.
- Group scheduling has a reserved seam (the `Ready` barrier over placement
  groups) rather than a promise of redesign.
- Code impact in `coppice-core`: new `JobState` variants
  (`Preparing`, `Finalizing`, `Aborted` replacing
  `Reserved/Assigned/Dispatching/Completing/Retrying/Cancelled`), new
  `AttemptState` and `AttemptOutcome` enums, `AbortJob` command.
