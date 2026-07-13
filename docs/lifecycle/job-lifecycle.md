# Job Lifecycle

Decided in
[ADR 0013](../decisions/0013-job-attempt-allocation-state-machines.md) (with
[ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md) for
partial scheduling and
[ADR 0029](../decisions/0029-structural-job-attempt-link.md) for the job
machine's structural attempt link): three machines — job, attempt, allocation
— joined at an explicit readiness barrier.

## Jobs, attempts, allocations

A **job** is the durable unit of user intent; it keeps its `JobId` forever and
its state machine is deliberately coarse. An **attempt** is one execution of
the job (`AttemptId`); retries mint a new attempt and re-queue the job. An
**allocation** (`AllocationId`) is an attempt's claim on one node's resources;
it can be committed *before* the node has space and accrue capacity as it
frees. All agent reports are attempt-scoped, which makes duplicates and stale
reports safe to ignore.

## Job machine (user-visible, replicated)

`Submitted → Accepted → Queued → Attempting(AttemptId) →
{Succeeded, Failed, Aborted}`

| From | To | Owner (who commits) | Trigger |
| --- | --- | --- | --- |
| — | Submitted | Coordinator apply of `SubmitJob` | API validated, leader proposed |
| Submitted | Accepted | Coordinator apply | Admission checks pass (synchronous in v1) |
| Accepted | Queued | Coordinator apply | Enqueued under its quota entity |
| Queued | Attempting(id) | Coordinator apply of `CommitPlacements` | Attempt + allocation(s) committed, possibly accruing |
| Attempting(id) | Succeeded / Failed / Aborted | Coordinator apply (resolution) | The attempt reached `Terminal`; outcome, retry policy, and abort resolved in the same apply |
| Attempting(id) | Queued | Coordinator apply (resolution) | Retry: the next attempt gets a fresh id via a later `CommitPlacements`; `Revoked` requeues without consuming retry budget |
| Submitted / Accepted / Queued | Aborted | Coordinator apply of `AbortJob` | No live attempt — abort is immediate |

`Attempting` structurally carries the id of the one in-flight attempt: there
is no live job state without an attempt, and no second slot to fill.
`Attempting(a) → Attempting(b)` is illegal — a new attempt id only ever
arrives by way of `Queued` (the revoke-and-reseat re-plan of
[ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md)
still works: within one `CommitPlacements` apply the revocation resolves the
job to `Queued` and the reseat placement moves it to `Attempting(new)`, so
the job passes through `Queued` in the same apply). There is no job-level
`Finalizing`: the window between "exit observed" and "outcome recorded" is
honestly `Attempting(id)` with the *attempt* in `Finalizing` — once the
attempt reaches `Terminal`, resolution (retry, abort precedence, terminal
outcome) completes atomically in the same apply, and the job never rests in
a resolution state of its own. `Succeeded`, `Failed`, and `Aborted` are
terminal; no other edges are legal.

`JobRecord.current_attempt` is gone; `JobState::attempt(&self)` is a derived
accessor over `Attempting` that cannot disagree with the state. Execution
detail — preparing, running, exiting — is a join at read time between
`Attempting(id)` and that attempt's own state, which `AttemptStateChanged`'s
job-scoped key already supports (ADR 0008); the job enum itself stays this
coarse. See
[ADR 0029](../decisions/0029-structural-job-attempt-link.md) for the full
rationale.

## Attempt machine

`Accruing → Ready → Dispatching → Running → Finalizing → Terminal{outcome}`,
plus a direct edge from every non-terminal state to `Terminal` for early
endings (abort before start, revocation, pull/start failure, node lost).

- **Accruing** — allocations committed but not all funded; skipped when
  capacity is immediately available.
- **Ready** — the barrier: all allocations funded, dispatch may begin. The
  barrier is defined over a *placement group*; in v1 every group is a
  singleton (`GroupId` = `JobId`). Future gang scheduling evaluates the same
  barrier across several jobs' attempts — no new states.
- **Dispatching / Running / Finalizing** — as observed via the agent.

Each terminal attempt records an **outcome** with a classification:

| Outcome | Classification | Retried by default? |
| --- | --- | --- |
| `Exited { code: 0 }` | Success | — |
| `Exited { code ≠ 0 }` | User error | No (retry policy may opt in) |
| `OomKilled` | User error | No |
| `MaxRuntimeExceeded` | User error (policy kill) | No |
| `Aborted` | User request | Never |
| `Revoked` | Platform (scheduler re-plan) | Always requeued, free |
| `PullFailed` / `StartFailed` | User or platform per error detail | Platform yes; user no |
| `NodeLost` | Platform | Yes |
| `AgentError` | Platform | Yes |

## Allocation machine (per node)

`Accruing → Funded → Active → Released`, with early release from `Accruing`
(scheduler revocation, abort) and `Funded` (abort). Revocation is legal only
while accruing; funded allocations are stable. Funding is deterministic
replicated bookkeeping: freed node capacity is pledged to that node's accruing
allocations in commit order during apply. See
[ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md) —
accruing allocations *are* the reservation mechanism, and strict backfill is
checked against their projected funding time.

## Abort

The user command is `AbortJob`; it sets `abort_requested` (who, when, optional
message), legal in every non-terminal state.

- No live attempt → `Aborted` immediately.
- Attempt `Accruing`/`Ready` → allocations released without agent
  interaction; outcome `Aborted`.
- `Dispatching` → `StopJob` sent; the agent journals a tombstone for the
  allocation so a racing `StartJob` is refused.
- `Running` → SIGTERM, configurable grace period (default 30 s), SIGKILL;
  agent reports outcome `Aborted`.
- Abort always wins over retry; once requested, resolution out of
  `Attempting` never returns the job to `Queued`.
- **Truth wins the race**: the job ends `Aborted` only if the abort mechanism
  actually terminated it. A container that exited naturally first keeps its
  real outcome, with `abort_requested` still visible in history.

## Transition ownership

- **API/user** requests transitions (submit, abort, manual retry); it never
  commits them.
- **Scheduler** proposes placements, accruals, and revocations; the
  coordinator validates and commits, and rejected proposals are normal.
- **Coordinator apply loop** commits every transition deterministically,
  including funding, retry, and abort resolution.
- **Agent** is the source of observed attempt transitions, reported and then
  committed by the leader.
- **Reconciler** turns desired/observed discrepancies into commands (node
  lost → attempt `NodeLost`; orphan container → `StopJob`).

The enums in `coppice-core` (`JobState`, `AttemptState`, `AttemptOutcome`) are
the code-side anchor for this document.
