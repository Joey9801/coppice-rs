# 30. Structural job–attempt link: `Attempting(AttemptId)`

- **Status:** Accepted
- **Date:** 2026-07-13
- **Amends:** [ADR 0013](0013-job-attempt-allocation-state-machines.md) (the
  job machine; attempt and allocation machines are unchanged)

## Context

ADR 0013 gave attempts their own state machine, but left two residues of the
older design in the job machine:

1. **Mirror states.** `JobState::{Preparing, Running, Finalizing}` are coarse
   shadows of the attempt machine, maintained by hand: the apply loop performs
   a job transition alongside nearly every attempt transition
   (`mark_attempt_running`, `record_attempt_exited`, `resolve_job`), which is
   duplicated bookkeeping with no independent information.
2. **A weak link.** The job↔attempt association is
   `JobRecord.current_attempt: Option<AttemptId>` sitting *beside* the state
   enum. The invariant — live job states have `Some`, queued/terminal states
   have `None`, and only five (job × attempt × allocation) state triples are
   legal — is enforced only by convention in the apply loop and ~70 lines of
   testkit assertions. Every reader must handle a "supposed to be impossible"
   `None`, and in a Raft-replicated state machine a handle-the-impossible
   branch is a place replicas can deterministically do the wrong thing
   forever.

The rule "at most one attempt in flight per job" is likewise an invariant
rather than a property of the representation.

## Decision

The job machine collapses to one live-execution state that **carries the
attempt it points at**:

`Submitted → Accepted → Queued → Attempting(AttemptId) →
{Succeeded, Failed, Aborted}` (with `Attempting → Queued` on requeue)

```rust
pub enum JobState {
    Submitted,
    Accepted,
    Queued,
    Attempting(AttemptId),
    Succeeded,
    Failed,
    Aborted,
}
```

| From | To | Owner (who commits) | Trigger |
| --- | --- | --- | --- |
| — | Submitted | Coordinator apply of `SubmitJob` | API validated, leader proposed |
| Submitted | Accepted | Coordinator apply | Admission checks pass (synchronous in v1) |
| Accepted | Queued | Coordinator apply | Enqueued under its quota entity |
| Queued | Attempting(id) | Coordinator apply of `CommitPlacements` | Attempt + allocation(s) committed, possibly accruing |
| Attempting(id) | Succeeded / Failed / Aborted | Coordinator apply (resolution) | The attempt reached `Terminal`; outcome, retry policy, and abort resolved in the same apply |
| Attempting(id) | Queued | Coordinator apply (resolution) | Retry: the next attempt gets a fresh id via a later `CommitPlacements`; `Revoked` requeues without consuming retry budget |
| Submitted / Accepted / Queued | Aborted | Coordinator apply of `AbortJob` | No live attempt — abort is immediate |

Rules that fall out of the shape:

- **One attempt in flight is structural.** `Attempting` holds exactly one id;
  there is no second slot to fill and no live state without an attempt.
- **`Attempting(a) → Attempting(b)` is illegal.** A new attempt id only ever
  arrives via `Queued`. The revoke-and-reseat re-plan (ADR 0014) still works:
  within one `CommitPlacements` apply, the revocation resolves the job to
  `Queued` and the reseat placement moves it to `Attempting(new)` — the job
  passes through `Queued` in the same apply.
- **No job-level `Finalizing`.** The window between "exit observed" and
  "outcome recorded" is honestly `Attempting(id)` with the attempt in
  `Finalizing`. Once the attempt reaches `Terminal`, resolution (retry, abort
  precedence, terminal outcome) completes atomically within the same apply,
  exactly as before — the job never rests in a resolution state of its own.
- **`JobRecord.current_attempt` is deleted** (struct field and snapshot
  field). `JobState::attempt(&self) -> Option<AttemptId>` provides the old
  accessor as a *derived* view that cannot disagree with the state.
  `JobRecord.attempts: Vec<AttemptId>` remains the durable history; the final
  attempt of a terminal job is `attempts.last()`.

### Abort semantics (unchanged, restated against the new shape)

`AbortJob` on `Submitted`/`Accepted`/`Queued` is immediate. On
`Attempting(id)`: attempt `Accruing`/`Ready` → terminate now with outcome
`Aborted`; `Dispatching`/`Running` → `StopJob`, truth wins the race;
`Finalizing` → the pending flag steers resolution. Abort still always wins
over retry.

### Observability

The job enum no longer distinguishes preparing/running/finalizing. Detail is
a **join at read time**: UIs and APIs combine `Attempting(id)` with that
attempt's state, which the event stream already supports
(`AttemptStateChanged` carries the owning job as a scope key, per ADR 0008).
Anything that wants a flat "phase" tag (state pills, metrics labels, list
filters) derives it from the joined pair; derived presentation state is never
replicated. `JobStateChanged` events now carry the attempt id inside the
`Attempting` payload — an enrichment, since the association is stamped while
authoritative.

### Wire representation

`coppice.core.v1.JobState` stops being a flat enum and becomes a message with
a `oneof`: unit states encode as empty nested messages, `Attempting` carries
the `AttemptId`. The halfway encoding — flat enum plus a separate attempt-id
field — is rejected: it would re-create the weak link on the wire and require
decode-time cross-field validation. `storage.v1.JobRecord.current_attempt`
(field 6) is removed with its tag and name reserved. The descriptor baseline
(`proto/baseline.binpb`) is deliberately refreshed; this is a pre-release
breaking change made under ADR 0003's evolution rules, not around them.

Raft log size and codec cost are unaffected: no command carries a `JobState`
(commands commit decisions; state is derived by apply). Snapshots shrink
marginally for live jobs — the id that used to be encoded twice (`state` +
`current_attempt`) is encoded once.

## Consequences

- The apply loop's job-side bookkeeping shrinks: state and link change in one
  assignment, `mark_attempt_running` and `record_attempt_exited` no longer
  touch the job, and no path can forget to clear a pointer on requeue —
  forgetting is a type error.
- The testkit's job↔attempt invariant table reduces to attempt↔allocation
  combinations; "live job has an attempt" is no longer checkable because it
  is no longer falsifiable.
- Clients switching on the bare job state see less granularity and must join
  the attempt for detail; the derived phase covers flat-tag consumers.
- `JobState` equality and the transition table become payload-aware; the one
  new rule (`Attempting(a) → Attempting(b)` illegal) is stated explicitly in
  the table above.
- Proto churn: `JobState` message with `oneof`, snapshot field removal,
  baseline refresh, and conversion-code updates in `coppice-proto`.
