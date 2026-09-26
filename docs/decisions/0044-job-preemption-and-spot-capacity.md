# 44. Job preemption: opt-in, notice, resumption identity, and spot-only nodes

- **Status:** Proposed
- **Date:** 2026-09-24
- **Builds on:** [ADR 0013](0013-job-attempt-allocation-state-machines.md)
  (attempt and allocation machines, `Revoked`, abort-as-a-flag, truth wins
  the race), [ADR 0014](0014-accruing-allocations-replace-reservations.md)
  and [ADR 0027](0027-finite-projected-ready-accrual-protection.md)
  (accruing allocations, guaranteed release events, `projected_ready`),
  [ADR 0019](0019-deterministic-quota-arithmetic.md) and
  [ADR 0029](0029-runtime-declaration-incentives.md) (charge multipliers,
  true-up, the platform-class refund rule), [ADR 0021](0021-effective-score-ranking.md)
  (`effective_score`), [ADR 0030](0030-structural-job-attempt-link.md)
  (`JobRecord.attempts` as durable lineage), [ADR 0041](0041-graceful-scale-in-drain-and-node-eviction.md)
  (agent drain, `shutdown_grace`, spot wiring as documentation)
- **Amends:** ADR 0041 — the operator drain is redefined in §7
- **Graduates:** [FF-5](../roadmap/future-features.md#ff-5-job-preemption),
  the identity half of [FF-6](../roadmap/future-features.md#ff-6-checkpoint-awareness),
  the spot half of [FF-22](../roadmap/future-features.md#ff-22-spot-capacity-and-autoscaling)

This record is a firm statement of design, not a schedule. It fixes the
contracts — what a job declares, what it is told, what it is charged, and
how the scheduler and agent behave — so that later work can implement them
in pieces without re-opening the design. Section 9 records the
alternatives weighed on review and why they were set aside.

## Context

Nothing in Coppice can interrupt running work for the platform's benefit.
Every platform-initiated termination today is either a user abort
(`AbortJob`), a limit breach, or the node falling silent (`NodeLost`).
ADR 0041 made planned scale-in a drain-and-wait: the agent stops taking
placements and waits up to `shutdown_grace` for work to finish, and work
that outlives the window is *left running* for the ASG to kill, which the
coordinator later classifies `NodeLost`. That is the honest verdict for a
job that was never told anything, but it means a job that could have saved
its progress in thirty seconds instead loses hours.

The motivating deployment is **spot capacity**: EC2 instances that are
reclaimed on a two-minute notice, at a price that makes them the right
home for a large class of batch work *if* that work can be interrupted
cheaply. Two things make that class large in this domain: long jobs that
already checkpoint to external storage for their own crash safety, and
worker-pattern jobs whose natural unit of work is short. Neither can use
spot today, because a reclaim is indistinguishable from a crash and the job
has no way to know it is about to happen.

The same primitive — terminate a running attempt on the platform's
initiative, with warning — is what lets a high-priority job start on a full
cluster instead of waiting, which FF-5 has wanted since the wishlist was
written. Two features, one mechanism.

Three existing decisions shape the answer:

- ADR 0013 already has a free, non-punitive requeue outcome (`Revoked`),
  but only for allocations that are still *accruing*: "a funded allocation
  is stable". Preemption is the deliberate exception to that rule, and must
  stay a distinct outcome so the terminal state never lies about why an
  attempt ended.
- ADR 0014/0027 built the scheduler around **guaranteed release events**:
  the only capacity the scheduler may plan against is capacity with a hard
  bound on when it frees. A preemption *manufactures* such a bound. That
  observation is the core of this design — the incoming job does not need
  a new waiting mechanism, it accrues against the release the preemption
  guarantees.
- ADR 0029 prices declarations through multipliers folded into the charge
  at placement and settled at true-up, and refunds unused charge in full
  for platform-class outcomes. A preemptibility discount and preemption's
  economics both fit that arithmetic without new state-machine math.

## Decision

### 1. What a job declares

The job spec gains an optional `[preemption]` table:

```toml
[preemption]
preemptible = true          # default false
notice = "2m"               # wanted notice before SIGTERM; default: policy floor
notice_signal = "SIGUSR1"   # default SIGUSR1; from a fixed allowlist
```

- **`preemptible`** is the *permission*: the scheduler may choose to
  terminate this job's running attempt to make room for other work, and the
  job may be placed on nodes that only accept such work (§6). It is
  immutable after submission, like every other placement-relevant field.
- **`notice`** and **`notice_signal`** are the *notice contract*, and are
  independent of `preemptible`. Any platform-initiated termination that has
  time to give — a priority preemption, a spot reclaim, a drain with a
  deadline — delivers the signal and then waits out the notice before the
  ordinary stop path. A non-preemptible job may declare a notice contract
  and will receive it when its node drains; it will simply never be
  *chosen* for preemption. The allowlist is `SIGHUP`, `SIGINT`, `SIGUSR1`,
  `SIGUSR2`, `SIGTERM`; a job that names `SIGTERM` gets one signal and a
  longer grace, which is a legitimate contract for images that already
  handle it.
- `notice` is validated against replicated policy: `preemption_notice_floor
  ≤ notice ≤ preemption_notice_cap` (defaults **60 s** and **5 min**). The
  floor is a *guarantee* for every termination whose clock the platform
  controls — a priority or relocation preemption and an operator deadline
  drain (§5, §7). It is **not** a guarantee for an external interruption,
  where the provider holds the clock and the platform can only pass on
  what it was given (§8). The cap bounds how long a preemption can hold
  capacity hostage, and is deliberately short: the feature exists for spot
  capacity, whose notice is shorter still, so a long window buys little
  and costs the cluster a lot.

**A quota entity may switch preemption off for its subtree.** `QuotaEntity`
gains `preemptible: bool` (default `true`). A job's *effective*
preemptibility is `job.preemptible && every ancestor entity's
preemptible`, resolved at `commit_placements` and **recorded on the
attempt**, so a mid-flight policy edit neither exposes nor protects work
already running. A job that is not effectively preemptible receives no
discount (§6). This is for the production queue whose jobs look exactly
like the pre-production runs beside them: the same image submitted under
the production entity is simply never a victim, without every submitter
having to remember to say so.

### 2. What a job is told

Every container is started with a reserved environment block, injected by
the agent from fields carried on `StartJob`. User `env` may not set names
beginning with `COPPICE_`; env validation (ADR 0042's sibling in
`coppice-core::env`) rejects them at the API and at apply.

| Variable | Value |
| --- | --- |
| `COPPICE_JOB_ID` | The job's id |
| `COPPICE_ATTEMPT_ID` | This attempt's id |
| `COPPICE_ATTEMPT_INDEX` | 0 for the first attempt, then 1, 2, … (`JobRecord.attempts` position) |
| `COPPICE_PREVIOUS_ATTEMPT_ID` | The id of the most recent earlier attempt that reached `Running`; unset if none did |
| `COPPICE_PREVIOUS_ATTEMPT_OUTCOME` | That attempt's outcome name (`preempted`, `node_lost`, `exited`, …); unset with the id |
| `COPPICE_PREEMPTIBLE` | `1` or `0` |
| `COPPICE_NOTICE_SIGNAL` | The declared signal name, e.g. `SIGUSR1` |
| `COPPICE_NOTICE_MIN_S` | The policy floor: the notice this attempt is *guaranteed* before a preemption or operator deadline drain stops it; an external interruption (§8) is best effort and may leave less |
| `COPPICE_NOTICE_S` | The declared (or defaulted) notice: the most this attempt will get |

This is the **resumption identity** contract, and it is deliberately all
that crosses incarnations in this record: the control plane carries *who
you are and what happened to the last incarnation that ran*, never
checkpoint data and not yet a checkpoint pointer. The "previous" attempt
is the last one that reached `Running`, not the immediately preceding
lineage entry: an attempt revoked while accruing (ADR 0013) never executed
and can have touched nothing, and the workload has no reason to know it
existed. `COPPICE_ATTEMPT_INDEX` still counts every lineage entry, so the
two can differ by more than one. Because the block is derived from
`JobRecord.attempts` and the attempt map, it is correct even when the
previous attempt's end was never reported — a spot instance that dies
before its exit report lands ends `NodeLost`, and the successor still
learns its predecessor's id.

The previous attempt id is a **disambiguator, not a discovery mechanism**:
a job derives its checkpoint location from `COPPICE_JOB_ID` (stable across
attempts), keeps a job-scoped manifest of completed checkpoints there, and
uses the previous id only to recognise a checkpoint the last incarnation
may have left half written. A job that keys its checkpoints by attempt id
alone cannot be helped by this block: if attempt 1 checkpointed and
attempt 2 was killed before it could, attempt 3 learns only attempt 2's
id. Exposing the whole lineage as structured data — a file injected into
the container or a query on the agent's local node service — is the right
answer to that case and belongs to the FF-6 record beside the checkpoint
pointer, since both are "what the platform knows about your history" and
both want the same delivery surface. Cramming the lineage into an
environment variable was considered and rejected (§9).

A later record will add `COPPICE_CHECKPOINT` (an opaque pointer the
workload reported through the agent — the FF-6 reporting surface) beside
these; the names here are chosen so that addition is purely additive.

### 3. What preemption looks like to the job

The sequence on the node, for an attempt with a notice contract:

1. **Notice.** On receiving the command (`PreemptJob`, or a deadline
   drain reaching its notice phase, §7) the agent first journals a
   *notice intent* (allocation, reason), fsynced. It then delivers
   `notice_signal` to the container's PID 1, *then* journals a *notice mark* (allocation,
   deadline), *then* reports the mark to the coordinator (§4, §8). The
   two records mean different things and neither is redundant. The
   **intent** records that the platform asked, before anything could
   happen, so no exit after it can be mistaken for the job's own failure
   (§4). The **mark** records that the signal was delivered, and only it
   starts a countdown the coordinator may plan against. Its order relative
   to the signal is the reverse of ADR 0009's start barrier, for the
   reverse reason: a container started without a record is an orphan,
   but a signal delivered without a record is only a duplicate. On
   recovery an intent without a mark over a still-running container is
   delivered again with a fresh countdown; an intent with a mark resumes
   the countdown the mark recorded. The contract with the workload is
   that the notice signal **may arrive more than once** and every
   delivery means the same thing. Docker delivers signals to PID 1 only;
   the documentation carries the same shell-wrapper warning it already
   carries for SIGTERM.
2. **The notice window.** Nothing else happens for `notice` (or the
   shorter, best-effort window an interruption leaves, §8). The
   job checkpoints. It may then keep running — the common case, and the
   right one if the window turns out to be a false alarm in a future
   revision — or exit.
3. **Stop.** The ordinary ADR 0013 stop path: `SIGTERM`, `abort_grace`,
   `SIGKILL`, with the allocation's tombstone journaled.

Outcome resolution follows ADR 0013's **truth wins the race**, applied to
the notice window exactly as to the abort window:

| The container… | Outcome |
| --- | --- |
| exits `0` during the window | `Exited { code: 0 }` — it finished; it is not re-run |
| exits non-zero during the window | `Preempted` — it left because it was told to |
| is terminated by the stop path | `Preempted` |
| breaches `max_runtime` or a memory/disk limit during the window | the limit outcome — the limit was real |

The documented contract for workloads is therefore: *after checkpointing,
either keep running or exit non-zero (`75`, `EX_TEMPFAIL`, by convention);
exit zero only when the work is actually complete.*

Every job has a notice contract — `notice` defaults to the policy floor
(§1) — so every attempt a deadline drain reaches is signalled before it
is stopped. The table's third row nonetheless does not depend on a notice
having been delivered: the agent classifies a kill it performed by the
reason it performed it (§4), exactly as its stop path classifies
`Aborted` today.

### 4. State: outcome and one preemption record on the attempt

**`AttemptOutcome::Preempted { reason }`** is a new terminal outcome,
class `Platform`, with

```rust
pub enum PreemptReason {
    /// The scheduler chose this attempt to make room for `for_job`, which
    /// outranks it by priority class (§5).
    Priority { for_job: JobId },
    /// The scheduler moved this attempt off a persistent node to make room
    /// for `for_job`, because a `preemptible_only` node could take it at
    /// once (§6).
    Relocation { for_job: JobId },
    /// The node was drained with a deadline by an operator (§7).
    Drain,
    /// The node's host reported an external interruption (spot reclaim, §8).
    Interruption,
}
```

Being `Platform` class buys the economics without a carve-out: ADR 0029's
`f = 1000` rule refunds the unused charge in full. Retry treatment is a
deliberate carve-out, because `Platform` class alone does not confer one:
today every `Platform` outcome except `Revoked` consumes retry budget and
fails the job when the budget is spent, and `NodeLost` keeps that rule.
`Preempted` gets **`Revoked`'s arm** in resolution: the job returns to
`Queued` without consuming budget, unless an abort is pending. A
preemption is the platform's choice, made with warning, and a job should
be able to survive any number of them; a lost node is nobody's choice and
its retry policy is unchanged by this record. This holds for every reason,
including `Drain` of a non-preemptible job: non-preemptibility protects a
job from being *chosen* by the scheduler, not from an operator evacuating
the node it is on, and the free retry is what makes that evacuation
honest. There is no cap on how many times a job may be preempted; the
class rule (§5) guarantees it is never by work it outranks, and repeated
interruption of low-class work is by design. Each attempt gets a fresh
`max_runtime` clock, as for `NodeLost` today.

**`Revoked` gains one case.** ADR 0013 makes `Revoked` the outcome of an
attempt whose allocation was still accruing, and says a funded allocation
is stable. A deadline drain (§7) adds the one exception besides preemption
itself: a `StartJob` that reaches the agent after the node's admission has
closed, or whose runtime bound no longer fits before the notice phase, is
**refused at the door** and reported `Revoked` — the attempt never ran,
never touched storage, and the job is requeued free. It stays pre-`Running`
only.

**Classification is the agent's**, as it is today. The agent's terminal
report carries the outcome, and apply trusts it, exactly as apply trusts a
reported `Aborted` now (ADR 0013's stop path classifies from the agent's
own tombstone, not from the coordinator's record). Two facts in the
agent's journal decide §3's table: whether *the agent's stop path*
terminated the container, and why, and whether a **notice intent** (§3)
for the allocation exists. A kill the agent performed for a `PreemptJob`
or a deadline drain reports `Preempted { reason }` whether or not a notice
was ever delivered — a job whose signal was still being delivered was
killed by the platform for the platform's reasons and that is the truth.
A non-zero exit the agent did *not* cause reports `Preempted` if a notice
intent exists for the allocation, and `Exited` otherwise — including while
a `PreemptJob` is still in flight, since nothing had been asked of the
node yet. The intent, not the mark, is the classifier on purpose: a
workload that exits the instant the signal lands, on a node whose agent
crashes before the mark is journaled, leaves an intent and an exit and no
mark, and the platform asked for that exit. The generous reading costs one
free retry when a job that would have failed anyway happened to do so
inside that window; the strict reading would burn budget on a platform
kill, which is the failure mode this section exists to rule out. Three
journal additions make this durable across an agent restart: the notice
intent itself; the allocation tombstone gaining a **reason** (`Abort` or
`Preempt { reason }`), so a tombstone found on recovery resolves as the
command that wrote it rather than as `Aborted` unconditionally; and a
deadline drain being itself a **journal record** (`DrainDeadline`, §8),
which stands as the intent for every attempt it comes to cover and lets a
restarted agent still enforce the deadline and still classify the kills it
performs at it. If the agent never reports at all — a spot host that
vanishes before the `Preempted` report lands — the coordinator's liveness
monitor resolves `NodeLost` as today, with `NodeLost`'s ordinary retry
accounting. Only a *reported* preemption is free; an interruption whose
report is lost is a lost node, and the retry budget is the submitter's
declared tolerance for exactly that.

**`Attempt.preemption: Option<Preemption>`** is the only preemption state
in the replicated model. There is no new allocation phase: the allocation
stays `Active`, consuming and counted as used, until the attempt ends,
and "preempting" is a status *derived* from this record for `job status`
and the node view.

```rust
pub struct Preemption {
    pub reason: PreemptReason,
    /// When the coordinator committed to ending this attempt: the apply of
    /// the proposal that chose it (§5), or the agent's first report of a
    /// notice intent for a termination the agent initiated (§7, §8).
    pub requested_at: Timestamp,
    /// Set once, from the agent's reported notice mark: the attempt has
    /// been told, and `deadline` is the bound the scheduler plans against.
    pub acknowledged: Option<Acknowledged>,
}

pub struct Acknowledged {
    pub notified_at: Timestamp,
    pub deadline: Timestamp,
    /// How much of the declared notice the attempt did not get, when a
    /// deadline drain's command reached the agent too late to give it all
    /// (§7). Zero for every scheduler-timed preemption.
    pub notice_shortfall: Duration,
}
```

The record is set at most once per attempt, legal only while the attempt
is `Running`, and its two halves distinguish **requested** from
**acknowledged**. `requested_at` is stamped by apply when the coordinator
commits to the termination: for a scheduler-chosen victim, in the same
batch as the proposal (§5); for a drain or interruption, when the agent's
report first carries the intent (§8). `acknowledged` is stamped by apply
from the agent's reported notice mark, which exists only once the signal
has actually been delivered (§3), and is the *only* thing that gives the
scheduler a bound. The deadline is **never an agent wall-clock time**: the
mark carries the notice *remaining* as a duration, and the leader stamps
`deadline = observed_at + remaining`, with `observed_at` the leader's own
receive time — the same arithmetic the release sweep already uses for
`max_runtime` (coordinator-observed start plus a duration), and skew-safe
in the same direction: report latency can only push the stamped deadline
*later* than the agent's true stop, never earlier. A re-reported mark is
idempotent and never moves the deadline later. The trust is also the same
as `max_runtime`'s: a bound the agent has journaled and will enforce
locally whatever happens to its connection, which is what "guaranteed"
has always meant there.

**Release events come from the attempt**, as they already do. ADR 0027's
`collect_release_events` walks allocation → attempt → job to derive
`started_at + max_runtime`; it now also reads `preemption.acknowledged`
on the same attempt and contributes `deadline + abort_grace`, taking the
earlier of the two when both exist. An attempt whose preemption is
requested but not acknowledged contributes nothing new — the coordinator
has asked, but has manufactured no bound yet — and an accrual waiting on
it has whatever `projected_ready` the node's other events give it. No
special lending rule is needed: ADR 0027's strict backfill lend test
already respects every release event in commit order, and a preemption's
release is pledged to the accrual its proposal committed like any other.

**Abort wins over preempt** in ADR 0013's sense: a job with
`abort_requested` pending never returns to `Queued`. A `Preempted` attempt
resolved with an abort pending ends the *job* `Aborted`, the arm apply
already has for `Revoked`; the attempt's own outcome stays whatever the
agent reported, because that is what happened to it.

### 5. Scheduler: priority preemption

The pass (`coppice-scheduler::engine`) stays a pure function of
`(snapshot, now)`; every rule below is computed from the snapshot and
re-validated at apply.

**When it is considered.** For a candidate `J` that the ordinary seating
loop could not place at once — no free fit and no legal backfill — the
pass compares the best `projected_ready` the ordinary rules offer
(`natural`: the earliest finite accrual bound on any node, or indefinite)
with the bound a preemption would manufacture (`now + notice + abort_grace`
for the victims' longest notice). Preemption is tried when `natural` is
indefinite, **or** when it is finite but later than the manufactured bound
by at least **`preemption_min_improvement`** (a scheduler-side knob,
default **1 h**). The comparison is the one ADR 0027's improvement moves
already make, with a deliberately longer threshold than
`replan_min_improvement`: an improvement move costs nothing but a
re-plan, a preemption costs a victim its progress, and a high-class job
that would start within the hour anyway does not get to spend that. In
the finite-first ordering preemption therefore slots in after "finite
accrual within the threshold" and before "indefinite accrual", so a
high-priority job on a full cluster of unbounded runners is not parked
behind them, and one on a cluster of week-long bounded runners is not
parked behind them either. A job that is *already* accruing — with an
indefinite bound, or a finite one the threshold beats — is also a
candidate, subject to the per-node guard below: the pass may revoke that
accrual (free, `Revoked`) and reseat it as the preemption-funded accrual
in the same batch, exactly as ADR 0014's revoke-and-reseat re-plan does
today. `J` may itself be preemptible; the class rule below is what keeps
that from cycling.

**The class rule, and the score rule.** A victim must satisfy both:

- **Strictly lower priority class** than `J`: `victim.priority <
  J.priority`, comparing the user-chosen `Job.priority` index (the key
  into `priority_multipliers`). This is the intuitive rule — a job
  displaces only work its submitter already declared less important — and
  it is the **churn bound**: a preempted job can never turn around and
  preempt the job that displaced it, or anything that job's class
  outranks, so a chain of preemptions is strictly increasing in class and
  bounded by the number of classes. A score test alone could not give
  this, because two same-class jobs would trade a node back and forth as
  their entities' penalties drift.
- **Lower `effective_score`** than `J`: ADR 0021's formula applied to the
  running victim (the same multiplier and entity penalty, age from
  `submitted_at`) must be below `J`'s. This is the **fairness bound**:
  the class rule on its own would let a heavily over-quota entity's
  high-class job evict an under-quota entity's running work, turning
  priority from ADR 0021's bounded queue advantage into an eviction
  privilege that quota could not answer. With both rules, priority
  decides *who may be a victim* and quota decides *whether the incoming
  job has earned it*; an entity deep in penalty keeps its place in the
  queue, as ADR 0021 intends, rather than jumping it by eviction. The
  score rule adds no churn: it only narrows the set the class rule
  allows.

The one exception to both is relocation (§6), which depends on neither
class nor score because the victim loses nothing but its progress.

**Victims.** On each candidate node, the running attempts that are
effectively preemptible (§1) and satisfy both rules against `J`, ordered
by class ascending, then `effective_score` ascending, then **work at
risk** ascending, where work at risk is `(now − started_at) × requested`.
The pass takes the smallest prefix `V` that, together with the node's
free capacity and its other guaranteed releases inside the notice bound,
fits `J`. A later record that lands checkpoint pointers redefines work at
risk as *time since the last checkpoint*; nothing else in this section
changes.

**The proposal.** `PlacementProposal` gains `preemptions: Vec<AllocationId>`
beside `revocations`, each paired with the placement it enables in the same
batch. The placement for `J` is committed **accruing** on the victims'
node: the preemptions' manufactured release event funds it when the
victims exit, through the ordinary pledge-in-commit-order path, and
ADR 0027's one-accrual-per-node guard applies unchanged. No new waiting
state, no reservation, and **no stored link** from the accrual to its
victims: the pairing exists in the proposal and in the apply that
validates it, and nothing afterwards needs it, because the guard below is
derived from the victims themselves. The accrual can still be
revoked-and-reseated if a free fit opens elsewhere before the victims are
gone — in which case the preemption *stands*: a notice already delivered
is never retracted (§9).

**The per-node guard.** A node has **outstanding scheduler-initiated
terminations** while any live attempt on it carries a `preemption` record
with reason `Priority` or `Relocation` — a fact read off the attempts,
with no bookkeeping of its own. While it does:

- the pass proposes **no further eviction group** on that node, whatever
  `J` arrives and however it would fit; and
- the node's accruing job (at most one, by the K-guard) may be reseated
  **only to a free fit** — a placement that starts it at once. A move to
  another accrual, even to a strictly better finite bound, is refused.

Recovery keeps precedence over the guard: if the node is lost, or is
draining (§7), the ordinary revoke-and-reseat of its accrual applies
exactly as ADR 0041 defines, because the guard restricts *improvement*
moves and never the moves that keep an accrual off a node that cannot
deliver. Drain- and interruption-reason preemptions on a node do not
raise the guard: they are not the scheduler's doing, and a draining
node's admission is governed by §7 instead.

The guard is a deliberate trade of a little scheduling flexibility for
state that is derived rather than kept. It was checked against the
re-planning the engine already does:

- *Improvement move to a finite bound elsewhere* (ADR 0027,
  `try_improve_accrual_bound`): refused while the guard holds. The cost
  is bounded by the victims' notice plus `abort_grace` — the guard lifts
  when they exit — so `J` waits at most one notice window it would
  otherwise have skipped.
- *Reseat to a free fit* (ADR 0014's lend reseat, `best_reseat_target`):
  allowed. The victims are still terminated (§9), and the capacity they
  free goes to the next accrual the pass opens there, which is what would
  happen in any case where a beneficiary leaves early.
- *A second eviction group for a higher-class `K` on the same node*:
  refused until the first group is gone. `preemption_cooldown` already
  refuses it for five minutes after the first group's `requested_at`,
  so the guard only makes explicit what the cooldown implies, and closes
  the window the cooldown would leave if it were set shorter than a
  notice.
- *The beneficiary preempting on a third node while its first victims
  are still being told*: impossible, because that would be a move from
  its accrual to another accrual, which the guard refuses; there is one
  live attempt per job (ADR 0030), so "its accrual" is well defined.
- *The node lost or drained mid-notice*: the accrual is revoked and
  reseated by the recovery rules; the victims are lost or drained with
  the node; nothing dangles, because nothing referenced the pairing.
- *A victim exits early, or the beneficiary is aborted*: the guard is a
  function of live attempts and lifts by itself; the accrual funds early
  or is released, as today.

**Apply validation** re-checks each victim: allocation `Active`, the
attempt's recorded effective preemptibility, the class rule (or the
relocation condition, §6), the per-node guard, and that `J` fits on the
node once `V` is released, all against the snapshot. The **score rule is
proposal-side only**: `effective_score` depends on the scheduler-local
`w_age` (ADR 0021), which is not replicated, so apply cannot recompute it
deterministically and does not try — exactly as apply does not re-derive
the candidate ordering behind any other placement. The class rule, which
is replicated state, is the invariant apply enforces. Any failure rejects
the batch as a proposer bug, exactly as for revocations. Apply then sets
`preemption = { reason, requested_at, acknowledged: None }` on each
victim attempt, and the dispatch loop sends the agent a
**`PreemptJob { allocation, notice_us, grace_us, reason }`**, a new agent
command kept distinct from `StopJob` because it carries a window and a
reason and because its journal entries are a notice intent and mark, not
a tombstone. Recording the request on the attempt costs classification
nothing, because the agent classifies from its own journal (§4): an
attempt that exits non-zero before the command reaches the node is
`Exited`, whatever the coordinator had decided. The agent starts the
notice clock when it delivers the signal, and its report of the mark
(§8) is what stamps `acknowledged`: the promised notice is measured from
delivery, not from proposal, so a slow or disconnected agent delays the
release rather than shortening the warning. Until then `J`'s accrual has
no manufactured bound, and if the agent never answers, the node is the
liveness monitor's problem, not the scheduler's. `PreemptJob` is re-sent
on reconnect like any undelivered command, and is idempotent on the
journaled intent. A victim that exits for any reason before its deadline
releases its capacity exactly as any `Active` allocation would, which
funds `J` early.

**Rate limits**, all scheduler-side and all cheap:

- `max_preempting_placements_per_cycle` (default **2**) bounds the number
  of preemption-funded *placements* a pass may propose, as
  `max_placements_per_cycle` bounds seating. It counts beneficiaries, not
  victims: a placement's whole victim set `V` is proposed together or not
  at all, because a partial set frees nothing `J` can use and a cap on
  victims would leave a job needing four evictions permanently blocked
  behind a cap of two. The size of `V` is bounded by the node instead — a
  node is never considered for `J` if `J` would not fit even with every
  eligible victim on it released — and by the fact that it is the
  *smallest* prefix that fits.
- **Per-node cooldown** (`preemption_cooldown`, default **5 min**): a node
  whose most recent scheduler-initiated `requested_at` is younger than
  the cooldown hosts no new victims. This smooths a burst of high-class
  submissions across nodes; it is not what bounds chains — the class rule
  is.
- `preemption_min_improvement` (default **1 h**, above) bounds how little
  a preemption may be worth.

There is deliberately **no per-job shield** against repeated preemption.
A large backlog of low-class work submitted to soak up quiet periods is
*expected* to be interrupted whenever higher-class work arrives; that is
what the submitter asked for and was discounted for, and the class rule
already guarantees the interruption is never by the work it displaced.

### 6. Pricing and placement

**The discount** is a replicated policy multiplier,
`preemptible_multiplier: PriorityMultiplier` (Q32.32, validated `≤ 2³²`,
i.e. ≤ 1.0, default **0.5**). It is folded into the charge multiplier at
`commit_placements` exactly as ADR 0029 folds the unbounded-runtime
multiplier — `m' = ⌊m' × preemptible_multiplier / 2³²⌋` when the job is
*effectively* preemptible (§1: its own flag and every ancestor entity's),
multiplying with the unbounded multiplier when both apply —
and recorded on the charge record, so the attempt is charged *and settled*
at the discounted rate, and a policy edit mid-flight does not reprice.
Nothing else in the arithmetic changes.

The discount lowers the entity's decayed usage for the same work, which
raises its penalty-adjusted score; that is a second, indirect incentive and
it is intended. A preempted attempt's **actual consumption is charged** at
the discounted rate, as `NodeLost` charges it today: a preemptible job that
never checkpoints pays for the work it throws away, and the discount is the
compensation for the risk, not an exemption from quota pressure.

**Spot-only nodes.** The agent TOML gains
`[preemption] preemptible_only = true`. It rides `Register` and lands on
the replicated `Node` record beside `labels`, following ADR 0020's rule
that node facts come from the node. The placement gate becomes:

```
node.accepts(job) = node.admits(job, now)            // §7's drain rules
                 && (!node.preemptible_only || effectively_preemptible(job))
```

where `effectively_preemptible` is §1's conjunction over the job's flag
and its entity chain, computed from the snapshot by the pass and again by
apply — the same walk `charge_ancestors` already does — and checked in the
scheduler's candidate filter *and* in apply's `CommitPlacements`
validation, the two-gate pattern ADR 0041 set, with a new
`RejectionReason::NodeRequiresPreemptible`. A job made non-preemptible by
its entity never lands on a spot node, whatever its spec says. Preemptible jobs are
allowed on ordinary nodes — forbidding it would starve them whenever spot
is scarce — and there is **no placement bonus** pulling them toward
`preemptible_only` nodes. From the job's point of view a persistent node
where it can run to completion is the better home, and when the cluster
has room it should get one; a bonus would trade that away for nothing.
Best-fit packing decides, with `preemptible_only` as the tie-break only
when two nodes score equally.

The pressure that keeps preemptible work from squatting on persistent
capacity that non-preemptible work needs is **relocation**, a second
preemption trigger with its own condition: the pass may preempt an
effectively preemptible attempt `v` on a persistent node for a
non-preemptible `J` that cannot otherwise be placed, *regardless of class*,
if and only if a `preemptible_only` node has free capacity for `v` **at
proposal time**. Relocation is otherwise an ordinary preemption: `v` gets
its notice, resolves `Preempted`, and returns to `Queued`; `J` accrues on
the persistent node against the manufactured release. The spot capacity
is a *precondition*, not a hold — ADR 0030 allows one live attempt per
job, so `v`'s successor cannot exist until its predecessor resolves, and
this record adds no reservation to bridge the gap. It is a bet that the
capacity that was free a notice window ago is still free, and when it is
not, `v` is placed by the ordinary rules like any requeued job: it lost a
preemption it opted into and nothing more. Relocation carries
`PreemptReason::Relocation { for_job }` so the history says what happened,
and it counts against `max_preempting_placements_per_cycle`, the node
cooldown and the per-node guard like any preemption. Without spot capacity
to move to, no relocation happens and the class and score rules alone
govern.

### 7. Node draining, redefined (amends ADR 0041)

**Draining and preemption are different things**, and this record keeps
them apart throughout: *draining* is a node's admission and evacuation
policy; *preemption* is the termination of one attempt. A drain without a
deadline never preempts anything. A drain with a deadline preempts, near
the deadline, whatever has not finished. Spot interruption is a deadline
drain the agent starts on the provider's behalf. ADR 0041's drain was a
cordon: admit nothing, wait, leave the rest to the operator. That is
replaced here, because a node being emptied for maintenance can still do
useful work for as long as that work is guaranteed to be gone in time.

**State.** `Node.schedulable` is **replaced** by

```rust
pub struct Drain {
    /// Absent for a regular drain: work finishes naturally.
    pub deadline: Option<Timestamp>,
    pub requested_at: Timestamp,
}
// Node.drain: Option<Drain>
```

set by `SetNodeDrain { node, drain: Option<Drain> }` (actor-carrying,
`Verb::Drain`, replacing `SetNodeSchedulable`), through the same HTTP
write path ADR 0041 defined. `NodeRecord.draining`, the agent's
*announcement* that it is shutting down (ADR 0041's SIGTERM drain), is
unchanged and remains a separate fact, and an agent-initiated interruption
(§8) rides the same announcement with a deadline beside it. The CLI is
`coppice node drain <node> [--deadline <d>] [--wait]` and
`coppice node undrain <node>`. Draining an already-draining node with a
different deadline **replaces** it, earlier or later, subject to the same
validation, but only while the current deadline's notice phase has not
begun: the API rejects a replacement once `now ≥ T_notice` of the
deadline on record, because from then on notices may have been issued
and neither shortening nor postponing them is legal (below). After that
point the only operator moves are to let the drain run or to `undrain`,
which cancels nothing already issued. The API check is a courtesy, not
the safety: a replacement accepted moments before `T_notice` can reach
the agent after it has issued notices. The agent therefore treats every
replacement **as a withdrawal followed by a new plan** — attempts
already holding a notice intent keep their stops at their journaled
deadlines, never postponed and never brought forward, and the new
deadline governs everything else on the node. **A notice already issued
wins over a new deadline in both directions.** For a later replacement
that means the notified attempts still stop early; for an earlier one it
means they may stop *after* the new `T_stop`, and the node is then empty
by the old deadline rather than the new — the agent stops everything
else at the new `T_stop` and the notified attempts at their own. The
node view reports the effective emptying time as the later of the two,
so an operator who replaced a deadline in that window sees exactly what
it will cost, and `job status` shows which attempts were already
notified. The heartbeat echo (below) shows which target the agent is
actually running. `DeclareNodeLost` sets a regular
drain (`deadline: None`) on the record in place of the `schedulable =
false` it writes today, so a lost node's admission is closed by the same
gate as everything else and stays closed until it re-registers as a fresh
record (ADR 0041) or an operator undrains it.

**What a drain admits.** No drain of any kind opens a **new accrual** on
the node, and an accrual already there is re-planned off it exactly as
ADR 0041's improvement move does today. What a draining node may still
take is **bounded backfill**: a job with an enforced `max_runtime` that
fits the node's *free* capacity now, and whose bound
`now + max_runtime + abort_grace` lies at or before the node's admission
horizon. The horizon depends on the drain:

- **Regular drain (no deadline)** lets existing work finish naturally.
  Nothing is signalled and nothing is killed by the drain itself;
  `max_runtime` and resource limits apply as always. If every live
  commitment on the node has a finite enforced completion bound — a
  running attempt's `started_at + max_runtime + abort_grace`, a funded
  attempt that has not yet started at `now + max_runtime + abort_grace`,
  an accrual still waiting at `projected_ready + max_runtime +
  abort_grace` — the horizon `H` is the latest of them, and backfill is
  admitted only if it can finish by `H`, so backfill can **never move the
  horizon later**. If any live commitment is unbounded, or an accrual's
  `projected_ready` is indefinite, there is no `H` and nothing is
  admitted. `H` is recomputed by every pass from the snapshot; a late
  start moves it, backfill does not. When the node has no live
  allocation the drain is **complete** — a derived status, nothing
  written — and admission stays closed until the operator undrains. The
  trade is stated plainly: utilisation goes up, the worst-case emptying
  time is unchanged, and the *actual* emptying time may be later than it
  would be admitting nothing. A node running an unbounded job never
  completes a regular drain; the operator adds a deadline.
- **Regular drain with deadline `D`** requires the node to be *empty* by
  `D` — containers gone and their exits reported, not merely termination
  begun — with one stated exception: an attempt notified under an
  earlier plan keeps that plan's stop (replacement, below), so the
  effective emptying deadline the node view reports may be later than
  `D` after a replacement. Working back from `D` with `abort_grace`, the notice cap and a
  cleanup margin gives two instants that every rule keys off:

  ```
  T_stop   = D − drain_cleanup_margin − abort_grace
  T_notice = T_stop − preemption_notice_cap
  ```

  `drain_cleanup_margin` is a new replicated policy field (default
  **30 s**) for reaping and reporting. Work finishes naturally where it
  can; an attempt that exits before `T_notice` never hears about the
  drain. At `T_notice` the agent delivers **one common notice** to every
  attempt still running, sized at the cap so that every job — preemptible
  or not — receives at least its declared notice; at `T_stop` it runs the
  stop path on everything still there (§3). Both kinds resolve
  `Preempted { Drain }` with the free retry (§4); natural completions and
  limit breaches keep their usual outcomes. Backfill is admitted only if
  its bound lies at or before `T_notice`, and **admission closes at
  `T_notice`** in both gates, so nothing is admitted once notices have
  begun. A `StartJob` that reaches the agent after `T_notice`, or after a
  delay that pushes its bound past `T_notice`, is refused at the door and
  reported `Revoked` (§4): the agent checks the bound again at start,
  because the pass checked it at proposal and dispatch is not instant.
- **Spot interruption** is a deadline drain the agent begins itself with
  the provider's deadline (§8). It admits **nothing** — the
  announcement's `draining = true` closes both gates as ADR 0041 defined
  — because a host the provider has already claimed is not a place to
  start anything. Its notice is best effort (§8).

**The deadline never moves later.** The API rejects a `D` that leaves
less than `preemption_notice_cap + abort_grace + drain_cleanup_margin`
from the coordinator's receipt, and never adjusts one it accepts. If the
command reaches the agent late — a disconnected session, a leadership
change, a slow dispatch — the agent does not extend: it delivers the
notices at once, stops at `T_stop`, and reports each notice's
**shortfall** (declared notice minus the notice actually given) in its
mark, which apply records as `Acknowledged.notice_shortfall` (§4). The
attempts still resolve `Preempted { Drain }` and still requeue free; the
node's drain status shows the largest shortfall and `job status` shows
each attempt's, so an operator who drained late can see what that cost
and a user can see why their notice was short. This is the one place the
floor (§1) can be missed for a platform-timed termination, and it is
reported rather than silently absorbed into a later deadline. An operator
who cannot wait `T_notice` out drains without a deadline, stops the host,
and accepts `NodeLost` for what was running — `node remove` is the
decommission verb for an *empty* node and terminates nothing.

**Durability and acknowledgement.** The `Node.drain` record is the source
of the intent; the agent's heartbeat is the acknowledgement, carrying
`drain_target_us`, the deadline it last accepted, exactly as sent (absent
for a regular drain or no drain). The leader's dispatch loop reconciles
record against echo on every heartbeat, in both directions: a deadline
on the record that the echo does not match sends the existing-but-unused
advisory `Drain` agent command with `deadline_us` set; no deadline on the
record while the echo still reports one sends `Drain` with `deadline_us`
unset, the **withdrawal**. A command lost to a disconnected session or a
leadership change is therefore re-sent by whichever leader next sees the
mismatch. The agent journals the target it accepted, so a re-sent
identical target is a no-op and only a new target is acted on. A regular
drain needs no agent command at all: it changes only what the scheduler
admits.

**Cancelling a drain.** `node undrain` clears `Node.drain`, reopens
admission, and — through the withdrawal above — cancels a notice and stop
phase the agent has **not yet begun**. It does not retract a notice
already delivered, postpone a deadline already acknowledged, or
invalidate a release bound already published: those attempts still
terminate at their deadlines and still resolve `Preempted { Drain }`, on a
node that is by then accepting work again. The withdrawal therefore
**narrows the agent's plan from the node to the notified attempts**: the
node-wide stop at `T_stop` is dropped, and each attempt that holds a
notice intent keeps an individual stop at its own acknowledged deadline,
exactly as a `PreemptJob` victim does. An attempt holding an intent but
no mark — the agent crashed between intent and delivery — is still
delivered its signal on recovery with a fresh countdown (§3), withdrawal
or not, and the mark that results is what fixes its stop; the intent
was a commitment to end that attempt, and a withdrawal does not reach
back past it. Work admitted after the undrain is never touched by the
old plan, and an attempt the notice phase had not yet reached is simply
left running. This is the same rule as §9's
"no retraction" for scheduler preemption, for the same reason — a notice
is a promise the workload may already be acting on.

**Amendments to ADR 0041**, stated explicitly so that record can be read
with these in hand:

1. `Node.schedulable` and `SetNodeSchedulable` are replaced by
   `Node.drain` and `SetNodeDrain`. The admin cordon becomes the regular
   drain; there is no separate "admit nothing, evacuate nothing" state,
   because a regular drain with an indefinite horizon is exactly that.
2. `NodeRecord::accepts_placements()` is replaced by `admits(job, now)`,
   which encodes the horizon rules above; both gates (scheduler candidate
   filter and apply's `CommitPlacements`) call it, and
   `RejectionReason::NodeNotSchedulable` becomes `NodeDraining`.
   Retention GC's precondition "does not accept placements" reads
   "is draining, by the record or by the announcement".
3. `coppice node drain` gains `--deadline`; its `--wait` counts down the
   same two numbers and additionally shows the horizon or the deadline.
4. The agent's SIGTERM drain is **unchanged**: drain-and-wait for
   `shutdown_grace`, work left running at the end, no notices. Merely
   stopping the agent asks nothing of workloads. Planned scale-in that
   wants checkpoints performs an explicit deadline drain first and stops
   the agent when it is empty; the ASG lifecycle hook ADR 0041 documents
   is where that call belongs.
5. The advisory `Drain` agent command stops being dead code: it carries
   the operator's deadline, and its absence withdraws it.

### 8. Agent: interruption, notices and the journal

ADR 0041 kept cloud wiring out of the product. This record reverses that
for exactly one piece, because a user-data script racing a 120-second
notice against systemd's stop ordering is the wrong place for the only
time-critical step in the design:

- **`[preemption] interruption_source = "aws-imds"`** (agent TOML,
  default none) starts a poller on `spot/instance-action` at the
  five-second cadence AWS recommends. A notice begins a **deadline drain**
  with the instance's stated deadline, announced in the heartbeat as
  `draining = true` with `interruption_deadline_us`. Other sources are
  added as variants of the same enum; the poller is small and the
  interface is "here is a deadline". The source handles `terminate` and
  `stop` actions and treats `hibernate` as `terminate`; Coppice does not
  resume hibernated hosts.
- **Running a deadline drain**, whoever started it. The agent journals a
  `DrainDeadline { deadline, reason, target }` record (§4) so the plan
  survives its own restart, then waits for `T_notice` (§7) — or, for an
  interruption, acts at once, since the provider's deadline leaves no
  room for a notice phase to be scheduled later. At the notice phase it
  follows §3's sequence for every attempt still running: intent, signal,
  mark, report. At `T_stop` it runs the stop path on everything still
  running under *this* plan — attempts it notified, and attempts whose
  container was still starting — while an attempt that holds a notice
  intent from an earlier plan is left to its own journaled stop (§7),
  all classified `Preempted { reason }` by the agent
  because the agent is what killed them (§4), and it refuses any
  `StartJob` that arrives after admission closed (§7). Work is *not* left
  running at a deadline drain's end: the node is known to be about to be
  empty or gone, and a reported `Preempted` beats an inferred `NodeLost`
  because it carries the reason. The stop path is the same one that
  serves `PreemptJob`. A withdrawal (§7) journals the cancellation and
  drops the node-wide plan: a notice phase not yet begun never happens,
  and if it had begun, only the attempts holding a notice intent keep
  their stops, each at its own journaled deadline, as if each had been a
  `PreemptJob`. Nothing started after the withdrawal is covered by the
  old plan.

**The notice mark reaches the coordinator** as a new `AttemptStatus`
field, `notified { reason, remaining_us, shortfall_us }`, on the next
report after the mark is journaled (and again in the `ObservedSet` after
a reconnect, so a mark is never lost to a dropped stream). Because the
signal precedes the journal and the journal precedes the report (§3), the
coordinator can only ever learn of a notice that was delivered, and so
can only ever publish a release bound the workload has actually been
warned about. The journaled mark carries the deadline as an
**agent-local wall-clock** time, because the agent's monotonic clock does
not survive a restart and the mark's whole purpose is to; that value is
for the agent's own enforcement only. `remaining_us` is how much of the
notice window is left *as of the report*, computed from that journaled
deadline; the agent never sends the wall-clock value itself. The leader's
ingestion proposes from it (machine-proposed, no actor, like
`SetNodeDraining`), carrying its own receive time as the command's
`observed_at`: `preemption.requested_at` is set to `observed_at` if no
record exists yet (a drain or interruption the coordinator did not
initiate), and `acknowledged` is stamped with `notified_at =
observed_at`, `deadline = observed_at + remaining` and the reported
shortfall. A re-reported mark (reconnect, duplicate) is idempotent: the
record is set at most once and the deadline is never moved later by a
re-report, so a delayed duplicate cannot loosen a bound already
published. Every notice the agent delivers means the same thing — *this
attempt will end* — and there is no advisory variant.

**The floor is guaranteed only where the platform holds the clock.** A
priority or relocation preemption is timed entirely by Coppice: the agent
starts the window when it delivers the signal, the coordinator plans
against a deadline it derives from that delivery, and nothing external
can shorten either, so `preemption_notice_floor` is a real promise there.
An operator deadline drain is timed by Coppice too, and the API refuses a
deadline that cannot honour every declared notice; the one way it can
fall short is a command delivered late, which is reported as a shortfall
rather than absorbed (§7). An **external interruption is best effort**,
and the contract says so: the provider holds the clock, its own notice is
documented as best effort (EC2 states that a termination notice may not
arrive at all, and a hibernation gives no advance window), and a notice
can reach the agent late after an outage of the agent or its poller. The
agent delivers the signal at once with whatever remains, possibly less
than the floor and possibly nothing, and reports the true `remaining_us`
and shortfall; the workload contract is "checkpoint now, the window may
be short", and `COPPICE_NOTICE_MIN_S` documents itself as the
scheduler-side floor for exactly this reason. A workload whose checkpoint
safety needs a *guaranteed* window should not be placed on spot capacity,
which is a decision its submitter makes with `preemptible`. The floor
default of 60 s is still chosen to fit inside EC2's two minutes with the
default 30 s `abort_grace` and margin to spare, so that the *usual*
interruption meets the floor; operators who raise `abort_grace` on spot
hosts should lower the floor to keep that true, and the agent logs a
warning at startup when `interruption_source` is set and the arithmetic
does not close. That warning is a sizing aid, not the guarantee.

### 9. Considered and rejected

Each of these was weighed on review and settled the simpler way. Preemption
already strains interpretability — "why did my job stop, and why then" —
and every branch below would add a second story to tell.

- **Retraction.** If the accrual a preemption enabled is revoked-and-
  reseated elsewhere before the victims stop, the preemption still stands.
  A notice is a promise; a workload that has begun a checkpoint is not
  helped by a "never mind", and a second signal is a second contract to
  document and test.
- **Opting out after the fact.** `preemptible` is immutable, like every
  other placement-relevant field. Clearing it on a running job would need
  the discount clawed back and would make "is this attempt a victim
  candidate" a function of time, for a case the entity-level override
  (§1) already covers where it matters.
- **A per-job or per-entity shield.** Rejected in favour of the class
  rule, which bounds chains structurally. Repeated interruption of
  low-class backfill is expected behaviour, not something to protect
  against; an entity that must not be interrupted switches preemption off
  outright.
- **Tiered discounts by notice length.** One discount for every
  preemptible job. A node that can be emptied a few tens of seconds
  faster has some residual value, but not enough to justify a second
  price and a second thing for users to reason about; the cap on
  `notice` (§1) bounds the cost of the long end instead.
- **Excluding spot capacity from `projected_ready`.** A
  `preemptible_only` node's release events count like any other's. If the
  provider reclaims the node, an accrual waiting on it is handled by the
  existing revocation path and re-seated elsewhere — the first case in
  which a revoked accrual's projected start can *degrade* rather than
  improve, which is honest, and better than pretending spot capacity
  does not exist while jobs run on it.
- **A score-margin test as the churn bound, or a "preemptible jobs never
  preempt" rule.** Both replaced by the class rule, which is simpler to
  explain and strictly stronger as a churn bound. A score comparison
  survives only as the *fairness* condition beside it (§5), where it
  narrows the victim set and never widens it.
- **A `Preempting` allocation phase.** An earlier draft moved a victim's
  allocation through a new phase carrying the deadline. The attempt
  already carries `started_at`, the release sweep already walks
  allocation → attempt → job, and the allocation's capacity is consumed
  identically before and after a notice; the phase duplicated the
  attempt's record on a second object for nothing but a display label,
  which is now derived (§4).
- **Courtesy notices on a plain agent shutdown.** Signalling on a SIGTERM
  drain, with an `enforced = false` mark that classified an exit but
  published no bound, gave a notice two meanings. Every notice now means
  the attempt will end; an operator who wants checkpoints before a
  scale-in runs a deadline drain first (§7).
- **A stored beneficiary–victim link.** Keeping the victim list on the
  preemption-funded accrual needed lifetime rules across reseats,
  acknowledgements and victim exits. The per-node guard (§5) is derived
  from the victims' own records and costs at most one notice window of
  flexibility.
- **Extending a late operator deadline.** Silently moving `D` later to
  keep the floor would make the operator's deadline a suggestion. It is
  refused if it cannot fit, and a late delivery is reported as a
  shortfall on each affected attempt (§7).
- **Coordinator-side outcome classification.** An earlier draft resolved
  `Preempted` from the coordinator's `preempt_requested` flag alone. That
  left a job without a notice contract, killed at a deadline drain, with
  no mark to classify from and therefore `Exited` non-zero — an operator's
  drain failing legitimate work through its retry budget. The agent has
  always classified its own kills (ADR 0013's tombstone rule), and a
  reason on the tombstone extends that to preemption without a second
  classifier.
- **The whole lineage in an environment variable.** A list of every prior
  attempt and outcome would cover the "intermediate attempt never
  checkpointed" case, but it is unbounded for a repeatedly interrupted
  job, and a structured history is the FF-6 record's delivery problem —
  a file in the container or the agent's local node service — not a
  string to parse out of the environment (§2).
- **A single notice record.** One record journaled before the signal
  mirrors the start barrier but inverts its safety: recovery finds a mark
  for a signal that may never have been sent, and either kills without
  warning or re-sends inside a countdown that had already begun. One
  record journaled after the signal makes the mark mean "delivered", but
  leaves a workload that exits on the signal, under an agent that crashes
  before journaling, with no evidence the platform asked. Two records —
  intent before, mark after (§3) — give each fact its own witness, and
  cost only a tolerated duplicate signal in the crash window.

## Consequences

- Spot instances become a first-class home for opted-in work: a reclaim is
  a `Preempted { Interruption }` with a warning, not a crash, and the
  successor attempt knows its predecessor's identity without any new
  replicated state beyond a flag, a phase, and a reason.
- High-priority work can start on a full cluster by accruing against a
  manufactured release event, reusing ADR 0014/0027's machinery rather
  than adding a waiting mechanism; the scheduler stays pure and replayable
  (FF-23).
- `Job.priority` acquires a second meaning beyond its cost multiplier: it
  is the preemption *class*. Two jobs in the same class never displace
  each other, whatever their scores, so users who want work to be
  displaceable by their own later submissions must give those submissions
  a higher class.
- The user-visible job enum is untouched (ADR 0030). New replicated
  surface is one attempt outcome, one attempt record (`preemption`), two
  node fields (`preemptible_only`, `drain`, the latter replacing
  `schedulable`), one quota-entity flag, and three policy fields
  (`preemptible_multiplier`, `preemption_notice_floor` /
  `preemption_notice_cap`, `drain_cleanup_margin`). No allocation state
  changes. Beside it: one agent command (`PreemptJob`), one report field,
  three scheduler-side knobs (`max_preempting_placements_per_cycle`,
  `preemption_cooldown`, `preemption_min_improvement`), and four agent
  journal records (notice intent, notice mark, a reason on the tombstone,
  `DrainDeadline`) that are local to the node and never replicated.
- ADR 0041 is amended as §7 lists: the cordon becomes the regular drain,
  `Node.schedulable` goes, the placement gate becomes horizon-aware, and
  the agent's SIGTERM drain is explicitly notice-free.
- `Job.priority` alone never evicts anything: the incoming job must also
  outscore its victims under ADR 0021, so quota penalties bound the
  eviction privilege the way they already bound queue position.
- `COPPICE_*` becomes a reserved env prefix; a job that set such a name
  today is rejected. The project has no external users (see `README.md`),
  so no compatibility shim is added.
- The `Drain` agent command stops being dead code, as ADR 0041 anticipated,
  and carries the operator deadline or its withdrawal.
- The IMDS poller is the first cloud-specific code in the agent. It is
  isolated behind one config enum and one "here is a deadline" seam.
- Gang scheduling (FF-1) will need victims chosen per placement group with
  simultaneous notice to every member; `Preemption` is placed on the
  attempt, and the victim loop iterates attempts, precisely so that a
  group-aware loop is a change of iteration unit, not of state.
- The FF-6 reporting surface (checkpoint pointers, a workload-scoped
  credential on the agent's node service) is the next record in this line;
  it changes the victim-cost term and the successor's environment, and
  nothing else here.
