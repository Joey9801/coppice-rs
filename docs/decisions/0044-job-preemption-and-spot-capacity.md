# 44. Job preemption, node draining, and spot capacity

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
- **Amends:** ADR 0041 — the operator drain is redefined in §5
- **Graduates:** [FF-5](../roadmap/future-features.md#ff-5-job-preemption),
  the identity half of [FF-6](../roadmap/future-features.md#ff-6-checkpoint-awareness),
  the spot half of [FF-22](../roadmap/future-features.md#ff-22-spot-capacity-and-autoscaling)

This record is a firm statement of design, not a schedule. It fixes the
contracts — what a job declares, what it is told, what it is charged, and
how the scheduler, coordinator and agent behave — so that later work can
implement them in pieces without re-opening the design.

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
cluster instead of waiting (FF-5), and what lets an operator empty a node
by a deadline. Three features, one mechanism.

Three existing decisions shape the answer:

- ADR 0013 already has a free, non-punitive requeue outcome (`Revoked`),
  but only for allocations that are still *accruing*: "a funded allocation
  is stable". Preemption is the deliberate exception to that rule, and must
  stay a distinct outcome so the terminal state never lies about why an
  attempt ended.
- ADR 0014/0027 built the scheduler around **guaranteed release events**:
  the only capacity the scheduler may plan against is capacity with a hard
  bound on when it frees. A preemption *manufactures* such a bound, so the
  incoming job needs no new waiting mechanism: it accrues against the
  release the preemption guarantees.
- ADR 0029 prices declarations through multipliers folded into the charge
  at placement and settled at true-up, and refunds unused charge in full
  for platform-class outcomes. A preemptibility discount fits that
  arithmetic unchanged.

Two terms are kept apart throughout. **Preemption** is the termination of
one attempt by the platform. **Draining** is a node's admission and
evacuation policy. A drain without a deadline never preempts anything.

## Decision

### 1. Scope and job declarations

The job spec gains an optional `[preemption]` table:

```toml
[preemption]
preemptible = true          # default false
notice = "2m"               # default: the policy floor
notice_signal = "SIGUSR1"   # default SIGUSR1; from a fixed allowlist
```

- **`preemptible`** is the *permission*: the scheduler may choose this
  job's running attempt as a victim (§4), and the job may be placed on
  nodes that accept only such work. It is immutable after submission,
  like every other placement-relevant field.
- **`notice`** and **`notice_signal`** are the *notice contract*, which
  every job has, preemptible or not: a non-preemptible job is never
  *chosen* by the scheduler, but it is still evacuated by a deadline
  drain or an interruption (§5) and is told first. The allowlist is
  `SIGHUP`, `SIGINT`, `SIGUSR1`, `SIGUSR2`, `SIGTERM`; a job naming
  `SIGTERM` gets one signal and a longer grace.
- `notice` is validated against replicated policy,
  `preemption_notice_floor ≤ notice ≤ preemption_notice_cap` (defaults
  **60 s** and **5 min**). The cap bounds how long a termination can hold
  capacity and is deliberately short, because spot notice is shorter
  still.

**What the declared notice guarantees.** A scheduler preemption gives
exactly the declared notice, measured from delivery of the signal. An
operator deadline drain gives at least the declared notice. Two things
can cut either short, and both are reported as a shortfall on the
attempt: a drain command that reaches the agent late (§5), and an
external interruption of the host while the window is running (§3). An
external interruption is itself **best effort**: the provider holds the
clock, and the platform passes on whatever window remains, possibly
none.

**A quota entity may switch preemption off for its subtree.**
`QuotaEntity` gains `preemptible: bool` (default `true`). A job's
*effective* preemptibility is `job.preemptible && every ancestor
entity's preemptible`, resolved at `commit_placements` and **recorded on
the attempt**, so a policy edit neither exposes nor protects work already
running. This is for the production queue whose jobs look exactly like
the pre-production runs beside them.

**Pricing.** `preemptible_multiplier: PriorityMultiplier` is a replicated
policy field (Q32.32, validated ≤ 1.0, default **0.5**). For an
effectively preemptible job it is folded into the charge multiplier at
`commit_placements` exactly as ADR 0029 folds the unbounded-runtime
multiplier, `m' = ⌊m' × preemptible_multiplier / 2³²⌋`, multiplying with
it when both apply, and is recorded on the charge record so a policy edit
does not reprice a running attempt. A job that is not effectively
preemptible gets no discount. A preempted attempt's actual consumption is
charged at the discounted rate, as `NodeLost` charges it today: the
discount compensates for the risk and is not an exemption from quota
pressure.

### 2. Workload contract, outcomes, and retry accounting

**Environment.** Every container is started with a reserved block,
injected by the agent from fields on `StartJob`. User `env` may not set
names beginning with `COPPICE_`; validation in `coppice-core::env`
rejects them at the API and at apply.

| Variable | Value |
| --- | --- |
| `COPPICE_JOB_ID` | The job's id, stable across attempts |
| `COPPICE_ATTEMPT_ID` | This attempt's id |
| `COPPICE_ATTEMPT_INDEX` | Position in `JobRecord.attempts`: 0, 1, 2, … |
| `COPPICE_PREVIOUS_ATTEMPT_ID` | The most recent earlier attempt that reached `Running`; unset if none did |
| `COPPICE_PREVIOUS_ATTEMPT_OUTCOME` | That attempt's outcome name (`preempted`, `node_lost`, `exited`, …); unset with the id |
| `COPPICE_PREEMPTIBLE` | `1` or `0`: the attempt's effective preemptibility |
| `COPPICE_NOTICE_SIGNAL` | The declared signal name |
| `COPPICE_NOTICE_S` | The declared notice in seconds, with the guarantee §1 states |

This is the **resumption identity**: who the attempt is and what happened
to the last incarnation that ran. No checkpoint data or pointer crosses
the control plane in this record. Attempts that never ran (revoked while
accruing, or refused at the door) are skipped, because they can have
touched nothing; `COPPICE_ATTEMPT_INDEX` still counts them. The block is
derived from `JobRecord.attempts`, so it is correct even when the previous
attempt's end was never reported.

The previous attempt id is a disambiguator for a half-written checkpoint,
not a discovery mechanism. A job derives its checkpoint location from
`COPPICE_JOB_ID` and keeps a job-scoped manifest there. A job that keys
checkpoints by attempt id alone cannot find attempt 1's checkpoint from
attempt 3 if attempt 2 never wrote one. Exposing the whole lineage as
structured data, by a file in the container or a query on the agent's
local node service, belongs to the FF-6 record together with
`COPPICE_CHECKPOINT`, a name reserved here.

**What the workload experiences.**

1. The notice signal is delivered to the container's PID 1. It **may
   arrive more than once**, and every delivery means the same thing:
   *this attempt will end*. Docker signals PID 1 only, so the
   shell-wrapper warning that applies to `SIGTERM` applies here.
2. The notice window passes. The job checkpoints, then keeps running or
   exits.
3. The ordinary ADR 0013 stop path runs: `SIGTERM`, `abort_grace`,
   `SIGKILL`.

**Outcomes** follow ADR 0013's *truth wins the race*:

| The container… | Outcome |
| --- | --- |
| exits `0` before the stop | `Exited { code: 0 }`: it finished and is not re-run |
| exits non-zero after the platform asked | `Preempted` |
| is terminated by the stop path | `Preempted` |
| breaches `max_runtime` or a resource limit first | the limit outcome |

The documented convention is: after checkpointing, keep running or exit
non-zero (`75`, `EX_TEMPFAIL`); exit zero only when the work is complete.

**`AttemptOutcome::Preempted { reason }`** is a new terminal outcome,
class `Platform`:

```rust
pub enum PreemptReason {
    /// Chosen by the scheduler for a higher-class job (§4).
    Priority { for_job: JobId },
    /// Moved off a persistent node because spot capacity could take it (§4).
    Relocation { for_job: JobId },
    /// The node was drained with a deadline by an operator (§5).
    Drain,
    /// The node's host reported an external interruption (§5).
    Interruption,
}
```

**Accounting.**

- *Refund.* `Platform` class gets ADR 0029's `f = 1000` rule: unused
  charge is refunded in full.
- *Retry.* `Platform` class does not by itself confer a free retry:
  today every `Platform` outcome except `Revoked` consumes budget.
  `Preempted` gets **`Revoked`'s arm**: the job returns to `Queued`
  without consuming budget, for every reason, including `Drain` of a
  non-preemptible job. There is no cap on how many times a job may be
  preempted.
- *`NodeLost` is unchanged.* An interruption whose `Preempted` report
  never arrives is resolved `NodeLost` by the liveness monitor with
  ordinary retry accounting. Only a reported preemption is free.
- *Abort wins.* A `Preempted` attempt resolved with `abort_requested`
  pending ends the job `Aborted`. The attempt's own outcome stays as
  reported.
- Each attempt gets a fresh `max_runtime` clock.

**`Revoked` gains one case.** An attempt whose `StartJob` is refused by
the agent because its start no longer fits a drain's admission cutoff
(§5) is reported `Revoked`. It never ran, and the job is requeued free.
`Revoked` remains pre-`Running` only.

### 3. Shared termination state, delivery, and recovery

Every platform termination, whatever started it, uses one replicated
record, one agent protocol, and one set of precedence rules.

**Replicated state: `Attempt.preemption`.** There is no new allocation
phase. The allocation stays `Active`, consuming and counted as used,
until the attempt ends. "Preempting" is a display status derived from
this record.

```rust
pub struct Preemption {
    pub reason: PreemptReason,
    /// When the coordinator committed to, or first learned of, the
    /// termination.
    pub requested_at: Timestamp,
    /// Set from the agent's reported notice mark.
    pub acknowledged: Option<Acknowledged>,
}

pub struct Acknowledged {
    pub notified_at: Timestamp,
    /// The attempt's stop time, as the coordinator plans against it.
    pub deadline: Timestamp,
    /// Declared notice minus notice actually given; zero when none was lost.
    pub notice_shortfall: Duration,
}
```

- The record is created at most once per attempt, only while it is
  `Running`. For a scheduler preemption, apply creates it with
  `acknowledged: None` in the batch that commits the proposal (§4). For
  a drain or interruption the coordinator first learns of the termination
  from the agent's reported mark, and apply creates the record and
  acknowledges it together.
- `deadline` is **never an agent wall-clock time**. The mark carries the
  time *remaining* as a duration and the leader stamps `deadline =
  observed_at + remaining`, with `observed_at` its own receive time.
  This is the arithmetic the release sweep uses for `max_runtime`, and is
  skew-safe in the same direction: report latency can only make the
  stamped deadline later than the agent's true stop.
- `deadline` **only ever moves earlier**. A re-reported mark never
  postpones it, and an interruption may shorten it (precedence, below).
  A published release bound therefore stays valid.

**Release events come from the attempt.** ADR 0027's
`collect_release_events` already walks allocation → attempt → job for
`started_at + max_runtime`. It additionally contributes
`acknowledged.deadline + abort_grace`, taking the earlier of the two. An
unacknowledged request contributes nothing. No special lending rule is
needed: the strict backfill lend test already respects every release
event in commit order.

**Agent protocol.** For each attempt it is to terminate, the agent:

1. journals a **notice intent** (allocation, reason), fsynced;
2. delivers the notice signal;
3. journals a **notice mark** (allocation, stop time as agent-local
   wall-clock, shortfall);
4. reports the mark as a new `AttemptStatus` field,
   `notified { reason, remaining_us, shortfall_us }`, and repeats it in
   the `ObservedSet` after a reconnect;
5. runs the stop path at the stop time, journaling a tombstone that
   carries the reason.

The intent records that the platform asked. The mark records that the
signal was delivered, and only a mark gives the coordinator a bound, so
the coordinator never publishes a release the workload was not warned of.
The mark follows the signal, the reverse of ADR 0009's start barrier,
because a signal without a record is only a duplicate.

Agent journal additions, all local to the node and never replicated:

| Record | Purpose |
| --- | --- |
| Notice intent | Classification: the platform asked before this exit |
| Notice mark | Enforcement across a restart: the attempt's stop time |
| Tombstone reason (`Abort` or `Preempt { reason }`) | A tombstone found on recovery resolves as the command that wrote it |
| Drain plan (`deadline`, `reason`, accepted target) | A restarted agent still runs the node's plan (§5) |

**Classification is the agent's**, as it is for `Aborted` today: the
agent's terminal report carries the outcome and apply trusts it.

- A kill performed by the agent's stop path for a preemption, drain or
  interruption is `Preempted { reason }`, whether or not a notice was
  delivered.
- A non-zero exit the agent did not cause is `Preempted` if a notice
  intent exists for the allocation, and `Exited` otherwise. An exit
  before a `PreemptJob` reaches the node is therefore the job's own.
- The intent, not the mark, is the classifier: a workload that exits on
  the signal under an agent that crashes before journaling the mark
  leaves an intent and no mark. Reading that as `Preempted` costs at
  most one free retry; reading it as `Exited` would charge the job for
  a platform kill.

**Commands.** `PreemptJob { allocation, notice_us, grace_us, reason }` is
a new agent command, re-sent on reconnect like any undelivered command
and idempotent on the journaled intent. The existing, unused advisory
`Drain` command gains an optional `deadline_us` (§5).

**Stop-time precedence.** A notified attempt has exactly one stop time.
These rules decide it, and are the only rules that do:

| Event | Attempt not yet notified | Attempt already notified |
| --- | --- | --- |
| `PreemptJob` | Notice; stop = delivery + declared notice | No effect: one record per attempt |
| Deadline drain reaches `T_notice` | Notice; stop = the plan's `T_stop` | Keeps its stop |
| Operator replaces the deadline | Governed by the new plan | Keeps its stop |
| Operator undrains | Left running | Keeps its stop |
| External interruption | Notice at once; stop = the provider's `T_stop` | Stop = the earlier of its own and the provider's `T_stop` |

- **Nothing the platform chooses retracts, postpones or advances an
  issued notice.** A notice is a promise the workload may already be
  acting on.
- **An external interruption may shorten any window**, because the host
  is going regardless and a reported `Preempted` is better for the job
  than an inferred `NodeLost`. The agent first journals a replacement
  mark with the earlier stop and the resulting shortfall, fsynced, and
  only then re-reports it, so a restart can never restore a stop later
  than one the coordinator has published. On recovery the latest mark
  for an allocation is the one in force.
- **Recovery of an intent without a mark.** The agent delivers the
  signal again. If the attempt is covered by an active deadline drain
  or interruption, its stop is the plan's `T_stop` and any shortfall is
  reported. Otherwise (a scheduler preemption, or an attempt whose drain
  was withdrawn) it gets a fresh countdown of its declared notice.
- **Recovery of an intent with a mark** resumes the journaled stop time.

### 4. Priority preemption and relocation

The pass (`coppice-scheduler::engine`) stays a pure function of
`(snapshot, now)`.

**When preemption is considered.** For a candidate `J` with no free fit
and no legal backfill, the pass compares `natural`, the best
`projected_ready` the ordinary rules offer, with the bound a preemption
would manufacture, `now + notice + abort_grace` for the victims' longest
notice. Preemption is tried when `natural` is indefinite, or later than
the manufactured bound by at least **`preemption_min_improvement`**
(scheduler-side, default **1 h**). The threshold is deliberately longer
than `replan_min_improvement`, because a preemption costs a victim its
progress. In ADR 0027's finite-first ordering preemption sits after
"finite accrual within the threshold" and before "indefinite accrual". A
job already accruing is a candidate on the same test, by
revoke-and-reseat, subject to the per-node guard.

**Victim eligibility.** A victim must be effectively preemptible (§1),
must not be on a draining node, and, for a priority preemption, must
satisfy both:

- **Class rule:** `victim.priority < J.priority`, comparing the
  `Job.priority` index. This is the churn bound: a chain of priority
  preemptions is strictly increasing in class.
- **Score rule:** the victim's `effective_score` (ADR 0021's formula
  applied to the running job) is below `J`'s. This is the fairness
  bound: an over-quota entity's high-class job does not evict an
  under-quota entity's running work.

Relocation (below) is the one deliberate exception to both rules.

**Victim choice.** Eligible victims on a node are ordered by class
ascending, then `effective_score` ascending, then work at risk ascending,
where work at risk is `(now − started_at) × requested`. The pass takes
the smallest prefix `V` that, with the node's free capacity and its other
guaranteed releases inside the notice bound, fits `J`. A node is not
considered if `J` would not fit with every eligible victim released.

**Proposal and apply.** `PlacementProposal` gains
`preemptions: Vec<AllocationId>`, committed in the same batch as the
placement they enable. `J` is committed **accruing** on the victims'
node, and the manufactured release funds it through the ordinary
pledge-in-commit-order path. ADR 0027's one-accrual-per-node guard
applies unchanged. No link from the accrual to its victims is stored.

Apply re-validates each victim: allocation `Active`, recorded effective
preemptibility, the class rule or the relocation condition, the per-node
guard, and that `J` fits once `V` is released. The score rule is
**proposal-side only**, because `effective_score` depends on the
scheduler-local `w_age`, which is not replicated. Any failure rejects the
batch as a proposer bug. Apply then creates each victim's `preemption`
record and dispatch sends `PreemptJob`.

The notice is measured from delivery, so a slow agent delays the release
and never shortens the warning. Until the mark is reported `J`'s accrual
has no manufactured bound. A victim that exits early releases its
capacity as any `Active` allocation does.

**The per-node guard.** A node has *outstanding scheduler terminations*
while any live attempt on it carries a `preemption` record with reason
`Priority` or `Relocation`. This is read off the attempts. While it
holds:

- the pass proposes no further eviction group on that node; and
- the node's accruing job may be reseated only to a **free fit**, a
  placement that starts it at once.

Recovery keeps precedence: if the node is lost or draining, its accrual
is re-planned off it as ADR 0041 defines. The guard costs a beneficiary
the chance of a better accrual elsewhere until its victims have exited,
which is the acknowledgement delay plus the notice plus `abort_grace`.
Because a job has one live attempt (ADR 0030), the guard also prevents a
beneficiary from preempting on a second node while its first victims are
outstanding.

**Rate limits**, all scheduler-side:

| Knob | Default | Bounds |
| --- | --- | --- |
| `max_preempting_placements_per_cycle` | 2 | Preemption-funded placements per pass. Counts beneficiaries: a victim set is proposed whole or not at all |
| `preemption_cooldown` | 5 min | A node whose latest scheduler-initiated `requested_at` is younger hosts no new victims |
| `preemption_min_improvement` | 1 h | How little a preemption may be worth |

There is deliberately no per-job shield. Low-class backfill is expected
to be interrupted repeatedly, and the class rule guarantees it is never
by work it outranks.

**Spot-only nodes.** The agent TOML gains
`[preemption] preemptible_only = true`. It rides `Register` onto the
replicated `Node` record. The placement gate becomes

```
node.accepts(job) = node.admits(job, now)            // §5
                 && (!node.preemptible_only || effectively_preemptible(job))
```

checked in the scheduler's candidate filter and in apply's
`CommitPlacements` validation, with a new
`RejectionReason::NodeRequiresPreemptible`. Preemptible jobs are allowed
on ordinary nodes, and there is **no placement bonus** toward spot nodes:
a persistent node where a job can run to completion is the better home.
`preemptible_only` is a tie-break between equally scored nodes.

**Relocation** is the pressure that keeps preemptible work from holding
persistent capacity that non-preemptible work needs. The pass may
preempt an effectively preemptible attempt `v` on a persistent node for
a non-preemptible `J` that cannot otherwise be placed, regardless of
class and score, if and only if a `preemptible_only` node has free
capacity for `v` at proposal time. The spot capacity is a precondition,
not a hold: ADR 0030 allows one live attempt per job, so `v` requeues
after its notice and is placed by the ordinary rules. Relocation counts
against every rate limit and the per-node guard.

### 5. Drain modes, admission, deadlines, and cancellation

This section **amends ADR 0041**. Its drain was a cordon: admit nothing
and wait. A node being emptied can still do useful work for as long as
that work is guaranteed to be gone in time.

**State.** `Node.schedulable` is replaced by `Node.drain: Option<Drain>`:

```rust
pub struct Drain {
    /// Absent for a regular drain.
    pub deadline: Option<Timestamp>,
    pub requested_at: Timestamp,
}
```

set by `SetNodeDrain { node, drain }` (actor-carrying, `Verb::Drain`),
which replaces `SetNodeSchedulable` on the same HTTP write path.
`DeclareNodeLost` sets a regular drain where it sets
`schedulable = false` today. `NodeRecord.draining`, the agent's own
announcement, is unchanged. The CLI is
`coppice node drain <node> [--deadline <d>] [--wait]` and
`coppice node undrain <node>`.

**Admission on a draining node.** Both gates call
`NodeRecord::admits(job, now)`, which replaces `accepts_placements()`.

- No new accrual opens. An accrual already there is re-planned off the
  node as ADR 0041's improvement move does today.
- The scheduler chooses no preemption victims there.
- **Bounded backfill** is admitted: a job with an enforced `max_runtime`
  that fits the node's free capacity now and whose bound
  `now + max_runtime + abort_grace` is at or before the node's
  **cutoff** (below).
- An admitted placement records its cutoff on the attempt, and
  `StartJob` carries `latest_start = cutoff − max_runtime −
  abort_grace`. The agent **refuses a launch after `latest_start`** and
  reports `Revoked` (§2). Admission is checked at proposal, and the
  bound is enforced at launch, because dispatch and image pulls are not
  instant.
- An agent that has announced `draining`, including for an interruption,
  admits nothing.

**Regular drain (no deadline).** Existing work finishes naturally. The
drain signals nothing and kills nothing. The cutoff is the horizon `H`,
the latest *enforced* completion bound among the node's live
commitments:

| Commitment | Bound |
| --- | --- |
| Drain backfill, not yet started | Its recorded cutoff |
| Drain backfill, running | The earlier of its recorded cutoff and `started_at + max_runtime + abort_grace` |
| Any other running attempt with `max_runtime` | `started_at + max_runtime + abort_grace` |
| Running attempt without `max_runtime` | None |
| Any other attempt not yet started, or an accrual | None: nothing bounds its start |

If any commitment has no bound there is no `H` and nothing is admitted.
A backfill attempt's recorded cutoff bounds it **for its whole life**,
not only until it starts. The agent refuses a launch after
`latest_start` and enforces `max_runtime` from the actual launch, so the
cutoff is a true bound on completion. `started_at` is the coordinator's
observation of the start and is later than the launch by the report
latency, so the running formula alone could exceed the cutoff. Taking
the earlier of the two means backfill can never move `H` later. When the node has no live allocation the drain is
**complete**, a derived status, and admission stays closed until the
operator undrains. While an unbounded job runs, the drain's completion
time cannot be bounded. Backfill raises utilisation and leaves the
worst-case emptying time unchanged, though the node may empty later than
it would admitting nothing.

**Deadline drain.** The node must be empty by `D`: containers gone and
exits reported.

```
T_stop   = D − drain_cleanup_margin − abort_grace
T_notice = T_stop − preemption_notice_cap
```

`drain_cleanup_margin` is a new replicated policy field (default
**30 s**).

- Work finishes naturally where it can. An attempt that exits before
  `T_notice` never hears of the drain.
- The backfill cutoff is `T_notice`, and admission closes at `T_notice`.
- At `T_notice` the agent notifies every attempt still running, with a
  common window sized at the cap, so each job gets at least its declared
  notice unless one of §1's two exceptions applies. At `T_stop` it stops what remains under this plan, including
  containers still starting. Work is not left running.
- Preemptible and non-preemptible attempts alike resolve
  `Preempted { Drain }` with the free retry. Natural completions and
  limit breaches keep their usual outcomes.

**The deadline never moves later.** The API rejects a `D` that leaves
less than `preemption_notice_cap + abort_grace + drain_cleanup_margin`
from the coordinator's receipt. If the command reaches the agent after
`T_notice`, the agent notifies at once, stops at `T_stop`, and reports
each attempt's shortfall, which `job status` and the node view display.
An operator who cannot wait drains without a deadline, stops the host,
and accepts `NodeLost` for what was running. `node remove` is the
decommission verb for an empty node and terminates nothing.

**Durability.** `Node.drain` is the source of intent. The agent's
heartbeat echoes `drain_target_us`, the deadline it last accepted. On
every heartbeat the leader's dispatch loop reconciles record against
echo: a deadline the echo does not match sends `Drain` with
`deadline_us`; no deadline on the record while the echo reports one
sends `Drain` with `deadline_us` unset, the **withdrawal**. A command
lost to a disconnect or a leadership change is re-sent by the next
leader to see the mismatch. The agent journals the target it accepted,
so a repeated target is a no-op. A regular drain sends no agent command.

**Replacement and cancellation.** Both follow §3's precedence table.

- The API accepts a replacement deadline, earlier or later, only while
  `now < T_notice` of the deadline on record. That check is a courtesy:
  a replacement can still reach the agent after notices went out.
- The agent treats a replacement as a withdrawal followed by a new plan.
  Attempts already notified keep their stops. The node may therefore be
  empty later than a replacement `D`, and the node view reports the
  effective emptying time.
- `node undrain` clears the record and reopens admission. A notice phase
  not yet begun never happens. Attempts already notified still stop and
  still resolve `Preempted { Drain }`, on a node that is by then
  accepting work. Work admitted after the undrain is untouched.

**External interruption.** ADR 0041 kept cloud wiring out of the
product. This record reverses that for one piece, because the only
time-critical step in the design should not live in a user-data script.

- `[preemption] interruption_source = "aws-imds"` (agent TOML, default
  none) polls `spot/instance-action` every five seconds. It handles
  `terminate` and `stop` and treats `hibernate` as `terminate`. Other
  sources are variants of the same enum.
- A notice starts a deadline drain with the provider's deadline as `D`
  and `T_stop` computed as above. The agent announces `draining = true`
  with `interruption_deadline_us`, admits nothing, and notifies every
  running attempt at once.
- The notice is best effort (§1), and the interruption overrides later
  stop times (§3).
- The floor default of 60 s fits inside EC2's two minutes with the
  default `abort_grace`. The agent logs a warning at startup when
  `interruption_source` is set and that arithmetic does not close. The
  warning is a sizing aid, not a guarantee.

**The agent's SIGTERM drain is unchanged**: drain-and-wait for
`shutdown_grace`, work left running at the end, and no notices. Planned
scale-in that wants checkpoints runs a deadline drain first and stops
the agent once the node is empty. The ASG lifecycle hook ADR 0041
documents is where that call belongs.

**Amendments to ADR 0041, in summary.**

1. `Node.schedulable` and `SetNodeSchedulable` become `Node.drain` and
   `SetNodeDrain`. The cordon becomes the regular drain.
2. `accepts_placements()` becomes `admits(job, now)`, and
   `RejectionReason::NodeNotSchedulable` becomes `NodeDraining`.
   Retention GC's precondition reads "is draining, by the record or by
   the announcement".
3. `coppice node drain` gains `--deadline`, and `--wait` also shows the
   horizon or the deadline.
4. The agent's SIGTERM drain is explicitly notice-free.
5. The advisory `Drain` agent command carries the operator's deadline or
   its withdrawal.

### 6. Configuration, compatibility changes, and rationale

**Configuration.**

| Where | Field | Default |
| --- | --- | --- |
| Job spec `[preemption]` | `preemptible`, `notice`, `notice_signal` | `false`, floor, `SIGUSR1` |
| Quota entity | `preemptible` | `true` |
| Replicated policy | `preemptible_multiplier` | 0.5 |
| Replicated policy | `preemption_notice_floor`, `preemption_notice_cap` | 60 s, 5 min |
| Replicated policy | `drain_cleanup_margin` | 30 s |
| Scheduler config | `max_preempting_placements_per_cycle`, `preemption_cooldown`, `preemption_min_improvement` | 2, 5 min, 1 h |
| Agent TOML `[preemption]` | `preemptible_only`, `interruption_source` | `false`, none |

**Compatibility changes.** The project has no external users, so none of
these carries a shim.

- `COPPICE_*` becomes a reserved env prefix. A job that sets such a name
  is rejected.
- `Node.schedulable`, `SetNodeSchedulable`, `accepts_placements()` and
  `RejectionReason::NodeNotSchedulable` are replaced as §5 lists.
- `Job.priority` acquires a second meaning as the preemption class.
  Same-class jobs never displace each other by priority preemption.
- The IMDS poller is the first cloud-specific code in the agent, behind
  one config enum.
- A revoked accrual's projected start can now degrade, when the spot
  node it was waiting on is reclaimed. Before this record a reseat only
  ever improved it.

**Rationale: alternatives set aside.**

- **Retracting a notice.** A workload that has begun a checkpoint is not
  helped by a "never mind", and a second signal is a second contract.
- **Opting out after submission.** It would need the discount clawed
  back and would make victim eligibility a function of time. The entity
  switch covers the case that matters.
- **A per-job or per-entity shield.** The class rule bounds chains
  structurally. An entity that must not be interrupted switches
  preemption off.
- **Tiered discounts by notice length.** One price. The notice cap
  bounds the cost of the long end instead.
- **Excluding spot capacity from `projected_ready`.** It would pretend
  capacity that jobs are running on does not exist. Reclaim is handled by
  the existing revocation path.
- **A score margin as the churn bound.** Same-class jobs would trade a
  node as penalties drift. Score survives only as the fairness rule,
  which narrows the victim set.
- **A spot placement bonus.** It would move jobs off nodes where they
  could have finished uninterrupted. Relocation supplies the pressure
  only when non-preemptible work is actually waiting.
- **Simpler or different state.** A `Preempting` allocation phase
  duplicated the attempt's record. A stored beneficiary-to-victim link
  needed lifetime rules the per-node guard avoids. Coordinator-side
  classification failed jobs the platform had killed without a notice.
  A single journal record could not witness both "asked" and
  "delivered".
- **Softer drain rules.** Courtesy notices on agent shutdown gave a
  notice two meanings. Extending a late operator deadline would make the
  deadline a suggestion.
- **The lineage in an environment variable.** It is unbounded for a
  repeatedly interrupted job. Structured history is the FF-6 record's
  problem.

**Follow-ups.**

- FF-6: checkpoint reporting, `COPPICE_CHECKPOINT`, and the lineage
  surface. It is expected to refine the work-at-risk term in victim
  ordering.
- FF-1: gang scheduling needs victims chosen per placement group with
  simultaneous notice. That design is its own record.
