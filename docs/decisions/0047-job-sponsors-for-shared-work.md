# 47. Job sponsors: one job, several paying quota entities

- **Status:** Proposed
- **Date:** 2026-11-09
- **Amends:** [ADR 0005](0005-cost-based-soft-quotas.md) ("every job is
  submitted under exactly one leaf entity"),
  [ADR 0019](0019-deterministic-quota-arithmetic.md) (who a charge and its
  true-up land on), [ADR 0021](0021-effective-score-ranking.md) (the score
  of a job with more than one sponsor),
  [ADR 0023](0023-scoped-role-bindings.md) (job ownership),
  [ADR 0026](0026-client-minted-job-ids-idempotent-submission.md) (what a
  repeated submission of a known id means),
  [ADR 0044](0044-job-preemption-and-spot-capacity.md) (the class a victim
  is judged by) and [ADR 0046](0046-interval-charging-for-unbounded-jobs.md)
  (who a renewal charges)

## Context

Coppice is meant to sit under services that **deduplicate work**: several
users ask for the same computation, and one job runs for all of them. The
job id is client-minted and is the submission's identity (ADR 0026), so such
a service already has the hook it needs — derive the id from the work, and a
second request for the same work finds the first one's job.

What it cannot do is say *who the job is for*. A job has exactly one
`quota_entity` and one `priority`, both part of the immutable spec, and a
resubmission that differs in either is `SubmitSpecMismatch`. (Users call the
entity their "queue"; this record keeps the codebase's term.) Take user A
asking on entity `a` at priority 1, and user B asking for the same work on
entity `b` at priority 10. Today the service must pick one of them:

- **Cost lands on one party.** Whoever's entity was picked pays the whole
  job, on every ancestor; the other gets the result free. Or the service
  submits under a pool entity of its own, and then nobody's usage reflects
  the request and the pool's penalty is collective punishment.
- **Rank reflects one party.** `effective_score = m / P + age` reads one
  multiplier and one penalty product. If A's request was picked, B's
  urgency — which B is willing to pay for — buys nothing. If B's was
  picked, A pays a 10× premium A deliberately declined.
- **Lifecycle belongs to one party.** Only the recorded submitter may abort,
  and an abort stops the job for everyone.

A is conservative because A's quota is small; B wants the result soon and
accepts the price. Neither preference is wrong, and the design must honour
both at once.

### What "fair" has to mean

A job run once for several requesters is a shared good, so fairness here is
a cost-sharing question. The requirements this decision is held to:

1. **Solo equivalence.** A job with one sponsor behaves bit-for-bit as
   today.
2. **No harm.** Sharing never ranks a job below where any one of its
   sponsors would have ranked it alone, and never charges a sponsor more
   than running it alone at their own priority would have.
3. **No free lane.** Sponsors together get no better rank than the best of
   them could alone, so splitting one request across entities buys nothing
   (ADR 0005: priority is paid for).
4. **Charged once.** The sponsors together pay what the job would have cost
   one submitter at the priority it ran at. Cluster-wide usage counts the
   consumption once.
5. **Urgency is paid for by whoever asked for it.** A's share does not grow
   because B bid higher.
6. **No reward for waiting.** Joining late costs the same as joining early,
   and leaving cannot push cost onto those who stay.

## Decision

### 1. A job has sponsors

`Job.quota_entity`, `Job.priority` and `Job.submitted_by` are replaced by a
set of **sponsors**, keyed by leaf quota entity, at least one and at most
`MAX_SPONSORS` (64):

```
Sponsor { entity, priority, sponsored_by, joined_at }     // on the job spec
JobRecord.multipliers: entity → PriorityMultiplier        // resolved at proposal
```

Everything else in the spec — image, command, entrypoint, env, requests,
`max_runtime`, retry policy, preemption declarations — is the **work**, and
stays immutable. A sponsor's multiplier is resolved from the policy table by
the API when the sponsorship is proposed, as a submission's is today.

The key is the entity because the entity is the accounting unit: the quota
tree is where a deployment declares who counts as one party. Several
requests from one entity are one sponsorship at the highest priority asked
for; a deployment that wants per-user fairness gives users leaf entities.

Two quantities are *derived* from the sponsor set wherever they are needed,
never stored: the job's **tier**, the largest sponsor multiplier, and its
**class**, the largest sponsor priority.

### 2. Rank: the best sponsor's own score

Each sponsor has the score ADR 0021 would give a job of its own:

```
score_i(now) = m_i / P_i(now) + w_age · max(0, now − joined_at_i) / H
effective_score(j, now) = max over sponsors i of score_i(now)
```

`P_i` is the penalty product over sponsor `i`'s own path. Ties break by the
job's `submitted_at`, then `JobId`, as now. The scheduler already memoizes
the penalty product per leaf entity, so a pass costs one extra comparison
per extra sponsor.

The maximum is what requirements 2 and 3 pin down between them: any
combination below it delays some sponsor relative to submitting alone, and
anything above it — a sum, most obviously — gives a group a rank no member
paid for, and makes one request split across ten entities outrank the same
request made once. Each sponsor's age runs from its own `joined_at` for the
same reason: a late joiner does not inherit a wait it never did.

The queue-position explainer names the carrying sponsor and lists every
sponsor's score beside it.

### 3. Cost: split each priority band among those who asked for it

A placement is charged once, at the job's tier `M`, exactly as a solo job at
that priority: `amount = cost_from_rate(rate, runtime_s, m')` with ADR
0029's and ADR 0044's folds applied to `M`. That amount, and every later
movement of it, is divided by one pure function in `coppice_core::quota`:

```
stakes sorted by (multiplier, entity id):  m_1 ≤ m_2 ≤ … ≤ m_n = M,  m_0 = 0
band_k   = ⌊X · m_k / M⌋ − ⌊X · m_(k−1) / M⌋            — u128; bands sum to X exactly
share_i(X) = Σ over k ≤ i of ⌊band_k / (n − k + 1)⌋      — each band's remainder to stake n
```

Read it as tiers of service. The cost of running the job at the lowest
priority anyone asked for is split equally among all sponsors. Each step up
is split equally among those who asked for at least that much. The top
sponsor alone pays the last step.

| Sponsors (multiplier) | Shares of a job costing `M × C` |
| --- | --- |
| a: 1 | a: 1 C |
| a: 1, b: 1 | a: 0.5 C, b: 0.5 C |
| a: 1, b: 10 | a: 0.5 C, b: 9.5 C |
| a: 1, b: 2, c: 10 | a: 0.33 C, b: 0.83 C, c: 8.83 C |

This is the Shapley value of the cost game in which each sponsor needs the
job run at their own priority or better — the "airport game" of the
cost-sharing literature — and it satisfies requirements 2, 4 and 5 by
construction: shares sum to the amount, `share_i ≤ X · m_i / M` (the
sponsor's own solo price), a sponsor's share does not depend on any higher
multiplier, and adding a sponsor never raises anyone else's share. In the
motivating case A pays half of what A would have paid alone, and B pays
slightly less than B would have, for a job that runs when B's bid says it
should.

An equal split fails requirement 2 (A would pay 5 C against a solo price
of 1 C). A split proportional to multipliers passes it but fails
requirement 5: A's share rises toward the full solo price as B bids higher.

Each stake's share is charged to that stake's leaf and every ancestor on its
path. Two sponsors under one team therefore charge the team the whole job,
once.

### 4. Placement is the commitment point

An attempt records the **stakes** it is charged to, a copy of the job's
sponsors at placement: `Stake { entity, priority, multiplier }`. Stakes join
the attempt's recorded rate, multiplier, refund fraction and effective
preemptibility as things fixed at placement. Per-stake amounts are not
stored: a stake's share is `share_i(charge.amount)`, a function of the
record.

- **Placement** charges each stake `share_i(amount)` at `proposed_at`.
- **Renewal** (ADR 0046) grows `amount`; each stake is charged the increase
  in its share.
- **Settlement** computes the attempt's true-up exactly as today — one
  `Refund` or `Surcharge` for the whole attempt — and applies
  `share_i(true_up)` to each stake. Shares are linear in the amount, so
  this is the same as having charged each stake its share of the actual
  cost.
- An attempt is **effectively preemptible** only if no entity on *any*
  stake's path has preemption switched off. As a victim it is judged by the
  **class of its stakes**, the class it is paying for, where apply reads
  `spec.priority` today. A queued beneficiary uses its job's current class.

**Joining a job with an attempt in flight** adds a stake and re-splits the
whole amount charged so far. The joiner will receive the whole result, so
it owes a whole share, not a share of the remainder:

```
before = share(amount) over the old stakes;  after = share(amount) over old + new
each stake's (after − before), decayed from charge.charged_at to now,
is charged if positive and refunded if negative
```

The joiner's multiplier is capped at the attempt's tier for this stake: a
higher priority can no longer buy anything from an attempt already placed,
so it is not charged for. Decaying the transfer puts every entity where it
would be had the joiner been a sponsor at placement, which is requirement
6: there is no cheaper moment to join. A re-split moves usage between
sponsors and creates none. For an interval-charged attempt the transfer
decays from the latest charge only, the same bounded approximation ADR 0046
already accepts for refunds.

**Withdrawing** removes the sponsor from the job at once: it stops counting
toward rank, class and the job's liveness, and it is not a stake in any
later attempt. If an attempt is in flight, its stake stays until that
attempt settles, and is refunded with the others if the attempt is revoked
or aborted before it runs. A queued job has charged nothing, so withdrawing
from one is free. When the last sponsor withdraws, the job is aborted
through the existing abort path, and its settlement refunds every stake.

Stakes therefore only ever grow. Releasing a withdrawn stake was
considered, with a refund for the part of the run not yet elapsed, and
rejected: when the top sponsor leaves, its premium has to land somewhere,
and the only candidates are the sponsors who declined to pay it. Releasing
stakes also invites a sponsor to lend its tier or its good standing to win
a placement and then leave before paying. The cost of the rule is that a
sponsor who withdraws from a placed job may pay its share of a run it no
longer wants. That share is never more than the job would have cost it
alone.

### 5. Commands and identity

`SubmitJob` becomes **submit-or-join**. The command carries the work, one
sponsorship (the caller's entity and priority) and its resolved multiplier.
At apply, with `Verb::Submit` checked on the caller's entity in every
branch:

| State of the job id | Result |
| --- | --- |
| Unknown | Create the job with one sponsor |
| Known, the work differs | `SubmitSpecMismatch` |
| Known, entity already a sponsor, priority ≤ recorded | Accepted no-op (the ADR 0026 retry) |
| Known and live, entity already a sponsor, priority higher | Raise the sponsorship |
| Known and live, entity not a sponsor | Join (`TooManySponsors` past the cap) |
| Known and terminal, anything else | `JobTerminal` |

Metadata stops being part of the identity comparison: it is mutable, it
belongs to the job, and a joining submission neither checks nor changes it.
The response says which row applied.

Two further commands address an existing job by id alone:

- `SetSponsorship { job, entity, priority }` sets a sponsorship's priority
  exactly, creating it if absent. Lowering is how a sponsor backs off. With
  an attempt in flight it changes the job's sponsors only, never a stake.
- `WithdrawSponsorship { job, entity }`, as section 4.

HTTP: `POST /api/v1/jobs` keeps its `quota_entity` and `priority` fields,
now read as the caller's sponsorship, plus
`PUT` and `DELETE /api/v1/jobs/{job}/sponsors/{entity}` taking a
`QuotaEntityRef` (ADR 0045).

Authorization follows the entity being spent:

- Creating, raising or lowering a sponsorship needs `submitter` over its
  entity. Nobody can spend an entity they could not submit under.
- Withdrawing one needs to be the principal that created it, or `operator`
  over its entity.
- `AbortJob` stops the job for everyone, so it needs that authority over
  **every** current sponsor. For a solo job this is today's rule.
- Updating metadata needs it over any one sponsor.

Coppice does not decide which requests are the same work. That is the
deduplicating service's knowledge, and it expresses it by minting the same
job id. On `JobTerminal` the service chooses between serving its cached
result and minting a fresh id.

### 6. Reads and events

- Job reads carry `sponsors` with paths, the job's tier and class, and per
  attempt each stake's share of the charge and of the settlement.
- An `entity` filter (ADR 0031, ADR 0043) matches a job if any current
  sponsor's entity matches. A job's event scope keys are the union of its
  sponsors' entity chains. `JobSponsorChanged` is emitted on join, change
  and withdrawal and carries the prior keys, as a metadata update does, so
  a subscriber sees the event that takes a job out of its set.

### 7. Wire and state

No compatibility shims. `Job` tags 4, 6 and 11 are retired by reservation
and replaced by `repeated Sponsor sponsors`, written in ascending entity
order (the `Label` rule). `JobRecord.multiplier` becomes the per-sponsor
map, and `AttemptRecord` gains `repeated Stake stakes`. Three commands
change (`SubmitJob`, plus the two new ones); `CommitPlacements` and
`RenewIntervalCharges` keep their shapes. Nothing derived is stored on the
state machine.

## Consequences

- The motivating case resolves: one job, ranked by B's bid, with A paying
  half the base cost and B the rest. Each is better off than alone, and so
  is the cluster, which runs the work once.
- All six requirements hold, with requirement 1 testable directly: with one
  sponsor every formula above reduces to the current one, and the existing
  determinism and lifecycle suites must pass unchanged.
- **Declaring a low priority and riding on another sponsor's urgency is
  possible.** A sponsor who privately wants the job soon can bid low once
  someone else has bid high. Any rule that shares a cost by declared need
  has this weakness. It is bounded here: every sponsor pays an equal share
  of at least the lowest band, and the high bidder never pays more than
  alone.
- **Results that already exist are outside this.** A request arriving after
  the job is terminal cannot join, so a requester served from the service's
  cache pays nothing. The same holds for attempts that settled before a
  sponsor joined: a joiner shares the attempt in flight and later ones.
  Extending the re-split to settled attempts within the retention window is
  possible with the same arithmetic, and is left until there is evidence
  the gap is used.
- A withdrawn sponsor can be charged for a run it cancelled (section 4).
- Apply does `O(sponsors × tree depth)` work where it did `O(tree depth)`,
  on placement, renewal, settlement and join. With the cap at 64 and the
  depth cap this is bounded and small. Job records grow by one sponsor
  entry per extra sponsor.
- Multiplicity inside one entity is the caller's to track: if two users of
  one entity ask for the same job and one cancels, the service must not
  withdraw the entity's sponsorship.
- Scheduler, API, CLI and web all read the sponsor set where they read one
  entity and one priority today. That is the bulk of the implementation
  work, and it is mechanical.

## Alternatives considered

- **A pool entity owned by the deduplicating service**, with chargeback
  done outside Coppice. Needs no change, but the scheduler then ranks by
  the pool's standing, not the requesters', which is the gap itself.
- **One job per requester, cancelling the rest when one finishes.** Ranks
  correctly, but charges only the winner, and risks running the work twice
  when two copies place in the same pass.
- **Score as the sum of sponsor scores.** Treats breadth of demand as
  urgency. Rejected under requirement 3.
- **Charging each sponsor its full solo price.** Simple, and nobody pays
  more than alone, but the cluster then bills the same consumption several
  times and deduplication saves its users nothing.
- **Late joiners pay only for the remaining runtime.** Rewards waiting for
  someone else to start the job (requirement 6).
- **Sponsorships keyed by principal instead of entity.** Makes cost follow
  head-count within an entity, but a service acting for its users presents
  one principal, so the key would not mean what it appears to.

## Implementation sketch

Each step leaves the tree working, in the order a PR would carry them:

1. `coppice-core`: `Sponsor`, `Stake`, and `quota::share` with property
   tests for exact sum, the solo bound, independence from higher
   multipliers, and monotonicity in the sponsor set.
2. `coppice-proto` and `coppice-state`: the record and command changes,
   submit-or-join, the two sponsorship commands, per-stake charging in
   placement, renewal, settlement and join, and the victim class read from
   stakes.
3. `coppice-scheduler`: the maximum over sponsors in `score`, and the
   explainer.
4. `coppice-api`, `coppice-client`, `coppice-cli`, `web`: the sponsor
   routes, reads, filters and event keys.
5. Fold this record into `docs/scheduling/quotas-and-priorities.md`, the
   command catalog and the lifecycle doc, and mark it Accepted.
