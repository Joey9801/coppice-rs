# 48. Job sponsors: one job, several paying quota entities

- **Status:** Proposed
- **Date:** 2026-11-10
- **Amends:** [ADR 0005](0005-cost-based-soft-quotas.md) (one leaf entity
  per job), [ADR 0019](0019-deterministic-quota-arithmetic.md) (which
  entities a charge lands on), [ADR 0021](0021-effective-score-ranking.md)
  (the score of a job with several sponsors),
  [ADR 0023](0023-scoped-role-bindings.md) (job ownership),
  [ADR 0026](0026-client-minted-job-ids-idempotent-submission.md) (a
  repeated submission of a known id),
  [ADR 0044](0044-job-preemption-and-spot-capacity.md) (the class of a
  victim) and [ADR 0046](0046-interval-charging-for-unbounded-jobs.md)
  (renewal and settlement)
- **Builds on:** ADR 0047, per-class age weights and a penalty-scaled age
  term (PR #186, not yet merged)

## Context

Coppice is meant to run under services that deduplicate work. Several users
ask for the same computation, and one job runs for all of them. The job id
is client-minted and is the identity of a submission (ADR 0026). Such a
service can derive the id from the work, so a second request for the same
work finds the first job.

The service cannot say who the job is for. A job has one `quota_entity` and
one `priority`. Both are in the immutable spec, and apply rejects a
resubmission that changes either as `SubmitSpecMismatch`. Users call the
entity their "queue". This record uses the term from the code.

Example: user A asks on entity `a` at priority 1. User B asks for the same
work on entity `b` at priority 10. The service must pick one of them. That
choice has three results:

- The entity that the service picked pays for the whole job, on every
  ancestor. The other user gets the result free. If the service submits
  under a pool entity of its own, no requester's usage shows the request,
  and the penalty of the pool falls on all of its users.
- The score reads one multiplier and one penalty product. If the service
  picked A's request, B's urgency buys nothing, although B accepts the
  price. If it picked B's request, A pays a 10× premium that A declined.
- Only the recorded submitter can abort, and an abort stops the job for
  every requester.

A bids low because A's quota is small. B wants the result soon and accepts
the price. The design must respect both preferences.

### Fairness requirements

One job that runs for several requesters is a shared good, so fairness is a
cost-sharing question. This decision must meet six requirements:

1. A job with one sponsor behaves exactly as it does today.
2. Sharing never ranks a job below the rank that any one sponsor gets
   alone. Sharing never charges a sponsor more than the job costs that
   sponsor alone, at its own priority.
3. Sponsors together get no better rank than the best of them gets alone.
   A request that is split across entities then buys nothing. ADR 0005
   requires that users pay for priority.
4. The sponsors together pay what one submitter pays at the priority the
   job ran at. Cluster-wide usage counts the consumption once.
5. The sponsor that asked for urgency pays for it. A's share does not grow
   because B bid higher.
6. A sponsor that joins late pays the same as one that joins early. A
   sponsor that leaves cannot push cost onto the sponsors that stay.

## Decision

### 1. A job has sponsors

A set of sponsors replaces `Job.quota_entity`, `Job.priority` and
`Job.submitted_by`. The key of a sponsor is its leaf quota entity. A job
has at least one sponsor and at most `MAX_SPONSORS` (64).

```
Sponsor { entity, priority, sponsored_by, since }       // on the job spec
JobRecord.multipliers: entity → PriorityMultiplier      // resolved at proposal
```

The other spec fields are the work, and they stay immutable: image,
command, entrypoint, env, requests, `max_runtime`, retry policy and
preemption declarations. The API resolves the multiplier of a sponsor from
the policy table when it proposes the sponsorship, as it does for a
submission today.

The key is the entity because the entity is the accounting unit. The quota
tree is where a deployment declares who is one party. Several requests from
one entity are one sponsorship, at the highest priority requested. A
deployment that wants per-user fairness gives each user a leaf entity.

`since` is the time the sponsorship last took its current priority. It
starts at the join and resets on each priority change.

The code derives two values from the sponsor set and never stores them on
the job. The tier of a job is its largest sponsor multiplier. The class of
a job is its largest sponsor priority.

### 2. Rank: the best score of any sponsor

Each sponsor has the score that ADR 0047 gives a job of its own:

```
score_i(now) = (m_i + w(class_i) · max(0, now − since_i) / H) / P_i(now)
effective_score(j, now) = max over sponsors i of score_i(now)
```

`P_i` is the penalty product over the path of sponsor `i`. Ties break by
the `submitted_at` of the job, then by `JobId`, as they do now. The
scheduler already memoizes the penalty product per leaf entity, so each
extra sponsor costs one comparison per pass.

Requirements 2 and 3 leave only the maximum. A lower value delays some
sponsor compared with a solo submission. A higher value, such as a sum,
gives a group a rank that no member paid for. With a sum, one request
split across ten entities outranks the same request made once.

Each sponsor score is a solo score, so every property of the ranking
formula carries over. Two properties of ADR 0047 depend on `since`:

- A sponsor that joins an old job starts with no age. A default-class
  sponsor that joins a backfill job queued for three weeks ranks as a fresh
  default job.
- A priority change resets the age. Without the reset, a sponsor could wait
  in a class with zero age weight, then raise its priority and claim weeks
  of age in an aging class.

The queue-position explainer names the sponsor with the best score and
lists the score of every sponsor.

### 3. Cost: each priority band is split among the sponsors that asked for it

Apply charges a placement once, at the tier `M` of the job, as it charges a
solo job at that priority: `amount = cost_from_rate(rate, runtime_s, m')`.
The folds of ADR 0029 and ADR 0044 apply to `M`. One pure function in
`coppice_core::quota` divides that amount and every later change to it:

```
stakes sorted by (multiplier, entity id), each multiplier capped at M:
    m_1 ≤ m_2 ≤ … ≤ m_n = M,   m_0 = 0
band_k     = ⌊X · m_k / M⌋ − ⌊X · m_(k−1) / M⌋          (u128, bands sum to X exactly)
share_i(X) = Σ over k ≤ i of ⌊band_k / (n − k + 1)⌋      (each band's remainder goes to stake n)
```

All sponsors split equally the cost of the job at the lowest priority that
any of them requested. Each step up is split equally among the sponsors
that requested that priority or a higher one. The top sponsor pays the last
step alone.

| Sponsors (multiplier) | Shares of a job that costs `M × C` |
| --- | --- |
| a: 1 | a: 1 C |
| a: 1, b: 1 | a: 0.5 C, b: 0.5 C |
| a: 1, b: 10 | a: 0.5 C, b: 9.5 C |
| a: 1, b: 2, c: 10 | a: 0.33 C, b: 0.83 C, c: 8.83 C |

This split is the Shapley value of a cost game in which each sponsor needs
the job to run at its own priority or better. The cost-sharing literature
calls it the airport game. The split meets requirements 2, 4 and 5:

- The shares sum to the amount.
- `share_i ≤ X · m_i / M`, which is the solo price of the sponsor.
- The share of a sponsor does not depend on any higher multiplier.
- A new sponsor never raises the share of another sponsor.

In the example, A pays half of its solo price. B pays 9.5 C against a solo
price of 10 C, and the job runs when B's bid ranks it.

An equal split fails requirement 2, because A pays 5 C against a solo price
of 1 C. A split in proportion to the multipliers fails requirement 5,
because A's share rises toward its solo price as B bids higher.

Apply charges each share to the leaf of its stake and to every ancestor of
that leaf. Two sponsors under one team charge the team for the whole job,
once.

### 4. Placement fixes who pays for an attempt

An attempt records its stakes, its tier and its class. The stakes are a
copy of the sponsors of the job at placement. Like the recorded rate,
multiplier and refund fraction, they do not change with later policy
edits. The record does not store an amount per stake, because the share of
a stake is `share_i` of the recorded amount.

This section covers an attempt with a declared `max_runtime`. Section 5
covers interval-charged attempts.

- Placement charges each stake `share_i(amount)` at `proposed_at`.
- Settlement computes one true-up for the attempt, as it does today. It
  applies `share_i(true_up)` to each stake. Shares are linear in the
  amount, so each stake pays its share of the actual cost.
- An attempt is effectively preemptible only if no entity on the path of
  any stake has preemption switched off.
- Apply judges a victim by the class recorded on its attempt, where it
  reads `spec.priority` today. A queued beneficiary uses the current class
  of its job. The scheduler scores a victim from the stakes of its attempt.

If a sponsor joins a job that has an attempt in flight, apply adds a stake
and splits the charged amount again. The joiner receives the whole result,
so it owes a whole share:

```
before = share(amount) over the old stakes
after  = share(amount) over the old stakes and the new stake
for each stake: decay (after − before) from charge.charged_at to now,
                then charge it if positive, or refund it if negative
```

The cap in the share function limits the multiplier of the joiner to the
tier of the attempt. A higher priority cannot buy anything from an attempt
that is already placed, so the joiner does not pay for it. The class of the
attempt does not change either. A late default-class sponsor cannot protect
a running backfill attempt from preemption.

The decay puts each entity where it would be if the joiner had been a
sponsor at placement. That is requirement 6: no moment to join is cheaper
than another. The transfer moves usage between sponsors and creates none.

A withdrawal removes the sponsor from the job immediately. The sponsor
stops counting toward the rank, the class and the liveness of the job, and
it is not a stake in a later attempt. If an attempt is in flight, its stake
stays until that attempt settles. If the attempt is revoked or aborted
before it runs, the settlement refunds that stake with the others. A queued
job has charged nothing, so a withdrawal from it is free. When the last
sponsor withdraws, apply aborts the job through the existing abort path.

We considered a rule that releases a withdrawn stake and refunds the part
of the run that has not elapsed. We rejected it for two reasons. When the
top sponsor leaves, its premium falls on the sponsors that declined to pay
it. A sponsor could also lend its tier or its low penalty to win a
placement, then leave before it pays. The cost of the chosen rule is that a
sponsor can pay its share of a run that it cancelled. That share is at most
the solo price of the sponsor.

### 5. Interval-charged attempts commit one interval at a time

A job with no `max_runtime` pays in intervals (ADR 0046), and it can run
without end. The rule of section 4 would bill a withdrawn sponsor for as
long as the attempt runs. For these attempts the commitment is the interval
already charged:

- A withdrawal closes the stake. A closed stake keeps what it has paid and
  shares no later charge.
- Each renewal sets the tier and the class of the attempt from its open
  stakes, uncapped. It charges the new intervals at that tier and splits
  them among the open stakes. A top sponsor that leaves pays its premium to
  the end of the paid interval. The other sponsors then continue at their
  own tier.
- A sponsor that joins at a higher priority raises the tier and the class
  from the next renewal.

Two parts of ADR 0046 change to support this:

- The multiplier of an attempt can now differ between intervals. Settlement
  therefore measures the unused or overrun seconds and prices them at the
  multiplier of the latest charge. It applies the result to the stakes that
  shared that charge. With one constant multiplier this equals the current
  arithmetic.
- `ChargeInterval` gains `present_value`: the charges so far, decayed to
  `charged_at`. Each renewal decays it to `proposed_at` and adds the new
  tranche. A join splits `present_value`, not the nominal amount. The
  nominal amount of a long run overstates the usage that the incumbents
  still carry, so a split of it would refund them more than they hold.

After a join, the joiner and the incumbents carry the usage that they would
carry if the joiner had been a sponsor from the start.

### 6. Commands and identity

`SubmitJob` becomes submit-or-join. The command carries the work, one
sponsorship (the entity and priority of the caller) and its resolved
multiplier. Apply checks `Verb::Submit` on the entity of the caller in
every row:

| State of the job id | Result |
| --- | --- |
| Unknown | Create the job with one sponsor |
| Known, the work differs | `SubmitSpecMismatch` |
| Known, entity is a sponsor, priority ≤ recorded | Accepted no-op (the ADR 0026 retry) |
| Known and live, entity is a sponsor, priority higher | Raise the sponsorship |
| Known and live, entity is not a sponsor | Join (`TooManySponsors` past the cap) |
| Known and terminal, any other case | `JobTerminal` |

Metadata leaves the identity comparison. It is mutable and belongs to the
job, and a joining submission does not check or change it. The response
names the row that applied.

Two more commands address an existing job by id:

- `SetSponsorship { job, entity, priority }` sets the priority of a
  sponsorship exactly, and creates the sponsorship if it is absent. A
  sponsor uses it to lower its bid.
- `WithdrawSponsorship { job, entity }` withdraws, as in sections 4 and 5.

`POST /api/v1/jobs` keeps its `quota_entity` and `priority` fields, which
now describe the sponsorship of the caller. `PUT` and
`DELETE /api/v1/jobs/{job}/sponsors/{entity}` take a `QuotaEntityRef`
(ADR 0045).

Authorization follows the entity that pays:

- To create or change a sponsorship, a principal needs `submitter` over its
  entity.
- To withdraw a sponsorship, a principal must have created it or hold
  `operator` over its entity.
- `AbortJob` stops the job for every sponsor, so it needs that authority
  over every current sponsor. For a solo job this is the rule of today.
- A metadata update needs that authority over one sponsor.

Coppice does not decide which requests are the same work. The deduplicating
service knows that, and it says so when it mints the same job id. On
`JobTerminal`, the service serves its cached result or mints a new id.

### 7. Reads and events

- Job reads carry the sponsors with their paths, and the tier and class of
  the job. For each attempt they carry the share of each stake in the
  charge and in the settlement.
- An `entity` filter (ADR 0031, ADR 0043) matches a job if the entity of
  any current sponsor matches. The event scope keys of a job are the union
  of the entity chains of its sponsors.
- Apply emits `JobSponsorChanged` on a join, a priority change and a
  withdrawal. The event carries the prior keys, as a metadata update does,
  so a subscriber sees the event that removes a job from its set.

### 8. Wire and state

There are no compatibility shims. `Job` tags 4, 6 and 11 are reserved, and
`repeated Sponsor sponsors` replaces them, in ascending entity order (the
`Label` rule).

`JobRecord.multiplier` becomes the map of multipliers per
sponsor. `AttemptRecord` gains `repeated Stake stakes`, `tier` and `class`.
`SubmitJob` changes and two commands are new. `CommitPlacements` and
`RenewIntervalCharges` keep their shapes. The state machine stores no
derived value.

## Consequences

- The example resolves to one job that ranks by B's bid. A pays half of the
  base cost and B pays the rest. Both pay less than they pay alone, and the
  cluster runs the work once.
- Requirement 1 has a direct test. With one sponsor, each formula reduces
  to the current one, so the determinism and lifecycle suites must pass
  unchanged.
- A sponsor can bid low and rely on the urgency of another sponsor. Any
  rule that shares a cost by declared need has this weakness. Two limits
  apply: each sponsor pays an equal share of the lowest band, and the high
  bidder never pays more than its solo price.
- A request that arrives after the job is terminal cannot join. A requester
  that the service serves from its cache pays nothing. Attempts that
  settled before a sponsor joined are also not shared. The same arithmetic
  can extend the split to settled attempts inside the retention window. We
  leave that until there is evidence that users exploit the gap.
- A withdrawn sponsor can pay for a run that it cancelled: the whole
  attempt if the job declared a `max_runtime`, one interval if it did not.
- A join charges a lump to the entity of the joiner. Under ADR 0047 that
  lump divides the age credit of the other queued work of that entity,
  as a placement does.
- A demand estimator that counts queued jobs by class (ADR 0047) must count
  a job under the class of its best-scoring sponsor. The class of the job
  can be higher than that when the top sponsor is far over quota.
- Per-stake amounts are derived. A durable history store (issue #43) must
  record them at settlement if chargeback needs them.
- Apply does `O(sponsors × tree depth)` work on placement, renewal,
  settlement and join, where it did `O(tree depth)`. The sponsor cap and
  the depth cap bound that work. A job record grows by one entry per extra
  sponsor.
- The caller tracks multiplicity inside one entity. If two users of one
  entity request the same job and one cancels, the service must keep the
  sponsorship of the entity.
- The scheduler, API, CLI and web read one entity and one priority today.
  They must read the sponsor set. That is most of the implementation work.

## Alternatives considered

- A pool entity that the deduplicating service owns, with chargeback
  outside Coppice. This needs no change. The scheduler then ranks by the
  usage of the pool and ignores the requesters.
- One job per requester, with the others cancelled when one finishes. The
  rank is correct, but only the winner pays. Two copies can also place in
  one pass and run the work twice.
- A score that is the sum of the sponsor scores. Rejected under
  requirement 3.
- A charge of the full solo price to each sponsor. No sponsor pays more
  than alone, but the cluster bills one consumption several times, and
  deduplication saves its users nothing.
- A late joiner that pays only for the remaining runtime. This rewards a
  requester that waits for another to start the job (requirement 6).
- Sponsorships keyed by principal. Cost would follow the number of users in
  an entity. A service that acts for its users presents one principal, so
  the key would not identify a user.

## Implementation sketch

Each step leaves the tree in a working state:

1. `coppice-core`: `Sponsor`, `Stake` and `quota::share`. Property tests
   cover the exact sum, the solo bound, independence from higher
   multipliers and monotonicity in the sponsor set.
2. `coppice-proto` and `coppice-state`: the record and command changes,
   submit-or-join, the two sponsorship commands, charging per stake, and
   the victim class from the attempt.
3. `coppice-scheduler`: the maximum over sponsors in `score`, and the
   explainer.
4. `coppice-api`, `coppice-client`, `coppice-cli` and `web`: the sponsor
   routes, reads, filters and event keys.
5. Update `docs/scheduling/quotas-and-priorities.md`, the command catalog
   and the lifecycle doc from this record, and mark it Accepted.
