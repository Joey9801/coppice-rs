# Future Features Wishlist

This is the register of features Coppice does **not** have and is not currently
building, but would like to be able to build one day. It is a dream wishlist,
not a scope commitment: entries here have deliberately *not* been vetted for
feasibility or effort.

The register exists for one practical reason: work done today should not make
these features unnecessarily hard tomorrow. Each entry therefore records three
things — what the feature is, which parts of the current design already
anticipate it, and what *design pressure* it exerts on work happening now (the
concrete habits that keep the door open).

## How to use this register

- Each entry has an ID (`FF-N`). IDs are stable; add new entries with the next
  free number.
- When a feature graduates from "wishlist" to "being designed", open the
  specific unresolved questions as `OD-N` entries in
  [open-decisions.md](open-decisions.md) and record decisions as
  [ADRs](../decisions/). Leave the `FF` entry in place with a link.
- This register is *not* ordered by priority or by likely implementation order.
  Entries FF-1 through FF-7 are the anchor set (features already known to be
  wanted); the rest are candidates collected so that they can shape design
  discussions early.

## At a glance

| ID | Feature | Existing seam / anticipation |
| --- | --- | --- |
| [FF-1](#ff-1-gang-scheduling) | Gang scheduling | `GroupId` placement groups, `Ready` funding barrier ([ADR 0013](../decisions/0013-job-attempt-allocation-state-machines.md), [ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md)) |
| [FF-2](#ff-2-gang-failure-accounting) | Gang failure accounting | Outcome taxonomy, platform/user classification ([ADR 0033](../decisions/0033-aligned-limit-breach-outcomes.md)) |
| [FF-3](#ff-3-custom-resource-types) | Custom resource types (GPUs, …) | `Resources` doc-flagged as placeholder for map-keyed dimensions |
| [FF-4](#ff-4-topology-aware-scheduling) | Topology-aware scheduling | Label-filter seam in the scheduler; nothing else yet |
| [FF-5](#ff-5-job-preemption) | Job preemption | Revocation machinery, strict backfill ([ADR 0006](../decisions/0006-reservations-and-strict-backfill.md), [ADR 0027](../decisions/0027-finite-projected-ready-accrual-protection.md)) |
| [FF-6](#ff-6-checkpoint-awareness) | Checkpoint awareness | Attempt identity and lineage hooks; nothing else yet |
| [FF-7](#ff-7-job-relationships) | Job relationships | Quota-entity tree as hierarchy precedent; client-minted job ids ([ADR 0026](../decisions/0026-client-minted-job-ids-idempotent-submission.md)) |
| [FF-8](#ff-8-array-jobs-and-parameter-sweeps) | Array jobs / parameter sweeps | — |
| [FF-9](#ff-9-deadline-aware-scheduling) | Deadline-aware scheduling | `max_runtime` declaration, `projected_ready` machinery |
| [FF-10](#ff-10-advance-reservations-and-maintenance-windows) | Advance reservations & maintenance windows | Accruing allocations generalize |
| [FF-11](#ff-11-elastic-and-malleable-jobs) | Elastic / malleable jobs | — |
| [FF-12](#ff-12-speculative-execution) | Speculative execution | Conflicts with [ADR 0030](../decisions/0030-structural-job-attempt-link.md) — flagged |
| [FF-13](#ff-13-utilization-based-overcommit) | Utilization-based overcommit | Agent telemetry pipeline |
| [FF-14](#ff-14-recurring-jobs-and-submission-triggers) | Recurring jobs & submission triggers | — |
| [FF-15](#ff-15-data-locality-and-cache-aware-scheduling) | Data locality & cache-aware scheduling | [ADR 0010](../decisions/0010-image-cache-boundary.md) hooks: `cache_affinity_bonus`, `prepare_cache` |
| [FF-16](#ff-16-volumes-and-storage-orchestration) | Volumes & storage orchestration | Disk as a first-class dimension |
| [FF-17](#ff-17-secret-injection) | Secret injection | Named future work in [security.md](../operations/security.md) |
| [FF-18](#ff-18-alternative-executors) | Alternative executors | `Executor` trait |
| [FF-19](#ff-19-hard-quotas-and-admission-control) | Hard quotas & admission control | Cost-based soft quotas ([ADR 0005](../decisions/0005-cost-based-soft-quotas.md)) as substrate |
| [FF-20](#ff-20-usage-accounting-and-chargeback) | Usage accounting & chargeback | Blocked on durable history (KOI-1) |
| [FF-21](#ff-21-power-and-energy-aware-scheduling) | Power/energy-aware scheduling | — |
| [FF-22](#ff-22-spot-capacity-and-autoscaling) | Spot capacity & autoscaling | OD-15(b), [deployment-story.md](deployment-story.md) |
| [FF-23](#ff-23-scheduling-simulator-and-capacity-planning) | Scheduling simulator & capacity planning | Pure deterministic scheduler passes |
| [FF-24](#ff-24-federation) | Federation / multi-cluster | — |
| [FF-25](#ff-25-external-integration-surface) | Webhooks, event subscriptions, SDKs | KOI-6 (`SubscribeEvents`), [ADR 0008](../decisions/0008-event-delivery-guarantees.md) |
| [FF-26](#ff-26-interactive-debugging-of-running-attempts) | Interactive debugging of running attempts | Log-retrieval path as precedent |
| [FF-27](#ff-27-job-templates-and-defaults) | Job templates & defaults | — |

---

## The anchor set

### FF-1: Gang scheduling

Schedule a set of N attempts as a unit: none start until all N can start, so
that tightly coupled workloads (distributed training, MPI-style jobs) never
deadlock waiting for missing peers.

**Already anticipated.** This is the most deliberately pre-shaped feature in
the design. Attempts fund through the `Ready` barrier defined over a `GroupId`
placement group (`crates/coppice-core/src/attempt.rs`); v1 groups are
singletons, so gangs add *members* to an existing mechanism rather than a new
mechanism. Accruing allocations ([ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md))
are exactly the multi-node reservation primitive a gang needs.

**Known open questions when this starts:** all-or-nothing versus min/max gang
sizes; deadlock and livelock between multiple concurrently accruing gangs (the
per-node accrual K-guard needs a gang-aware generalization, e.g. ordered
funding by effective score); a gang-wide `projected_ready`; whether a
`PlacementProposal` spanning many nodes validates and commits atomically.

**Design pressure now.** Never let new code assume a placement group is a
singleton or that `GroupId == JobId`. Plumb `GroupId` through anywhere an
attempt travels — events, API DTOs, journal records — even while it is
redundant. Keep the scheduler snapshot able to represent partially funded
groups without treating them as anomalies.

### FF-2: Gang failure accounting

When one member of a gang fails early, the platform must decide deliberately
what happens to the others: fail fast and kill the siblings, tolerate loss down
to a min-healthy count, restart the one member, or restart the whole gang —
and account for whose "fault" the wasted sibling compute was.

**Already anticipated.** The per-attempt outcome taxonomy with
platform-versus-user classification ([failure-handling.md](../operations/failure-handling.md),
[ADR 0033](../decisions/0033-aligned-limit-breach-outcomes.md)) is the right
substrate: a sibling killed because of a peer failure is morally a `Revoked`
(requeues free, not charged as the sibling's failure), with a cause pointing at
the instigating attempt.

**Known open questions:** per-gang failure policy vocabulary; whether a gang
restart reuses the group or mints a new one; how retry budgets are charged
(per member, per gang, or to the instigator only); surfacing the causal chain
in the UI so "why did my worker die" has a one-click answer.

**Design pressure now.** Keep outcomes strictly per-attempt but give them room
for a *cause chain* (this attempt terminated because of that attempt/event).
The abort-is-a-flag pattern generalizes well to "revoke because sibling
failed" — keep termination-reason enums in the proto extensible rather than
closed.

### FF-3: Custom resource types

First-class support for resource dimensions beyond CPU/memory/disk: GPUs and
other accelerators, software licenses, network bandwidth, host-specific
devices.

**Already anticipated.** `Resources`
(`crates/coppice-core/src/resource.rs`) documents itself as a coarse
placeholder for a map-keyed extensible-dimension design.

**Known open questions:** the two fundamentally different kinds —
*fungible scalars* (licenses, bandwidth: "3 of them") versus
*identity-carrying devices* (GPU **3**, with an address and topology
position, which the agent must actually assign and mount); fractional/shared
devices (MIG partitions, time-slicing); how agents report typed inventory at
registration; how the `Executor` maps an assignment onto container device
mounts; whether dimension *kinds* are cluster policy or node-reported facts.

**Design pressure now.** Don't write new code that enumerates the three fields
by hand — route arithmetic and comparisons through `Resources` methods so the
struct-to-map migration touches one crate. Write packing/scoring math
(dominant-resource fractions etc.) as iteration over dimensions. Keep the
wire-format `Resources` message growable without renumbering.

### FF-4: Topology-aware scheduling

Placement that understands *where* resources are, at two scales:

- **Intra-node:** NUMA nodes, PCIe/NVLink proximity between GPUs, device-local
  memory. The agent must be able to bind a container to a NUMA node or a
  specific GPU set.
- **Inter-node:** racks, switches, network fabric islands. Mostly a soft
  preference for single jobs, but close to a hard requirement for gangs
  (FF-1): a training gang scattered across fabric islands may be worse than
  not running.

**Already anticipated.** Barely. The scheduler has a label-filter seam
(`node_satisfies_labels`) but the job proto carries no selector yet, and nodes
are topologically flat.

**Known open questions:** the topology model itself (a tree of domains is
probably enough: fabric island → rack → node → NUMA node → device); constraint
vocabulary ("spread across ≥ N racks", "pack within one switch", "GPUs must be
NVLink-connected"); how topology terms compose with best-fit packing and with
gang-wide funding.

**Design pressure now.** Land plain label selectors early — labels are the
poor man's topology and give users a migration path. Let node registration
carry a structured description that can grow a topology section. Keep
scheduler scoring modular: best-fit-by-dominant-leftover should stay *one
term* among several, not the shape of the code.

### FF-5: Job preemption

Evict a running attempt to make room for more important work, rather than only
waiting for capacity.

**Already anticipated.** The mechanics mostly exist: `Revoked` is a first-class
outcome that requeues without charging the job, and strict backfill
([ADR 0006](../decisions/0006-reservations-and-strict-backfill.md),
[ADR 0027](../decisions/0027-finite-projected-ready-accrual-protection.md))
already reasons about revoke-and-reseat for *accruing* allocations. Preemption
is largely "extend revocation to *active* allocations, with a policy for
choosing victims" — and `effective_score` gives a natural victim ordering
(preempt the lowest-scored running work first).

**Known open questions:** the preemption cost model (wasted work versus wait
time — transformed by checkpoints, FF-6); grace semantics (notice signal,
deadline, and whether the notice carries "checkpoint now"); churn control so
the cluster doesn't thrash near capacity; how preemption interacts with
cost-based *soft* quotas, where it becomes the enforcement mechanism of last
resort rather than a routine event.

**Design pressure now.** Keep "revoke a running attempt" a distinct command
and outcome from user abort — they already are; preserve that. Make sure the
agent's termination path can carry a reason and a deadline, not just a kill.
Keep `effective_score` computable for *running* work, not only queued work.

### FF-6: Checkpoint awareness

The platform knows that a job has checkpoints: that a stopped attempt can
resume from saved state rather than cold-starting. This transforms the
economics of preemption (FF-5) and of spot/autoscaled capacity (FF-22).

**Already anticipated.** Not concretely, but the state machines leave room:
attempts have durable identity, so "attempt N+1 resumed from the checkpoint
written by attempt N" is expressible as lineage.

**Known open questions:** the reporting channel (does the workload tell the
agent "checkpoint written at T, pointer P", does the agent watch for it, or
both?); control-plane storage of checkpoint *metadata and pointers only*
(checkpoint data itself lives in user storage — the control plane should never
be in the data path); the executor-side contract (a "checkpoint now" signal
with a deadline, versus purely application-initiated); transparent
system-level checkpointing (CRIU-style) versus application-level — probably
application-level first, it is vastly simpler and covers the ML training case;
how the scheduler prices resumability (a checkpointed job is a *cheap*
preemption victim).

**Design pressure now.** Add nothing that assumes attempts are memoryless.
Keep room in the requeue path for "resumable from X" to travel with the job.
The agent↔coordinator protocol should be able to grow a small
workload-originated reporting surface (also needed by FF-7 and FF-11).

### FF-7: Job relationships

Jobs that relate to other jobs. Several distinct kinds hide under one banner:

- **Dependencies:** job B runs only after job A succeeds (through to full
  DAG workflows).
- **Spawning:** a running job submits sub-jobs.
- **Grouping:** jobs clustered for UI, quota, and bulk operations.
- **Worker pattern:** long-lived worker jobs fed tasks at runtime, keeping
  warm state between tasks — potentially unified with checkpointing (FF-6),
  where a worker processes one sub-job per checkpoint epoch and consecutive
  tasks are cheaper than cold starts.

**Already anticipated.** The quota-entity tree is precedent for cheap
hierarchy in replicated state. Client-minted job ids
([ADR 0026](../decisions/0026-client-minted-job-ids-idempotent-submission.md))
make atomic submission of a dependent graph tractable — a client can mint all
ids and wire up references before submitting anything.

**Known open questions:** dependency semantics on failure (cascade-cancel,
cascade-hold); cycle prevention; whether grouping, dependency, and spawning are
one edge type or three; **workload identity** — a job that submits sub-jobs
needs job-scoped credentials, which is an authn/z design question
([ADR 0022](../decisions/0022-oidc-identity-and-authentication.md),
[ADR 0023](../decisions/0023-scoped-role-bindings.md) territory) as much as a
scheduling one; whether the worker/task pattern is a scheduler feature or a
library convention on top of spawning.

**Design pressure now.** Keep `JobId` references cheap and first-class in
replicated state. Don't let API/UI surfaces bake in the assumption that the
job list is flat. When touching authn, keep "a workload as a principal" in
mind — it unlocks FF-6 reporting and FF-11 too.

---

## Further candidates

### Scheduling and workload shapes

#### FF-8: Array jobs and parameter sweeps

One submission producing N indexed instances sharing a spec ("run this image
with `INDEX=0..9999`"). At the 1M-queued-jobs design target this is less a
convenience than a compression scheme: one job record plus per-index status
beats a million near-identical records in replicated state, in the scheduler
snapshot, and in the UI. Pairs naturally with grouping (FF-7).

#### FF-9: Deadline-aware scheduling

Jobs carry a complete-by time; the scheduler weighs deadline risk, not only
score and age. Builds directly on the declared-runtime incentives
([ADR 0029](../decisions/0029-runtime-declaration-incentives.md)) and the
`projected_ready` machinery — "will this finish by T if started at
`projected_ready`" is the same arithmetic pointed forwards.

#### FF-10: Advance reservations and maintenance windows

Reserve capacity for a future interval — a scheduled large experiment, or the
inverse: an operator marking nodes unavailable from Tuesday 02:00. Accruing
allocations are reservations anchored at *now*; this generalizes the anchor to
a future time. Maintenance windows are the operator-facing special case and
would compose with drain (OD-15(b)).

#### FF-11: Elastic and malleable jobs

Jobs that can grow and shrink their worker count at runtime, cooperating with
the scheduler ("I want 8–64 workers; give and take as capacity allows").
Effectively gang scheduling (FF-1) with a dynamic gang size, plus a
workload↔scheduler negotiation channel (the same workload-originated surface
as FF-6/FF-7). Big utilization win for batch ML workloads.

#### FF-12: Speculative execution

Straggler mitigation: launch a duplicate attempt of a slow job, first finisher
wins, the other is revoked. **Flag:** this is the one wishlist entry that
directly conflicts with a settled decision —
[ADR 0030](../decisions/0030-structural-job-attempt-link.md) makes
`Attempting(AttemptId)` structurally single-in-flight. That decision is right
for everything else here, so speculative execution should be modelled some
other way if it ever lands (e.g. as sibling jobs under FF-7 relationships)
rather than by loosening the invariant.

#### FF-13: Utilization-based overcommit

Schedule against *observed* usage rather than requests alone: jobs
chronically using half their request leave reclaimable headroom. The agent
telemetry pipeline already collects per-attempt usage; the missing pieces are
feeding aggregates back into the scheduler snapshot and a pressure-eviction
story on the node (which is FF-5's machinery again). Classic
utilization-versus-predictability trade-off; cost-based quotas could price
overcommitted capacity more cheaply.

#### FF-14: Recurring jobs and submission triggers

Cron-style recurring submissions and event-triggered submissions ("when job A
succeeds, submit B" — the trigger half of FF-7 dependencies). Reasonable to
keep out of the replicated core and build as a thin service on the public API;
worth listing so the API keeps supporting idempotent programmatic submission
well ([ADR 0026](../decisions/0026-client-minted-job-ids-idempotent-submission.md)
already helps).

### Resources, data, and placement

#### FF-15: Data locality and cache-aware scheduling

Prefer nodes that already hold what the job needs: container images (the
[ADR 0010](../decisions/0010-image-cache-boundary.md) seams —
`cache_affinity_bonus`, `prepare_cache` — exist precisely for this), and one
step further, datasets: declared data dependencies, staged pre-fetch onto
candidate nodes, and scoring that knows a warm node saves minutes. For
ML workloads dataset gravity often dominates every other placement signal.

#### FF-16: Volumes and storage orchestration

Today disk is a scalar scratch dimension. Future shapes: managed scratch
lifecycle guarantees, persistent volumes surviving across attempts (which
would give checkpoints, FF-6, a natural home), and awareness of shared
filesystems (mount requirements as constraints, quota on shared-FS usage).

#### FF-17: Secret injection

Deliver credentials to workloads without baking them into images or argv.
Already named as future work in [security.md](../operations/security.md).
Scope question: full secret store versus injection broker for external stores
(Vault, cloud secret managers) — the latter keeps secrets out of replicated
state, which the control plane's design principles strongly suggest.

#### FF-18: Alternative executors

The `Executor` trait (`crates/coppice-agent/src/executor.rs`) is the seam:
containerd/Podman backends, bare-process execution for trusted single-tenant
clusters, microVMs (Firecracker/Cloud Hypervisor) for hard multi-tenant
isolation — the security posture ([ADR 0011](../decisions/0011-container-security-posture.md))
names the isolation boundary as the reason multi-tenancy is soft today, and a
VM executor is the main path to changing that.

### Multi-tenancy, policy, and accounting

#### FF-19: Hard quotas and admission control

Cost-based soft quotas ([ADR 0005](../decisions/0005-cost-based-soft-quotas.md))
deliberately have no hard limits. Some organizations will eventually want
them anyway: hard caps per entity, guaranteed minimums (reserved floors a
tenant can always claim — preemption, FF-5, is what makes floors enforceable),
and admission control (reject or park submissions past a backlog bound). Best
understood as optional layers *on top of* the soft-quota substrate, not a
replacement.

#### FF-20: Usage accounting and chargeback

Durable per-entity usage rollups: CPU/GPU-hours by quota entity over time,
exportable for billing/showback. Blocked on the durable history store
(KOI-1) — worth remembering when that store is designed that *aggregation*,
not just record retrieval, is a consumer.

#### FF-21: Power and energy-aware scheduling

Energy as a first-class concern: per-job energy attribution (metering), soft
power caps per rack (a topology-domain constraint — FF-4's model carries it),
scheduling deferrable work into cheap/green energy windows, and powering down
idle nodes (the on-prem sibling of autoscaling scale-in, FF-22). Increasingly
material for GPU fleets where power is the binding constraint.

### Operations and scale

#### FF-22: Spot capacity and autoscaling

Zero-touch scale-up/scale-in is already planned
([deployment-story.md](deployment-story.md), OD-15(b) drain). The wishlist
extension is *spot/preemptible* cloud capacity: nodes that vanish on short
notice. The interruption notice is a preemption signal (FF-5) from outside,
and checkpointing (FF-6) is what makes spot economics actually work — these
three are one story.

#### FF-23: Scheduling simulator and capacity planning

Because a scheduler pass is a pure function of `(snapshot, now)` with
deterministic fixed-point arithmetic, Coppice is unusually well placed to
offer offline replay and what-if analysis: replay a captured snapshot stream
against a candidate policy change, or answer "what would adding ten nodes do
to queue times?" This is cheap leverage other schedulers can't easily copy —
**preserving scheduler purity is what keeps it cheap**, which makes this
entry a design-pressure generator for every scheduling feature above.

#### FF-24: Federation

Multiple Coppice clusters behind one submission surface — different
datacenters, or cloud burst overflow from an on-prem cluster. Almost
certainly a layer above the cluster (a router speaking the public API), not a
change within it; listed mainly so the API surface keeps enough job-spec
portability for a router to make placement choices.

#### FF-25: External integration surface

Webhooks on job state transitions, a robust streaming event subscription
(KOI-6's unserved `SubscribeEvents`, semantics per
[ADR 0008](../decisions/0008-event-delivery-guarantees.md)), and typed client
SDKs generated from the API. This is what CI systems, workflow engines, and
notification bots build on — and a good external trigger surface lowers the
pressure to pull FF-14-style features into the core.

### User experience

#### FF-26: Interactive debugging of running attempts

Exec-into / attach / port-forward for a *running* attempt, with the same
best-effort, authz-gated posture as log retrieval
([ADR 0034](../decisions/0034-best-effort-job-log-retrieval.md)). Coppice is
deliberately a batch system, but batch users still debug; a scoped escape
hatch beats users side-channeling onto nodes.

#### FF-27: Job templates and defaults

Named, versioned job templates (image, resources, policy defaults) that
submissions reference and override. Mostly API/UX-layer sugar, but templates
are also where per-team *policy* defaults (retry policies, runtime bounds,
future gang/checkpoint settings) naturally attach.

---

## Cross-cutting design pressure

Distilled from the entries above — the short list of habits that keep this
whole register cheap to reach:

1. **Never assume singleton placement groups.** `GroupId` is the gang seam;
   plumb it everywhere an attempt travels, even while redundant (FF-1, FF-2,
   FF-11).
2. **Treat `Resources` as opaque and iterable.** No new code should enumerate
   cpu/memory/disk by hand; the struct-to-map migration should touch one crate
   (FF-3, FF-13, FF-21).
3. **Keep the scheduler pure and its scoring modular.** Every feature above
   lands as another filter or score term; purity is also what makes the
   simulator (FF-23) nearly free.
4. **Leave proto room.** Termination/outcome reasons as open enums with cause
   chains; node registration able to grow structured topology and typed
   resource inventory; job spec able to grow selectors, deadlines, gang and
   checkpoint sections (FF-2, FF-3, FF-4, FF-9).
5. **Plan for workloads as principals.** Job-scoped credentials and a small
   workload-originated reporting channel underlie sub-jobs, checkpoint
   reports, and elastic negotiation (FF-6, FF-7, FF-11).
6. **Distinguish revocation from failure from abort, always.** Preemption,
   gang sibling kills, spot interruptions, and pressure eviction are all
   "revoked through no fault of the job" — one well-modelled concept serves
   all four (FF-2, FF-5, FF-13, FF-22).
