# Open Design Decisions

This is the register of significant design questions that are **not yet
settled**. It exists so that a human or agent picking up Coppice can quickly see
what is undecided, why it matters, and what the trade-offs are — without
re-deriving it from the whole design.

## How to use this register

- Each entry has an ID (`OD-N`), a status, the question, why it matters, and the
  main options or considerations.
- When a question is **resolved**, write an [ADR](../decisions/) capturing the
  decision and its rationale, change the entry's status to _Resolved_, and link
  the ADR. Do not delete the entry — the register should show what has been
  decided as well as what remains.
- Keep the design docs (the rest of `docs/`) describing the _current intended_
  design; keep the _reasoning_ in ADRs; keep the _list of what's still undecided_
  here.
- Add new questions as `OD-N` with the next free number.

## Status at a glance

| ID | Question | Blocking? | Status |
| --- | --- | --- | --- |
| [OD-1](#od-1-raft-library-and-persistence-layer) | Raft library & persistence layer | Yes — foundational | Resolved — [ADR 0002](../decisions/0002-openraft-with-custom-segment-storage.md) |
| [OD-2](#od-2-command-and-snapshot-schema-versioning) | Command & snapshot schema versioning | Yes — hard to change later | Resolved — [ADR 0003](../decisions/0003-protobuf-serialization-and-cluster-version-gates.md) |
| [OD-3](#od-3-job-lifecycle-state-machine) | Job lifecycle state machine | Yes — touches everything | Resolved — [ADR 0013](../decisions/0013-job-attempt-allocation-state-machines.md) (superseding [ADR 0004](../decisions/0004-job-lifecycle-and-attempts.md)) |
| [OD-4](#od-4-quota-and-priority-policy) | Quota & priority policy | High | Resolved — [ADR 0005](../decisions/0005-cost-based-soft-quotas.md) |
| [OD-5](#od-5-reservation-and-backfilling-model) | Reservation & backfilling for large jobs | Medium | Resolved — [ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md) (superseding [ADR 0006](../decisions/0006-reservations-and-strict-backfill.md)) |
| [OD-6](#od-6-follower-read-consistency-model) | Follower read consistency | Medium | Resolved — [ADR 0007](../decisions/0007-per-endpoint-read-consistency.md) |
| [OD-7](#od-7-event-subscription-delivery-guarantees) | Event subscription delivery guarantees | Medium | Resolved — [ADR 0008](../decisions/0008-event-delivery-guarantees.md) |
| [OD-8](#od-8-agent-fencing-and-reconciliation-protocol) | Agent fencing & reconciliation | High | Resolved — [ADR 0009](../decisions/0009-fencing-and-reconciliation.md) |
| [OD-9](#od-9-image-cache-autonomy-vs-central-hints) | Image-cache autonomy vs. central hints | Low | Resolved — [ADR 0010](../decisions/0010-image-cache-boundary.md) |
| [OD-10](#od-10-container-execution-security-model) | Container execution security model | High | Resolved — [ADR 0011](../decisions/0011-container-security-posture.md) |
| [OD-11](#od-11-data-retention-policy) | Data retention policy | Low | Resolved — [ADR 0012](../decisions/0012-data-retention.md) |
| [OD-12](#od-12-abort-semantics-partial-scheduling-and-job-groups) | Abort semantics, partial scheduling & job groups | High | Resolved — [ADR 0013](../decisions/0013-job-attempt-allocation-state-machines.md), [ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md) |
| [OD-13](#od-13-base-score-and-the-exact-job-costing-formula) | Base score and the exact job-costing formula | High | Resolved — [ADR 0021](../decisions/0021-effective-score-ranking.md) |
| [OD-14](#od-14-coordinator-discovery-and-control-plane-pki) | Coordinator discovery & control-plane PKI | Medium | Resolved — [ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md) |
| [OD-15](#od-15-agent-enrollment-signer-and-decommission-protocol) | Agent enrollment signer & decommission protocol | High — gates zero-touch autoscaling | Open — plan in [deployment-story.md](deployment-story.md) |
| [OD-16](#od-16-user-authentication-and-principal-model) | User authentication & principal model | High | Resolved — [ADR 0022](../decisions/0022-oidc-identity-and-authentication.md) |
| [OD-17](#od-17-authorization-model-and-enforcement) | Authorization model & enforcement | High | Resolved — [ADR 0023](../decisions/0023-scoped-role-bindings.md) |

All eleven initial questions were resolved on 2026-07-07; the ADRs linked above
carry the decisions and rationale. New questions get the next free `OD-N`.

"Blocking?" is a rough judgement of how much other work is gated on the answer.

---

## OD-1: Raft library and persistence layer

**Resolved** (2026-07-07): openraft, over a custom append-only segment-file
storage layer for log, vote metadata, and snapshots —
[ADR 0002](../decisions/0002-openraft-with-custom-segment-storage.md).

**Question.** Which Raft implementation and durable log/snapshot storage does the
coordinator build on?

**Why it matters.** This is the most foundational choice: it shapes the
`coppice-consensus` API, the snapshotting model, the apply loop, and the
operational story (backup, disaster recovery). It is expensive to change once
built against.

**Considerations.**
- Build on an existing Rust Raft crate (e.g. an `openraft`-style library) vs. a
  more manual integration over a lower-level log.
- Persistence: embedded key-value store for the log and snapshots vs. custom
  segment files.
- Must support snapshotting so recovering coordinators don't replay an unbounded
  log (see [high-availability](../architecture/high-availability.md)).
- The library's determinism and threading model must let expensive scheduling
  stay off the apply path.

**Related:** [architecture/high-availability.md](../architecture/high-availability.md),
`coppice-consensus`.

## OD-2: Command and snapshot schema versioning

**Resolved** (2026-07-07): protobuf via prost for all durable and wire formats,
with additive-only evolution and a Raft-replicated `ClusterVersion` gating
semantic changes —
[ADR 0003](../decisions/0003-protobuf-serialization-and-cluster-version-gates.md).

**Question.** How are command formats, snapshot formats, and durable state
schemas versioned and evolved?

**Why it matters.** Old log entries and snapshots may be read by newer binaries,
and rolling upgrades run mixed versions briefly. A wrong choice here can make
rollback impossible or corrupt state during upgrade.

**Considerations.**
- Serialization format and its evolution rules (additive-only fields, explicit
  version tags, feature gates bumped through Raft).
- The upgrade choreography in [versioning](../architecture/versioning.md)
  (read-new/write-old, then flip) needs a concrete mechanism.
- How downgrade limits are expressed and enforced.

**Related:** [architecture/versioning.md](../architecture/versioning.md),
`coppice-state`.

## OD-3: Job lifecycle state machine

**Resolved** (2026-07-07): initially by
[ADR 0004](../decisions/0004-job-lifecycle-and-attempts.md); refined the same
day by [ADR 0013](../decisions/0013-job-attempt-allocation-state-machines.md)
(see OD-12) — coarse user-visible job machine, detailed attempt machine with a
`Ready` funding barrier, per-node allocation machine, and explicit abort
semantics with a terminal outcome taxonomy.

**Question.** What is the formal set of job states, the legal transitions between
them, and the owner of each transition?

**Why it matters.** The lifecycle is referenced by the API, scheduler, agent,
reconciler, and events. Ambiguity here causes correctness bugs and inconsistent
UI/observability.

**Considerations.**
- The `JobState` enum in `coppice-core` lists the states; the **transition
  table** (which edges are legal, who commits each) is what's missing.
- Where retries create new attempts vs. reuse identity.
- How cancellation interleaves with in-flight assignment/dispatch.

**Related:** [lifecycle/job-lifecycle.md](../lifecycle/job-lifecycle.md),
`coppice-core::job`.

## OD-4: Quota and priority policy

**Resolved** (2026-07-07): no hard limits; a generic tree of quota entities
with soft quotas, a single scalar cost per job, exponentially decayed usage,
and quota breaches penalizing effective priority —
[ADR 0005](../decisions/0005-cost-based-soft-quotas.md).

**Question.** What is the precise admission-control, queue-ordering, priority,
and fair-share specification?

**Why it matters.** Determines who gets capacity under contention. Must be
replicated where it affects decisions so failover doesn't produce inconsistent
outcomes.

**Considerations.**
- Ownership levels (user, project, org, queue, service account) and how they
  compose.
- Separation of admission control, queue ordering, scheduling priority,
  fair-share, hard limits, burst allowances, and (later) preemption.
- Which accounting state is authoritative/replicated vs. derived.

**Related:** [scheduling/quotas-and-priorities.md](../scheduling/quotas-and-priorities.md),
`coppice-scheduler`.

## OD-5: Reservation and backfilling model

**Resolved** (2026-07-07): initially by
[ADR 0006](../decisions/0006-reservations-and-strict-backfill.md); superseded
the same day by
[ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md)
(see OD-12) — accruing allocations *are* the reservations; the strict
enforced-`max_runtime` backfill rule carries over against projected funding
time.

**Question.** How are large ("whale") jobs given earmarked future capacity, and
how is smaller work safely backfilled around those reservations?

**Why it matters.** Without it, large jobs starve or block throughput. With a
naive version, reservations leak capacity or backfill violates them.

**Considerations.**
- Representation of a reservation in replicated state and its expiry/renewal.
- Backfill safety: guaranteeing backfilled jobs finish before (or don't delay)
  the reserved job.
- Interaction with runtime estimates, which are advisory and uncertain.

**Related:** [scheduling/scheduling-model.md](../scheduling/scheduling-model.md).

## OD-6: Follower read consistency model

**Resolved** (2026-07-07): per-endpoint consistency defaults
(strong / bounded-stale / eventual) with an explicit client override and
staleness surfaced in response metadata —
[ADR 0007](../decisions/0007-per-endpoint-read-consistency.md).

**Question.** Which reads may be served by followers, and with what freshness
guarantee?

**Why it matters.** Affects API scalability and correctness of what clients see.
Serving stale data from followers is cheap but can mislead; strong reads cost a
round-trip to the leader.

**Considerations.**
- Categories from [high-availability](../architecture/high-availability.md):
  strong (read-index/leader-confirmed), stale-tolerant follower reads, and
  eventually-consistent reads from derived stores.
- Which API endpoints fall into which category, and how staleness is surfaced to
  callers.

**Related:** [architecture/high-availability.md](../architecture/high-availability.md),
`coppice-api`.

## OD-7: Event subscription delivery guarantees

**Resolved** (2026-07-07): events derived at apply time with the Raft apply
index as cursor; per-scope total order, at-least-once delivery, bounded
reconnection buffer, gap indication → resync via query —
[ADR 0008](../decisions/0008-event-delivery-guarantees.md).

**Question.** What ordering, gap, and reconnection guarantees does the
event/subscription system provide?

**Why it matters.** Clients (UI, integrations) rely on updates but must be able
to recover from missed events by re-querying authoritative state.

**Considerations.**
- Cursor/sequence semantics and how a client detects it has fallen too far
  behind (gap indication → resync via query).
- Whether the event log is bounded and derived vs. tied to the Raft log.
- Per-scope ordering guarantees (per job, per queue).

**Related:** [architecture/components.md](../architecture/components.md) (Event
and Subscription System).

## OD-8: Agent fencing and reconciliation protocol

**Resolved** (2026-07-07): fencing token `(leader_term, node_epoch)` plus
per-node command sequence; allocation/attempt-scoped idempotency; agent journal
and full ObservedSet diff on restart —
[ADR 0009](../decisions/0009-fencing-and-reconciliation.md).

**Question.** What is the concrete protocol by which agents reject stale-leader
commands and reconcile running containers with coordinator intent?

**Why it matters.** This is the safety boundary against split-brain and
duplicate execution. Getting fencing wrong means jobs run twice or stale leaders
cause harm.

**Considerations.**
- Which epoch/term/fencing token travels on each message, and the exact reject
  rules (see [agent-coordinator](../protocols/agent-coordinator.md)).
- Reconciliation on agent restart: inspect local durable state, report actual
  running set, let the coordinator decide accept/cancel/retry/lost.
- Idempotency keys (job/allocation/attempt/command ids) and dedup windows.

**Related:** [protocols/agent-coordinator.md](../protocols/agent-coordinator.md),
[operations/failure-handling.md](../operations/failure-handling.md),
`coppice-proto::agent`.

## OD-9: Image-cache autonomy vs. central hints

**Resolved** (2026-07-07): agents own eviction absolutely; cache inventory is
observed state used for soft scoring only; central hints are advisory; nothing
cache-related is replicated in v1 —
[ADR 0010](../decisions/0010-image-cache-boundary.md).

**Question.** Where is the line between local agent cache autonomy and central
scheduling hints/policy?

**Why it matters.** Local disk safety must win, but central hints improve
startup latency and registry load. The boundary decides who evicts and how much
cache state enters replicated state.

**Considerations.**
- Agents own eviction under disk pressure; coordinator provides hints/policy.
- Only cache metadata affecting durable scheduling decisions enters replicated
  state; detailed inventory stays observed.
- Optional future features (prefetch, warming, eviction scoring).

**Related:** [scheduling/image-cache.md](../scheduling/image-cache.md).

## OD-10: Container execution security model

**Resolved** (2026-07-07): default-deny posture (no privileged, no host
mounts/network, non-root) with admin-allowlisted exceptions per queue or node
pool; mTLS node identity from day one; secrets deferred —
[ADR 0011](../decisions/0011-container-security-posture.md).

**Question.** What are the security boundaries for executing user containers?

**Why it matters.** User workloads are untrusted code running on shared nodes.
Unclear boundaries are a direct path to node/cluster compromise.

**Considerations.**
- Posture around privileged containers, host mounts, network access, and user
  identity/isolation.
- Secret handling: integration with a secret manager; never exposing secrets in
  logs, events, snapshots, or UI.
- Node identity and mutual authentication between coordinator and agents.

**Related:** [operations/security.md](../operations/security.md).

## OD-11: Data retention policy

**Resolved** (2026-07-07): per-store retention with terminal jobs evicted from
replicated state 72 h after completion via commanded cleanup, and a 90-day SQL
job-history store as a sink —
[ADR 0012](../decisions/0012-data-retention.md).

**Question.** How long are events, metrics, logs, and job history retained, and
where?

**Why it matters.** Drives storage cost, debuggability, and compliance. Needs to
respect the [storage boundaries](../architecture/data-storage-boundaries.md) so
retention choices don't leak high-volume data into replicated state.

**Considerations.**
- Separate retention per store (metrics TSDB, log store, event log, job-history
  store).
- What summarized job history is kept in/near authoritative state vs. an
  analytical store.

**Related:** [architecture/data-storage-boundaries.md](../architecture/data-storage-boundaries.md),
[operations/observability.md](../operations/observability.md).

## OD-12: Abort semantics, partial scheduling, and job groups

**Resolved** (2026-07-07): three linked state machines (job / attempt /
allocation) joined at a `Ready` funding barrier —
[ADR 0013](../decisions/0013-job-attempt-allocation-state-machines.md) and
[ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md),
superseding ADRs 0004 and 0006.

**Question.** How does the lifecycle support (a) aborting a job at any stage
with an explicit, honest terminal record distinguishing abort from OOM,
`max_runtime` breach, or natural exit; (b) whale jobs partially scheduled on
nodes that don't yet have space; and (c) future gang scheduling of job groups?

**Why it matters.** These three pressures determine whether the job state
machine survives contact with real features or needs redesign after clients
depend on it.

**Considerations.**
- Job enum coarse (detail on attempts) vs. mirroring attempt phases.
- Abort race: does the recorded terminal state report what actually stopped
  the job, or does an abort request override the true outcome?
- Whether accruing allocations subsume time-based reservations entirely.
- What the gang-scheduling seam is (a group-scoped readiness barrier) and what
  is deliberately deferred (funded-allocation wait policy for slow peers).

## OD-13: Base score and the exact job-costing formula

**Resolved** (2026-07-08): `base(job)` is the job's Q32.32 priority
multiplier alone, `m(j)`, divided by ADR 0005's ancestor penalty product
plus an additive age term bounded by the replicated decay half-life — no
size term (packing and strict backfill already bias small jobs; a second
term would double-count), no lump-charge smoothing (true-up plus decay
already cover it), and per-entity-memoized penalties keep rescoring
`O(entities)` —
[ADR 0021](../decisions/0021-effective-score-ranking.md).

**Question.** What exactly is `base(job)` in
`effective_score = base(job) / Π penalty(usage_a / quota_a)`, and does the
job-costing formula need refinement alongside it?

**Why it matters.** The quota arithmetic (ADR 0019) fixed the replicated
bookkeeping but not the scoring numerator. Within one quota entity the
ancestor-penalty product is a shared factor that cancels, so relative order
among a user's own queued jobs is decided entirely by `base(job)` and the
FIFO tie-break. The expected behaviour — a later-submitted high-priority job
outranks the same user's earlier low-priority jobs (the "Friday evening
backlog" scenario) — therefore holds only if `base` is monotone in the
requested priority. ADR 0005 defines priority's effect on *cost* (burn
budget faster) but is silent on its effect on *rank*.

**Considerations.**
- `base` presumably a monotone function of the priority multiplier, FIFO
  within a priority level; state and property-test the within-entity
  ordering guarantee next to the quota arithmetic in `coppice-core`.
- Whether `base` should also reflect job size/cost (cheap-job bias for
  backfill already comes from ADR 0006; avoid double-counting).
- Whether the placement-time charge of the full `max_runtime` cost needs
  smoothing for very long jobs (a whale's charge lands as one lump), or
  whether true-up plus decay is sufficient.
- Scheduler data structure: per-entity penalty products with per-entity
  queues ordered by `base`, so rescoring is O(entities touched), not O(jobs).

**Related:** [ADR 0005](../decisions/0005-cost-based-soft-quotas.md),
[ADR 0019](../decisions/0019-deterministic-quota-arithmetic.md),
[scheduling/quotas-and-priorities.md](../scheduling/quotas-and-priorities.md).

## OD-14: Coordinator discovery and control-plane PKI

**Status: Resolved** —
[ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md)
(2026-07-22). Discovery is a pluggable, strictly seed-only trait
(`static`/`dns`/`file`/`ec2-asg`; Consul deferred as just another impl);
the daemon runs one flagless command with derived intent, parks when no
cluster exists, and self-joins via an idempotent loop. First-ever
formation is `coppice coordinator init`, a local-only verb on the
daemon's root-owned Unix socket — no network formation, no formation
token, no resumability: a `formation_complete` marker bounds a fail-stop
over every partial-formation state, and recovery is wipe-and-rerun. PKI
is **cluster-owned**: the root is minted at formation; the key never
enters replicated state — it resides on voter disks plus any promotion
candidate past the key-transfer gate, every recipient root-equivalent,
under an explicit custody invariant — and coordinators and agents
obtain leaves via one token-based
enrollment endpoint with explicit transport trust (pinned cluster CA or
external cert — never TOFU); external PKI is a substitution, not a
requirement. Cluster-minted subjects anchor the machine self-service
membership grant amending ADRs 0022/0023 (one seat per installation
identity); replacing a live voter is the explicit operator verb
`ReplaceVoter{old,new}`, dead voters are removed on the leader's own
replication evidence inside the newcomer's promotion. In-process cert
reload (mtime watch + SIGHUP) lands as planned.

**Question.** (a) Which discovery backends feed the coordinator seed list —
static config (today), DNS, Consul — and is any of them load-bearing beyond
first-dial? (b) Where do coordinator↔coordinator certificates come from in a
templated deployment — externally managed PKI (Vault / cert-manager) with
in-process cert reload, or something the cluster itself issues?

**Why it matters.** ADR 0025 removed the last hand-allocated identity from
coordinator config, so replicas are now template-stampable *except* for
finding each other on first dial and being issued key material. ADR 0016
already commits to "discovery/config cannot hard-code node 1/2/3 forever".
The wrong shape here either adds a hard runtime dependency on an external
system to the consensus core, or bakes in long-lived certs that make
rotation a fleet-wide manual event.

**Considerations.** Authoritative addressing must stay in replicated
membership; discovery may only ever feed seed lists and admin-CLI targets.
DNS is dependency-free and probably sufficient; Consul adds health-checked
entries at the cost of an operational dependency. For PKI, short-lived
Vault-issued leaves require cert reload without restart in the tonic
servers — that reload seam is the only real code decision. Self-join
automation (`coppice coordinator join` driving add-learner/promote itself)
is gated on ADRs 0022/0023 formalizing who may drive membership RPCs.

**Related:** [deployment-story.md](deployment-story.md),
[ADR 0016](../decisions/0016-coordinator-rebuild-learner-join.md),
[ADR 0025](../decisions/0025-self-minted-coordinator-identity.md),
[ADR 0011](../decisions/0011-container-security-posture.md).

## OD-15: Agent enrollment signer and decommission protocol

**Status: half (a) resolved** —
[ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md)
decides the signer: the **cluster-owned CA** (root minted at formation;
CA certificate replicated; the key never in replicated state, residing
on voter disks plus any promotion candidate past the key-transfer gate,
every recipient root-equivalent; the leader signs). Agents share the coordinators' enrollment endpoint,
role-scoped revocable tokens, explicit transport trust anchors, and
renewal-as-revocation-lever; Vault-style external issuance remains a
substitution behind the same `[tls]` paths. **Half (b) — drain and
decommission — remains open.**

**Question.** (a) Who signs agent leaves in the ADR 0011 enrollment flow —
a coordinator-held CA (key on the leader, cert in replicated policy) or an
external signer (Vault) behind the same enrollment endpoint? (b) What is
the graceful scale-in protocol — the drain verb, agent-side SIGTERM drain,
ASG lifecycle-hook integration, and the retention/GC rule for departed
node records?

**Why it matters.** These are the two halves of zero-touch autoscaling that
are genuinely undecided (the rest of the plan is mechanical). Enrollment is
the trust root for the entire agent fleet; the signer choice trades
no-new-infra simplicity against key hygiene and revocation. Scale-in today
relies on the 90 s `DeclareNodeLost` timeout, which kills running work —
fine as a crash backstop, wrong as the *planned* path; and node records
are currently immortal, so a churning ASG grows state forever.

**Considerations.** `SetNodeSchedulable` already exists in the state
machine with no production caller — the drain verb is mostly surface. The
enrollment endpoint's protocol (token → CSR → leaf with CN = typed node
id, renewal under the current cert) is identical for either signer, so the
built-in signer can ship first with Vault as a backend later. Node-record
GC wants an ADR 0012-style retention window. Both surfaces are
authorization-shaped and should follow ADRs 0022/0023.

**Related:** [deployment-story.md](deployment-story.md),
[ADR 0011](../decisions/0011-container-security-posture.md),
[ADR 0009](../decisions/0009-fencing-and-reconciliation.md),
[ADR 0012](../decisions/0012-data-retention.md).

## OD-16: User authentication and principal model

**Resolved** (2026-07-12; decided 2026-07-08): one OIDC issuer per cluster;
principals are IdP subjects with no replicated user records; JWT access
tokens validated offline on every replica; PKCE flows for UI and CLI,
client-credentials for services; operator certificates under the
control-plane trust root as break-glass and the day-0 path —
[ADR 0022](../decisions/0022-oidc-identity-and-authentication.md).

**Question.** What is a principal in Coppice, what credential does the API
validate, how does each client kind (web UI, CLI, headless service) obtain
one, and what is the posture when the IdP is unavailable?

**Why it matters.** Every authorization and audit feature hangs off the
principal model, and the validation path decides whether follower reads
(ADR 0007) stay replica-local or silently acquire an IdP dependency. Jobs
previously had no owner: "abort your own job" was inexpressible.

**Considerations.**
- Replicated user records vs. IdP-owned identity (no local user database).
- Offline JWT validation vs. introspection; revocation latency vs.
  request-path IdP coupling.
- Service identity: IdP client-credentials vs. Coppice-minted tokens (which
  would breach the v1 no-secrets posture).
- Break-glass when the IdP is down: none vs. local secret vs. reusing the
  control-plane trust root for operator client certificates. (The
  provenance of that root — cluster-held vs. external PKI — remains OD-14
  and OD-15.)

**Related:** [operations/security.md](../operations/security.md),
[ADR 0020](../decisions/0020-node-config-vs-replicated-policy.md),
OD-14, OD-15.

## OD-17: Authorization model and enforcement

**Resolved** (2026-07-12; decided 2026-07-08): three built-in roles
(submitter / operator / admin) granted by replicated bindings, optionally
scoped to a quota-entity subtree; jobs record their submitter and owners
always manage their own jobs; reads open to authenticated principals in v1;
API-proposed commands carry an `Actor` and apply re-checks authorization
deterministically as the backstop, making the log the audit trail —
[ADR 0023](../decisions/0023-scoped-role-bindings.md). Coordinator
membership RPCs and enrollment administration are unscoped-admin cluster
verbs, unblocking the self-join automation gated in OD-14/OD-15.

**Question.** How are the verbs in
[operations/security.md](../operations/security.md) granted (roles, scoping,
subjects), and where is authorization enforced given that the API
pre-validates and apply is the deterministic backstop?

**Why it matters.** ADR 0020 requires authorization-shaped configuration to
be replicated policy so replicas cannot disagree on who is an admin;
enforcement location decides whether that property holds by construction or
by discipline. Grant scoping decides whether team-level delegation needs a
second organizational hierarchy next to the quota-entity tree.

**Considerations.**
- Global roles vs. subtree-scoped bindings vs. fully resource-scoped grants.
- Whether commands carry the acting principal (needed for ownership, audit,
  and apply-time checks) and how group claims stay compatible with apply's
  purity.
- API-only enforcement vs. an apply-time re-check (revocation races resolve
  in log order; unauthorized attempts become deterministic rejections).
- Lockout prevention on bindings replacement; delegated binding management
  deferred.

**Related:** [ADR 0005](../decisions/0005-cost-based-soft-quotas.md),
[architecture/command-catalog.md](../architecture/command-catalog.md).
