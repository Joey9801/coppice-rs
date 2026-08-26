# 38. Internal forwarding of follower-received client writes

- **Status:** Accepted
- **Date:** 2026-08-26
- **Amends:** [ADR 0031](0031-http-api-surface.md) (writes on a follower
  answered `NOT_LEADER`/421 with an always-empty `Coppice-Leader` hint;
  this closes the "internal forwarding" half of the follow-up the ADR
  named explicitly)
- **Builds on:** [ADR 0037](0037-coordinator-discovery-and-self-converging-membership.md)
  §4 (the coordinator mTLS admin channel, and the follower `/enroll`
  proxy this generalizes), [ADR 0026](0026-client-minted-job-ids-idempotent-submission.md)
  (client-minted ids make submission and quota-entity upsert idempotent
  under retry), [ADR 0023](0023-scoped-role-bindings.md) (scoped role
  bindings; the apply-time re-check stays the authority a forwarded write
  defers to)

## Context

Every replica runs the full `ControlPlane` (`crates/coppice-coordinator/src/tasks/api_server.rs`).
Reads are served by every replica, but the three client writes —
`SubmitJob`, `AbortJob`, `ConfigureQuotaEntity` — must be proposed by the
leader. A write that lands on a follower today gets
`ConsensusError::NotLeader { leader }` back from `Consensus::propose`,
mapped straight to `ApiError::NotLeader { leader_hint: None }` and
rendered as HTTP 421 with an empty `Coppice-Leader` header. The hint is
empty because it cannot be filled: raft membership records only the
peer-plane (raft) dial address of the leader, never a client-API address,
so a follower knows *who* leads by raft `CoordinatorId` but has nothing
dialable to hand a client. ADR 0031 named the fix as an open follow-up —
"advertising client addresses through membership — or internal
forwarding" — and left both options compatible with the existing
contract.

A client that cannot write through whichever coordinator it happens to be
pointed at breaks the plain expectation that any coordinator address
serves the API, reads and writes alike, and pushes leader-tracking into
every client and load balancer that talks to the cluster. That is the
acceptance criterion this ADR closes (GitHub issue #41).

## Decision

The follower forwards the write internally instead of redirecting.

### The transport is the existing admin channel, generalized

Forwarding rides the coordinator-to-coordinator mTLS admin channel
(ADR 0037 §4) — the same channel, and the same `admin_channel` dial
helper, the follower's `/enroll` proxy already uses ("A follower
receiving an enrollment request proxies it internally to the leader over
the mTLS admin channel — it never redirects a client carrying a token,"
ADR 0037 §4). Three additive RPCs join `RaftAdminService`:
`ForwardSubmitJob`, `ForwardAbortJob`, `ForwardConfigureQuotaEntity`.
Generalizing the enroll proxy rather than inventing a second internal
transport is the point: the trust root, the machine-vs-operator
authorization matrix, and the history-id stamping convention are already
there and need no new design.

### Forwarding happens at the control-plane level, never as a pre-built command

A forwarded request carries the DTO-shaped write — the same request a
direct client would send — never a pre-encoded `coppice_state::Command`.
The leader re-runs the full write path it would have run for a request
that arrived on its own listener: shape validation, the replicated
priority-multiplier lookup against *its own* applied view, its own
timestamp stamp, and the propose. A follower's applied view can be
stale, or its clock skewed; a forwarded pre-built command would smuggle
one replica's reading of replicated policy — or its clock — into the
raft log under the leader's name. Forwarding a request, not a decision,
keeps "the leader proposes what the leader itself would have proposed"
true regardless of which replica the client happened to reach.

The follower's own share of that path stops at the state-independent
checks — a missing command, an empty entrypoint override, an
out-of-range `max_runtime_seconds` — whose verdict is identical on every
replica, so refusing them locally costs a hop and misleads nobody.
Anything that reads applied state is deferred to the leader outright: a
follower behind a policy update that added a priority class would find
no multiplier for it and refuse, as `INVALID_ARGUMENT`, a submission the
leader would accept. A lagging replica must never veto a write on the
strength of a view it knows is behind, so the write path consults the
leadership status watch before the multiplier lookup and forwards
instead. Both stale readings are safe: a follower that still believes it
leads falls through to the propose, which reports `NotLeader` and
forwards one step later; the leader that briefly believes it follows
spends one self-directed hop, which the single-hop rule below already
covers.

### Single hop, always

The receiving coordinator never re-forwards. If it turns out not to be
the leader — its own view of leadership was stale, or leadership moved
between the follower's dial and the propose — it returns the ordinary
`NotLeader` outcome to the *originating follower*, which surfaces the
usual 421 to the client. There is no chain, no loop, and no forwarding
budget to reason about: a request crosses the admin channel at most
once, ever.

### A forwarding failure is never a fabricated success or a fabricated failure

A dial failure, a proxy timeout (10s, matching the enroll proxy's
`PROXY_TIMEOUT`), or a connection break after the request was sent all
resolve to the retriable `UNAVAILABLE` (503) — the same answer a local
propose timeout already produces, and honest about the outcome being
genuinely *unknown*: the write may have committed on the leader before
the connection broke. The 10s bounds the dial as well as the call,
because a leader address that blackholes packets would otherwise hold
the client for the kernel's connect budget rather than this one; a dial
that fails or times out says so in its own words, since nothing left
this replica and the write is therefore known not to have committed. What makes that safe to retry is already built:
ADR 0026 gives `SubmitJob` and `ConfigureQuotaEntity` client-minted ids,
so a retry of the identical request lands on the same job or entity as
an accepted no-op rather than a duplicate, and `AbortJob` is naturally
idempotent (`abort_requested` is a one-way flag). Forwarding therefore
adds no new idempotency obligation of its own — it inherits the one the
write path already had, unchanged by which replica the client happened
to reach first.

### 421 stays the fallback, for what forwarding cannot cover

Two cases still answer `NOT_LEADER`/421, unchanged: no leader is known
locally (an election is in progress), or the known leader has no address
in this replica's membership view. The `Coppice-Leader` hint stays
empty for exactly the reason it was empty before — there is still no
client-API address recorded anywhere in membership — and this ADR
deliberately does not add one. With forwarding covering the ordinary
case, the redirect becomes a degraded fallback rather than the normal
path, so advertising client addresses through membership would be new
machinery serving a case that has stopped mattering.

### Authorization: the hop is trusted, not privileged

The forwarded RPC is authenticated as a coordinator machine (or
operator) leaf presenting a chain-valid client certificate over the
admin listener, exactly like the enroll proxy — a client cannot reach
these RPCs directly. Client-side authorization is unchanged by this ADR:
the write path carries no `Actor` today (`configure_quota_entity`'s "No
authz — matching the existing submit_job/abort_job precedent (ADR 0023
is a separate subsystem)" note in `tasks/api_server.rs` still applies
verbatim, on the leader as on a direct call), and the apply-time
re-check ADR 0023 describes stays the authority once it lands.

What this ADR does commit to is the constraint on the shape that
enforcement takes: forwarding must never become a way to launder an
unauthenticated request into an authorized one. When per-request
authentication lands on the client listener, a follower authenticates
the client exactly as the leader would have, *before* it forwards, and
then asserts the already-authenticated `Actor` alongside the request
across this hop — trusted because the channel is mutually authenticated
coordinator mTLS, and re-checked at apply regardless. What must never
happen is a leader accepting a forwarded write it would have refused
directly, on nothing but a peer's say-so.

### Rejections relay across the hop, byte-identical

A committed-and-refused apply on the leader (`ApiError::Rejected`) must
still reach the client as `REJECTED`/409, unchanged. `RejectionReason`
has no proto encoding of its own, so the leader relays the *rendered*
reason text and the follower surfaces it under the same error code; the
JSON body a client sees is byte-identical to what a direct rejection on
the leader would have produced. This is the one place forwarding is
lossy on the wire between coordinators — internally, an
`ApiError::Rejected(RejectionReason)` becomes a string-carrying variant
for the trip across the admin channel — but ADR 0031's client-facing
error contract does not change: `code`, HTTP status, and message shape
are exactly what a direct write would have returned.

## Consequences

- Any coordinator address now serves the full API, reads and writes
  alike. Clients and load balancers stop needing leader affinity or
  redirect-following logic to write successfully — the acceptance
  criterion of GitHub issue #41.
- A write that lands on a follower now costs one extra internal hop
  before it reaches the leader, bounded by the 10s proxy timeout; the
  read path, which already load-balances across replicas (ADR 0007,
  ADR 0034), is untouched.
- The leader still concentrates all write fan-in, exactly as it already
  did through raft — forwarding changes where a write enters the
  cluster, not where it is decided.
- The 421 path becomes rare (election windows and membership gaps only),
  which makes it the path most likely to rot silently; it stays covered
  by tests rather than left to be rediscovered by an operator during an
  election.
- `ConsensusError::NotLeader`'s `leader` field, previously read only for
  a log line, is now load-bearing: it is what a follower resolves an
  admin-channel address from before it forwards.
- The seam lives in `crates/coppice-coordinator/src/tasks/api_server.rs`
  (a `LeaderWrites` trait a follower calls instead of proposing locally),
  its mTLS implementation in `crates/coppice-coordinator/src/clientwrite.rs`,
  and the leader-side handlers alongside the rest of `RaftAdminService`
  in `crates/coppice-coordinator/src/admin.rs`.
