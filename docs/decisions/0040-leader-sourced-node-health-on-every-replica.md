# 40. Leader-sourced node health on every replica

- **Status:** Accepted
- **Date:** 2026-09-12
- **Amends:** [ADR 0039](0039-best-effort-node-usage-telemetry.md)
  §"In-memory, leader-only, one rolling hour", and *only* its liveness half:
  the marks behind `NodeSummary.last_heartbeat` and node health are now
  fetched from the leader by whichever replica serves the read. The `used`
  numbers that ADR is actually about are still not forwarded, deliberately —
  see below.
- **Builds on:** [ADR 0038](0038-internal-forwarding-of-follower-received-writes.md)
  (the internal coordinator hop, its dial helper, and its one-hop rule; this
  is the same hop in the read direction),
  [ADR 0009](0009-agent-coordinator-protocol.md) (the liveness marks
  themselves, and the health monitor that acts on them)

## Context

GitHub issue #133. `GET /api/v1/nodes`, `GET /api/v1/nodes/{id}` and the
overview's `lost` node count all derive a node's health from the leader's
in-memory liveness marks — the same monotonic last-seen spans the health
monitor measures against the liveness deadline before proposing
`DeclareNodeLost`. Those marks are filled by the agent-session ingestion loop,
which runs only where agent sessions terminate: the leader.

So every other replica had nothing to derive health from, and said so
honestly: `health: "unknown"` and `last_heartbeat: null`, for every node in
the fleet. That was defensible while a client picked a coordinator and stayed
there. It stopped being defensible behind the AWS demo's load balancer
(issue #51), where each request lands on whichever of three coordinators the
balancer picked: the web UI and `coppice node list` show the whole fleet as
`unknown` roughly two thirds of the time, and flip between `unknown` and
`healthy` between polls for no reason a viewer can see. The smoke script had
already been written around it, retrying `GET /api/v1/nodes` up to ten times
hoping to land on the leader and treating the result as evidence when it did.

Health is not a decoration. It is the field an operator looks at to answer "is
my cluster up", the field a runbook says to check, and — as the `lost` count
— the field that says whether capacity is being counted at all. A value that
depends on which replica answered is worse than a coarse one.

ADR 0039 considered read-forwarding for this family of reads and declined it,
in terms worth quoting against: forwarding a dashboard poll to the leader
"would put leader load on every viewer", and "the follower shows no usage" is
correct under that ADR's absence rule. Both clauses are true of usage numbers
and neither survives contact with liveness marks, which is why this ADR splits
them apart rather than reversing that one.

## Decision

A replica that does not lead **fetches the leader's liveness marks** over the
ADR 0038 admin channel and derives health from them, so node health is the
same answer from every coordinator address.

### One additive RPC on the existing hop

`RaftAdminService` gains `FetchNodeLiveness`, alongside the `Forward*` writes
and under the same posture: coordinator-to-coordinator mTLS, the ADR 0037 §7
operator-or-machine gate, a formed cluster, and a matching history stamp. The
leader answers with one entry per node it is tracking — the node id, the wall
stamp of its last actual report (absent for a node it has only granted a grace
window), and the **monotonic** silence since that report or grant.

The span is computed on the leader, before it is encoded. Nothing about a
health verdict therefore depends on two coordinators' clocks agreeing — the
same property the in-process read already had, preserved across a network hop.
What the hop adds is latency: the span a reader sees is as stale as the round
trip. Against a 90 s liveness deadline and a poll loop measured in seconds,
that is noise.

`FetchNodeLivenessResponse` is a oneof of `Marks` and `NotLeader`, not a bare
repeated field, because empty and absent are different facts here: a leader
tracking no nodes has answered, and a replica that is not the leader has not.
A `repeated` field cannot tell those apart.

### Single hop, exactly as the writes are

A receiver that is not the leader answers `NotLeader` and fetches from nobody.
The asking replica then serves health `unknown` — precisely what it served
before this ADR. There is no chain, no retry against a second peer, and no
cache of another replica's answer.

### Every failure degrades to `unknown`, never to an error

This is the rule that makes the change safe, and it is the opposite of the
write path's. A forwarded write that fails must reach the client as a failure,
because the alternative is reporting an outcome nobody knows. A liveness fetch
that fails — no address for the leader, a dial that times out, an RPC error,
a `NotLeader` answer, a mark this build cannot decode — yields an empty map,
which renders as `health: "unknown"` with a null stamp. A health read must
never turn a node list into a 503 because a leader was briefly unreachable.

Each failure is logged at **debug**, not warn. An election, a membership view
a beat behind, and a leader that has just stepped down all reach this path, and
a dashboard polling every few seconds would otherwise produce a torrent of
warnings about a field that degraded exactly as designed.

### A 2 s budget, dial included

The fetch gets its own named budget, a fifth of the write path's 10 s, bounding
the dial as well as the call for the same reason ADR 0038 gives: a blackholed
address leaves a TCP connect hanging for the OS's retry budget, which is
minutes.

Ten seconds is the right number for a client's mutation and the wrong number
for a page load. The fallback answer here is *fine*; spending a viewer's
request on avoiding a degraded field would trade a good answer now for a stale
one later, on a surface that refreshes anyway.

### Usage numbers are still not forwarded

ADR 0039's refusal stands for what it was written about. The two are different
reads:

- **Volume.** A liveness map is one tiny entry per node with no history. A
  usage read is the live sample map *plus* an hour of per-node and cluster
  buckets — the thing that would actually put leader load on every viewer.
- **What the reader does with it.** `used` is a number to watch on a chart,
  and its absence is visibly an absence. Health is a *verdict* — healthy,
  lost, unknown — that operators and runbooks act on, and the surface where
  "it depends which replica you asked" is least tolerable.
- **Where the retained answer lives.** ADR 0039's own escape hatch for usage
  is `/metrics`, where the series is retained anyway. There is no such
  redirection for health: `/metrics` carries no health verdict, and pointing an
  operator at Prometheus to find out whether a node is alive is not an answer.

So `ControlPlane::usage_window()` stays synchronous and leader-local exactly as
ADR 0039 specified, and the liveness marks move out of `UsageSnapshot` into
`ControlPlane::node_liveness()`, which is async and may cross the hop. The
split in the type mirrors the split in the decision.

### One `node_health`, still

The marks reach the read model as an explicit parameter beside the usage
snapshot, and `node_health` remains the single function the node list, the node
detail, and the overview's `lost` count all call. A change that let those three
disagree about a node would be invisible in any one of them.

## Consequences

- Node health, `last_heartbeat`, and the `lost` count are the same from every
  coordinator address. The load-balanced deployment of issue #51 reads
  correctly, and the smoke script's leader-luck retry loop becomes a real
  assertion.
- `GET /api/v1/nodes`, `GET /api/v1/nodes/{id}` and `GET /api/v1/overview` on a
  non-leader now make one internal RPC per request, bounded at 2 s. The read
  reuses a single cached admin channel per leader — redialled only when the
  leader, its address, or the TLS material generation changes, or when the RPC
  fails at the transport — so a dashboard poll costs one round trip rather than
  a TLS handshake. The write path of ADR 0038 keeps dialling per call: it is
  rare, and one less piece of state on the path that mutates the cluster.
- `unknown` is no longer synonymous with "you asked a follower". It now means
  what it says: nothing to judge this node by — inside a new leader's grace
  window, not tracked at all, or the marks could not be fetched. The CLI's
  rendering comment said `unknown` was the only value a coordinator produced;
  that was already stale and is now corrected.
- `ControlPlane` grows a method and `UsageSnapshot` loses a field, which every
  implementation in the tree (and every test double) had to follow. That churn
  is the point: a snapshot that silently carried a leader-only fact into a
  follower's read is what made the bug invisible.
- The admin service now holds the node-liveness map, so the boot path creates
  it and hands the *same* handle to the task runtime that fills it. A runtime
  that constructed its own would compile and leave every follower's node health
  `unknown` — the bug this ADR fixes — so the map travels on
  `BootedCoordinator` rather than being made twice.
- A read that needs the leader is a read that can be slowed by the leader.
  Bounded at 2 s and degrading to the previous answer, the worst case is the
  behaviour this ADR replaces, arriving 2 s later.
- The hop is one more caller on the coordinator admin channel. If more reads
  ever want the same treatment, the question to ask each time is the one this
  ADR answered for liveness and ADR 0039 answered for usage: is the fact small,
  and is it a verdict someone acts on?
