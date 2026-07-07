# Main Components

## External API Layer

The external API is the user-facing entry point for job submission, job
abort, status queries, event subscriptions, administrative actions, and
UI support.

It should support authentication through SSO for user-facing access.
Authorization should be enforced at the API layer and again at the command
validation layer where appropriate.

The API layer may run on every coordinator replica. Read-only requests may be
served by followers when sufficiently fresh reads are acceptable. Mutating
requests must ultimately be routed to the current Raft leader or internally
forwarded to it.

The API should expose operations such as:

- Submit job.
- Abort job.
- Retry job.
- Query job status.
- Query node status.
- Query queue status.
- Query quota usage.
- Stream job or queue updates.
- Administer nodes, queues, projects, users, and policy configuration.

The API should be designed around durable state transitions rather than direct
imperative manipulation of workers. For example, aborting a job should commit
a desired state transition; agents then observe and enforce the updated desired
state.

## Coordinator Replicas

Coordinator replicas form the control plane. They maintain replicated
authoritative state using Raft. One coordinator is the leader at any given time.
The leader accepts commands, appends them to the replicated log, applies
committed commands to the state machine, and emits derived work to other
subsystems.

Followers replicate the log, apply committed entries, and may serve selected
read requests depending on consistency requirements.

Coordinator responsibilities include:

- Maintaining authoritative scheduler state.
- Participating in Raft consensus.
- Applying state-machine commands.
- Handling API requests or forwarding them to the leader.
- Receiving agent heartbeats and status reports.
- Publishing state-change notifications.
- Managing leases or epochs for safe dispatch.
- Coordinating scheduler workers.
- Persisting snapshots and recovering state after restart.

The coordinator should be internally structured so that consensus and
state-machine application remain isolated from expensive computation. Raft
application should be deterministic, bounded, and reliable. CPU-intensive
scheduling should run asynchronously using snapshots or read-only views of
state, then submit proposed scheduling decisions back through Raft as commands.

## Raft-Replicated State Store

Raft is used to replicate the authoritative control-plane state.

The replicated state machine should contain semantic system state, not arbitrary
implementation details. It should store the facts required to recover scheduling
correctness after failover.

Examples of state that should be replicated:

- Job definitions.
- Job lifecycle state.
- Queue membership.
- Priority and quota accounting state.
- Node membership and schedulability state.
- Allocations, including accruing allocations (the reservation mechanism —
  see [ADR 0014](../decisions/0014-accruing-allocations-replace-reservations.md)).
- Placement decisions.
- Assignment epochs or fencing tokens.
- Durable policy configuration.
- Durable image-cache metadata if it affects scheduling decisions.
- Important progress or runtime estimates when they influence scheduling
  fairness or allocation funding.

Examples of state that generally should not be replicated directly:

- High-frequency CPU, memory, disk, and I/O samples.
- Raw telemetry streams.
- Detailed logs.
- Ephemeral scheduler scratch state.
- Recomputable indexes.
- Transient RPC connection state.
- Large event buffers.
- Per-container runtime details that are only useful for observability.

The Raft log should contain commands that mutate the authoritative state. It
should not contain nondeterministic scheduler computation. Scheduling workers
may compute proposed placements, but the final accepted placement must be
committed through the replicated log.

## Scheduler Engine

The scheduler engine is responsible for turning queued jobs into placement
decisions.

It should not directly mutate authoritative state. Instead, it should operate on
a consistent snapshot or versioned view of the cluster state, compute a batch of
proposed assignments, accruing allocations, and revocations, and submit those
proposals back to the coordinator leader for validation and commitment.

Its detailed responsibilities and operating model are described in
[../scheduling/scheduling-model.md](../scheduling/scheduling-model.md).

## Node Agent

Each compute node runs an agent responsible for local execution and reporting.

The agent should be treated as an eventually consistent executor of coordinator
intent. It should not be the source of truth for global scheduling state, but it
is the source of truth for local observed runtime state.

Agent responsibilities include:

- Registering with the coordinator.
- Advertising node resources and labels.
- Reporting health and capacity.
- Pulling or receiving assigned work.
- Starting containers.
- Stopping containers.
- Enforcing local resource limits.
- Reporting job lifecycle transitions.
- Reporting actual resource usage.
- Reporting image-cache state.
- Managing local image cache under disk pressure.
- Recovering local state after restart.
- Reconciling running containers with coordinator intent.
- Handling coordinator failover safely.

The agent must be robust against duplicated commands, stale leaders, network
partitions, partial failures, and process restarts. Commands from the
coordinator should include epochs, assignment identifiers, or fencing tokens so
that stale commands can be rejected or ignored.

The agent should maintain enough local durable state to reconcile containers
after restart, but global correctness should come from the coordinator's
replicated state. The message-level details are in
[../protocols/agent-coordinator.md](../protocols/agent-coordinator.md).

## Event and Subscription System

Users and internal systems may subscribe to updates for subsets of jobs, queues,
projects, or nodes.

The event system should be derived from committed state changes. It should not
publish uncommitted Raft proposals as authoritative updates.

A practical model is:

- Raft commits state changes.
- The state machine emits internal change notifications.
- An event fanout layer filters notifications by subscription.
- Clients receive ordered updates within a defined scope.
- Clients can reconnect using a cursor, version, or sequence number.
- If the client falls too far behind, it receives a gap indication and must
  resynchronize through a query.

The event system should not require the Raft log itself to be retained as a
user-facing event stream indefinitely. It may use a separate bounded event log,
derived event store, or pub/sub layer.

The API should distinguish between authoritative state and notification
delivery. Lost streaming updates must be recoverable by re-querying current
state.

The concrete guarantees — Raft apply index as the cursor, per-scope total
order, at-least-once delivery, a bounded reconnection buffer, and
gap-indication → resync — were decided in
[ADR 0008](../decisions/0008-event-delivery-guarantees.md).

## Web UI

The web UI should be built on the same public API used by other clients.

Primary UI capabilities should include:

- Job submission.
- Job list and filtering.
- Job details and lifecycle history.
- Live updates for selected jobs.
- Queue and quota views.
- Node health and resource views.
- Scheduler diagnostics.
- Administrative controls.
- Failure and retry visibility.
- Audit history for important user actions.

The UI should be designed for operational debugging, not only for happy-path job
monitoring. It should help answer why a job is pending, why it was placed where
it was, why it failed, and what policy or quota decision affected it.
