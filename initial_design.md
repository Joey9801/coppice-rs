# Batch Job Scheduler High-Level Design

## 1. Purpose

This system is a distributed batch job scheduler for running containerized workloads across a fleet of compute nodes. Users submit jobs as Docker images with resource requirements, placement constraints, priorities, quotas, and runtime metadata. The system schedules those jobs onto nodes, supervises execution, tracks state, exposes APIs and streaming updates, and remains available despite coordinator failures.

The design targets approximately:

* 1,000 compute nodes.
* Up to 100 concurrent jobs per node.
* Approximately 1 million queued jobs.
* Job durations from roughly 1 minute to 1 day.
* Multi-resource scheduling across CPU, memory, disk, and future resource types such as GPUs.
* High-availability control plane using a replicated consensus system.
* Strong observability through metrics, logs, events, and audit trails.

The scheduler is intended to be a batch-oriented system, not a low-latency serverless platform. Throughput, fairness, correctness, debuggability, and failure recovery are more important than millisecond-level scheduling latency.

## 2. Core Responsibilities

The system has several primary responsibilities.

It accepts job submissions from users and services, validates them, records them durably, and exposes their lifecycle state.

It maintains an authoritative view of cluster state, including jobs, nodes, allocations, resource reservations, quotas, and relevant scheduling metadata.

It makes placement decisions using a scheduler that understands resource availability, hard constraints, soft preferences, quotas, priorities, affinity, anti-affinity, image locality, and other policies.

It dispatches work to node agents, supervises execution indirectly through those agents, and reconciles observed state with intended state.

It monitors actual resource usage and job progress where available, while keeping high-volume telemetry out of the strongly replicated control-plane state unless it is semantically important.

It supports push-style subscriptions for clients interested in state changes for a subset of jobs.

It provides a web UI and API for submission, monitoring, administration, debugging, and operational control.

It remains available through coordinator replication and leader failover.

## 3. Main Components

### 3.1 External API Layer

The external API is the user-facing entry point for job submission, job cancellation, status queries, event subscriptions, administrative actions, and UI support.

It should support authentication through SSO for user-facing access. Authorization should be enforced at the API layer and again at the command validation layer where appropriate.

The API layer may run on every coordinator replica. Read-only requests may be served by followers when sufficiently fresh reads are acceptable. Mutating requests must ultimately be routed to the current Raft leader or internally forwarded to it.

The API should expose operations such as:

* Submit job.
* Cancel job.
* Retry job.
* Query job status.
* Query node status.
* Query queue status.
* Query quota usage.
* Stream job or queue updates.
* Administer nodes, queues, projects, users, and policy configuration.

The API should be designed around durable state transitions rather than direct imperative manipulation of workers. For example, cancelling a job should commit a desired state transition; agents then observe and enforce the updated desired state.

### 3.2 Coordinator Replicas

Coordinator replicas form the control plane. They maintain replicated authoritative state using Raft. One coordinator is the leader at any given time. The leader accepts commands, appends them to the replicated log, applies committed commands to the state machine, and emits derived work to other subsystems.

Followers replicate the log, apply committed entries, and may serve selected read requests depending on consistency requirements.

Coordinator responsibilities include:

* Maintaining authoritative scheduler state.
* Participating in Raft consensus.
* Applying state-machine commands.
* Handling API requests or forwarding them to the leader.
* Receiving agent heartbeats and status reports.
* Publishing state-change notifications.
* Managing leases or epochs for safe dispatch.
* Coordinating scheduler workers.
* Persisting snapshots and recovering state after restart.

The coordinator should be internally structured so that consensus and state-machine application remain isolated from expensive computation. Raft application should be deterministic, bounded, and reliable. CPU-intensive scheduling should run asynchronously using snapshots or read-only views of state, then submit proposed scheduling decisions back through Raft as commands.

### 3.3 Raft-Replicated State Store

Raft is used to replicate the authoritative control-plane state.

The replicated state machine should contain semantic system state, not arbitrary implementation details. It should store the facts required to recover scheduling correctness after failover.

Examples of state that should be replicated:

* Job definitions.
* Job lifecycle state.
* Queue membership.
* Priority and quota accounting state.
* Node membership and schedulability state.
* Allocations and reservations.
* Placement decisions.
* Assignment epochs or fencing tokens.
* Durable policy configuration.
* Durable image-cache metadata if it affects scheduling decisions.
* Important progress or runtime estimates when they influence scheduling fairness or reservations.

Examples of state that generally should not be replicated directly:

* High-frequency CPU, memory, disk, and I/O samples.
* Raw telemetry streams.
* Detailed logs.
* Ephemeral scheduler scratch state.
* Recomputable indexes.
* Transient RPC connection state.
* Large event buffers.
* Per-container runtime details that are only useful for observability.

The Raft log should contain commands that mutate the authoritative state. It should not contain nondeterministic scheduler computation. Scheduling workers may compute proposed placements, but the final accepted placement must be committed through the replicated log.

### 3.4 Scheduler Engine

The scheduler engine is responsible for turning queued jobs into placement decisions.

It should not directly mutate authoritative state. Instead, it should operate on a consistent snapshot or versioned view of the cluster state, compute a batch of proposed assignments or reservations, and submit those proposals back to the coordinator leader for validation and commitment.

The scheduler must handle:

* Multi-dimensional resource bin packing.
* Hard placement constraints.
* Soft placement preferences.
* Priority ordering.
* Quota and fairness.
* Affinity and anti-affinity.
* Image locality and cache pressure.
* Large “whale” jobs requiring significant fractions of nodes.
* Starvation avoidance.
* Backfilling smaller jobs without permanently blocking larger jobs.
* Dynamic node availability.
* Jobs with uncertain runtime.

The scheduler should be designed as an asynchronous subsystem. Scheduling can be CPU-intensive and should not block Raft application, API handling, or agent heartbeat processing.

A useful model is:

1. Maintain an authoritative queue of pending work.
2. Select candidate jobs according to priority, fairness, and quota policy.
3. Classify jobs into ordinary jobs, constrained jobs, and large jobs where useful.
4. Compute feasible placements against a snapshot of cluster state.
5. Use reservations or earmarked future capacity for large jobs that cannot run immediately but must not be starved.
6. Backfill around reservations when safe.
7. Submit a batch of proposed placements and reservations for atomic validation.
8. Recompute when the proposal conflicts with newer committed state.

The scheduler should expect proposals to fail validation due to concurrent changes, node loss, job cancellation, quota updates, or leader changes. Failed proposals are normal and should trigger recomputation, not exceptional control flow.

### 3.5 Node Agent

Each compute node runs an agent responsible for local execution and reporting.

The agent should be treated as an eventually consistent executor of coordinator intent. It should not be the source of truth for global scheduling state, but it is the source of truth for local observed runtime state.

Agent responsibilities include:

* Registering with the coordinator.
* Advertising node resources and labels.
* Reporting health and capacity.
* Pulling or receiving assigned work.
* Starting containers.
* Stopping containers.
* Enforcing local resource limits.
* Reporting job lifecycle transitions.
* Reporting actual resource usage.
* Reporting image-cache state.
* Managing local image cache under disk pressure.
* Recovering local state after restart.
* Reconciling running containers with coordinator intent.
* Handling coordinator failover safely.

The agent must be robust against duplicated commands, stale leaders, network partitions, partial failures, and process restarts. Commands from the coordinator should include epochs, assignment identifiers, or fencing tokens so that stale commands can be rejected or ignored.

The agent should maintain enough local durable state to reconcile containers after restart, but global correctness should come from the coordinator’s replicated state.

### 3.6 Event and Subscription System

Users and internal systems may subscribe to updates for subsets of jobs, queues, projects, or nodes.

The event system should be derived from committed state changes. It should not publish uncommitted Raft proposals as authoritative updates.

A practical model is:

* Raft commits state changes.
* The state machine emits internal change notifications.
* An event fanout layer filters notifications by subscription.
* Clients receive ordered updates within a defined scope.
* Clients can reconnect using a cursor, version, or sequence number.
* If the client falls too far behind, it receives a gap indication and must resynchronize through a query.

The event system should not require the Raft log itself to be retained as a user-facing event stream indefinitely. It may use a separate bounded event log, derived event store, or pub/sub layer.

The API should distinguish between authoritative state and notification delivery. Lost streaming updates must be recoverable by re-querying current state.

### 3.7 Web UI

The web UI should be built on the same public API used by other clients.

Primary UI capabilities should include:

* Job submission.
* Job list and filtering.
* Job details and lifecycle history.
* Live updates for selected jobs.
* Queue and quota views.
* Node health and resource views.
* Scheduler diagnostics.
* Administrative controls.
* Failure and retry visibility.
* Audit history for important user actions.

The UI should be designed for operational debugging, not only for happy-path job monitoring. It should help answer why a job is pending, why it was placed where it was, why it failed, and what policy or quota decision affected it.

## 4. State Model

The system should be explicit about desired state, observed state, and derived state.

Desired state is what the control plane intends to happen. Examples include submitted jobs, cancellations, assignments, reservations, node drain requests, and policy configuration.

Observed state is what agents and monitoring systems report. Examples include running containers, exited containers, resource usage, health checks, and image cache contents.

Derived state is recomputable from desired and observed state. Examples include indexes, queue projections, scheduling candidate sets, UI aggregates, and many metrics.

Only durable semantic state required for correctness should be stored in the Raft state machine. Derived state should be rebuilt from snapshots or committed state where possible.

## 5. Job Lifecycle

A representative job lifecycle is:

* Submitted.
* Accepted.
* Queued.
* Reserved, if future capacity is being earmarked.
* Assigned.
* Dispatching.
* Running.
* Completing.
* Succeeded or failed.
* Retrying, if policy allows.
* Cancelled, if requested before completion.

The exact lifecycle should be carefully defined so that every transition has a clear owner.

User-facing commands may request transitions, such as submit, cancel, or retry.

The scheduler owns transitions from queued to reserved or assigned.

The coordinator owns commitment of those transitions.

The agent owns local execution and reports observed transitions such as started, exited, failed to pull image, or lost container.

The reconciler resolves discrepancies between desired state and observed state.

## 6. Scheduling Model

Scheduling should be treated as a policy-driven optimization process with correctness constraints.

Hard constraints must never be violated. Examples include:

* Required resource capacity.
* Required node labels.
* Required CPU architecture.
* Required GPU type.
* Required isolation properties.
* Hard affinity or anti-affinity.
* Node drain or maintenance state.
* User, project, or queue restrictions.

Soft constraints influence scoring but may be violated when necessary. Examples include:

* CPU brand preference.
* Image locality.
* Spreading or packing preferences.
* Preferred zones or racks.
* Preferred co-location.
* Cache warmth.
* Historical reliability.

The scheduler should support extensible resource dimensions. CPU, memory, and disk should be first-class from the start, but the representation should allow future scalar or structured resources such as GPUs, accelerators, licenses, NUMA-local resources, or special devices.

Bin packing should be heuristic. Full optimal packing is not practical at the target scale. The scheduler should use a combination of candidate pruning, scoring, batching, and incremental recomputation.

Large jobs require special care. A strict single-job-at-a-time admission loop can allow a large unschedulable job to block throughput. Conversely, ignoring the large job allows smaller jobs to continuously consume capacity and starve it. The design should support reservations or earmarked future capacity so that large jobs can make progress while safe backfilling continues around them.

Runtime estimates may come from several sources:

* User-provided maximum runtime.
* Historical runtime for similar jobs.
* Image, command, queue, project, or user history.
* Explicit user-provided estimate.
* Agent-side progress reports.
* Job self-reporting through a controlled progress or ETA channel.
* Conservative defaults when no better signal exists.

Runtime estimates are advisory unless tied to explicit policy such as maximum runtime enforcement. Persist only the estimates and progress signals that affect durable scheduling decisions, fairness, reservations, or user-visible semantics.

## 7. Quotas and Priorities

Quota and priority management should be built into the scheduler rather than bolted on later.

The system should support multiple levels of ownership, such as user, project, organization, queue, or service account.

The design should distinguish between:

* Admission control.
* Queue ordering.
* Scheduling priority.
* Fair-share allocation.
* Hard resource limits.
* Burst allowances.
* Preemption policy, if added later.

Quota accounting should be replicated when it affects scheduling decisions. Recomputable projections may be derived, but the committed state must be sufficient to avoid inconsistent decisions after failover.

The scheduler should be able to explain why a job is pending due to quota, priority, constraints, resource shortage, reservation, or policy.

## 8. High Availability Model

The coordinator control plane should use Raft.

Raft provides a replicated log, leader election, and a deterministic replicated state machine. The system should be designed around that model.

Only the leader should accept authoritative writes. Followers may receive client requests, but they should either redirect to the leader, proxy to the leader, or reject with leader information.

Read paths can be divided into categories:

* Strong reads requiring confirmation from the leader or a read-index style mechanism.
* Stale-tolerant reads served from followers.
* UI or observability reads from derived stores where eventual consistency is acceptable.

The Raft state machine must be deterministic. Given the same sequence of committed commands, every replica must arrive at the same state.

Therefore, the state machine should avoid:

* Wall-clock-dependent decisions during application.
* Randomness during application.
* Network calls during application.
* Expensive scheduling computation during application.
* Dependence on local machine state.
* Iteration over unordered maps where ordering affects results.
* Version-dependent behavior that changes the meaning of old log entries.

Commands should carry enough information for deterministic application. For example, a scheduling command should say “assign these jobs to these nodes under this expected state version,” not “run the scheduler now.”

The leader should use epochs or terms to fence interactions with agents. Agents must reject stale coordinator commands from old leaders.

The system should support snapshots so new or recovering coordinators do not need to replay an unbounded log.

## 9. State-Machine Evolution and Versioning

The replicated state model must be designed for evolution.

Old log entries may be replayed by newer binaries. Snapshots may be read by newer binaries. During rolling upgrades, different coordinator replicas may briefly run different versions.

The system should use explicit versioning for:

* Command formats.
* Snapshot formats.
* Durable state schemas.
* Policy definitions.
* Feature gates.
* Agent protocol compatibility.

Backward-compatible changes are the safest. Examples include adding optional fields, adding new command types not emitted until all replicas support them, or adding derived indexes.

Riskier changes require migration planning. Examples include changing command semantics, changing scheduler policy in a way that affects existing reservations, changing quota accounting, or changing job lifecycle meaning.

A useful upgrade strategy is:

1. Deploy code that can read old and new formats but still writes old format.
2. Confirm the whole cluster is upgraded.
3. Enable a feature gate or cluster-version bump through Raft.
4. Begin writing the new format or using the new semantics.
5. Keep downgrade limitations explicit.

Rollback is not always possible. If a new version writes log entries or snapshots that the old version cannot understand, rolling back to the old binary may be unsafe or impossible without a forward-fix. To preserve rollback capability, the system must avoid enabling irreversible format or semantic changes until it is intentionally committed to them.

Bad scheduler behavior is easier to roll back than bad state-machine format changes if placement policy is kept outside the deterministic application path. However, any committed placements, reservations, or quota changes remain part of history and must be corrected through new commands rather than by rewriting the log.

## 10. Agent-Coordinator Protocol

The agent-coordinator protocol should be designed around reconciliation and idempotency.

Coordinator-to-agent messages may include:

* Desired assignments.
* Start job command.
* Stop job command.
* Drain instruction.
* Cache preparation request.
* Cache eviction hint.
* Health or configuration update.

Agent-to-coordinator messages may include:

* Heartbeats.
* Resource inventory.
* Running job set.
* Job state transitions.
* Exit status.
* Resource usage summaries.
* Image-cache inventory.
* Local errors.
* Node health signals.

Messages should include identifiers that make retries safe:

* Job ID.
* Allocation ID.
* Attempt ID.
* Coordinator term or epoch.
* Node ID.
* Command ID or sequence number.
* Desired-state version where useful.

The protocol should tolerate duplicate, delayed, and reordered messages. The coordinator should not assume that a sent command was executed until the agent reports observed state. The agent should not assume a command is current unless it passes epoch and identity checks.

## 11. Resource Monitoring

The system should monitor both requested and actual resource usage.

Requested resources are used for scheduling and isolation.

Actual usage is used for:

* Observability.
* Debugging.
* Historical estimation.
* Detecting over- or under-requesting.
* Future scheduling improvements.
* Policy enforcement where appropriate.

High-frequency raw telemetry should go to a metrics or time-series system, not Raft.

The replicated state may store summarized or semantically meaningful observations, such as:

* Job started at time.
* Job completed at time.
* Exit code.
* Final status.
* Peak resource summaries, if needed.
* Runtime estimate updates used by scheduling.
* Progress milestones used for reservation decisions.

Prometheus metrics should be emitted by coordinators, agents, scheduler workers, API servers, and event delivery components.

Important metric categories include:

* Queue depth by project, queue, priority, and state.
* Scheduling latency.
* Time pending by reason.
* Placement attempts and failures.
* Resource utilization by node and cluster.
* Requested versus actual usage.
* Agent heartbeat health.
* Raft leader changes, commit latency, apply latency, and snapshot metrics.
* API latency and error rates.
* Event subscription counts and lag.
* Image pull latency and cache hit rate.
* Job success, failure, retry, and cancellation rates.

## 12. Image Cache Management

Image caching should be part of scheduling policy but not dominate correctness.

Agents should report local image-cache inventory and disk pressure. The scheduler can use this as a soft placement preference to improve startup latency and reduce registry load.

Image cache state may be stale. The scheduler should treat cache hits as an optimization, not as a hard guarantee unless the system explicitly supports pre-pulled image requirements.

Agents should own local cache eviction under disk pressure. The coordinator may provide hints or policy, but local safety must come first.

For intelligent caching, the system may later add:

* Predictive prefetching.
* Queue-aware image warming.
* Project-specific cache policy.
* Eviction scoring based on recency, frequency, size, and expected demand.
* Registry rate-limit awareness.

Only cache metadata that affects durable scheduling or policy decisions needs to enter the replicated state. Detailed cache contents can be reported periodically and treated as observed state.

## 13. Failure Handling

The design should assume failures are normal.

Coordinator leader failure should trigger Raft election. A new leader reconstructs authoritative state from the replicated log and snapshot, resumes communication with agents, fences stale leaders using terms or epochs, and reconciles desired state against observed agent reports.

Agent failure should cause the node to become unhealthy after missed heartbeats. Running jobs on that node may be marked unknown, lost, retryable, or failed depending on policy and whether the agent later reconnects with durable local state.

Network partitions should be handled conservatively. A coordinator outside the Raft majority cannot commit writes. An agent receiving commands from stale or minority coordinators must reject them using epoch checks.

Job execution failure should be classified. Examples include image pull failure, container start failure, runtime nonzero exit, resource exhaustion, node loss, timeout, cancellation, and internal scheduler or agent error. Retry policy should be explicit and should avoid retry storms.

Node restart should trigger local reconciliation. The agent should inspect existing containers and local durable state, then report what is actually running. The coordinator decides whether to accept, cancel, retry, or mark jobs lost.

## 14. Security Model

User-facing access should use SSO.

Internal communication between coordinators and agents should be authenticated and encrypted. Agents should have node identity. Coordinators should authenticate to agents, and agents should reject unauthenticated or stale commands.

Authorization should cover:

* Submitting jobs.
* Viewing jobs.
* Cancelling or retrying jobs.
* Accessing logs or artifacts.
* Administering queues and quotas.
* Draining or disabling nodes.
* Changing policy.
* Viewing node-level information.

Job execution should use container isolation, resource limits, and a clearly defined security posture around privileged containers, host mounts, network access, secrets, and user identity.

Secrets should not be stored casually in job definitions. If jobs need secrets, the system should integrate with a secret-management mechanism and avoid exposing secret values in logs, events, snapshots, or UI.

## 15. Observability and Debuggability

The system should be designed to explain its decisions.

For each pending job, it should be possible to determine why it is not running. Reasons may include insufficient resources, hard constraint mismatch, quota exhaustion, priority ordering, reservation behavior, image availability, node health, or scheduler backoff.

For each placed job, it should be possible to determine why the selected node was chosen at a high level. This does not require logging every scoring detail forever, but the system should retain enough diagnostic information to debug surprising decisions.

For each failed job, it should be possible to distinguish user workload failure from platform failure.

Important observability outputs include:

* Structured logs.
* Metrics.
* State-change events.
* Audit logs.
* Scheduler decision summaries.
* Agent reconciliation reports.
* Raft health metrics.
* API access logs.

## 16. Data Storage Boundaries

Raft should store authoritative control-plane state.

A metrics system should store high-frequency telemetry.

A logging system should store logs.

An event or notification layer should handle client subscriptions.

A durable artifact store may be needed for job outputs, logs, metadata, or diagnostics, depending on product requirements.

A relational or analytical store may be useful for historical reporting, but it should not become a second source of truth for active scheduling decisions unless explicitly designed as such.

## 17. Suggested Initial Scope

The initial version should aim for a small but structurally correct system.

A reasonable first cut includes:

* Coordinator cluster using Raft.
* Job submission and cancellation.
* Basic job lifecycle.
* Node agent registration and heartbeat.
* Docker container execution.
* CPU, memory, and disk resource requests.
* Basic hard constraints using node labels.
* Simple priority queues.
* Basic quota accounting.
* Basic scheduler with heuristic bin packing.
* Assignment commitment through Raft.
* Agent reconciliation.
* Basic event streaming for job updates.
* Prometheus metrics.
* Snapshot and restart support.
* Minimal web UI or CLI for operation.

The first version should avoid overfitting to advanced features, but it should leave room for them in the state model and APIs.

## 18. Important Open Design Decisions

Several areas require further design before implementation.

The exact Raft library and persistence layer need to be selected.

The command and snapshot schema versioning strategy needs to be defined.

The job lifecycle state machine needs to be formalized.

The scheduler’s quota and priority policy needs a precise specification.

The reservation and backfilling model for large jobs needs more detailed design.

The consistency model for reads from followers needs to be decided.

The event subscription delivery guarantees need to be specified.

The agent fencing and reconciliation protocol needs to be defined.

The image-cache policy needs to distinguish between local agent autonomy and central scheduling hints.

The security model for container execution needs clear boundaries.

The data retention policy for events, metrics, logs, and job history needs to be chosen.

## 19. Design Principles

The design should follow several principles.

Keep the replicated state machine deterministic and boring.

Keep expensive scheduling computation outside the Raft apply path.

Commit decisions, not computations.

Separate desired state from observed state.

Treat agents as unreliable but reconcilable executors.

Make every command idempotent or safely retryable.

Use epochs or fencing tokens wherever stale leaders could cause harm.

Persist semantic state, not telemetry noise.

Design for explanation and debugging from the start.

Prefer simple, correct scheduling policies initially, with clear extension points.

Assume schema and behavior will evolve, and design upgrade paths explicitly.

Avoid making rollback impossible accidentally.

Use derived state and caches aggressively, but make them rebuildable.

Make failure recovery a normal path, not a special case.

