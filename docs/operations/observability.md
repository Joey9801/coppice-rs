# Observability, Debuggability, and Resource Monitoring

## Design for explanation

The system should be designed to explain its decisions.

For each pending job, it should be possible to determine why it is not running.
Reasons may include insufficient resources, hard constraint mismatch, quota
exhaustion, priority ordering, reservation behavior, image availability, node
health, or scheduler backoff.

For each placed job, it should be possible to determine why the selected node
was chosen at a high level. This does not require logging every scoring detail
forever, but the system should retain enough diagnostic information to debug
surprising decisions.

For each failed job, it should be possible to distinguish user workload failure
from platform failure.

Important observability outputs include:

- Structured logs.
- Metrics.
- State-change events.
- Audit logs.
- Scheduler decision summaries.
- Agent reconciliation reports.
- Raft health metrics.
- API access logs.

## Resource monitoring

The system should monitor both requested and actual resource usage.

Requested resources are used for scheduling and isolation.

Actual usage is used for:

- Observability.
- Debugging.
- Historical estimation.
- Detecting over- or under-requesting.
- Future scheduling improvements.
- Policy enforcement where appropriate.

High-frequency raw telemetry should go to a metrics or time-series system, not
Raft.

The replicated state may store summarized or semantically meaningful
observations, such as:

- Job started at time.
- Job completed at time.
- Exit code.
- Final status.
- Peak resource summaries, if needed.
- Runtime estimate updates used by scheduling.
- Progress milestones used for reservation decisions.

## Metrics

Prometheus metrics should be emitted by coordinators, agents, scheduler workers,
API servers, and event delivery components.

Important metric categories include:

- Queue depth by project, queue, priority, and state.
- Scheduling latency.
- Time pending by reason.
- Placement attempts and failures.
- Resource utilization by node and cluster.
- Requested versus actual usage.
- Agent heartbeat health.
- Raft leader changes, commit latency, apply latency, and snapshot metrics.
- API latency and error rates.
- Event subscription counts and lag.
- Image pull latency and cache hit rate.
- Job success, failure, retry, and cancellation rates.

See [../architecture/data-storage-boundaries.md](../architecture/data-storage-boundaries.md)
for where these outputs are stored.
