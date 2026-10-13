# Failure Handling

The design should assume failures are normal.

## Coordinator leader failure

Coordinator leader failure should trigger Raft election. A new leader
reconstructs authoritative state from the replicated log and snapshot, resumes
communication with agents, fences stale leaders using terms or epochs, and
reconciles desired state against observed agent reports.

## Coordinator disk loss

A coordinator that loses its durable state is replaced, never repaired: the
machine rejoins with a fresh node ID as a non-voting learner, catches up via
snapshot and log replay, and is promoted while the departed identity is
removed. Rejoining with an empty disk under an existing voter ID is refused at
startup — see
[ADR 0016](../decisions/0016-coordinator-rebuild-learner-join.md), and
[cluster-lifecycle](cluster-lifecycle.md) for the exact replace commands.

## Agent failure

Agent failure should cause the node to become unhealthy after missed heartbeats.
Running jobs on that node may be marked unknown, lost, retryable, or failed
depending on policy and whether the agent later reconnects with durable local
state.

## Network partitions

Network partitions should be handled conservatively. A coordinator outside the
Raft majority cannot commit writes. An agent receiving commands from stale or
minority coordinators must reject them using epoch checks.

## Job execution failure

Job execution failure is classified through the attempt outcome taxonomy
(`Exited`, `MemoryLimitExceeded`, `RuntimeLimitExceeded`, `DiskLimitExceeded`,
`Aborted`, `Revoked`,
`PullFailed`, `StartFailed`, `NodeLost`, `AgentError`, `Preempted` — see
[ADR 0013](../decisions/0013-job-attempt-allocation-state-machines.md)), each
classified as success, user error, user request, or platform failure. Retry
policy keys off that classification and should avoid retry storms.

## Platform termination (preemption and spot interruption)

When the platform ends a running attempt on purpose — a spot reclaim, a
scheduler preemption for a higher-class job, or (later) a deadline drain
([ADR 0044](../decisions/0044-job-preemption-and-spot-capacity.md)) — the
workload is sent its declared notice signal first, and the agent reports the
notice so the coordinator records the attempt's stop deadline (the attempt
shows as *preempting*). The attempt then ends `Preempted { reason }`, which
is free: the job requeues without consuming retry budget, however often it
happens, and the unused charge is refunded in full. A pending abort still
wins, ending the job `Aborted`.

A scheduler preemption only ever takes work that is effectively
preemptible, of a strictly lower class than the job it makes room for, and
scoring below it; `coppice job status` names the job it made way for. The
victim keeps running through its whole notice: the waiting job accrues on
the node and starts once the victim has gone. Churn is bounded per pass and
per node (scheduler `[scheduler]` knobs, `scheduler-v1.md`), and a node
with an eviction under way takes no second one.

Only a *reported* preemption is free. A host that disappears before the
agent's `Preempted` report reaches the coordinator is resolved `NodeLost`
by the liveness monitor, with ordinary retry accounting. The next attempt
learns how its predecessor ended from `COPPICE_PREVIOUS_ATTEMPT_OUTCOME`
(`preempted` or `node_lost`) and resumes from its own checkpoint.

## Node restart

Node restart should trigger local reconciliation. The agent should inspect
existing containers and local durable state, then report what is actually
running. The coordinator diffs that report against replicated intent and
commits adopt, stop, or lost per allocation.

See [../architecture/high-availability.md](../architecture/high-availability.md)
for the consensus and fencing mechanics these recovery paths rely on.
