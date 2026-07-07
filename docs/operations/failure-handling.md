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
[ADR 0016](../decisions/0016-coordinator-rebuild-learner-join.md).

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
(`Exited`, `OomKilled`, `MaxRuntimeExceeded`, `Aborted`, `Revoked`,
`PullFailed`, `StartFailed`, `NodeLost`, `AgentError` — see
[ADR 0013](../decisions/0013-job-attempt-allocation-state-machines.md)), each
classified as success, user error, user request, or platform failure. Retry
policy keys off that classification and should avoid retry storms.

## Node restart

Node restart should trigger local reconciliation. The agent should inspect
existing containers and local durable state, then report what is actually
running. The coordinator diffs that report against replicated intent and
commits adopt, stop, or lost per allocation.

See [../architecture/high-availability.md](../architecture/high-availability.md)
for the consensus and fencing mechanics these recovery paths rely on.
