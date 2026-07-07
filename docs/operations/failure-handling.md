# Failure Handling

The design should assume failures are normal.

## Coordinator leader failure

Coordinator leader failure should trigger Raft election. A new leader
reconstructs authoritative state from the replicated log and snapshot, resumes
communication with agents, fences stale leaders using terms or epochs, and
reconciles desired state against observed agent reports.

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

Job execution failure should be classified. Examples include image pull failure,
container start failure, runtime nonzero exit, resource exhaustion, node loss,
timeout, cancellation, and internal scheduler or agent error. Retry policy
should be explicit and should avoid retry storms.

## Node restart

Node restart should trigger local reconciliation. The agent should inspect
existing containers and local durable state, then report what is actually
running. The coordinator decides whether to accept, cancel, retry, or mark jobs
lost.

See [../architecture/high-availability.md](../architecture/high-availability.md)
for the consensus and fencing mechanics these recovery paths rely on.
