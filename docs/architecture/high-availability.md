# High Availability Model

The coordinator control plane should use Raft.

Raft provides a replicated log, leader election, and a deterministic replicated
state machine. The system should be designed around that model.

Only the leader should accept authoritative writes. Followers may receive client
requests, but they should either redirect to the leader, proxy to the leader, or
reject with leader information.

Read paths can be divided into categories:

- Strong reads requiring confirmation from the leader or a read-index style
  mechanism.
- Stale-tolerant reads served from followers.
- UI or observability reads from derived stores where eventual consistency is
  acceptable.

## Determinism requirements

The Raft state machine must be deterministic. Given the same sequence of
committed commands, every replica must arrive at the same state.

Therefore, the state machine should avoid:

- Wall-clock-dependent decisions during application.
- Randomness during application.
- Network calls during application.
- Expensive scheduling computation during application.
- Dependence on local machine state.
- Iteration over unordered maps where ordering affects results.
- Version-dependent behavior that changes the meaning of old log entries.

Commands should carry enough information for deterministic application. For
example, a scheduling command should say "assign these jobs to these nodes under
this expected state version," not "run the scheduler now."

## Fencing and snapshots

The leader should use epochs or terms to fence interactions with agents. Agents
must reject stale coordinator commands from old leaders.

The system should support snapshots so new or recovering coordinators do not
need to replay an unbounded log.

Evolving this replicated state safely across binary versions is covered in
[versioning.md](versioning.md). The recovery behaviour on leader loss, agent
loss, and partition is covered in
[../operations/failure-handling.md](../operations/failure-handling.md).
