# Agent–Coordinator Protocol

The agent-coordinator protocol should be designed around reconciliation and
idempotency.

## Coordinator-to-agent messages

Coordinator-to-agent messages may include:

- Desired assignments.
- Start job command.
- Stop job command.
- Drain instruction.
- Cache preparation request.
- Cache eviction hint.
- Health or configuration update.

## Agent-to-coordinator messages

Agent-to-coordinator messages may include:

- Heartbeats.
- Resource inventory.
- Running job set.
- Job state transitions.
- Exit status.
- Resource usage summaries.
- Image-cache inventory.
- Local errors.
- Node health signals.

## Identifiers for safe retries

Messages should include identifiers that make retries safe:

- Job ID.
- Allocation ID.
- Attempt ID.
- Coordinator term or epoch.
- Node ID.
- Command ID or sequence number.
- Desired-state version where useful.

## Delivery guarantees

The protocol should tolerate duplicate, delayed, and reordered messages. The
coordinator should not assume that a sent command was executed until the agent
reports observed state. The agent should not assume a command is current unless
it passes epoch and identity checks.

## Fencing and reconciliation

Decided in [ADR 0009](../decisions/0009-fencing-and-reconciliation.md):

- The fencing token is **`(leader_term, node_epoch)`**; `node_epoch` is
  replicated per node and bumped on (re)registration or when the node is
  declared lost. Every coordinator→agent command carries the token plus a
  per-node `command_seq`. Agents durably track the highest accepted token and
  reject lower terms, mismatched epochs, and already-seen sequence numbers.
- `StartJob` is idempotent on `AllocationId`; status reports are idempotent on
  `(AttemptId, attempt_state)` because the attempt state machine is monotonic.
- Agents journal `(allocation_id, attempt_id, job_id, epoch)` durably before
  starting a container, and label containers with allocation/attempt IDs. On
  restart the agent rebuilds its actual set from journal + runtime, registers
  (receiving a fresh `node_epoch`), and reports the full **ObservedSet**; the
  coordinator diffs it against replicated intent and commits adopt / stop /
  lost per allocation. The same diff runs periodically against heartbeats.

The wire schema is `coppice.agent.v1`
(`proto/coppice/agent/v1/agent.proto`): `AgentCommand` carries the fencing
token and `command_seq` in one common `CommandHeader` on every
coordinator→agent command, and `AgentReport` covers registration,
heartbeats (capacity, running set, image-cache inventory), attempt status,
and the ObservedSet. That file is the code-side anchor for this document;
evolution follows [schema-style](../architecture/schema-style.md).

## The session, as implemented

The transport is one long-lived tonic bidirectional stream per agent
(`coppice.agent.v1.AgentService/Session`, stubs in `coppice-agent-net`),
served over mTLS (ADR 0011) on the coordinator's dedicated agent listener.
Sessions terminate on the leader only: a follower refuses with
`FAILED_PRECONDITION` and, when it knows one, a leader hint in the
`x-coppice-leader-hint` metadata; the agent rotates endpoints on refusal.
The agent's client certificate carries its `NodeId` as the subject CN; the
gateway rejects a session whose reports claim any other node. The
enrollment/CSR flow of ADR 0011 will replace ad-hoc issuance later; the
CN↔NodeId binding is the stable part.

### Connect, register, reconcile, work

1. **Connect** (mTLS) and send `Register { capacity, labels }` with
   `node_epoch = 0`.
2. The leader's ingestion proposes `RegisterNode`; the apply bumps the
   node's epoch. The gateway then sends **`RegisterAccepted`** — an empty
   command whose `CommandHeader` carries the fresh fencing token. This is
   how the agent "receives its new node_epoch" (ADR 0009, restart step 2).
3. The agent journals the token, then sends the full **ObservedSet** built
   from its journal plus the container runtime — *before accepting any new
   work*.
4. Normal operation: heartbeats every `heartbeat_interval` (capacity,
   running allocations, image-cache inventory), `AttemptStatus` reports on
   observed transitions, commands down.
5. Any stream break: reconnect with backoff and go to step 1. At-least-once
   reporting is achieved by the reliable stream plus this full resync on
   every reconnect; there are no per-report acks on the wire.

### Fencing check order (agent side, every inbound command)

The agent durably journals the highest accepted `(leader_term, node_epoch)`
and keeps the highest processed `command_seq` in session memory (a restart
re-registers, and the epoch bump retires the old sequence space):

1. `leader_term` below the watermark, or `node_epoch` below the watermark →
   **silently dropped** (logged); there is no nack on the wire. A deposed
   leader's commands fail closed here.
2. A token raising either component is journaled (fsync) *before* the
   command is acted on, and resets the sequence watermark.
3. `command_seq` already processed → acknowledged without acting: a
   duplicate `StartJob` re-reports the attempt's current status rather than
   re-executing.

### Journal record types

The journal (`proto/coppice/agent/v1/journal.proto`, written through the
filesystem seam with the storage engine's crash-safety discipline) holds:

- `FencingUpdate { leader_term, node_epoch }` — the accepted-token
  watermark.
- `StartIntent { allocation, attempt, job, node_epoch }` — journaled and
  fsynced **before** the container starts, so a running container always
  has durable intent behind it.
- `AllocationTombstone { allocation }` — journaled before a stop is acted
  on, so a racing or re-delivered `StartJob` is refused even across a
  restart (ADR 0013).
- `ObservedExit { allocation, attempt, job, outcome, runtime_us }` — the
  classified exit, journaled before it is reported, so an outcome observed
  while the coordinator was unreachable survives to the next ObservedSet.

### Restart reconciliation sequence

On (re)start the agent recovers the journal (truncating any torn tail,
then compacting), queries the container runtime by label, and reports —
never trusting its own memory over that pair:

- Runtime evidence wins: every container found in the runtime is reported
  with its true state (running, or exited with the classified outcome).
- A journaled exit with no surviving container reports the journaled
  outcome.
- A journaled intent with neither runtime evidence nor a journaled exit
  reports `running = false, outcome = AgentError` — the honest "I lost
  it". The agent never restarts pending journal intents after a crash: the
  re-registration epoch bump has already fenced them; it reports the doubt
  and lets the coordinator re-plan.

The leader diffs the set against replicated intent and commits
`ReconcileNode` verdicts: *adopt* (intended and running), *lost* (intended
but absent — the carried outcome runs the full terminal path, retry policy
applies). *Stop* verdicts never enter the log: a running container with no
live intent gets `StopJob` sent directly. Reports carrying a stale
`node_epoch` are demoted to exactly that stop-only reconciliation input —
they never become attempt-progress commands. The same diff runs
continuously against heartbeat running-sets, with one asymmetry: an
attempt in `Dispatching` that is absent from a *heartbeat* is not lost
(its `StartJob` may still be in flight), while one absent from a
post-registration *ObservedSet* is (the epoch bump already fenced any
in-flight start).

Node liveness is bookkept from report arrival times; the leader's
housekeeping tick proposes `DeclareNodeLost` for a node silent past the
deadline. If that node later reappears with a running container for a
lost attempt, the container has no live intent — it is stopped, and the
already-terminal attempt is untouched (truth-wins-the-race: the recorded
outcome never lies about what stopped the work).
