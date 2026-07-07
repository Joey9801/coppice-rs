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

The message enums in `coppice-proto` (`agent` module) are the code-side anchor
for this document.
