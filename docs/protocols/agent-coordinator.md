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

The concrete fencing and reconciliation protocol is an
[open design decision](../roadmap/open-decisions.md). The message enums in
`coppice-proto` (`agent` module) are the code-side anchor for this document.
