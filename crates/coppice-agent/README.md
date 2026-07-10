# coppice-agent

The node agent runs on every compute node. It executes containers on behalf of
the coordinator and reconciles what is actually running against coordinator
intent. This is a high-level summary; the full specification lives in the main
docs, linked throughout.

## Operating model

The agent is an **eventually-consistent executor of coordinator intent**
([ADR 0009](../../docs/decisions/0009-fencing-and-reconciliation.md),
[agent-coordinator](../../docs/protocols/agent-coordinator.md)). It is never a
source of truth for desired state — only for its own **observed** local state:
what containers exist and how they ended. The one law it lives by is *never
trust memory over journal plus runtime, and never claim a container is running
without runtime evidence*.

It registers with the coordinator, advertises capacity and labels, starts and
stops containers, enforces local limits, reports observed lifecycle transitions,
and rebuilds its running set from durable state after a restart. It is built to
be robust against duplicated commands, deposed leaders, partitions, and process
restarts.

## The session

The transport is one long-lived tonic bidirectional stream to the coordinator
**leader** (a follower refuses with a leader hint; the agent rotates endpoints),
served over mTLS with the node's `NodeId` as the certificate subject CN
([ADR 0011](../../docs/decisions/0011-container-security-posture.md)). The
correctness core (`session.rs`) is transport-free — fencing, dedup, StartJob
idempotency, the tombstone rule, and truth-wins classification are plain methods
over the journal and executor, unit-testable without a live server. `run`
(`session/runner.rs`) wraps that core in the stream plus the reconnect/backoff
loop and owns the timers (heartbeat cadence, max-runtime watchdogs).

Each connection: connect, send `Register`, receive `RegisterAccepted` carrying a
fresh fencing token, then send the full **ObservedSet** *before accepting any new
work*. Delivery is at-least-once by construction: the stream is reliable and
every reconnect re-registers and re-sends the ObservedSet, so there are no
per-report acks on the wire.

## Fencing and epochs

Every inbound command carries a `(leader_term, node_epoch)` fencing token and a
per-node `command_seq`. The agent durably tracks the highest accepted token and:

- **rejects** any command below the watermark (a deposed leader fails closed,
  silently — no nack);
- **journals a raised token (fsync) before acting** on the command that raised
  it, so a restarted agent never regresses to obeying a deposed leader;
- treats an already-seen `command_seq` as a **duplicate** — acknowledged without
  re-acting; a duplicate `StartJob` re-reports the attempt's current status
  rather than re-executing.

`StartJob` is idempotent on `AllocationId`; status reports are idempotent because
the attempt state machine is monotonic
([job-lifecycle](../../docs/lifecycle/job-lifecycle.md), ADR 0013).

## The executor trait

The container runtime sits behind the `Executor` trait (`executor.rs`), so all
correctness logic above it is runtime-agnostic. Container identity rides as
labels (allocation/attempt/job ids) so `observe()` can rebuild the running/exited
set after a restart without trusting agent memory.

- `FakeExecutor` is an in-process, deterministic implementation that drives the
  whole agent in tests (including agent-restart scenarios via `fork`).
- **`DockerExecutor` is a stub**: every method returns `Unimplemented`. The real
  Docker implementation lands behind the same trait later, with ADR 0011's
  locked-down defaults (no privileged containers, no host mounts/network,
  non-root UID, always-applied resource limits) enforced unconditionally.

## The local journal

A single append-only file (`journal.rs`) in the node's data directory, guarded by
a `LOCK` and written through the `coppice_consensus::fs` seam with the same
crash-safety discipline as coordinator storage (CRC-framed records, torn-tail
truncation, atomic compaction on recovery). Every append fsyncs before returning
— this is ADR 0009's **fsync-before-container-start barrier**: intent is durable
before a container ever starts, tombstones before a stop is acted on, classified
exits before they are reported. On restart the recovered journal (`JournalState`:
watermark, intents, tombstones, exits) is reconciled against the runtime to build
the ObservedSet.

## Restart reconciliation

`build_observed_set` (`observed.rs`) is pure and encodes ADR 0009's precedence:

1. **Runtime evidence wins** — a surviving container is reported with its true
   state, even if the journal disagrees or the intent is missing.
2. A journaled exit with no surviving container reports the journaled outcome.
3. A journaled intent with neither a container nor an exit reports
   `running = false, outcome = AgentError` — the honest "I lost it". The agent
   never restarts a pending intent after a crash; the re-registration epoch bump
   has already fenced it, so it reports the doubt and lets the coordinator
   re-plan.

The coordinator diffs the ObservedSet (and, continuously, heartbeat running-sets)
against replicated intent and commits adopt/lost/stop verdicts.

## Image cache

Per [ADR 0010](../../docs/decisions/0010-image-cache-boundary.md) agents own
eviction absolutely and cache state is observed, never replicated. Central
`PrepareCache`/`EvictImageHint` commands are advisory: the agent accepts and
**ignores** them today, and the heartbeat's image-cache inventory is empty in v1
([image-cache](../../docs/scheduling/image-cache.md)).

## Configuration

A single node-local TOML file (`config.rs`,
[ADR 0020 conventions](../../docs/decisions/0020-node-config-vs-replicated-policy.md)):
`node_id` (typed form `node-<uuid>`), data directory, coordinator endpoints, mTLS material (by path),
advertised capacity, labels, and timing knobs. Anything two replicas must agree
on is cluster policy and never appears here. Unknown keys and bare-integer
durations fail-stop naming the offending key. Launched with
`coppice agent --config <path>` (the single `coppice` binary, [coppice-cli](../coppice-cli)).

See [components](../../docs/architecture/components.md) for where the agent sits
in the system and [failure-handling](../../docs/operations/failure-handling.md)
for the failure model it is built against.
