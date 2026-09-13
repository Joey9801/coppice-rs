# 41. Graceful scale-in: drain, agent shutdown drain, node eviction, and health

- **Status:** Accepted
- **Date:** 2026-09-13
- **Resolves:** [OD-15(b)](../roadmap/open-decisions.md#od-15-agent-enrollment-signer-and-decommission-protocol),
  issue #49
- **Builds on:** [ADR 0009](0009-fencing-and-reconciliation.md) (epochs,
  drain survives re-registration), [ADR 0012](0012-data-retention.md)
  (commanded eviction), [ADR 0023](0023-scoped-role-bindings.md)
  (`Verb::Drain`), [ADR 0037](0037-coordinator-discovery-and-self-converging-membership.md)
  (`/readyz`), [ADR 0040](0040-leader-sourced-node-health-on-every-replica.md)
  (leader-local liveness marks)

## Context

Planned worker scale-in is indistinguishable from a crash today. The state
machine has carried `SetNodeSchedulable` since ADR 0009 and the scheduler
and `CommitPlacements` both honour `schedulable = false`, but nothing
proposes it: there is no API route, no CLI verb, and the agent installs no
signal handler at all, so `systemctl stop coppice-agent` (or an ASG
terminating the instance) simply kills the process. The coordinator notices
90 s later, proposes `DeclareNodeLost`, and every attempt on the node ends
`NodeLost` — the platform-outcome retry path — whether or not the work was
seconds from finishing. Node records are immortal: `DeclareNodeLost` marks
them unschedulable and leaves them, so an autoscaling group that churns
instances daily grows replicated state forever.

Neither daemon exposes a liveness probe. The coordinator has `/readyz`
(ADR 0037 §3), which the AWS NLB uses; the agent has only `/metrics`, and
only when `metrics_addr` is set. The AWS demo runbook documents worker
replacement as "the timeout path, not drain" for exactly these reasons.

The deployment story (A4) already sketches the shape: a drain verb, an
agent-initiated drain on SIGTERM, and an ADR 0012-style retention rule for
departed node records. This ADR fixes the details that sketch left open.

## Decision

### Two drain flags, one placement gate

A node stops receiving placements for two different reasons with two
different owners, and the record keeps them apart:

- **`Node.schedulable`** (existing) is the **admin cordon**. Only
  `SetNodeSchedulable` (actor-carrying, `Verb::Drain`) and
  `DeclareNodeLost` write it. It survives agent restarts, exactly as
  ADR 0009 decided — an operator who drained a node for maintenance must
  not have the drain undone by a reboot.
- **`NodeRecord.draining`** (new, replicated) is the **agent's own
  announcement** that it is shutting down. It is set from the agent's
  reports (below) and **cleared by re-registration** whose report does not
  carry it: an agent that comes back has, by definition, not gone away,
  and its earlier "I am leaving" intent is void. `systemctl restart
  coppice-agent` therefore never leaves a node permanently drained.

The single placement gate is `NodeRecord::accepts_placements() =
node.schedulable && !draining`. Both the scheduler's candidate filter and
apply's `CommitPlacements` validation use it; `RejectionReason::NodeNotSchedulable`
covers both causes. Everything else that reads `schedulable` — improvement
moves off a drained node, the `draining` display label — keys off the same
helper, so the two flags never disagree about what a placement may do.

### The drain verb is an HTTP write like any other

`POST /api/v1/nodes/{node}/drain` and `POST /api/v1/nodes/{node}/undrain`
propose `SetNodeSchedulable { schedulable: false | true }` with the
request's actor, through the same `precheck` → `ControlPlane` → propose →
forward-to-leader path as job abort (ADR 0031, ADR 0038). `Intent::Drain`
maps to the existing `Verb::Drain`: unscoped `operator` or `admin`. The
response is empty; the node's state is read back from `GET /nodes/{node}`.

`coppice node drain <node-id> [--wait [<duration>]]` and `coppice node
undrain <node-id>` are the CLI. `--wait` is a client-side poll of
`GET /nodes/{node}` at one-second cadence until `running_count` and
`accruing_count` are both zero, printing progress as they fall, with a
deadline (default 10 min) after which the command exits non-zero with the
counts still outstanding. Draining is idempotent: draining an already
drained node succeeds and `--wait` simply waits. Nothing streams; polling
is the established CLI pattern (`job logs --follow`, `promote --wait`).

The advisory `Drain` command on the agent stream is unchanged and still
unused by the coordinator: placement enforcement lives in apply, and the
agent has nothing to do differently for an admin cordon — its running
work continues to completion either way.

### Agent shutdown is a drain, then a bounded wait

The agent installs a SIGTERM / SIGINT handler (the session runner also
takes an external shutdown watch so tests never raise a real signal — the
coordinator's pattern). On shutdown:

1. The session marks itself **draining**. Every subsequent `Heartbeat` and
   `Register` report carries `draining = true`; the next heartbeat is sent
   immediately rather than on the tick. The session keeps serving —
   heartbeats, attempt status, exit reports, reconnects — throughout.
2. The leader's ingestion turns a report whose `draining` differs from the
   replicated record into a proposal of the new `SetNodeDraining { node,
   draining, at }` command (machine-proposed, no actor, like
   `DeclareNodeLost`). For a `Register` the value rides `RegisterNode`
   itself, so a re-registration that does not carry the flag clears it at
   the same log position that bumps the epoch. A drain that begins while
   the agent is **reconnecting** therefore lands as soon as it registers
   again: the intent is agent-local state, not stream state, and survives
   the stream.
3. The agent waits until its **accountable live work** is empty — journaled
   intents with no journaled exit, the same accountability rule the
   heartbeat's `running` set already follows (a container observed exited
   but not yet journaled is still claimed) — or until `shutdown_grace`
   (agent TOML, default `5m`) elapses.
4. It then stops its listeners and joins every task it spawned (exit
   watcher, reaper, NodeService, metrics/health server, TLS reload) under
   the same deadline, drops the telemetry sinks last so segment janitors
   finish their drains, and exits 0.

Work still running at the deadline is **left running and left to the
backstop**: the agent does not stop containers it could not wait for.
Killing them locally would report an exit code the coordinator classifies
as the job's own failure; letting the node fall silent classifies them
`NodeLost`, the platform outcome that retries elsewhere — which is the
correct verdict for work a planned termination could not accommodate.
Operators size `shutdown_grace` (and the unit's `TimeoutStopSec`, and the
ASG lifecycle hook's heartbeat timeout) to the longest job they expect to
let finish.

The agent reports **`draining`**, not "unschedulable": it is the agent's
observation about itself, and the record's `draining` flag exists so the
admin cordon cannot be confused with it.

### Node records are evicted, two ways

A new housekeeping command, `EvictNodes { nodes, evicted_at }`, removes
node records outright. It is proposed from two places and applies
identically: every listed node must exist (missing ids are skipped, for
idempotence across leader changes), must **not accept placements**, and
must have **no live allocation** (`state != Released`, the same predicate
`DeclareNodeLost` and the liveness monitor use); a node failing either
check rejects the whole command as a proposer bug. Apply deletes the
record; nothing else references an empty node.

1. **Explicit removal** — `coppice node remove <node-id>` /
   `POST /api/v1/nodes/{node}/remove` proposes `EvictNodes` for one node
   with the request's actor (`Verb::Drain`, the same cluster-verb rule).
   This is the decommission verb: drain, let the work finish, stop the
   agent, remove. It does not require the node to be silent — that is the
   operator's judgement — and an agent that is still alive will simply
   re-register as a fresh record (`epoch = 1`, schedulable) on its next
   registration, which is the honest outcome of removing a node that was
   not actually gone. `revoke-identity` remains the way to stop that.
2. **Retention GC** — the leader's housekeeping tick proposes `EvictNodes`
   for every node that does not accept placements, has no live
   allocation, and has been **silent for at least `node_retention`**
   according to the leader-local liveness marks (ADR 0040). The window is
   a replicated policy field, `PolicyConfig::node_retention`, default
   **24 h**, settable at formation via a new `[retention]` table
   (`node = "24h"`, `terminal = "72h"`) alongside the existing
   `terminal_retention`. Silence is measured the way the liveness
   deadline is: from the last report this leadership term saw, or from
   the term's seed mark — a leader change restarts the clock, which only
   ever delays eviction.

`DeclareNodeLost` is unchanged and remains the backstop for ungraceful
death. A lost node's record is now eventually collected by rule 2, which
is what makes a churning ASG's state bounded.

Why a retention window at all, rather than evicting the moment a drained
node is empty and silent: a node drained for maintenance is expected to
come back, and its record carrying the cordon is the only thing that
stops the returning agent from taking work before the operator undrains
it. Twenty-four hours covers a working day of maintenance; longer
maintenance sets a longer window or undrains on return.

### Liveness and readiness on both daemons

Every HTTP surface gains `GET /healthz`: 200 with `{"status":"ok"}` for
as long as the process is serving, no other condition. It is the
`systemd`/load-balancer *liveness* signal — "restart me if this fails" —
and deliberately says nothing about readiness.

The coordinator's `/readyz` (ADR 0037 §3) is already the readiness
signal and already distinguishes the states the issue asks for: `phase`
(`waiting` is the uninitialized-cluster state — no formation has run, so
no replicated policy exists; `formation-failed`; `joining`; `learner`;
`voter`), `is_leader`, replication lag, and `?require=healthy`. This ADR
adds nothing to it beyond documenting that mapping.

The agent's metrics listener (`metrics_addr`, optional) becomes its
operational listener, serving `/metrics`, `/healthz`, and a new
`/readyz`. The agent's readiness report is:

| `phase` | HTTP | Meaning |
| --- | --- | --- |
| `starting` | 503 | Process up, not yet registered with a coordinator |
| `ready` | 200 | Registered on a live session; Docker reachable |
| `reconnecting` | 503 | Session lost; reconnect loop running |
| `docker-unavailable` | 503 | Registered, but the last container observation failed |
| `draining` | 503 | Shutdown in progress; `running` counts the work still being waited for |

The body carries `phase`, `node_id`, `registered`, `docker_ok`,
`draining`, `running`, and `reason`. `docker_ok` is the outcome of the
most recent `observe()`; the agent still fails startup outright when no
daemon is reachable, so this only ever reports a daemon that went away.
An ASG or lifecycle hook that wants "is this node finished draining"
polls `/readyz` and waits for `running` to reach zero (or for the process
to exit).

### Cloud wiring is documented, not built

The repository's Terraform stays fixed-size and hook-free. The
operations guide documents the two integrations this protocol is
designed for: an **ASG lifecycle hook** on `autoscaling:EC2_INSTANCE_TERMINATING`
whose handler runs `coppice node drain --wait` (or just `systemctl stop`,
now that stop is a drain) and completes the action when the wait returns,
with a heartbeat timeout sized to `shutdown_grace`; and a **spot
interruption** watcher that stops the unit on the two-minute notice, so
the agent uses the notice as its drain window. Both are user-data shapes,
not product code.

## Consequences

- Planned scale-in never rides the 90 s timeout: a stopped agent announces
  itself within one heartbeat, receives no new placements from that log
  position, and its running work finishes or is honestly retried.
- Two flags on the node record instead of one is a small replicated cost
  (one bool) that buys the restart-safety property outright; collapsing
  them would either make `systemctl restart` a permanent drain or make an
  admin cordon vanish on reboot.
- `EvictNodes` is the third "proposer decides the clock" housekeeping
  command; apply stays deterministic and idempotent under leader change.
  A removed node that re-registers is a genuinely new record — its
  `epoch` restarts at 1, and any command fenced by the old epoch is
  already dead.
- The retention clock is leader-local and term-scoped, so eviction can be
  delayed by leader churn but never hastened. Nothing depends on eviction
  happening promptly.
- The advisory `Drain` command on the agent stream stays dead code on the
  coordinator side. It is kept because removing a proto message is a
  breaking-gate change with no benefit; a future coordinator push (for
  example to let the agent stop prefetching) has the slot ready.
- The agent gains a shutdown seam and joins its tasks; the unit's
  `TimeoutStopSec` and the documented ASG hook timeout both derive from
  `shutdown_grace`, so a job longer than the window is retried elsewhere
  rather than killed as a user failure.
- `metrics_addr` keeps its name while serving health: the agent has one
  plain-HTTP operational port, and renaming a config key for a second
  route buys nothing. Agents with no `metrics_addr` have no probe surface,
  as before; the AWS demo sets it.
