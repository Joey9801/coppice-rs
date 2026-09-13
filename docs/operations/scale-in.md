# Scale-in: draining, decommissioning, and health probes

How to take a compute node out of service without losing the work on it,
how the agent behaves when its host is being retired, how departed node
records are cleaned up, and which endpoints a supervisor or load balancer
should probe. The design is
[ADR 0041](../decisions/0041-graceful-scale-in-drain-and-node-eviction.md);
this page is the operator's view of it.

## Two reasons a node stops taking work

A node record carries two independent flags, and a placement lands on a
node only when **both** allow it:

| Flag | Set by | Cleared by | Survives an agent restart? |
| --- | --- | --- | --- |
| cordon (`schedulable = false`) | `coppice node drain`, or the coordinator declaring the node lost | `coppice node undrain` | **Yes** — it is the operator's desired state |
| `draining` | the agent itself, on SIGTERM | the agent registering again without it | **No** — an agent that came back has not gone away |

`coppice node list` and `coppice node show <id>` both fold the two flags
into a single `scheduling` label: `drained` when the node is cordoned
(whether or not the agent is also draining), `draining` when the agent
alone is shutting down, and `schedulable` when neither flag is set.
`coppice node show <id>` additionally reports the running and accruing
counts.

Neither flag touches running work. Attempts already on the node run to
completion and report normally; only *new* placements are refused, from
the log position at which the flag lands.

## Draining a node by hand

```sh
coppice node drain <node-id> --wait          # cordon, then wait up to 10 m
coppice node drain <node-id> --wait 30m      # a longer deadline
coppice node drain <node-id>                 # cordon and return at once
coppice node undrain <node-id>               # take it back into service
```

`--wait` polls the node once a second and prints a line whenever the
running or accruing counts change. It exits 0 once both are zero and
non-zero, with the outstanding counts, at the deadline; the cordon stays
in place either way. Draining an already drained node succeeds.

The verbs are `POST /api/v1/nodes/{id}/drain`, `/undrain`, and `/remove`
on the client API; they need an unscoped `operator` or `admin` binding
(ADR 0023's cluster verbs), and any replica accepts them.

## What the agent does on SIGTERM

`systemctl stop coppice-agent` (or any SIGTERM / SIGINT) is a drain:

1. The agent announces `draining` on its next report, sent immediately,
   and on every report after that. The leader replicates it, and no new
   placement lands on the node from that log position.
2. The agent keeps serving — heartbeats, exit reports, reconnects — until
   every container it is accountable for has exited and been reported,
   or until `shutdown_grace` (agent config, default `5m`) elapses.
3. It then stops its listeners, joins its background tasks, flushes its
   telemetry segments, and exits 0.

Work still running at the deadline is **left running**, not killed: the
agent logs the outstanding allocation ids and exits. Once the host goes
away the coordinator's liveness monitor declares the node lost and those
attempts end `NodeLost` — the platform outcome, retried elsewhere under
the job's retry policy. That is the correct verdict for work a planned
termination could not accommodate; stopping the containers locally would
instead report an exit the coordinator classifies as the job's own
failure.

Size the three timeouts together, longest job you want to let finish
first:

| Knob | Where | Default | Rule |
| --- | --- | --- | --- |
| `shutdown_grace` | `agent.toml` | `5m` | how long the agent waits for its work |
| `TimeoutStopSec` | `coppice-agent.service` | `330` | `shutdown_grace` plus margin, or systemd SIGKILLs the wait |
| lifecycle-hook heartbeat timeout | ASG | — | at least `shutdown_grace` plus margin |

If the agent is **reconnecting** when the signal arrives, the drain is
not lost: the announcement is agent-local state, so the next successful
registration carries it and the coordinator replicates it then.

## Decommissioning: removing the record

Node records are kept after the agent goes away so that a node drained
for maintenance comes back to its own record, cordon included. They are
removed two ways:

- **Explicitly.** `coppice node remove <node-id>` deletes the record of a
  node that is cordoned or draining *and* holds no live allocation; a
  node still taking work or still holding work is refused (409). Drain,
  let `--wait` finish, stop the agent, remove. Removing a node whose agent
  is still running is allowed but pointless: it registers again as a fresh
  record on its next connection. Use `coppice node revoke-identity` to stop
  that.
- **By retention.** The leader's housekeeping evicts any node that no
  longer accepts placements, holds no live allocation, and has been
  silent for `node_retention` — 24 h by default, set at formation:

  ```toml
  [retention]
  node = "24h"
  terminal = "72h"
  ```

  This is what keeps replicated state bounded when an autoscaling group
  churns instances. Silence is measured by the current leader from the
  last report it saw, so a leader change restarts the clock; eviction can
  be delayed by that, never hastened. A node drained for longer than the
  window and then brought back registers as a new, schedulable record —
  extend the window or undrain on return.

A node the coordinator declared lost is cordoned by that declaration and
is collected by the same rule once its window elapses.

## Health probes

Both daemons answer plain HTTP on their operational listener:

| Endpoint | Meaning | Use it for |
| --- | --- | --- |
| `GET /healthz` | 200 whenever the process is serving | systemd / ASG *liveness*: restart on failure |
| `GET /readyz` | 200 only when the daemon can take traffic; 503 with a `phase` and `reason` otherwise | load-balancer targets, lifecycle hooks, rollout gates |

**Coordinator** — on the client listener (port 7070 in the examples).
`/readyz` is described in
[cluster-lifecycle.md](cluster-lifecycle.md); its `phase` distinguishes an
uninitialised cluster (`waiting` — no formation has run, so no replicated
policy exists), a failed formation, a joining or learner replica, and a
`voter`; `is_leader` names the role, and `?require=healthy` adds the
leader's fleet-health verdict.

**Agent** — on the metrics listener, which exists only when `metrics_addr`
is set (the AWS demo sets it; `deploy/examples/agent.toml` shows it).

| `phase` | HTTP | Meaning |
| --- | --- | --- |
| `starting` | 503 | up, not yet registered with a coordinator |
| `ready` | 200 | registered on a live session, Docker reachable |
| `reconnecting` | 503 | session lost, reconnect loop running |
| `docker-unavailable` | 503 | registered, but the last container observation failed |
| `draining` | 503 | shutdown in progress; `running` is the work still being waited for |

The body carries `phase`, `node_id`, `registered`, `docker_ok`,
`draining`, `running`, and `reason`. A hook that wants "has this node
finished draining" polls `/readyz` until `running` is zero or the process
has exited.

## Cloud wiring

The repository's Terraform keeps both autoscaling groups fixed-size and
hook-free; these are the shapes the protocol is designed for, expressed
as user-data and ASG configuration rather than product code.

**Scale-in through an ASG lifecycle hook.** Put a hook on
`autoscaling:EC2_INSTANCE_TERMINATING` with a heartbeat timeout of at
least `shutdown_grace` plus a margin. The handler — a Lambda, or a
systemd unit on the instance watching instance metadata's
`autoscaling/target-lifecycle-state` — runs `systemctl stop
coppice-agent` (which is the drain) and calls
`complete-lifecycle-action` when the unit has stopped. Nothing needs the
node id or an API token on the instance: the agent announces its own
drain. An operator driving the scale-in from outside can equally run
`coppice node drain <id> --wait` first and terminate afterwards.

**Spot interruption.** The two-minute interruption notice
(`spot/instance-action` in instance metadata) is the drain window. A
small watcher unit that stops `coppice-agent` on the notice gives running
work two minutes to finish and reports exits cleanly; whatever is left
becomes `NodeLost` when the instance is reclaimed, exactly as with a
missed deadline above. Set `shutdown_grace` no longer than the notice on
spot hosts so the agent's own wait does not outlive the instance.

**Ungraceful termination** — an instance killed without either — still
takes the 90 s liveness path: every attempt on it ends `NodeLost` and
retries, and the record is evicted after `node_retention`. The AWS demo's
smoke test exercises exactly this path on purpose.
