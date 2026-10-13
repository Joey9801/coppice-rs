# Scale-in: draining, decommissioning, and health probes

How to take a compute node out of service without losing the work on it,
how the agent behaves when its host is being retired, how departed node
records are cleaned up, and which endpoints a supervisor or load balancer
should probe. The design is
[ADR 0041](../decisions/0041-graceful-scale-in-drain-and-node-eviction.md),
as amended by the regular drain of
[ADR 0044 §5](../decisions/0044-job-preemption-and-spot-capacity.md);
this page is the operator's view of it.

## Two reasons a node stops taking work

A node record carries two independent flags, either of which takes the
node out of ordinary service:

| Flag | Set by | Cleared by | Survives an agent restart? |
| --- | --- | --- | --- |
| drain record (`drain`) | `coppice node drain`, or the coordinator declaring the node lost | `coppice node undrain` | **Yes** — it is the operator's desired state |
| `agent_draining` | the agent itself, on SIGTERM or a provider interruption notice ([Cloud wiring](#cloud-wiring)) | the agent registering again without it | **No** — an agent that came back has not gone away |

`coppice node list` and `coppice node show <id>` both fold the two flags
into a single `scheduling` label: `drained` when the node has a drain
record (`drained, agent draining` if the agent is also draining),
`agent draining` when the agent alone is shutting down, and `open` when
neither flag is set.
`coppice node show <id>` additionally shows the drain record (when it was
requested and its status, below), the agent's announcement on its own
line, and the running and accruing counts. A node whose host reported a
provider interruption is also marked `interruption pending` in the list,
and `show` prints the deadline the agent read. The API exposes the same as
`drain: { requested_at, horizon, complete } | null`, `agent_draining`, and
`interruption_deadline` on every node summary.

Neither flag touches running work. Attempts already on the node run to
completion and report normally, and an accrual already there keeps
funding until the scheduler re-plans it elsewhere. What changes is
admission, from the log position at which the flag lands:

- **Agent draining** admits nothing.
- **Drained by an operator** admits only **bounded backfill** (below).

## Horizon and bounded backfill

A drained node keeps doing useful work for as long as that work is
guaranteed to be gone in time. "In time" is the node's **horizon**: the
latest *enforced* completion bound among the work still on it — for a
running attempt, its start plus its `max_runtime` plus the policy's
`abort_grace`, or, once its agent has delivered a termination notice
(ADR 0044 §3), the notice deadline plus `abort_grace` if that is earlier.

- If anything on the node has no enforced bound — a running job with no
  `max_runtime` and no notice delivered, an attempt not yet started, an
  accrual — the horizon is
  **unbounded**: nothing says when the node will be empty, and nothing new
  is admitted.
- Otherwise the node takes a job as backfill only when the job has a
  `max_runtime`, `now + max_runtime + abort_grace` lands at or before the
  horizon, and it fits the node's free capacity right now (backfill is
  always fully funded; no accrual ever opens on a drained node). The
  attempt records the horizon as its cutoff, so backfill never moves the
  horizon later: draining a node with backfill empties it no later than
  draining it with nothing admitted would — it may just use the time.
- Dispatch and image pulls take time, so the agent also refuses to start a
  backfill container after `cutoff − max_runtime − abort_grace`, giving up
  on an image pull still running at that point; such an attempt ends
  `revoked` and requeues free of retry budget.
- With no live allocation left the drain is **complete**. Admission stays
  closed until `coppice node undrain`.

## Deadline drains

A drain may carry a **deadline** `D` by which the node must be empty —
containers gone and exits reported
([ADR 0044](../decisions/0044-job-preemption-and-spot-capacity.md) §5):

```text
T_stop   = D − drain_cleanup_margin − abort_grace
T_notice = T_stop − notice_cap
```

(`drain_cleanup_margin` and `notice_cap` are the formation policy's
`[preemption]` knobs, 30 s and 5 m by default; `abort_grace` 30 s.)

- Work finishes naturally where it can; an attempt that exits before
  `T_notice` never hears of the drain.
- Bounded backfill is admitted up to `T_notice` — regardless of the
  horizon, since nothing outlives the deadline — and admission closes there.
- At `T_notice` every attempt still running gets its job's notice signal,
  with the common `notice_cap` window, and at `T_stop` the agent stops
  whatever remains. Preemptible or not, each ends `Preempted { drain }` and
  is requeued without consuming retry budget.
- The deadline must leave at least `notice_cap + abort_grace +
  drain_cleanup_margin` (6 m by default) from now; a shorter one is refused.
  An operator who cannot wait drains without a deadline, stops the host,
  and accepts `NodeLost` for what was running.
- A deadline can be replaced, earlier or later, only before the recorded
  deadline's `T_notice`. Attempts already notified keep their stops, so the
  node may empty later than a replacement deadline; the node view's
  "effectively empty by" says when. `coppice node undrain` cancels the plan:
  a notice phase not yet begun never happens, but attempts already notified
  still stop and still end `Preempted { drain }`. A plain `coppice node
  drain` on a node with a deadline keeps the deadline; undrain and drain
  again to drop it.
- The deadline lives on the replicated drain record; the agent journals the
  deadline it accepted and echoes it on every heartbeat, and the leader
  re-sends it whenever the two disagree, so a disconnect, an agent restart
  or a leader change cannot lose it. If the agent hears of it only after
  `T_notice`, it notifies at once, still stops at `T_stop`, and reports the
  notice it could not give as a **shortfall**, shown by `job status` and
  the node view.

## Draining a node by hand

```sh
coppice node drain <node-id> --wait          # drain, then wait up to 10 m
coppice node drain <node-id> --wait 30m      # a longer deadline
coppice node drain <node-id>                 # drain and return at once
coppice node drain <node-id> --deadline 30m --wait   # empty within 30 minutes
coppice node drain <node-id> --deadline 2026-10-14T02:00:00Z
coppice node undrain <node-id>               # take it back into service
```

`--deadline` takes an RFC 3339 instant or a duration from now (`15m`,
`2h`). With a deadline, `--wait` and `coppice node show` also print the
deadline, the notice time, the effective emptying time, and the largest
notice shortfall when one was reported.

`--wait` polls the node once a second and prints a line to stderr
whenever its progress changes: the horizon ("empty by <time>", or
"horizon unbounded" while unbounded work runs) and the running and
accruing counts. It exits 0 once the drain is complete and non-zero, with
what is outstanding, at the deadline; the drain stays in place either
way. Draining an already drained node succeeds and keeps the original
drain (its `requested_at` does not move).

The verbs are `POST /api/v1/nodes/{id}/drain`, `/undrain`, and `/remove`
on the client API; they need an unscoped `operator` or `admin` binding
(ADR 0023's cluster verbs), and any replica accepts them.

## What the agent does on SIGTERM

`systemctl stop coppice-agent` (or any SIGTERM / SIGINT) is a drain, and
deliberately a **notice-free** one: it waits, it never signals or stops a
workload. Planned scale-in that wants jobs to checkpoint runs a deadline
drain first (`coppice node drain <id> --deadline <d> --wait`) and stops the
agent once the node is empty; by then there is nothing left for the
SIGTERM drain to wait for.

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
for maintenance comes back to its own record, drain included. They are
removed two ways:

- **Explicitly.** `coppice node remove <node-id>` deletes the record of a
  node that is draining by either flag *and* holds no live allocation; a
  node still taking work or still holding work is refused (409). Drain,
  let `--wait` finish, stop the agent, remove. Removing a node whose agent
  is still running is allowed but pointless: it registers again as a fresh
  record on its next connection. Use `coppice node revoke-identity` to stop
  that.
- **By retention.** The leader's housekeeping evicts any node that is
  draining by either flag, holds no live allocation, and has been
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
  window and then brought back registers as a new, open record —
  extend the window or undrain on return.

A node the coordinator declared lost is drained by that declaration and
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
| `interrupted` | 503 | the host reported an external interruption: every attempt is notified and will be stopped before the provider's deadline (ADR 0044 §5); wins over `draining` |

The body carries `phase`, `node_id`, `registered`, `docker_ok`,
`draining`, `interrupted`, `running`, and `reason`. A hook that wants "has this node
finished draining" polls `/readyz` until `running` is zero or the process
has exited.

## Cloud wiring

The repository's Terraform keeps both autoscaling groups fixed-size and
hook-free; these are the shapes the protocol is designed for, expressed
as user-data and ASG configuration rather than product code.

**Scale-in through an ASG lifecycle hook.** Put a hook on
`autoscaling:EC2_INSTANCE_TERMINATING`. Two handler shapes:

- *With notices (recommended where jobs checkpoint).* The handler — a
  Lambda holding an operator credential for the client API — runs
  `coppice node drain <id> --deadline <d> --wait`, with `<d>` inside the
  hook's heartbeat timeout (at least `notice_cap + abort_grace +
  drain_cleanup_margin` away), then `systemctl stop coppice-agent` on the
  instance (an empty node's SIGTERM drain exits at once) and
  `complete-lifecycle-action`. Running work gets its notice at `T_notice`,
  ends `Preempted { drain }` and is requeued free; the node is empty by
  the deadline.
- *Without.* The handler — or a systemd unit on the instance watching
  instance metadata's `autoscaling/target-lifecycle-state` — runs
  `systemctl stop coppice-agent` (the notice-free drain) and calls
  `complete-lifecycle-action` when the unit has stopped, with a heartbeat
  timeout of at least `shutdown_grace` plus a margin. Nothing needs the
  node id or an API token on the instance: the agent announces its own
  drain. Work still running at `shutdown_grace` ends `NodeLost`.

**Spot interruption.** The agent handles this itself
([ADR 0044](../decisions/0044-job-preemption-and-spot-capacity.md) §5);
no watcher unit is needed. With `[preemption] interruption_source =
"aws-imds"` it polls `spot/instance-action` every five seconds, and a
notice starts a drain plan with the reclaim time as its deadline: the node
announces `draining` (and the deadline, shown in the node view), admits
nothing, and every running attempt gets its job's notice signal at once,
then the ordinary stop at `deadline − drain_cleanup_margin − abort_grace`. Each attempt
ends `Preempted { interruption }` and is requeued without consuming retry
budget; `/readyz` answers `interrupted` meanwhile. None of this waits for a
coordinator. Only work whose `Preempted` report never arrives (the
instance vanished first) becomes `NodeLost`. Do not stop `coppice-agent`
on the notice: the ADR 0041 SIGTERM drain sends no notices and would leave
the reclaim to the liveness path. Size jobs' declared notice so that
notice + `abort_grace` + `drain_cleanup_margin` fits the provider's two
minutes; the agent
warns when one does not.

**Ungraceful termination** — an instance killed without either — still
takes the 90 s liveness path: every attempt on it ends `NodeLost` and
retries, and the record is evicted after `node_retention`. The AWS demo's
smoke test exercises exactly this path on purpose.
