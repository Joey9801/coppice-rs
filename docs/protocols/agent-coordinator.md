# Agent–Coordinator Protocol

The agent-coordinator protocol should be designed around reconciliation and
idempotency.

## Coordinator-to-agent messages

Coordinator-to-agent messages may include:

- Desired assignments.
- Start job command.
- Stop job command.
- Drain instruction: the operator's deadline drain, or its withdrawal
  (ADR 0044 §5; see "Operator deadline drains" below).
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
- `StartJob` also carries the attempt's *resumption identity*
  ([ADR 0044](../decisions/0044-job-preemption-and-spot-capacity.md) §2):
  its index in the job's lineage, the most recent earlier attempt that
  reached `Running` with its outcome (derived from replicated
  `JobRecord.attempts`, so a lost node's attempt still counts), the
  attempt's recorded effective preemptibility, and the job's notice
  contract. The agent injects it as the reserved `COPPICE_*` env block.
- A drain-backfill `StartJob` also carries `latest_start_us`
  ([ADR 0044](../decisions/0044-job-preemption-and-spot-capacity.md) §5):
  the attempt's recorded `drain_cutoff − max_runtime − abort_grace`, so the
  job cannot outlive the draining node's horizon. The agent refuses a
  launch past it — on arrival, by abandoning an image pull still running
  at the bound, and again at the launch point after the pull — journaling
  and reporting the attempt terminal `Revoked` with runtime 0. The
  coordinator accepts that `Revoked` — from the terminal status or from a
  reconnect's ObservedSet — only for a drain backfill that never started,
  and requeues it free of retry budget.
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
heartbeats (capacity, running set, image-cache inventory, node usage),
attempt status, attempt notices, and the ObservedSet. That file is the code-side anchor for this document;
evolution follows [schema-style](../architecture/schema-style.md).

## The session, as implemented

The transport is one long-lived tonic bidirectional stream per agent
(`coppice.agent.v1.AgentService/Session`, stubs in `coppice-net`),
served over mTLS (ADR 0011) on the coordinator's dedicated agent listener.
Sessions terminate on the leader only: a follower refuses with
`FAILED_PRECONDITION` and, when it knows one, a leader hint in the
`x-coppice-leader-hint` metadata; the agent rotates endpoints on refusal.
The agent's client certificate carries its `NodeId`'s typed string form
(`node-<uuid>`, ADR 0024) as the subject CN; the
gateway rejects a session whose reports claim any other node. The
enrollment/CSR flow of ADR 0011 will replace ad-hoc issuance later; the
CN↔NodeId binding is the stable part.

### Connect, register, reconcile, work

1. **Connect** (mTLS) and send
   `Register { capacity, labels, service_addr, host_facts, detected_capacity }`
   with `node_epoch = 0`. `capacity` is the **advertised** vector — detection
   with `[capacity]` overrides applied, minus the system reservation —
   while `detected_capacity` is what detection alone read and `host_facts`
   describes the machine (OS, kernel, CPU model, core counts, totals, agent
   version). The last two are display-only: nothing schedules on them, and
   every one of their fields is independently best-effort, so an unreadable
   value arrives empty rather than blocking registration. Registration is
   their only refresh — a re-imaged host publishes new facts by
   re-registering, which overwrites the previous set wholesale.
2. The leader's ingestion proposes `RegisterNode`; the apply bumps the
   node's epoch. The gateway then sends **`RegisterAccepted`** — an empty
   command whose `CommandHeader` carries the fresh fencing token. This is
   how the agent "receives its new node_epoch" (ADR 0009, restart step 2).
3. The agent journals the token, then sends the full **ObservedSet** built
   from its journal plus the container runtime — *before accepting any new
   work*.
4. Normal operation: heartbeats every `heartbeat_interval` (capacity,
   running allocations, image-cache inventory, node usage), `AttemptStatus`
   reports on observed transitions, commands down.
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
- `StartIntent { allocation, attempt, job, node_epoch, max_runtime_us?,
  notice_signal, notice_us, abort_grace_us, drain_cleanup_margin_us }` —
  journaled and fsynced **before** the container starts, so a running
  container always has durable intent behind it. `max_runtime_us` is the
  enforced bound copied from `StartJob`, journaled so the max-runtime
  watchdog can be rebuilt: on every registration (reconnect or restart) the
  agent re-arms it for each running, unexited container as `max_runtime`
  minus the runtime the container reports (saturating at zero, which fires
  at once). No launch wall-clock is journaled; elapsed time comes from the
  runtime. The notice contract and `abort_grace_us` (from `StartJob`) let
  the agent signal and stop the attempt on its own, below, and
  `drain_cleanup_margin_us` (the policy's `drain_cleanup_margin` at
  dispatch) fixes the attempt's `T_stop` under any drain plan.
- `AllocationTombstone { allocation, reason }` — journaled before a stop is
  acted on, so a racing or re-delivered `StartJob` is refused even across a
  restart (ADR 0013). `reason` names the command that wrote it — `Abort`
  (a `StopJob`) or `Preempt { reason }` (the agent's own preemption stop) —
  and a refused `StartJob` resolves as that command: `Aborted` or
  `Preempted { reason }`.
- `NoticeIntent { allocation, reason }`, `NoticeMark { allocation,
  stop_at_us, shortfall_us, notified_at_us }` and `DrainPlan
  { operator_deadline_us?, notice_window_us, interruption_deadline_us? }` —
  the termination mechanism's records, below. `DrainPlan` is the node's
  whole plan: the latest record is in force, and one with neither component
  is "no plan". Compaction keeps the latest mark per allocation and the
  latest plan, and drops an empty plan.
- `ObservedExit { allocation, attempt, job, outcome, runtime_us }` — the
  classified exit, journaled before it is reported, so an outcome observed
  while the coordinator was unreachable survives to the next ObservedSet.

### Platform terminations (ADR 0044 §3, §5)

Two things start a platform termination. The coordinator's
**`PreemptJob { allocation, notice_us, reason }`** asks for one
attempt: the scheduler chose it as a victim (`Priority` or `Relocation`,
with the job it makes room for — ADR 0044 §4). The agent runs the steps
below for it with `stop = delivery + notice_us`, the job's declared notice
(never later than an active plan's `T_stop`). The command is re-sent on
every session open and leadership change while the coordinator's record is
unacknowledged, so it is idempotent on the journaled notice intent: an
attempt already notified — by an earlier `PreemptJob`, a plan or an
interruption — keeps its one stop time and its mark is simply re-reported;
one already ended or being aborted is unaffected; an intent left without a
mark by a crash is signalled again with a fresh countdown of the declared
notice. The command carries no grace: the stop path runs with the
`abort_grace_us` journaled from the attempt's `StartJob`, like every other
platform stop.

Otherwise the agent terminates attempts on the platform's behalf without a
per-attempt coordinator command. Two sources feed the node's **drain plan**,
each a component of the one journaled `DrainPlan` record, journaled before
anything acts on it:

- an **external interruption** (a spot reclaim, read from the configured
  `[preemption] interruption_source`), with the provider's reclaim time as
  its deadline `D`. From then on every `Register` and `Heartbeat` carries
  `draining = true` and `interruption_deadline_us = D` (agent wall-clock,
  display-only — the coordinator stores it on the node record for the node
  view and plans against nothing but marks), and every attempt still
  running is notified at once;
- the operator's **deadline drain**, from the coordinator's `Drain`
  command (below), notified at `T_notice = T_stop − notice_window`.

For each attempt, with `T_stop = D − drain_cleanup_margin − abort_grace`
(the attempt's own `drain_cleanup_margin_us` and `abort_grace_us`,
dispatched on `StartJob`), when its notice is due under the plan:

1. journal `NoticeIntent { allocation, reason }`, fsynced;
2. deliver the job's declared notice signal to PID 1;
3. journal `NoticeMark { stop_at = T_stop, shortfall }`, fsynced, where
   `shortfall = max(0, notice − (T_stop − now))`;
4. report **`AgentReport.attempt_notice`** `{ allocation, attempt, job,
   reason, remaining_us, shortfall_us }`, `remaining_us = T_stop − now`
   saturating at zero; the leader stamps the attempt's
   `preemption.acknowledged.deadline` as its own receive time plus
   `remaining`, never the agent's clock (`RecordAttemptNotified`);
5. at `T_stop`, journal the tombstone `Preempt { reason }` and run the
   ordinary stop (`SIGTERM`, `abort_grace`, `SIGKILL`) on a task of its own,
   so a node-wide stop over many containers never delays heartbeats or
   commands. The result is classified and reported as usual.

With both components active an attempt not yet notified is notified at
once with reason `Interruption` and the provider's `T_stop` — unless the
operator's notice time has already passed (both landing in one tick, or on
recovery), when its stop is the earlier of the two.
Once notified, an attempt has exactly one stop, and only an interruption
may move it — earlier, never later (ADR 0044 §3's precedence table): a
second interruption notice may only make its `D` earlier, and every mark
later than the interruption's `T_stop` gets a replacement mark, journaled
before it is re-reported, so a restart can never restore a stop later than
one the coordinator has published. A `StartJob` that arrives under an
interruption is still started — its committed intent predates the
announcement — and is notified at once.

**Starts in flight.** A container still starting at its plan `T_stop` never
starts: every start carries a launch bound, the earlier of its own
`latest_start_us` and its `T_stop` under the plan in force, and the agent
recomputes the bound of every start in flight whenever the plan changes. A
start in flight has been given no notice, so the bound may loosen as well
as tighten: a withdrawal, or a replacement with a later deadline, hands it
back, exactly as §3 treats any attempt not yet notified. The executor gives
up on the image pull once the bound passes — re-reading it, and its wait,
whenever it changes — and checks it again just before the container
starts. A refusal at the plan's bound journals a
`Preempt { reason }` tombstone and reports `Preempted { reason }`, runtime
0 (a free retry); one at the backfill's own `latest_start_us` is still
`Revoked`.

### Operator deadline drains (ADR 0044 §5)

The replicated `Node.drain` record is the source of intent; a regular drain
(no deadline) concerns placement only and sends the agent nothing.

- **`Drain { deadline_us?, notice_window_us }`.** `deadline_us` is the
  operator's deadline `D`; absent is the **withdrawal**. `notice_window_us`
  is the policy's `preemption_notice_cap`, the common window. A deadline
  equal to the one the agent holds is a no-op (no journal write); anything
  else journals the new plan, fsynced, then notifies whatever is already
  due — so a command reaching the agent after `T_notice` notifies at once,
  stops at `T_stop`, and reports the lost notice as shortfall. A
  replacement is a withdrawal followed by a new plan: attempts already
  notified keep their stops, the rest follow the new deadline. A
  withdrawal never retracts an issued notice: a notified attempt still
  stops at its stop and resolves `Preempted { Drain }`.
- **The echo.** Every `Heartbeat` carries `drain_target_us`, the operator
  deadline the agent last accepted (absent with none). An operator plan
  alone does **not** set `draining` — admission is the drain record's to
  close, at `T_notice`.
- **Reconciliation.** On every current-epoch heartbeat the leader compares
  the record's deadline with the echo (`drain_command_for` in
  `tasks/dispatch.rs`) and, on a mismatch, routes `Drain` with the record's
  deadline — or the withdrawal when the record has none — straight to the
  node, never through the log. A command lost to a disconnect or a
  leadership change is re-sent by the next leader to see the mismatch; a
  restarted agent recovers its target from the journal, so a matching echo
  sends nothing.
- **Admission at the door.** Under an operator plan, a `StartJob` arriving
  at or after its own `T_notice` (its dispatched grace and margin) is
  refused: the agent journals a `Preempt { Drain }` tombstone and reports
  the attempt terminal `Preempted { Drain }`, runtime 0 — a free retry. A
  passed `latest_start_us` answers `Revoked` first. A start already in
  flight at `T_notice` is notified once it has a container and stopped at
  `T_stop`; one still starting at `T_stop` never launches (above).

**Classification** is the agent's: a kill by its stop path is
`Preempted { reason }` whether or not the signal landed; a non-zero exit the
agent did not cause is `Preempted` if a notice intent exists for the
allocation and `Exited` otherwise; exit 0 is `Exited { code: 0 }`; a limit
kill or a `max_runtime` watchdog that fires first keeps its outcome.

**Nothing executor-bound blocks the loop.** A `StartJob` journals its
intent (the fsync-before-start barrier is unchanged) and its start then runs
on a task of its own, reporting `Running` or the start failure when it
finishes; a duplicate `StartJob` meanwhile reports nothing. A `StopJob`
journals its tombstone and its stop runs the same way. So an image pull or
an `abort_grace` wait never delays a notice, a stop or a heartbeat. A
notice for an attempt whose start is still in flight is delivered once it
has a container (a signal now would reach nothing, and a mark without one
would claim a notice never given); a stop time already past then fires at
once with the shortfall reported. A `StopJob` that races a start in flight
stops the container as soon as the start returns. An exit observed while
the agent's own stop of that allocation is in flight is held for the stop's
answer — an exit during our `SIGTERM` is the stop's, so a workload exiting
0 on it is still `Aborted` or `Preempted` — and is classified on its own
evidence only if the stop fails or finds no container.

**None of this needs a session.** The plan, the signals, the journaling and
the stop timers run in the agent's own loop through discovery, the dial and
the reconnect backoff as well as a live stream, because an interruption's
two minutes may be exactly when the coordinator is unreachable. Reports
produced while disconnected are not queued: after the reconnect, every
running allocation with a mark in force carries it in its
**`ObservedAllocation.notice`** `{ reason, remaining_us, shortfall_us }`
(remaining recomputed), and terminal outcomes come from the journal.

**Recovery.** On start the agent resumes every mark's stop time (at once if
past) and notifies whatever the recovered plan says is due. A notice intent
with no mark is signalled again: under an active plan its stop is the
plan's `T_stop` and the shortfall is reported; with no plan it gets a fresh
countdown of its declared notice. Under an interruption every mark later
than its `T_stop` is shortened, so a crash between an earlier interruption
and its replacement marks cannot leave a later stop in force. An
interruption whose `D` has already passed came back from the dead (an EC2
`stop`, or a reclaim that never landed): whatever still runs under it is
stopped at once and the interruption component is retired (the plan is
re-journaled without it) so the node does not drain forever. The operator
component is never retired by recovery — only a withdrawal ends it. An
interruption notice whose deadline has already passed while none is active
is ignored as stale. The ADR 0041 SIGTERM drain is unchanged and
notice-free.

### Restart reconciliation sequence

On (re)start the agent recovers the journal (truncating any torn tail,
then compacting), queries the container runtime by label, and reports —
never trusting its own memory over that pair:

- Runtime evidence wins: every container found in the runtime is reported
  with its true state (running, or exited with the classified outcome).
- A journaled exit with no surviving container reports the journaled
  outcome.
- Work this agent process is still acting on is **unresolved**: a start
  still in flight (intent journaled, no container yet), or an allocation
  whose exit the agent's own stop has not yet classified, reports
  `running = false` with no `outcome`. The leader neither adopts, loses, nor
  records an outcome for it; the agent's own `AttemptStatus` follows when
  the start or stop finishes. (A container the stop has not yet ended is
  reported running, as the runtime shows.)
- A journaled intent with neither runtime evidence nor a journaled exit
  reports `running = false, outcome = AgentError` — the honest "I lost
  it". The agent never restarts pending journal intents after a crash: the
  re-registration epoch bump has already fenced them; it reports the doubt
  and lets the coordinator re-plan.

The leader diffs the set against replicated intent and commits
`ReconcileNode` verdicts: *adopt* (intended and running), *lost* (intended
but absent — the carried outcome runs the full terminal path, retry policy
applies). *Stop* verdicts never enter the log: a running container with no
live intent gets `StopJob` sent directly. So does a running container
whose live attempt (`Dispatching | Running`, the job's current one) has a
pending `abort_requested`: the abort's `StopJob` is routed once when it
applies and is dropped if the node had no session then, so every
ObservedSet re-derives it from replicated state (a `Dispatching`
attempt is still adopted alongside). A running container whose `Running`
attempt carries an unacknowledged `Priority`/`Relocation` termination
record gets its `PreemptJob` re-sent the same way (never for an unresolved
allocation, whose stop or start is still in flight). Reports carrying a stale
`node_epoch` are demoted to exactly that stop-only reconciliation input —
they never become attempt-progress commands. The same diff runs
continuously against heartbeat running-sets, with one asymmetry: an
attempt in `Dispatching` that is absent from a *heartbeat* is not lost
(its `StartJob` may still be in flight), while one absent from a
post-registration *ObservedSet* is (the epoch bump already fenced any
in-flight start).

A heartbeat's running set is the agent's *accountability* set, not the
raw runtime state: a container the runtime already observes as exited,
held under a journaled intent whose exit the session has not yet
journaled and reported, is still claimed as running — its terminal
report is in flight, and disclaiming it would race the lost-verdict
against that report (the exit would be misclassified as an `AgentError`
loss). A genuinely lost container is absent from the runtime entirely
and is never claimed, and neither is a runtime-only recovery survivor
(exited, no intent record): those are reported terminal once in the
registration ObservedSet, and a claim would draw a `StopJob` the agent
has no intent to resolve.

A heartbeat also carries **node usage** (`Heartbeat.used`), the agent's
best-effort fold of the per-container samples its executor already
collects. Every dimension — cpu, memory, disk — is *job-attributable*:
it counts only what this node's containers consumed, never a host-level
reading, so a node's `used` is comparable to its `allocated`. The field
is `optional` and its absence means **not measured** (no runtime
telemetry, or live containers with no reading fresher than twice the
sampler interval) — never zero, which would misreport a wedged agent as
an idle node. The converse holds too: an agent with no containers at all
reports a present, all-zero vector, because an idle node's usage is a
measured fact rather than an unknown. Like the
image-cache inventory, usage is observed-only: the leader consumes it
outside the normalizer and it never becomes a Raft command.

Node liveness is bookkept from report arrival times; the leader's
housekeeping tick proposes `DeclareNodeLost` for a node silent past the
deadline. If that node later reappears with a running container for a
lost attempt, the container has no live intent — it is stopped, and the
already-terminal attempt is untouched (truth-wins-the-race: the recorded
outcome never lies about what stopped the work).
