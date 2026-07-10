# coppice-coordinator

The coordinator is the control-plane daemon: one process per replica that binds
Raft consensus, the deterministic state machine, the scheduler, and the API into
a single running node. This crate is the runtime library that wires those subsystems
together; the single `coppice` binary ([coppice-cli](../coppice-cli)) launches
it as `coppice coordinator --config …`. The full concurrency contract
lives in [coordinator-runtime](../../docs/architecture/coordinator-runtime.md).

## Structure

[`cli`](src/cli.rs) + [`lib.rs`](src/lib.rs) expose the deliberately tiny
command surface the `coppice` binary mounts, so integration tests drive the
exact paths the binary does. [`bootstrap`](src/bootstrap.rs) is the assembly half: it
loads config, initializes tracing, brings the consensus replica up through
`coppice-consensus::start`, stands up the mTLS Raft + admin server, and returns a
`BootedCoordinator`. [`runtime`](src/runtime.rs) is the task half: it constructs
the bounded channels, spawns every long-lived task, and owns the ordered drain
on shutdown.

Each background task is one row of the "Task inventory" table in the runtime doc
(`tasks/` — agent gateway, ingestion, dispatch, scheduler driver, housekeeping,
event fanout, API server). Consensus internals and the apply task itself live in
[`coppice-consensus`](../coppice-consensus); the scheduling pass lives in
[`coppice-scheduler`](../coppice-scheduler). This crate only orchestrates them —
it does not restate what those crates own.

## Leadership-driven activation

Every replica runs every task, but the leader-only loops (ingestion, dispatch,
scheduler driver, housekeeping) self-gate on the consensus status watch rather
than being spawned or killed by a supervisor. [`leadership`](src/leadership.rs)
supplies the two arms of that pattern — `wait_for_leadership` and
`until_leadership_lost` — so each loop starts its body on gaining leadership and
stops at an `.await` point it chose the moment leadership is lost or shutdown
flips, never mid-invariant. The every-replica tasks (API reads, event fanout,
the agent listener's accept-on-leader) run identically on followers, which is
what lets followers serve reads and event streams. See
[Leader transitions](../../docs/architecture/coordinator-runtime.md#leader-transitions)
and [high-availability](../../docs/architecture/high-availability.md).

## Node config vs replicated policy

[`config`](src/config.rs) reads exactly one TOML file at startup
([ADR 0020](../../docs/decisions/0020-node-config-vs-replicated-policy.md)):
addresses, data directory, the cluster id, TLS paths, Raft liveness
timing, SSO connection parameters, and observability settings. (The raft
node id is *not* config: it is minted at init and read back from the data
directory's manifest stamp,
[ADR 0025](../../docs/decisions/0025-self-minted-coordinator-identity.md).) Anything two
replicas must agree on — quotas, decay policy, retention, authorization mappings
— is **replicated cluster policy** held in the state machine, never in this file.
Unknown keys fail-stop naming the offending key, durations are humane strings
(bare integers rejected), and precedence is `CLI > file > built-in defaults`. The
CLI surface ([`cli`](src/cli.rs)) is correspondingly tiny: `--config` plus the
startup-intent flags `--bootstrap` / `--join`. See
[configuration](../../docs/operations/configuration.md).

Some parsed fields (SSO, the client API address, OTLP/metrics endpoints) are read
now but consumed by later changes; they are marked as such in the source.

## Bootstrap, membership, and rebuild

Startup intent follows the
[ADR 0016](../../docs/decisions/0016-coordinator-rebuild-learner-join.md) matrix:
`--bootstrap` (first coordinator of a new cluster), `--join` (a fresh replacement
replica), or neither (restart), cross-checked against the data directory's
stamped identity inside `coppice-consensus::start`. All coordinator↔coordinator
traffic is mutual TLS ([ADR 0011]) with no insecure fallback.

[`admin`](src/admin.rs) is the membership surface, both halves in one module: the
server implements the `RaftAdminService` RPCs (add-learner, promote-voter,
remove-node, cluster-status) over the local consensus seam, and the client
helpers back the hidden `coppice coordinator admin` subcommand and the multi-node
integration test. The promote wrapper polls while a learner is still catching up,
which is what makes replica replacement operable end to end. Every RPC first
checks the request's stamped cluster identity before touching Raft. See
[cluster-lifecycle](../../docs/operations/cluster-lifecycle.md).

## Liveness

[`liveness`](src/liveness.rs) is the leader health monitor's shared last-seen map
([ADR 0009]) — deliberately a `Mutex<BTreeMap>`, not a channel, so it adds no
`.await` edge and keeps the blocking graph acyclic. Ingestion `mark`s a node on
every inbound report; housekeeping `seed`s a grace window on gaining leadership
and, on its 60 s tick, proposes `DeclareNodeLost` for any node silent past the
liveness deadline that is still schedulable or holds live allocations. See
[failure-handling](../../docs/operations/failure-handling.md).

## Boundaries

- Bounded-channel capacities and cadences live in [`limits`](src/limits.rs); each
  constant mirrors one row of the runtime doc's channel-inventory table, so a
  capacity change is never made without checking the doc (or vice versa).
- Consensus, the apply task, view publishing, and the event tap belong to
  `coppice-consensus`; this crate programs against the `Consensus` seam and the
  `StateViews` read handle only.
- Some subsystems are honest stubs today: the API server holds the real
  `ControlPlane` impl but no HTTP transport is wired yet (`run_placeholder`), and
  housekeeping writes terminal jobs through a `StubHistoryStore` until the SQL
  job-history store lands. Snapshot-trigger and the storage-flush shutdown steps
  are staged for when the segment storage layer is in place.

[ADR 0011]: ../../docs/decisions/0011-container-security-posture.md
[ADR 0009]: ../../docs/decisions/0009-fencing-and-reconciliation.md
