# Docker executor design

Status: draft for review — no code yet.

The concrete `DockerExecutor` behind the existing `Executor` trait
(`crates/coppice-agent/src/executor.rs`), plus the agent-side machinery it
needs that deliberately lives *outside* the trait: image cache management,
job telemetry (metrics + logs) with modular sinks, and disk enforcement.

Everything here honors the seams already fixed by ADR 0009 (fencing,
journal-before-start, label-based reconciliation), ADR 0010 (agents own
eviction; cache state is observed, never replicated), ADR 0011 (locked-down
container posture, limits always enforced), and ADR 0013 (attempt outcomes;
truth wins the race).

## 1. What already exists and is not redesigned

- **The `Executor` trait** — `start` / `stop` / `observe` / `next_exit` /
  `cache_inventory`. Classification, journaling, fencing, and reporting all
  live above it in `Session`. This design keeps that boundary; the trait
  grows two small extensions (§10).
- **The journal** — `StartIntent` before start, `AllocationTombstone` before
  stop, `ObservedExit` before report. Restart recovery is journal ∪ runtime,
  runtime evidence wins. The Docker executor's job is to make `observe()`
  a faithful runtime half; **no new journal record types are needed** (§5).
- **The session runner** — owns the heartbeat and max-runtime watchdog
  timers and the exit-watcher task. Unchanged.

## 2. Module layout

```
crates/coppice-agent/src/
  executor.rs                  trait + FakeExecutor (extended, §10)
  executor/docker/
    mod.rs                     DockerExecutor: wiring, task supervision
    api.rs                     thin bollard wrapper (client, retries, errors)
    lifecycle.rs               start sequence, stop, reap, observe
    state.rs                   Docker state ↔ ContainerState mapping (§3)
    classify.rs                exit-evidence extraction (§4)
    events.rs                  docker events stream → ExitEvent queue
    limits.rs                  Resources → HostConfig translation (§6)
    cpuset.rs                  whole-core exclusive-affinity allocator (§6.3)
    disk.rs                    DiskEnforcer: quota / poll strategies (§6.2)
    cache.rs                   image cache manager (§7)
    stats.rs                   per-container metrics sampler (§8.1)
    logs.rs                    log follower + at-least-once resume (§8.2)
  telemetry/
    mod.rs                     TelemetryHub: fan-out, batching, backpressure
    sink.rs                    MetricsSink / LogSink traits
    fs_sink.rs                 filesystem sink + retention + read API (§8.4)
  pressure.rs                  HostDiskMonitor: shared pressure signal (§9)
```

Docker client: **bollard** (async, typed, events/stats/logs streaming).
No CLI shelling. `api.rs` is a thin convenience layer, not a mock seam —
correctness testing stays on `FakeExecutor` above the trait plus a gated
real-Docker integration suite (§12).

`telemetry/` and `pressure.rs` are executor-agnostic on purpose: the hub is
handed to `DockerExecutor` at construction, and tests can drive it from
`FakeExecutor` without Docker.

## 3. Container lifecycle and state mapping

Docker's raw states (`created`, `running`, `paused`, `restarting`,
`removing`, `exited`, `dead`) are wider than the two the agent reports.
One mapping table, in one place (`state.rs`), total over Docker's enum:

| Docker state | Mapped `ContainerState` | Notes |
| --- | --- | --- |
| `created` | *(not reported)* | Start-sequence debris: created but never started (crash inside `start`). Removed on observe; the journaled intent with no runtime evidence already reports `AgentError` via the existing session path. |
| `running` | `Running` | |
| `paused` | `Running` | We never pause; treat as running (it holds resources). Unpause-and-stop on reconcile if unowned. |
| `restarting` | `Running` | Unreachable — restart policy is always `no` (§5); mapped defensively. |
| `removing` | *(not reported)* | A reap in flight; terminal evidence was already captured. |
| `exited` | `Exited(ExitInfo)` | Evidence from inspect: `ExitCode`, `OOMKilled`, `FinishedAt − StartedAt`. |
| `dead` | `Exited(ExitInfo)` | Daemon-side failure; if no usable exit code, synthesize executor evidence that classifies as `AgentError` (§4). |

The executor's internal per-allocation phase machine (driving the start
sequence) is `Resolving → Pulling → Creating → Starting → Started`, with
every pre-`Started` failure mapping onto `StartError::Pull` or
`StartError::Start` (§4). It is executor-internal bookkeeping only — the
session never sees it, and it is deliberately *not* persisted (a crash
mid-start is already handled by intent-without-evidence → `AgentError`).

Restart policy is always `no`: a Docker-initiated restart would fabricate a
second run under the same attempt, violating attempt monotonicity.

## 4. Exit evidence and classification

Classification stays above the trait (ADR 0013): the executor reports
*evidence*, the session assigns outcomes. The evidence taxonomy:

**Natural exits** (via the events task or `observe`): `ExitInfo` from
inspect — `code`, `oom_killed` (kernel OOM kill against the memory limit),
`runtime`. `classify_exit` maps these to `Exited{code}` /
`MemoryLimitExceeded`.

*`OOMKilled` commit race (issue #34).* The daemon does not guarantee the
`OOMKilled` flag is committed to inspect state at the instant the `die`
event fires — some daemons set it from an async OOM-event handler that can
lag the exit, so an inspect issued at event time can read exit 137 under a
memory limit with the flag still unset. Every natural-exit evidence path
(die event, resync, stop's pre-inspect, `observe`) therefore routes its
inspect through a bounded settle (`settle_oom_flag`): when the racy shape
is present — SIGKILL exit code, explicit memory limit, flag unset — it
re-inspects on a short backoff (~1.6 s total) until the flag commits or the
budget runs out. The flag remains the **sole** OOM gate: an exhausted
budget still classifies `Natural` (an external SIGKILL is indistinguishable
by code alone), so this changes timing, never the classification contract.
The stop post-inspect and the disk enforcer's kill path skip the settle —
a 137 there is expected from their own SIGKILL, and waiting out the budget
on every hard kill would be pure latency for no evidence.

**Executor-initiated kills**: the disk enforcer's poll strategy (§6.2) is
the one place the *executor itself* decides to kill, analogous to the
kernel's OOM killer. `ExitInfo` gains a cause field so this survives the
trait boundary:

```rust
pub struct ExitInfo {
    pub code: i32,
    pub cause: ExitCause,      // replaces bare oom_killed: bool
    pub runtime: Duration,
    pub finished_at: Timestamp, // Docker's FinishedAt; lets the session
                                // janitor age exited containers (§5)
}
pub enum ExitCause { Natural, OomKilled, DiskKilled }
```

`classify_exit` maps `DiskKilled` into the **limit-breach outcome
family**. The existing taxonomy names the hard-limit breaches
incoherently (`OomKilled`, `MaxRuntimeExceeded`, and now a disk flavour),
yet all three are the same underlying event: the job used more of a
hard-limited resource than it asked for, and policy terminated it. The
design renames them as three aligned flat variants of the main outcome
enum:

```rust
AttemptOutcome::MemoryLimitExceeded    // was OomKilled
AttemptOutcome::DiskLimitExceeded     // new
AttemptOutcome::RuntimeLimitExceeded  // was MaxRuntimeExceeded
```

Classification is identical across the three — user error, policy kill,
no default retry (deterministic recurrence). This renames two existing
variants and adds one — the design's single replicated-contract change
(core enum + proto + descriptor breaking gate + coordinator resolution +
journal encoding) and needs its own small ADR amending ADR 0013's outcome
table.

**Caller-initiated kills** are unchanged: abort and max-runtime kills go
through `stop()`, and the caller assigns `Aborted` /
`RuntimeLimitExceeded`.

**Stop-vs-natural-exit race discrimination.** The verdict must never be
inferred from timestamps or event ordering on our side — the Docker
daemon serializes container state transitions and answers the race
atomically in its API responses, and that answer is the sole source of
truth:

1. `stop()` first inspects: already exited → `AlreadyExited` with that
   inspect's evidence.
2. Otherwise issue `POST /containers/{id}/stop` (SIGTERM → grace →
   SIGKILL). The daemon returns **304 Not Modified** iff the container
   had already exited when the stop took effect — that, and only that,
   yields `AlreadyExited` (evidence from a follow-up inspect). A 204
   means our stop terminated it → `Stopped`.
3. One carve-out on the 204 path: if post-stop inspect shows
   `OOMKilled`, the kernel's kill wins and the natural evidence
   (`ExitCause::OomKilled`) is reported instead.

The only genuinely ambiguous window is a container exiting *during the
grace period after our SIGTERM was delivered* — and that is correctly
attributed to the stop per ADR 0013: the abort mechanism terminated the
attempt, even if the process exited 0 while handling TERM. Crucially the
failure mode this guards against — an aborted container misreported as
having failed naturally just before the abort — cannot occur through
this path: a natural-exit verdict requires the daemon's own
already-exited answer, never our guess. The gated integration suite
races these explicitly (§12).

*Implementation note (S2):* bollard maps the stop endpoint's 304 to
success before the caller can see the status, so the 204/304 distinction
cannot be recovered through its typed API. The implementation therefore
answers "already exited" from the pre-stop inspect, and attributes the
residual window — a natural exit landing between that inspect and the
stop taking effect — to the stop. That errs only in the direction §4
already tolerates (the TERM-grace case): a natural exit may be recorded
as stop-terminated, never the reverse.

**Start failures** map with an explicit user/platform split:

| Failure | `StartError` | `user_error` |
| --- | --- | --- |
| Manifest unknown, unauthorized, bad reference syntax | `Pull` | `true` |
| Registry 5xx, network timeout, rate limit, local disk full during pull | `Pull` | `false` |
| Image alone exceeds the job's disk request (§6.2) | `Start` | `true` |
| Invalid command/entrypoint (exec format at create time) | `Start` | `true` |
| Daemon errors, name conflicts we can't resolve, cgroup failures | `Start` | `false` |

An exit may surface through both `next_exit` and a concurrent
`stop`/`observe`; the executor keeps a per-allocation claimed-exit set to
suppress duplicates best-effort, and the session's exit handling stays
idempotent on allocation (as today) as the backstop.

## 5. Identity, idempotency, and restart recovery

Containers carry the reconciliation contract as labels and a deterministic
name:

- Name: `coppice-<allocation-id>` — the Docker-level idempotency backstop.
  A create hitting a name conflict inspects the survivor: same allocation
  label → adopt (start already happened); anything else → platform
  `StartError`.
- Labels: `coppice.allocation`, `coppice.attempt`, `coppice.job`
  (typed string forms, ADR 0024), `coppice.node`, and
  `coppice.image-digest` (the resolved digest, for cache pinning across
  restart, §7) plus `coppice.disk-mode` (which enforcement strategy §6.2
  chose, so the poll enforcer resumes for the right containers).

`observe()` = `docker ps -a` filtered on `label=coppice.allocation`,
mapped by the §3 table. Everything the agent needs to recover is either in
the journal (intent/tombstone/exit — already there) or on the container
itself (labels above). **No journal schema change.** The label set *is*
the "tags stored for recovery" — the container is the durable record of
its own runtime facts, which is exactly the ADR 0009 split (journal =
intent, runtime = evidence).

Reaping: exited containers are *evidence* and must outlive the crash
window, so the executor never auto-removes them — only the session
decides removal, because only the session can see the journal. The trait
gains `reap(allocation)` (§10), which the session calls after the exit
is journaled (and after tombstoned stops resolve); recovery calls it too,
for every observed-exited container whose exit is already journaled.
Reap also ends the log follower and decrements the image pin (§7). The
safety net is likewise session-side: a periodic sweep diffs
`observe()` against `JournalState.exits` and reaps exited containers
whose exits are journaled and whose age — `now − finished_at`, from the
runtime evidence itself (`ExitInfo.finished_at`, §4) — exceeds a
generous bound (default 24h). Both inputs are available to the session
by construction (the journal for presence, the observation for age);
the executor itself never consults journal state.

Missed exits while the agent is down are covered by `observe()` on
restart; the events stream (`docker events --filter label=…`, `die`/`oom`
events) covers steady-state. On events-stream hiccups the executor
re-syncs via a full `observe()` diff before resuming.

## 6. Resource limits

All limits always enforced (ADR 0011), translated in `limits.rs`:

| Resource | Mechanism |
| --- | --- |
| CPU | `NanoCpus = cpu_millis × 1_000_000` (hard ceiling via CFS); whole-core requests additionally get an exclusive cpuset (§6.3) |
| Memory | `Memory = memory`, `MemorySwap = memory` (no swap headroom), kernel OOM kill enabled (it is our classification signal) |
| PIDs | `PidsLimit` from config default (fork-bomb hygiene, not user-visible policy) |
| Disk | §6.2 |

Security posture (ADR 0011), unconditional and config-free: no privileged,
no host mounts, own network namespace with outbound (default bridge; no
host networking), non-root UID (config default, e.g. 65534; job-requested
UID allowed, UID 0 rejected at start as user error),
`no-new-privileges` set unconditionally (a process can never gain
privileges via setuid/setgid binaries or file capabilities, closing the
escalation path the UID rule alone leaves open), and an **explicitly
pinned capability set**: `CapDrop=["ALL"]` plus a named `CapAdd` list
(`CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `NET_BIND_SERVICE`, `KILL` — the
subset of Docker's defaults that ordinary entrypoints actually need,
excluding `SETUID`, `SETGID`, `NET_RAW`, `MKNOD`, `SYS_CHROOT`,
`AUDIT_WRITE`, `SETPCAP`, `FSETID`, `SETFCAP`; `SETUID`/`SETGID` are
deliberately out — the container starts at its final non-root UID, so
identity switching inside it has no legitimate v1 use). Pinning via
drop-all-then-add means the effective set never silently varies with
daemon version or daemon-wide configuration; loosening for a specific
workload is an ADR 0011 admin exception, never a job field.

Spec-shape validation like the UID rule ultimately belongs upstream, at
coordinator admission (reject at accept time, before capacity is spent);
the agent enforces it unconditionally anyway — defence in depth, and the
backstop until the admission-side check exists.

### 6.1 The softening seam

Every enforced limit is expressed internally as:

```rust
struct Enforcement {
    limit: u64,              // the allocation's requested vector, always
    mode: EnforcementMode,   // Hard (v1) | Burstable { … } (future)
}
```

v1 constructs only `Hard`. The seam matters because the two mechanisms
differ in how softening lands later: CPU/memory bursting becomes
"native limit set above `limit`, poll-and-kill by overuse ranking under
host pressure" — which is *exactly the machinery the poll disk enforcer
already is*. So the future soft-limit killer is the disk enforcer's
ranking loop generalized across resources, fed by the shared pressure
signal (§9), not a new subsystem. No v1 code path branches on `mode`;
it exists so the config/proto surface and the enforcer's structure don't
have to be reshaped later.

### 6.2 Disk: hard limits, two strategies

`DiskEnforcer` chooses a strategy at startup and records the choice on
each container (`coppice.disk-mode` label):

**Native (xfs project quotas).** When the Docker data-root's backing
filesystem is xfs mounted with `pquota` and the storage driver is
overlay2, Docker supports per-container `storage_opt: size=<bytes>` —
a kernel-enforced hard cap on the writable layer. Detection at startup:
`docker info` (driver + backing filesystem) plus a probe create with a
`size` storage-opt, torn down immediately; the probe is the ground truth,
the info fields are just the log message.

**Poll fallback.** Everywhere else: a poller (default every 30s, config)
reads writable-layer usage through the Docker API only — one
`GET /system/df` sweep for all containers' `SizeRw`, with
`ContainerInspect(size=true)` as the single-container recheck before a
kill verdict. (Docker's documentation explicitly advises against
measuring the overlay2 directories directly; the executor never touches
the storage driver's on-disk layout.) A container past its limit is
**killed outright** — no pause-first — with evidence
`ExitCause::DiskKilled` → `DiskLimitExceeded`. The killed container is
kept around like every other exited container: evidence until the
session journals the exit and calls `reap` (§5). These API calls are
daemon-side expensive; the poller runs them serially and no more often
than the configured interval.

**Accounting includes the image.** A job's disk usage is defined as
`writable_layer + image_size` (the image's on-disk size from image
inspect). Consequently:

- The *enforced* writable-layer budget is `limits.disk −
  image_size`. If the image alone exceeds the request, the start fails as
  user error before the container is created (§4 table).
- Reported usage (metrics §8.1, and any future heartbeat usage summaries)
  is the sum, so the scheduler sees the honest number. Shared images mean
  the sum across jobs can exceed physical disk — accepted and documented;
  physical safety is the pressure monitor's job (§9), not per-job
  accounting's.

Known asymmetry, accepted: under native quotas a job that fills its budget
sees `ENOSPC` and exits on its own (classified `Exited{code}`); under the
poll fallback it is killed and classified `DiskLimitExceeded`. Both
are honest
records of what happened; unifying them would mean either killing under
quota mode too (strictly worse for the user) or not killing under poll
mode (no enforcement).

### 6.3 Whole-core CPU affinity

A job requesting an integer multiple of 1000 millicores gets
*exclusive* access to that many physical cores. `cpuset.rs` owns a host
core inventory: the host topology minus the whole cores covered by the
system reservation (§6.4) — `floor(reservation.cpu_millis / 1000)` cores
are excluded from job placement entirely, and the reservation's
fractional remainder coexists with fractional jobs in the shared pool
(the capacity inequality below keeps that sound):

- **The allocation unit is a complete SMT sibling group.** `CpusetCpus`
  addresses *logical* CPUs, so picking bare IDs would let two grants
  land on sibling hyperthreads of one physical core — no isolation at
  all. The inventory is therefore built from
  `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list`: one
  entry per physical core, and every grant or carve-out takes or returns
  whole sibling groups. Millicores are denominated in *physical* cores
  throughout (that is what the startup validation below pins capacity
  to).
- **Whole-core jobs** are granted N sibling groups — all logical
  siblings of each granted core — via `CpusetCpus`. Selection is
  NUMA-packed best-effort: the inventory carries each group's NUMA node
  (from the same sysfs read), and a grant first-fits within a single
  node before spilling across nodes — spilling is never refused, only
  avoided (see §6.5 for why the real fix sits above the agent).
  `NanoCpus` is set to the full logical width of the grant: the silicon
  is exclusively theirs, so SMT throughput on it comes free rather than
  being clipped to the nominal request.
- **Fractional jobs** are confined to the shared pool — the complement of
  all exclusive grants — also via `CpusetCpus`, with `NanoCpus` enforcing
  their millicore cap within it. When the pool shrinks or grows (an
  exclusive grant or release), running fractional containers are updated
  in place (`docker update`).
- **The arithmetic that makes this safe**: CPU is never oversubscribed —
  the scheduler only places jobs against the *advertised* capacity, and
  the agent validates at startup that
  `capacity.cpu_millis` (advertised + reservation, §6.4) does not exceed
  physical cores × 1000 (a config that does is rejected). Under that
  invariant, exclusive grants plus the reservation's whole cores never
  exhaust the sibling groups, and the shared pool always has at least
  as many physical cores as the fractional job requests plus the
  reservation remainder sum to (each pool core contributes at least its
  1000 millis of quota headroom, SMT or not) — so an exclusive grant is
  always satisfiable. A grant that cannot be satisfied anyway is an invariant
  breach and fails the start as a platform `StartError`.
- **Recovery**: grants are rebuilt on restart from container inspect
  (`HostConfig.CpusetCpus` of surviving labeled containers) — no
  persistence, same philosophy as cache pins.

### 6.4 System reservation

The agent reserves a configurable slice of every resource for itself and
non-job system operations (the daemon, kernel, telemetry store, image
pulls in flight):

```toml
[capacity]            # what the machine has (validated against physical)
cpu_millis   = 32000
memory       = "128GiB"
disk         = "1TiB"

[reservation]         # withheld for the agent + system; never placed
cpu_millis   = 1000
memory       = "2GiB"
disk         = "20GiB"
```

- **Advertised capacity = `capacity − reservation`**, computed once at
  startup (`Config::advertised_resources()`, replacing the verbatim
  `capacity_resources()` in `Register` and `Heartbeat`). The coordinator
  and scheduler only ever see the advertised vector; the reservation is
  invisible upstream by construction, not by scheduler policy. A
  reservation ≥ capacity in any dimension is a config error at startup.
- **CPU**: the reservation participates in the §6.3 no-oversubscription
  validation (`capacity ≤ physical`) and carves its whole cores out of
  the job cpuset inventory, so system work keeps real headroom rather
  than competing inside the job pool.
- **Disk**: the reservation keeps the advertised vector honest against
  the same filesystems the §9 pressure monitor watches; the pressure
  thresholds remain the runtime backstop, the reservation is the
  planning-time one.
- Defaults are deliberately non-zero (above) — a node that advertises
  every byte to jobs starves its own agent; an operator who truly wants
  that sets the reservation to zero explicitly. To be precise about what
  defaulting means: an omitted `[reservation]` table (or field) takes
  the default *values* — it does not scale to the node's capacity. A
  node smaller than the defaults (under ~1 core / 2 GiB / 20 GiB) fails
  the reservation ≥ capacity startup check and must configure its
  reservation explicitly.

### 6.5 Topology domains (forward-looking, deliberately unsolved here)

Three concerns share one underlying shape: resources are not really a
flat vector — they live in **topology domains**, and a placement that
fits in aggregate can still straddle domains and run badly:

- **NUMA**: a whole-core grant needlessly spread across NUMA nodes pays
  cross-node memory latency (§6.3 packs best-effort, but a node whose
  free cores are fragmented across domains can only spill).
- **Multiple disks**: a node can have enough *total* disk while no
  single filesystem fits the job's request; conversely a job's data
  split across disks behaves unlike its nominal budget. v1 sidesteps
  this by construction — there is one Docker data-root, so all job disk
  lives on one filesystem, and the §6.2 budget and §9 pressure signal
  are defined against exactly that filesystem. Multi-disk nodes are
  expressible only as "the data-root's filesystem" until this is
  solved properly.
- **GPU (later)**: a GPU grant must come with CPUs on the same socket /
  PCIe root complex, or DMA and launch latency suffer — the CPU and
  GPU allocators cannot choose independently.

These are recorded here, not solved here, because the real fix is
scheduler-shaped: the coordinator advertises and places against a flat
`Resources` vector today, so it cannot know that a node's remaining
capacity is fragmented across domains and will happily place a job the
node can only run badly. The eventual shape (its own ADR, touching
proto/scheduler/agent) is per-domain capacity advertisement and
domain-aware placement — at which point the agent-side allocators
become executors of a domain assignment rather than choosers.

What this design does commit to is not painting over the seam: the
cpuset inventory already carries NUMA node ids; the disk enforcer is
already scoped to a named filesystem rather than "the disk"; and a
future GPU allocator slots beside `cpuset.rs` consuming the same
topology inventory. Each allocator takes an optional domain constraint
when the scheduler learns to send one — an added parameter, not a
redesign.

## 7. Image cache manager

`cache.rs` owns pulls and eviction (ADR 0010: agent authority, absolute).

- **Pulls.** All pulls funnel through the manager: per-reference
  singleflight (n concurrent starts of the same image = one pull), a
  global concurrent-pull limit (config, default 2), resolution of the
  job's reference to a digest recorded on the container label. Pull
  policy: use the local image when the reference is already present
  (digest refs are exact; tag refs accept the local tag — tag-drift
  re-resolution is future work, noted below).
- **Pinning.** An image is pinned while any non-terminal allocation
  references it (assigned-but-not-started counts, per ADR 0010). Pins
  are not persisted, and restart recovery is deliberately partial:
  running/exited containers re-pin from their `coppice.image-digest`
  label, but a pre-start pin has no container and the journaled
  `StartIntent` carries no image identity — and it doesn't need to. The
  epoch bump already fenced every pending intent (the agent never
  restarts them; the coordinator re-plans), so the pin is simply
  re-established when the re-delivered `StartJob` arrives with its image
  reference. The window between registration and re-delivery can at
  worst evict an image that must be re-pulled — latency, never
  correctness, exactly ADR 0010's contract (and the TTL makes it
  unlikely in practice).
- **Eviction (v1): TTL since last use.** `last_used_at` = the end time of
  the last attempt that used the image (or pull time if never used). A
  janitor tick evicts unpinned images idle past `image_cache.ttl`
  (default 30m). The bookkeeping lives in a small local state file under
  `data_dir/image-cache.json` — lossy-OK: if missing or stale it is
  rebuilt conservatively from `docker image ls` with `last_used_at = now`
  (worst case: images live one extra TTL).
- **Inventory.** The manager maintains the `ImageCacheInventory`
  (digest, size, last-used) snapshot that `cache_inventory()` returns for
  heartbeats — already on the trait, currently a stub.
- **Hints.** `PrepareCache` → an ordinary pull through the manager
  (subject to the same limits, dropped under high pressure).
  `EvictImageHint` → evict-if-unpinned, freely ignored otherwise.

Planned upgrades, and the seams that keep them cheap:

1. **Pressure-aware eviction** (first upgrade): the janitor already ranks
   unpinned images by staleness; under §9 `High` pressure it evicts ahead
   of TTL, most-stale-first, until below the high-water mark. This is a
   policy change inside the janitor tick only.
2. **Coordinator-driven preloading**: `PrepareCache` already exists on the
   wire; the upgrade is coordinator-side signal quality, not agent
   structure.
3. **P2P image sharing**: pulls go through a `fetch(reference) → digest`
   internal seam in the manager; a peer-aware fetcher slots in behind it
   without touching pinning/eviction/inventory.

## 8. Telemetry: metrics, logs, sinks

### 8.1 Metrics collection

One sampler task per running container (`stats.rs`), sampling the Docker
stats API one-shot every `telemetry.metrics_interval` (default 10s):

```rust
struct MetricSample {
    allocation: AllocationId, attempt: AttemptId, job: JobId,
    at: Timestamp,                   // coppice_core::time — µs-quantised; only
                                     // the storage encoding is int64 µs
    cpu_usage_total: Duration,       // cumulative; rate is derived by readers
    cpu_throttled_total: Duration,
    memory_used_bytes: u64,
    memory_peak_bytes: u64,
    disk_writable_bytes: u64,        // from the disk poller's last reading
    disk_image_bytes: u64,           // constant per attempt; sum = usage (§6.2)
    net_rx_bytes_total: u64,
    net_tx_bytes_total: u64,
    blkio_read_bytes_total: u64,
    blkio_write_bytes_total: u64,
}
```

A fixed typed struct, extended additively when GPU (vram/cores) and
friends land — same evolution style as the protos. Counters are reported
cumulative; sinks and readers derive rates, so a missed sample loses
resolution, never mass.

Agent-*internal* metrics are completely separate from job telemetry and
follow the repo's established per-module `describe_metrics()` /
`gather_metrics()` fan-out (as in `coppice-coordinator`/`coppice-consensus`
— the agent crate doesn't have the pattern yet; **this change introduces
it**, with a crate-level fan-out in `lib.rs`). Starter set, landing with
the executor:

- `agent_running_jobs` gauge — gathered from executor state;
- `agent_disk_poll_duration_seconds` histogram — pushed per §6.2 poll
  sweep;
- `agent_cached_images` / `agent_cached_image_bytes` gauges — gathered
  from the cache manager's inventory snapshot;

plus the operational counters named throughout: pulls (count/duration),
evictions, limit kills by kind, sampler lag, sink queue drops, and
`agent_log_resume_replayed_chunks_total`.

### 8.2 Log collection

One follower task per running container (`logs.rs`): Docker logs API with
`follow=true, timestamps=true, since=<resume point>` (see below), demuxed
into:

```rust
struct LogChunk {
    allocation: AllocationId, attempt: AttemptId, job: JobId,
    at: Timestamp,              // Docker's per-line timestamp, µs-quantised
    stream: Stdout | Stderr,
    bytes: Bytes,               // raw, no re-framing of user content
}
```

**Resume across restarts.** A Docker timestamp is not a unique cursor —
several chunks can share one timestamp, and the logs API's `since`
filter only accepts whole seconds (Docker's `since` is an integer Unix
timestamp; bollard surfaces it as such) — so a naïve
`since=<last timestamp>` either replays or skips work at the boundary.
The API also supplies no stable record identifier. `(at, stream, bytes)`
is not one: two genuine occurrences may have identical values after
timestamp quantisation. Consequently no content-alignment rule can
always distinguish a replayed occurrence from a previously unseen one.
Suppressing a merely matching occurrence would make completeness depend
on an unprovable identity assumption.

The recovery contract is therefore deliberately **at-least-once**:

- derive `boundary = floor_to_second(MAX(at))` over the attempt's log
  chunks across all live segments. Segments are scanned newest-first
  until one containing log rows is found; if none exists, use the
  container's start time;
- re-fetch with `since=<boundary>` and append every returned chunk in
  daemon delivery order. Never delete, align, hash-deduplicate, or skip
  a returned occurrence based on `(at, stream, bytes)`;
- commit batches in arrival order to the open segment. A crash loses at
  most the uncommitted batch; the next recovery derives a new boundary
  from what actually committed and applies the same rule.

For one recovery, only chunks in the boundary second may already exist
in the store; later returned chunks were not present when the boundary
was chosen. Repeated crashes before time advances may therefore append
another copy of that second on each recovery. This is accepted rather
than risking silent loss. The replay counter makes the amplification
visible, while segment size/age rolling and live retention bound its
physical lifetime.

Readers order log rows by `(at, insertion_order)` and must tolerate
duplicate occurrences. They must not deduplicate identical rows because
identical user writes are semantically distinct. The precise contract is:

- uninterrupted collection appends each delivered chunk once;
- restart recovery is at-least-once;
- no chunk returned by the daemon during recovery is discarded;
- chunks already removed by Docker before recovery are unavailable
  unless they had previously reached the filesystem sink.

This composes with segment rolls because recovery only appends to the
open database: closed segments are never modified and no cross-file
transaction is required. Operationally, the daemon must use the `local`
or `json-file` log driver; its `max-size`/`max-file` rotation bounds how
far back a long-dead agent can catch up.

After an exit, the follower drains to end-of-stream before the session's
`reap` removes the container. Reap awaits the drain with a bounded
timeout, and on timeout **fails retryably, leaving the container
intact** — the session's periodic sweep simply retries later, so a slow
drain never costs tail logs in healthy operation. The backstop for a
genuinely wedged follower is forced: past `drain_force_after` (config,
default 10m) reap proceeds without the drain, counting the event
(`agent_log_drain_forced_total`, error-level) — forced tail loss is
metered, never silent.

### 8.3 Sink framework

```rust
trait MetricsSink: Send + Sync {
    fn append(&self, batch: &[MetricSample]) -> impl Future<Output = ()> + Send;
}
trait LogSink: Send + Sync {
    fn append(&self, batch: &[LogChunk]) -> impl Future<Output = ()> + Send;
}
```

- **Compile-time registry, config-selected** (recompile-to-add is
  explicitly acceptable): sink configuration is a serde
  internally-tagged enum, so each variant keeps `deny_unknown_fields`
  and its own custom fields:

  ```toml
  [[telemetry.sinks]]
  type      = "filesystem"          # the only v1 variant
  kinds     = ["metrics", "logs"]   # what this sink instance consumes
  retention = "60m"
  # dir defaults to <data_dir>/telemetry
  ```

  Future variants (`clickhouse`, `postgres`, `influx` for metrics;
  `loki`, `elastic` for logs) are new enum variants + impls. Multiple
  sinks, including multiple instances of one type, are just more array
  entries.
- **Fan-out and backpressure.** The `TelemetryHub` gives each sink
  instance a bounded queue (config, default 1024 batches) and a dedicated
  drain task, so a slow sink can never backpressure container execution
  or another sink. Queues are sized so they don't fill in any healthy
  state — expected volume (10s-cadence samples, container logs, batched
  local writes) is orders of magnitude below what a local disk absorbs.
  A full queue **drops oldest**, but that is a failure mode, not a
  policy: telemetry loss is sanctioned only in a crash (§8.4); any
  steady-state drop is a defect signal — error-level counter
  (`agent_telemetry_sink_dropped_batches`) plus rate-limited warn.
  Delivery from the hub to each sink is at-most-once within one process.
  End-to-end log ingestion across follower restarts is at-least-once as
  specified in §8.2. The filesystem sink is the local source of truth;
  remote sinks are best-effort exports.

### 8.4 Filesystem sink: format, segmentation, retention, reads

**Format.** Four candidates, contrasted on what this store actually has
to do — absorb modest append volume, survive crashes, serve time-range
reads, and delete old data cheaply:

| | Framed protobuf (journal-style) | ndjson | Avro (object container) | SQLite |
| --- | --- | --- | --- | --- |
| Write path | append, trivial | append, trivial | block append | batched `INSERT` in WAL mode — ample headroom at our volumes |
| Crash behavior | CRC scan truncates torn tail | skip partial last line | resync on block markers | native (WAL): loses at most the last uncommitted batch |
| Read/query story | custom scan tooling | scan-only (greppable) | scan-only, schema'd | **indexed time-range queries + rowid-cursor tailing — exactly the fetch-logs/metrics shapes** |
| Retention | delete whole files | delete whole files | delete whole files | delete whole files (via segmentation below — never `DELETE`+vacuum) |
| Dependencies | none new | binary log bytes need base64 (bloat) | `apache-avro` (heavy, patchily maintained) | `sqlx` + bundled SQLite — the one real cost |
| Operator debugging | needs a dump tool | `less`/`jq` | needs tooling | `sqlite3` CLI everywhere |

**Chosen: SQLite, one database file per attempt-segment.** The read story
decides it: this sink is explicitly the backing store for coordinator
reads, and indexed range access, tailing, and ad-hoc operator queries
fall out for free. The two concerns raised against it dissolve under the
chosen shape: *write throughput* — one transaction per flush batch (a
flush every few seconds per container) in WAL mode is far below SQLite's
ceiling, and if a pathological job ever out-writes it that surfaces as a
§8.3 defect signal, not silent loss; *streaming reads* — a rowid cursor
over time-ordered rows pages naturally, no full-file scan. The
per-segment-file split (below) answers both the "one big DB" contention
and fragmentation questions and keeps retention as pure file unlinks.

**Rust interface: `sqlx`**, and this sets the convention for all SQL in
the repo. Queries use sqlx's compile-time checking in offline mode: the
segment schema lives in a migrations file, `sqlx prepare` (via
`cargo sqlx prepare`) caches query metadata into the checked-in `.sqlx/`
directory, and CI runs the prepare check so a query drifting from the
schema fails the build rather than the agent.

**Layout — segmented by time, uniformly for short and long jobs:**

```
<data_dir>/telemetry/<job-id>/<attempt-id>/
  seg-<start-timestamp>.db    # tables: meta, metrics, log_chunks (time-indexed)
```

A segment rolls at a size bound (default 256 MiB) or age bound (default
6h), whichever first. Segments are non-overlapping and time-ordered, so
a range read opens only the intersecting segments and concatenates.
Rolling is what keeps long-running jobs sane: no single ever-growing
file, old segments of a *live* attempt reclaimable under pressure (live
cap: at most `telemetry.live_retention`, default 24h, of closed segments
per running attempt), and any corruption contained to one segment. The
log follower's resume point (§8.2) is derived from `log_chunks` rows
across the live segments — nothing separate is stored, and segment
rolls need no alignment to log time. Recovery reads the latest stored
timestamp across the segment set but appends all replayed rows only to
the current open segment.

**Retention mirrors the image cache:** a sweep deletes segments once the
attempt has been ended for `retention` (default 60m) — except under §9
`High`/`Critical` pressure, where oldest-ended segments go early,
oldest-first, until below the mark. A live attempt's open segment is
never swept.

**Durability:** WAL mode with `synchronous=NORMAL`, chosen
deliberately. Against an *agent-process* crash this loses at most the
final uncommitted flush batch; against an OS crash or power loss,
transactions committed since the last WAL checkpoint may additionally
roll back — accepted for telemetry rather than paying `FULL`'s fsync
per commit (telemetry ≠ correctness data). Those crash scenarios are
the *only* sanctioned telemetry loss; steady-state loss is a defect
(§8.3).

**The read path is the point:** this sink is the backing store for
coordinator-initiated reads over the existing command stream. The sink
exposes a `TelemetryStore` read API (list attempts; read log chunks by
`(attempt, stream?, time range | tail_n)`; read metric samples by time
range) implemented as SQL over the segment set. The wire surface — new
`AgentCommand` arms `FetchLogs { attempt, range } → LogChunks` report arms
(chunked, flow-controlled) — is **deliberately out of scope for this
component** and lands with the API-edge work; the store API is designed
now so that RPC is a translation layer only.

## 9. Shared host pressure signal

`pressure.rs`: one small monitor sampling free space every 30s on (a) the
Docker data-root filesystem and (b) `data_dir`'s filesystem, publishing a
watch channel:

```rust
enum DiskPressure { Ok, High, Critical }   // default thresholds: 85% / 95% used
```

Consumers, in escalation order: telemetry retention sweeps early under
`High` (§8.4) → image cache evicts ahead of TTL under `High` (§7) →
under `Critical`, both sweep to floor and the agent refuses new `StartJob`s
with platform `StartError` ("disk critical") rather than wedging the node.
The future soft-limit killer (§6.1) consumes the same signal. Job kills are
**never** triggered by host pressure in v1 — only by each job's own limit.

## 10. Trait and contract deltas

The complete list of changes outside the new modules:

1. `ExitInfo.oom_killed: bool` → `cause: ExitCause`, and `ExitInfo`
   gains `finished_at: Timestamp` (§4 — Docker's `FinishedAt`, consumed
   by the session janitor's age check; the fake stamps it from its
   clock). Mechanical updates to `FakeExecutor`, `classify_exit`,
   session tests.
2. The limit-breach outcome renames (§4): `MemoryLimitExceeded` (was
   `OomKilled`), `RuntimeLimitExceeded` (was `MaxRuntimeExceeded`), and
   the new `DiskLimitExceeded` — core enum, proto (descriptor breaking
   gate), coordinator resolution table, journal encoding, web UI strings.
   Needs a short ADR amending ADR 0013 (user error, policy kill, no
   default retry, for all three).
3. New trait method `reap(allocation)` (§5); `FakeExecutor` gains the
   trivial impl; session calls it post-journal on the exit path.
4. `coppice-agent` gains the crate-level `describe_metrics()` /
   `gather_metrics()` fan-out (§8.1) — additive, wired into the same
   place the coordinator's is exported.
5. `Config::capacity_resources()` is replaced by
   `advertised_resources()` (= capacity − reservation, §6.4) at its two
   call sites (`Register`, `Heartbeat`).
6. `Config` additions (all defaulted; a bare v1 config stays valid):

   ```toml
   [executor]
   docker_host           = "unix:///var/run/docker.sock"
   disk_enforcement      = "auto"      # auto | quota | poll
   disk_poll_interval    = "30s"
   default_uid           = 65534
   pids_limit            = 4096
   reap_janitor_after    = "24h"
   whole_core_affinity   = true        # §6.3; false = NanoCpus only

   [reservation]                       # §6.4; deducted before advertising
   cpu_millis   = 1000
   memory       = "2GiB"
   disk         = "20GiB"

   [image_cache]
   ttl                  = "30m"
   max_concurrent_pulls = 2

   [telemetry]
   metrics_interval     = "10s"
   drain_force_after    = "10m"       # §8.2; forced tail loss is metered
   segment_max          = "256MiB"    # §8.4
   segment_max_age      = "6h"
   live_retention       = "24h"
   [[telemetry.sinks]]
   type = "filesystem"
   kinds = ["metrics", "logs"]
   retention = "60m"

   [pressure]
   high_pct = 85
   critical_pct = 95
   ```

Everything else — journal schema, agent proto, session logic, fencing —
is untouched.

## 11. Concurrency model

`DockerExecutor` supervises long-lived tasks (all spawned at construction,
all resilient to Docker daemon restarts via reconnect-and-resync):

| Task | Role |
| --- | --- |
| events | `docker events` stream → inspect → `ExitEvent` queue (feeds `next_exit`) |
| disk enforcer | poll strategy only: usage sweep + kill decisions |
| cache janitor | TTL/pressure eviction, inventory refresh |
| pressure monitor | statfs sampling → watch channel |
| per-container | one stats sampler + one log follower per running container, started on `start`/adoption, ended on exit + drain |
| telemetry drains | one per sink instance |
| retention sweep | telemetry directory GC |

Shared state is one `Mutex<ExecutorState>` (allocation → container record,
phase, claimed-exit set) — the agent runs O(dozens) of containers, not
thousands; no lock-free cleverness warranted.

## 12. Testing

- **Unit** (no Docker): the §3 mapping table (total over Docker states),
  §4 start-error classification, limits translation, disk budget
  arithmetic (image-larger-than-request), reservation arithmetic
  (advertised = capacity − reservation; config error when reservation ≥
  capacity), cpuset allocator
  (grant/release/pool-complement invariants over SMT and non-SMT fake
  topologies — grants always whole sibling groups, invariant-breach
  start refusal, rebuild-from-inspect), cache eviction policy over a
  fake clock, sink
  segment roll + range reads spanning segments + crash recovery of a
  torn WAL, retention sweep ordering (incl. live-attempt cap), pressure
  thresholds.
- **The existing crash/session suites stay on `FakeExecutor`** — they
  prove the agent core; the Docker impl must only honor the trait.
  `FakeExecutor` grows `reap` and the `ExitCause` field.
- **Gated integration suite** (`#[ignore]` unless a Docker daemon is
  reachable; a small busybox/alpine image set): start→exit 0 / exit N,
  OOM kill classification, stop grace (TERM-trapping container),
  truth-wins (exit racing stop), restart reconciliation (kill the agent
  process, container survives, `observe` adopts), idempotent start under
  name conflict, **privilege-escalation denial** (a workload that tries
  `su`/`sudo`, a setuid-root binary, and file capabilities must never
  observe euid 0 — proving the §6 posture: non-root UID +
  `no-new-privileges` + no `SETUID`/`SETGID`; asserted both by the
  binaries failing and by the process's own euid), poll-mode disk kill,
  log follow across simulated agent
  restart (whole-second at-least-once replay, §8.2 — including identical
  chunks in the boundary second, a boundary second split across a
  segment roll, a metrics-only open segment, partial and full daemon-log
  rotation, and repeated crashes during replay). These tests assert that
  every chunk returned by the daemon is retained, permit another copy of
  the boundary second per recovery, and verify the replay counter.
  Metrics sample sanity. xfs-quota
  mode gets
  a CI job on an xfs loopback mount, or stays manually verified if CI
  can't provide one — the strategy split is behind `DiskEnforcer` either
  way.

