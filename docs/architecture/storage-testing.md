# Storage testing

This document specifies the test infrastructure that guards the Raft storage
layer: the filesystem seam the engine is written against, the SimFs crash
model, the crash-injection harness and its invariants, the storage benchmark
suite, and openraft's compliance suite. It is the implementation contract for
`coppice-testkit` and for anyone changing storage code.

**The gating rule, restated from [ADR 0002](../decisions/0002-openraft-with-custom-segment-storage.md)
and [ADR 0018](../decisions/0018-protobuf-records-in-parallel-containers.md):
changes to the storage layer do not merge while the crash-injection suite,
the storage benchmarks, or the compliance suite are red.** The suites exist
*before* the engine so the engine is written against them, and so that later
contributors can change storage code without being able to silently destroy
its crash-safety.

The durable formats and orderings under test are settled elsewhere and not
restated: segment/vote/manifest layout and the recovery procedure
([ADR 0017](../decisions/0017-log-manifest-truncation-and-purge.md)),
container versioning and identity stamps
([ADR 0015](../decisions/0015-durable-format-versioning.md),
[ADR 0016](../decisions/0016-coordinator-rebuild-learner-join.md)), and the
snapshot container ([ADR 0018](../decisions/0018-protobuf-records-in-parallel-containers.md)).

## The filesystem seam

`coppice_consensus::fs::{Fs, FsFile}` is the only way storage code touches
disk. The trait exposes exactly the durability events the format ADRs
distinguish — append, data fsync, rename, delete, parent-directory fsync,
directory scan, and the `LOCK` file — and nothing else. `RealFs` maps it
thinly onto `std::fs`; `coppice_testkit::simfs::SimFs` implements the same
contract with fault injection. `fs::write_atomic` is the single
implementation of the ADR 0017 atomic-swap discipline (write-new → fsync →
rename → parent-dir fsync); engine code calls it rather than restating the
sequence.

Two rules keep the seam meaningful:

- **Storage code never bypasses the seam.** An `std::fs` call in the storage
  layer is invisible to the crash suite and therefore untested by
  construction; there is no legitimate reason for one.
- **The seam is synchronous.** openraft's storage traits are async; the
  engine adapts via `spawn_blocking` over one shared engine mutex
  (openraft 0.9 serializes storage write IO through its core loop, so a
  dedicated writer thread would add machinery without changing the fsync
  schedule — see `storage-engine.md`). Decided once, for two reasons: the
  commit path *is* code that blocks on `fdatasync`, and running it inline
  on the tokio pool adds scheduling latency to every commit; and a sync seam
  has no await points, so a simulated crash at operation *k* is exactly
  reproducible from a seed instead of racing the runtime's scheduling.

## The SimFs crash model

SimFs simulates the durability semantics of a POSIX filesystem over a page
cache, not a disk that merely "fails writes":

- **Visible ≠ durable.** A write is visible to every reader immediately but
  survives a crash only if `sync_data` covered it. A create, rename, or
  delete is visible immediately but survives only after `sync_dir` of the
  parent directory. The interactions are modeled faithfully: a file whose
  data was fsynced but whose creation was never made durable **vanishes** at
  a crash; an un-synced delete means the file **reappears**.
- **A seeded adversary decides the fate of everything un-synced.** At
  `crash(seed)`, every un-fsynced data write is independently *dropped*,
  *applied*, or *torn* — a prefix survives, torn at a configurable
  granularity (default 4 KiB) with occasional arbitrary-byte tears to model
  sub-page writes. A gap left by a dropped earlier write under a surviving
  later one reads as zeros, as a page cache would leave it. Every un-synced
  namespace op independently did or did not happen; rename stays atomic
  (old file or new, never a mixture).
- **Crash points are exhaustive, not hand-picked.** Every seam call
  increments an operation counter; the harness can arm SimFs to "crash" at
  any operation index. From that point the fs is poisoned — all calls fail —
  so error-path cleanup in the code under test cannot cheat the crash.
- **Determinism.** All randomness flows from `coppice_testkit::rng::Rng`
  (SplitMix64, hand-rolled so its seed→stream mapping can never change out
  from under logged seeds). One `(workload, crash op, adversary seed)`
  triple reproduces a failure exactly.

**Out of scope, deliberately:** sector-level corruption of *already-durable*
bytes, firmware that lies about flush completion, misdirected writes, and
bit rot. The format's answer to at-rest corruption is detection (per-entry
and per-section CRC32C) plus fail-stop into the `coordinator replace` path
(ADR 0016) — recovery must never guess about committed state (ADR 0017).
The crash suite verifies the *detection and refusal*, not the hardware.
Likewise out of scope: deterministic simulation of consensus, network, or
the whole coordinator. That is a possible future harness; this one owns the
storage layer only.

## The crash harness

`coppice_testkit::harness` drives a storage implementation through a
workload against SimFs, crashes it at every operation index in turn, runs
recovery on the surviving state, and checks invariants. The storage
implementation plugs in through the `CrashSubject` trait (initialize a data
dir, open through recovery, apply operations, expose observed state); the
workload vocabulary is the abstract operation set of the log storage layer:
append batches, vote updates, suffix truncation, purge, snapshot
installation, forced rotation.

The harness keeps a **model of acknowledged state**: an operation is in the
model iff the subject returned success for it. Because the driver is
single-threaded, at most one operation is in flight at a crash, so the
recovered state must be explainable as *the acknowledged model, plus at most
the in-flight operation* — fully applied for atomic operations, or any
prefix of the batch for an in-flight append.

Invariants asserted after every recovery:

1. **Durability.** Every acknowledged log entry (not superseded by an
   acknowledged truncation or purge) is present with intact payload.
2. **At-most-one in flight.** Anything beyond the acknowledged model is
   explainable by the single in-flight operation (a prefix, for appends).
3. **Contiguity.** The recovered log is a contiguous index range; the
   manifest claims ≤ what physically exists ("pessimistic truth").
4. **Vote monotonicity.** The recovered vote is never older than the last
   acknowledged vote.
5. **Snapshot integrity.** At most one snapshot is current; a snapshot
   without a valid footer (truncated by construction) is never adopted; the
   previous snapshot survives until the new one is durable.
6. **Recovery idempotence.** Recovery may itself crash at any operation
   index; recovering again from the result must succeed and reach the same
   observed state. Opening an already-recovered directory twice observes
   identical state.

Each mandated crash scenario — kill during **append**, **snapshot install**,
**segment rotation**, **suffix truncation**, **purge**, and **manifest
swap** (ADR 0002 + ADR 0017) — is a named test whose workload forces that
event, swept across *every* crash point it contains. A seeded randomized
sweep layers arbitrary workloads and adversary seeds on top.

### Reproducing a failure

A harness failure panics with the full reproduction triple plus a listing of
the crashed disk, e.g.
`crash harness failure: scenario=purge crash_at=75 adversary_seed=0xa62d…: <invariant>`
(crashes injected *during recovery* extend the triple with
`recovery_crash_at` and `recovery_seed`).
Re-running the named test reproduces it (sweeps are exhaustive and
deterministic); for the randomized sweep, set `COPPICE_CRASH_SEED=<seed>` to
pin the master seed. Never fix a reproduced failure without adding its
scenario to the named tests if it wasn't already covered.

### The toy engine

The real segment storage engine (`docs/architecture/storage-engine.md`) now
implements `CrashSubject` in `crates/coppice-consensus/tests/crash_storage.rs`
and runs the full crash sweep against itself. The harness also still runs
against `coppice_testkit::toy` — a miniature storage engine implementing the
full ADR 0017 protocol (segments with CRC32C-framed entries, manifest-first
orderings, atomic-swap vote file, footer-last snapshots, the five-step
recovery procedure) over the fs seam, with simplified binary encodings in
place of protobuf. It exists for two reasons: it proves the harness catches
real bugs (its development is the harness's own crash test), and it is
executable documentation of the recovery procedure. The toy remains as the
harness's self-test.

## The benchmark suite

ADR 0018's thesis — *serialization is never the limiter; the serial apply
loop is* — is checked, not assumed. `coppice-consensus/benches` carries the
three mandated families, structured now, with engine-bound measurements
filled in when the engine lands:

- **Append throughput and latency under group commit** — batched appends
  through the seam on a real filesystem; the fsync is the expected limiter.
- **Snapshot encode/decode at the 1M-live-job scale** — over synthetic
  states from `coppice_testkit::synth` (ADR 0012's retention target sets the
  scale). Decode must feed a rebuild faster than install-snapshot streams.
- **Cold-recovery replay rate** — entries/second through recovery scan +
  decode; must exceed the apply loop's rate or recovery becomes the
  bottleneck ADR 0018 promises it isn't.

Regressions gate merges exactly as crash-suite failures do. Benchmarks run
on a real filesystem (tmpfs acceptable for CI trend lines; fsync numbers are
only meaningful on a durable device).

## The compliance suite

openraft ships a storage test suite (`openraft::testing::Suite`); ADR 0002
requires passing it. The wire-up point is
`crates/coppice-consensus/tests/openraft_compliance.rs`, feature-gated
(`storage-compliance`) until the engine exists, with instructions in-file:
flip it on, make it green, then delete the gate so it runs unconditionally.
