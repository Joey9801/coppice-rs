# coppice-testkit

Test and benchmark infrastructure for the storage and state layers. This crate
is **dev-only**: it appears solely in the `[dev-dependencies]` of other crates
and never in a shipping dependency graph. Its job is to make storage
crash-safety and state-machine behaviour testable *and reproducible from a
single logged number*.

Two mandates drive it: the crash-injection suite that gates any change to the
storage layer ([ADR 0002](../../docs/decisions/0002-openraft-with-custom-segment-storage.md)),
and the snapshot benchmark suite ([ADR 0018](../../docs/decisions/0018-protobuf-records-in-parallel-containers.md)).
Both are specified in [storage-testing](../../docs/architecture/storage-testing.md);
the engine they exercise is specified in [storage-engine](../../docs/architecture/storage-engine.md).

## Determinism first (`rng`)

Everything random in this crate flows from one seed through a hand-rolled
SplitMix64 generator. It is deliberately tiny and dependency-free: a crash-suite
failure is reproduced from its logged seed, so the seed→stream mapping must
never shift underneath us the way a `rand` upgrade can shift `StdRng`. A stream
test pins the first outputs as a reproducibility contract. Not cryptographic —
only adversarially varied. `fork()` derives independent child streams so, for
example, each simulated crash gets its own adversary without coupling to how
much randomness earlier work consumed.

## `simfs` — the fault-injecting filesystem

`SimFs` is a deterministic implementation of the `coppice_consensus::fs::Fs`
seam. A real filesystem on a healthy machine can't exercise the failure modes
the durable formats are built around — the page cache makes every write look
durable. `SimFs` models the cache explicitly: every operation is *visible*
immediately but *durable* only after the matching `sync_data`/`sync_dir`, with
file data and directory entries tracked separately. So an fsynced file whose
parent-directory `create_new` was never synced vanishes wholesale at a crash,
and an un-synced `remove_file` can resurrect its victim.

`crash(seed)` invokes a seeded adversary that independently decides the fate of
every un-synced operation — dropped, applied, or (for appends) torn mid-write,
at page granularity or an arbitrary byte. `set_crash_at(k)` additionally turns
every seam call into an enumerable crash point, killing the simulated process
at op *k* and poisoning the fs so later calls fail alike. Same seed and same
history always yield the same disk.

## `harness` — the crash-injection driver

The harness is the executable form of the crash contract. It drives a storage
implementation (anything implementing `CrashSubject`, over the opaque
`StorageOp` vocabulary — append, vote, truncate, purge, install-snapshot,
rotate) through a workload against `SimFs`, kills it at every seam-op index in
turn, runs recovery on each disk the adversary can produce, and asserts the
durability invariants. It keeps a `Model` of acknowledged state — an op enters
only once `apply` returned `Ok` — and requires the recovered state to be
explainable as *the acknowledged model plus at most the single in-flight op*
(any prefix, for a torn append). It also crashes *during recovery* to prove
recovery's own writes are crash-safe.

Coverage comes two ways: exhaustive named scenarios (`crash_sweep`) that pin
every crash point and adversary seed deterministically, and a seeded randomized
sweep (`random_sweep`, pinned by `COPPICE_CRASH_SEED`) that layers arbitrary op
mixes on top to find orderings the named tests didn't imagine. Every failure
panics with the full reproduction triple — scenario, crash point, adversary
seed — plus an expected-vs-observed diff and the invariant that fired.

## `toy` — a reference storage engine

A miniature storage engine implementing the manifest/segment/snapshot protocol
([ADR 0017](../../docs/decisions/0017-log-manifest-truncation-and-purge.md),
[ADR 0018](../../docs/decisions/0018-protobuf-records-in-parallel-containers.md))
and the container header ([ADR 0015](../../docs/decisions/0015-durable-format-versioning.md))
against the fs seam, stripped of protobuf and openraft so the crash orderings
are legible. It exists for two reasons: a harness that passes everything is
worse than none, so the suite needs a subject with *correct* orderings that
goes green and breaks loudly when an ordering is deliberately broken; and it is
executable documentation of the five-step recovery procedure. The real segment
engine replaces it as the harness subject when it lands, and must pass the
identical sweep.

## `synth` — synthetic state generation

`synth_state` builds a fully populated, internally consistent
`coppice_state::StateMachine` — jobs, attempts, allocations, nodes, and a
three-level quota-entity tree — deterministically from a seed. It feeds the
snapshot encode/decode benchmarks at the 1M-live-job scale required by ADR 0018,
and gives the determinism suite realistic states to replay commands against.
Both need the *same* generator, so a benchmark regression or a determinism
failure is reproducible from a logged number; every id is minted through the
testkit RNG, never `Uuid::new_v4`, or seeds stop reproducing.

Rather than replay through `StateMachine::apply` (far too slow at 1M jobs), it
constructs records directly, choosing only the legal
`JobState`/`AttemptState`/`AllocationState` combinations from the transition
tables in `coppice-core` and `coppice-state`, so consumers see the shapes apply
would have produced. `check_consistency` asserts the cross-reference and
accrual-queue invariants and is shared with the determinism suite, which runs
it on replayed states as well as synthetic ones.
