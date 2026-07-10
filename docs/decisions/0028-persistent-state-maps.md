# 28. Persistent ordered maps for the job-scaled state

- **Status:** Accepted
- **Date:** 2026-07-11
- **Amends:** the state representation assumed by
  [coordinator-runtime.md](../architecture/coordinator-runtime.md#clone-cost-analysis)
- **Resolves:** the view-publication and snapshot-capture clone halves of
  [KOI-5](../roadmap/known-open-issues.md#koi-5-view-and-snapshot-publication-do-not-fit-the-1m-job-target)

## Context

The coordinator runtime publishes read views and captures snapshots by
cloning the whole `StateMachine` on the sole apply task. The clone-cost
analysis in coordinator-runtime.md was explicit that this is the design's
load-bearing risk: at the documented 1M-live-job target the state is roughly
3–4M `BTreeMap` entries (~1 GB), a deep clone runs hundreds of milliseconds
to about a second, and that is unacceptable against a 100 ms publish cadence
and a 10 ms strong-read spacing. The same document pre-named both the trigger
(sustained `p99(coordinator_view_clone_seconds) > 25 ms`, or clone time over
10% of the publish interval) and the escape hatch (persistent, structurally
shared ordered maps), deferring the decision to its own ADR.

The trigger metric was never implemented, but the arithmetic doesn't need it:
its own table shows the trigger condition holding from roughly 100k live jobs
upward — well inside the documented operating envelope. Waiting for a
production histogram to confirm a conclusion the design already reached on
paper would just delay the fix past the point where the scale target is
advertised.

## Decision

The four `StateMachine` maps that scale with job count — `jobs`, `attempts`,
`allocations`, and `accrual_queue` — become `imbl::OrdMap`. Cloning an
`OrdMap` is O(1) (a reference-counted root share); mutation pays O(log n)
with path copying, touching only the nodes along the mutated path.

`nodes` and `quota_entities` stay `BTreeMap`: they are bounded by cluster and
account size (~10³), their deep-clone cost is noise, and the stdlib map keeps
the cheaper per-operation profile where structural sharing buys nothing.

Consequences for the runtime design:

- **View publication** (`ViewPublisher::publish_at`) still clones on the
  apply task, but the clone is now O(1) in the job-scaled maps. The
  cadence/spacing machinery remains as a rate bound on watch-channel churn,
  not as protection against clone cost.
- **Snapshot capture** (`ApplyRequest::Snapshot`) shares the published view's
  clone (one clone serves both) and is likewise O(1).
- **Apply** pays the O(log n) path-copy overhead on every mutation. The
  recovery-replay benchmark bounds this cost (below); it is the price of the
  swap and is accepted.
- **Determinism is preserved:** `OrdMap` iterates in key order and implements
  structural `Eq`, so ordered iteration in apply, scheduler scans, snapshot
  encoding, and the determinism harness's state-equality assertions are
  unchanged.

## Measured cost

Benchmarks on the implementation commit (release mode, Apple Silicon dev
machine, otherwise idle; same flags before/after; `scan_decode` never touches
the state maps and is the no-change control):

| Measure | Before (`BTreeMap`) | After (`OrdMap`) |
| --- | --- | --- |
| recovery_replay/apply — 100k-command replay through `apply` | 24.3 ms | 25.5 ms (+5%) |
| recovery_replay/open/recovery — cold open incl. state rebuild | 10.0 ms | 10.4 ms (+4%) |
| recovery_replay/scan_decode (control) | 76.2 ms | 76.1 ms (±0) |
| 1M-job scheduler pass (real seating: 512 placements, ~500 revocations) | 667 ms | 842 ms (+26%; budget 5 s) |
| Whole-state clone at 1M jobs | est. hundreds of ms – ~1 s (never measured) | **5.4 ms** |
| 1M-job snapshot encode (streamed, 892 MB container) | — | 1.15 s |

The apply-path tax is ~5%, in line with imbl's advertised overhead. The
scheduler pass pays more (+26%): its full-queue scans are ordered-map
iteration, which is where a persistent tree costs most relative to a packed
B-tree — still 6× inside the pass's 5 s budget, and a candidate for revisit
only if a future scheduler budget tightens. The clone at 1M jobs lands at
5.4 ms (the residual is the still-deep-cloned small maps plus `Arc` fan-out),
under the 25 ms trigger with room to spare — versus an unpublishable
sub-second-to-second deep copy before. The ignored release-mode test
`clone_at_1m_is_structurally_shared` pins this bound.

## Alternatives considered

- **Wait for the metric trigger.** Rejected above: the design's own
  arithmetic already fires the trigger inside the documented envelope, and
  the histogram now exists to verify the fix rather than to justify it.
- **Copy-on-write snapshots via `Arc<BTreeMap>` per map.** An `Arc` around a
  whole map makes the *clone* free but turns every subsequent mutation into a
  full-map copy (`Arc::make_mut`), which is strictly worse under the
  coordinator's continuous-apply workload.
- **Swap all six maps for uniformity.** Buys nothing measurable (the two
  small maps clone in microseconds) and imposes the O(log n) path-copy tax on
  maps that mutate at heartbeat frequency.
