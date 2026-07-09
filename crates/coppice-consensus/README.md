# coppice-consensus

The async seam between the coordinator control plane and Raft. It is a **thin
adapter over openraft 0.9** ([ADR 0002](../../docs/decisions/0002-openraft-with-custom-segment-storage.md)),
not an abstraction meant to swap Raft libraries: openraft owns election,
replication, and membership-change correctness, and this crate converts at that
boundary so no openraft type appears in any other crate's signatures.

## The openraft-free surface

Everything downstream consumes the types defined here, never openraft's:

- `Consensus` — propose commands, take a linearizable read barrier, drive
  membership changes, trigger snapshots. The leader accepts writes; followers
  redirect via `ConsensusError::NotLeader`. `OpenraftConsensus` is the only
  implementation.
- `StateView` / `StateViews` — immutable, cheaply cloned snapshots of applied
  state, published through a `watch` channel (no locks on the read path).
- `EventTap` / `EventTapReceiver` — the derived event stream.
- `ConsensusError` — every failure a caller can observe, with a
  retryable/terminal split (`is_retryable`).

`node::start` is the entry point: it runs the identity matrix, opens or stamps
the segment store, spawns the apply task, builds the openraft node with its gRPC
transport, and hands back a `StartedNode`. The full wiring — which tasks own
what, and shutdown ordering — is in
[coordinator-runtime](../../docs/architecture/coordinator-runtime.md).

Two coordinates run through the whole design and must not be conflated: the Raft
applied **log index** (the read/event cursor of
[ADR 0007](../../docs/decisions/0007-per-endpoint-read-consistency.md)) and the
state machine's **version** (the applied-command count the scheduler uses for
`expected_version`). `StateView` exposes both.

## The apply loop

`apply_loop::run` is the canonical single-writer of `coppice_state::StateMachine`.
It owns the state by value with no lock — nothing else can name it, so a `&mut`
across an `.await` is impossible by construction. Committed entries arrive from
the openraft state-machine adapter over a small bounded channel; the adapter
awaits the reply, so backpressure lands on openraft's replication rather than on
a lock. Each batch runs `apply → emit events → publish view → reply`, in exactly
that order, and never awaits a full channel.

The actual state-machine `Command`/apply logic — what each command does and how
it can be rejected — lives in **coppice-state**, not here. This crate only
sequences and publishes it.

## Reads and consistency

The apply task publishes `StateView`s; every other subsystem (API read path,
scheduler, event fanout) reads views. A strong read pairs
`Consensus::read_index` (a leader-confirmed barrier index N) with
`StateViews::at_least(N)`, which waits for a published view at that index or
beyond. Publishing is cadence-bounded to cap the full-state clone rate, with a
demand signal so an outstanding strong read is served early instead of waiting
out the cadence. See
[ADR 0007](../../docs/decisions/0007-per-endpoint-read-consistency.md).

## Events

Events are *derived, not authoritative*: apply produces them as a side output,
keyed by applied log index. They are not replicated — every replica derives the
identical stream. Emission must never block apply, so the tap is a bounded
channel with `try_send`; on overflow the batch is dropped and the receiver
synthesizes a `TapItem::Gap` so downstream clients resync.

## The segment storage engine

The `storage` module is the custom segment-file store that implements openraft's
`RaftLogStorage` and `RaftStateMachine` traits
([ADR 0002](../../docs/decisions/0002-openraft-with-custom-segment-storage.md)) —
the deliberately-accepted cost of purpose-built files over an embedded KV store.
It is written against the small synchronous `fs` seam (`Fs`/`FsFile`), which
exposes exactly the durability events the formats need so both `RealFs` and the
fault-injecting `SimFs` in coppice-testkit implement it and the crash-injection
suite sees every byte. The on-disk layout, framing, and recovery algorithm are
documented in [storage-engine](../../docs/architecture/storage-engine.md); the
format ADRs are the source of truth:

- Two-layer format versioning —
  [ADR 0015](../../docs/decisions/0015-durable-format-versioning.md).
- Manifest, logical log truncation, and ordered segment purge —
  [ADR 0017](../../docs/decisions/0017-log-manifest-truncation-and-purge.md).
- Protobuf records inside parallel-decodable containers —
  [ADR 0018](../../docs/decisions/0018-protobuf-records-in-parallel-containers.md).

### Snapshots

Snapshots are file-backed (`SnapshotData = SnapshotFile`), never an in-memory
buffer, so a snapshot streams disk-to-disk through openraft's install-snapshot
(the `generic-snapshot-data` feature). The container is sharded into independent
per-section streams that encode and decode across cores, and a rebuilding node
can begin decoding sections as chunks arrive rather than after full transfer
([ADR 0018](../../docs/decisions/0018-protobuf-records-in-parallel-containers.md)).

## Membership and rebuild

Coordinator node IDs are allocate-once **instance identities, never reusable
slots** ([ADR 0016](../../docs/decisions/0016-coordinator-rebuild-learner-join.md)).
A rebuilt replica always joins with a fresh ID and an empty directory; the
departed ID is removed. `start` enforces the identity matrix — restart vs
bootstrap vs join — and fail-stops rather than starting "helpfully" on an
unexpectedly empty directory. Rebuild is a learner join: `add_learner` brings a
fresh node up via snapshot install plus log replay with no quorum impact, and
`promote_voter` raises it to voter (optionally removing a departed voter in the
same joint change) only once its replication lag is within `PROMOTION_LAG_MAX`.

## Boundaries

- No openraft type crosses out of the `adapter` and `net` modules; the error
  zoo is mapped to `ConsensusError` at that boundary.
- The state-machine command semantics live in **coppice-state**; the durable
  format details live in the docs above. This crate is the sequencing,
  publishing, and Raft-integration seam between them.
- The Raft transport is mounted on the coordinator's mTLS mesh; this crate
  builds the tonic service (`RaftTransportServer`) but does not run the server —
  the coordinator mounts it.
