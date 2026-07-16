# 33. Explicit time types: `Timestamp` and `Duration` over raw microseconds

- **Status:** Accepted
- **Date:** 2026-07-16
- **Amends:** [ADR 0031](0031-http-api-surface.md) (narrows its "integers as
  JSON numbers" DTO convention; the rest of that ADR stands)
- **Relates to:** [ADR 0019](0019-deterministic-quota-arithmetic.md) (the
  determinism rules the µs quantisation serves),
  [ADR 0018](0018-protobuf-records-in-parallel-containers.md) /
  [ADR 0003](0003-protobuf-serialization-and-cluster-version-gates.md) (the
  protobuf encoding, deliberately unchanged)

## Context

Every instant and every span in the workspace was a bare integer of
microseconds, distinguished only by an `_us` suffix on the field name:
`submitted_at_us: i64`, `max_runtime_us: Option<u64>`, `tick_us: i64`. Some
409 such sites existed across eleven crates.

The suffix is a comment. It does not stop a millisecond, a second, or a
duration from being assigned to an instant, and the compiler has nothing to
say about any of it. The failure mode is quiet and uniform: a factor of
1000, in a value nobody eyeballs.

The same representation leaked onto the `/api/v1` surface, where it is worse.
An integer on the wire carries neither its epoch nor its scale, so every
client — browser, CLI, future REST consumer — has to be *told* what
`"submitted_at_us": 1760000000000000` means and re-derive it correctly. The
web client duly mirrored the idiom (`submittedAtUs: number`), and mixed it
with a JavaScript `Date.now()` that is milliseconds, putting a ×1000
conversion in front of every consumer.

Two consumers constrain any replacement:

- **The replicated state machine.** Quota decay divides timestamps into ticks
  and every replica must compute a bit-identical answer from the same
  committed commands (ADR 0019). Sub-microsecond precision reaching
  replicated state is a divergence bug the first time it crosses a boundary
  that rounds it.
- **The protobuf corpus.** It encodes instants as `int64` Unix microseconds
  and durations as `int64`/`uint64` microseconds, and it is frozen behind a
  descriptor-diff breaking gate. Whatever the domain uses must encode into
  that exactly.

## Decision

### Two newtypes in `coppice_core::time`

`Timestamp` wraps `chrono::DateTime<Utc>`. `Duration` is a signed span.
Both are **quantised to whole microseconds**, and that invariant is the
reason they are newtypes rather than the bare chrono types: a bare
`DateTime<Utc>` carries nanoseconds it cannot encode, so a value would not
survive its own protobuf round trip, and a nanosecond that reaches the state
machine is an ADR 0019 divergence. Construction truncates toward −∞, so
quantisation is idempotent and order-preserving.

Three properties fall out, each load-bearing:

- **`Duration` is signed**, because it is the difference of two `Timestamp`s
  and those regress — command timestamps come from different leaders, and a
  leader change can hand apply an instant earlier than the last one. The
  clock-skew rule of ADR 0019 is now written as `.max(Duration::ZERO)` at the
  call sites that want it, rather than implied by an unsigned type that would
  wrap. Where a negative span is meaningless — `quota::runtime_seconds_ceil`
  — it clamps explicitly, because reinterpreting −1 µs as `u64` would charge
  ~584 000 years of runtime.
- **`Duration`'s range is exactly `i64` microseconds**, not `TimeDelta`'s.
  `TimeDelta` reaches ~±292 000 *years* and its own `MAX` has no microsecond
  representation at all (`TimeDelta::MAX.num_microseconds()` is `None`), so
  wrapping it directly would have admitted values that cannot be encoded.
  Storing `i64` µs makes `as_micros` total and exact, and makes "every
  `Duration` survives the wire" true by construction. (`TimeDelta::checked_mul`
  also does not bound-check against its own `MAX`; not depending on it avoids
  the question.)
- **`Timestamp::from_micros` is fallible.** `i64` µs spans ~±292 000 years,
  `DateTime<Utc>` only ~±262 000, so the wire type is *wider* than the domain
  type. The gap is unreachable by any honest producer but reachable by a
  corrupt record or a hostile peer, so decoding rejects it rather than
  saturating — a timestamp from the year 300 000 is a decode failure, not a
  very old job. The other direction is total, which is what lets
  `coppice_proto::convert` keep its "domain → pb is infallible" contract.

`Timestamp::now()` is the workspace's single clock read, and it belongs to
proposers and read handlers. It must never be called from apply.

### The protobuf wire does not change

Not one `.proto` file is edited; the descriptor breaking gate passes
untouched. Instants stay `int64` Unix microseconds, durations stay
`int64`/`uint64` microseconds. Microseconds are a *good* binary encoding —
fixed-width, exact, cheap — and the problem was never the wire, it was the
absence of a type between the wire and the code. `coppice_proto::convert`
maps at the boundary, which is where the validation now lives.

### The JSON surface changes

Instants become ISO 8601 strings, durations become `_seconds`-suffixed
numbers. See the ADR 0031 amendment for that decision and its reasoning.
`web/src/api/types.ts` mirrors it with `Date` for instants — the same
split as Rust: an explicit domain type inside, an unambiguous string on the
wire.

## Consequences

**Easier.** Unit errors in time arithmetic are compile errors. Instant-minus-
instant yields a `Duration` and instant-plus-instant does not typecheck.
Magic constants become self-describing (`72 * 3_600 * 1_000_000` is
`Duration::from_hours(72)`). API responses are readable and self-describing,
and a client that mishandles time now fails loudly at parse rather than
quietly by a factor of 1000. Seven hand-rolled
`SystemTime::now() → duration_since(UNIX_EPOCH) → as_micros()` helpers
collapse into `Timestamp::now()`; three of them silently mapped a clock
error to the Unix epoch via `unwrap_or(0)`, and that fallback is gone.

**Harder.** `std::time::Duration` still exists at the tokio boundary, so
files needing both import one and alias the other, crossing over with
`Duration::to_std()`. Test fixtures constructing an instant from a literal
need `Timestamp::from_micros(..)` and its `Option`, which the test modules
wrap in a local `ts()` helper — the price of the range check being real.
`chrono` joins the dependency set of every crate that names an instant.

**Unchanged.** Replicated arithmetic is bit-identical: the decay path still
divides `i64` microseconds by an `i64` tick with `div_euclid`, and the
quota property tests — exact decay composition, the clock-skew no-op — pass
unmodified in substance. The synthetic-state generator produces
byte-identical fixtures before and after, confirming the refactor is
representation-only.
