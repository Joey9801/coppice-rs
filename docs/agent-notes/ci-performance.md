# CI performance notes

The fleet integration test suite went from ~10 minutes to well under 4
minutes wall-clock via two changes, worth knowing if CI time regresses
again:

- **Shard the fleet tests across a matrix** (see
  `scripts/check-fleet-shards.sh`) instead of running them serially in one
  job. The script also acts as a guard that every fleet test is assigned to
  exactly one shard — keep it free of `cargo` invocations so it stays fast
  to run as a pure partition check.
- **Test-only pacing knobs** — `[pacing]` and `[token_kdf]` config sections
  that only exist for tests let the fleet tests use much shorter Raft
  election/heartbeat timings and a cheap KDF instead of the production
  defaults, without touching production code paths.

## Debug-build crypto is slow — budget for it

Tests that exercise real cryptography in an unoptimized debug build are
disproportionately expensive compared to release builds:

- **argon2** password hashing costs roughly 300ms per hash in a debug
  build. Any test that hashes more than a handful of passwords will show up
  as a slow outlier — use the lowest-cost argon2 params the test allows, or
  the `[token_kdf]` test knob above, rather than production parameters.
- **reqwest with the native-roots feature** can stall for ~12 seconds
  loading the system trust store on first use in a test process. If a test
  that touches TLS/HTTPS is mysteriously slow to start, this is a likely
  cause — prefer a test-only root store or reuse a shared client rather
  than triggering the native-roots load repeatedly.
