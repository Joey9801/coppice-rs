# CI flakes and how to tell a real failure from one

## Rerun vs. fresh dispatch

A GitHub Actions **rerun** of a failed workflow run is not reliable evidence
either way. We've seen a rerun of the same run fail 4/4 times while a fresh
`workflow_dispatch` off the exact same tree passed cleanly — reruns can carry
some kind of context/caching artifact from the original run that a clean
dispatch doesn't have. When you need to know whether a failure is real,
bisect with **fresh `workflow_dispatch` runs on control branches off `main`**,
not reruns of the failing run.

## Fleet-suite flakes

The multi-node "fleet" integration tests (`crates/coppice-coordinator/tests/
sso_fleet.rs`, `refleet.rs`, sharded by `scripts/check-fleet-shards.sh`) are
the most flake-prone part of the suite because they spin up real Raft
clusters under CI's shared, often CPU-throttled runners. Known root causes so
far:

- **`set-address` wedged by a joint→uniform membership entry** — a dial
  override + retry fixed the product-side race.
- **`rotate_ca` turnover poll raced a lagging follower's stage-phase
  status** — fixed by pinning the incoming-root serial in the test rather
  than polling a moving target.
- **Unbounded `Fleet::stop()` join with no nextest terminate** — a hung
  shard doesn't get killed by the test harness, so one wedged leader
  graceful-shutdown (suspected: mid-membership-change) can burn the whole CI
  budget instead of failing fast.

If a fleet shard hangs or fails intermittently, reproduce locally first with
a CPU-hog running alongside the test binary in a tight loop — this
reproduces the CPU-starvation conditions CI runners create far more
reliably than running the test alone.

## Docker Hub anonymous pull-rate limit

`crates/coppice-agent/tests/docker_executor.rs` pulls images anonymously
from Docker Hub, both locally and in CI. Anonymous pulls are rate-limited;
hitting the limit surfaces as a `429` and takes roughly 30 minutes to clear.
This is a live flake vector for any CI run that touches the Docker executor
tests, not just a local annoyance — if you see a `429`-shaped Docker pull
failure in CI, that's the likely cause, not a real regression.

## Other resolved flakes (context if you hit something similar)

- **Docker telemetry segment creation** — a Docker Hub pull abort mid-drain
  could leave `create_segment` writing a schema-less sqlite segment that
  poisoned later readers, and the test harness was swallowing the
  underlying `Err`. Fixed by creating segments under a temp name and
  renaming into place atomically once the schema is written.
- **`oom_classification`** — the CI daemon can occasionally lose the OOM
  notification entirely due to a cgroup v2 race; the `Killed` verdict is
  correct by design in that case, so the fix was retrying the test on a
  `Killed` result rather than treating it as a hard failure.

## Filing a new flake

If you've confirmed (via fresh dispatch, not a rerun) that a failure is a
genuine, previously-unseen flake unrelated to the change under review, file
a GitHub issue with: the run IDs, the relevant pasted log lines, a cause
hypothesis, and a fix sketch. Don't just leave a comment noting it — an
issue is what lets the next person find it instead of re-diagnosing from
scratch.
