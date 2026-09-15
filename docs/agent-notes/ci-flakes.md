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

- **Docker telemetry segment creation** (issue #112) — dropping the
  telemetry hub aborts its drain tasks, and an abort landing inside
  `create_segment` between the sqlite file's creation and its schema
  migration left a schema-less `seg-*.db` that poisoned every later reader
  with `no such table`; the `docker_executor` harness was folding that
  `Err` into a misleading "no log rows" failure. Fixed by building segments
  under a `.tmp` name and renaming into place once the schema and `meta`
  rows are committed (the sweep reclaims stale temps), and by making the
  harness surface store errors.
- **`a_join_interrupted_at_every_step_resumes_the_same_identity_and_converges`**
  (issue #148) — the `PromoteVoterIssued` halt once landed in phase
  `joining` on a loaded runner. The convergence loop heard `AddLearner`
  succeed on the admin channel and went straight on to `ClusterStatus` and
  `PromoteVoter`, while the leader's first append — the only way the joiner
  learns of its own seat, and of who leads — had not arrived; the leader's
  key transfer then dialled back to a joiner that knew no leader, was
  refused, and came back as an endpoint-verification failure. Fixed by
  making the loop wait, at the probe cadence, until its *own* membership
  holds its seat before it asks the leader anything further
  (`convergence.rs`, step 4's local half). Reproduce deterministically with
  the `raft-append-entries-received` gate on the joiner — see
  `a_joiner_waits_to_see_its_own_seat_before_asking_for_promotion` — which
  holds the joiner's inbound replication still while its loop keeps
  ticking; a plain CPU-hog loop never hit it locally in 55 iterations.
- **`oom_classification`** — the CI daemon can occasionally lose the OOM
  notification entirely due to a cgroup v2 race; the `Killed` verdict is
  correct by design in that case, so the fix was retrying the test on a
  `Killed` result rather than treating it as a hard failure.
- **`daemon_log_rotation_bounds_catchup`** (issue #139) — the daemon's own
  `docker logs --follow` skips a whole file when its follower falls two
  rotations behind between reads (moby logs "file rotations were missed
  while following logs; some log messages have been skipped over"; nothing
  on the API reports it). The test rotated 8k files of ~1 KiB lines, so a
  sub-second daemon stall inside the stop grace skipped a still-retained
  file and the retention oracle blamed the executor. Fixed by having the
  printer emit a fixed number of lines and park, and adopting only once the
  daemon's files are final, so no rotation can race the follower's tail
  read; the failure now names the first missing line and prints the daemon
  and stored line heads. Reproduce a daemon-side skip locally by
  `kill -STOP`-ing dockerd inside the Colima VM for ~0.5 s on the `kill`
  event of a fast-rotating container, then `kill -CONT`.

## Filing a new flake

If you've confirmed (via fresh dispatch, not a rerun) that a failure is a
genuine, previously-unseen flake unrelated to the change under review, file
a GitHub issue with: the run IDs, the relevant pasted log lines, a cause
hypothesis, and a fix sketch. Don't just leave a comment noting it — an
issue is what lets the next person find it instead of re-diagnosing from
scratch.
