#!/usr/bin/env bash
#
# End-to-end demo of the `coppice` CLI against a throwaway local cluster:
# bring up `coppice dev`, submit a short dummy job, show `job status` and
# `job logs` returning real data, wait for the job to finish, tear down.
#
# Needs a reachable Docker daemon: the `fake` executor runs the job lifecycle
# without containers and captures no output, so logs would be empty.
#
#   scripts/demo.sh                            # build (debug) and run
#   KEEP_LOGS=1 scripts/demo.sh                # keep the work dir + dev.log
#   DEMO_DATA_DIR=/path scripts/demo.sh        # put dev state on another fs
#   DEMO_ITERATIONS=30 scripts/demo.sh         # a longer-running dummy job
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/coppice-demo.XXXXXX")"
DEV_LOG="$WORK_DIR/dev.log"
SPEC="$WORK_DIR/job.toml"
DEV_PID=""

# The image and its loop bound: ~12s of work with a log line every second, so
# `job status` is observably non-terminal and `job logs` has something to show.
IMAGE="${DEMO_IMAGE:-busybox:1.36}"
ITERATIONS="${DEMO_ITERATIONS:-12}"

say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
# The echoed command line goes to stderr so `run` stays usable in a capture.
run() { printf '\033[2m$ %s\033[0m\n' "$*" >&2; "$@"; }

# The `state` line of `job status`. Matched on the whole record (two fields)
# so the neighbouring `state since <timestamp>` line cannot be picked up.
job_state() {
  "$COPPICE" job status "$1" 2>/dev/null | awk '$1 == "state" && NF == 2 { print $2 }'
}

cleanup() {
  local status=$?
  if [[ -n "$DEV_PID" ]] && kill -0 "$DEV_PID" 2>/dev/null; then
    say "Tearing down the dev cluster (SIGINT to pid $DEV_PID)"
    # `coppice dev` waits on Ctrl-C and then runs its ordered shutdown
    # (agent session, task runtime, raft transport, consensus).
    kill -INT "$DEV_PID" 2>/dev/null || true
    for _ in $(seq 1 100); do
      kill -0 "$DEV_PID" 2>/dev/null || break
      sleep 0.1
    done
    kill -0 "$DEV_PID" 2>/dev/null && kill -9 "$DEV_PID" 2>/dev/null || true
    wait "$DEV_PID" 2>/dev/null || true
    echo "dev cluster stopped"
  fi
  if [[ "${KEEP_LOGS:-0}" == "1" ]]; then
    echo "artifacts kept in $WORK_DIR"
  else
    rm -rf "$WORK_DIR"
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

# --- 0. Preflight -----------------------------------------------------------

if ! docker info >/dev/null 2>&1; then
  echo "error: no reachable Docker daemon (needed for the docker executor)." >&2
  echo "       start Docker/Colima and retry." >&2
  exit 1
fi

# The agent's disk-pressure monitor watches the data dir's filesystem and
# refuses container starts at >=95% used, so a full host turns the demo into a
# run of start_failed attempts. Fail here with the real reason instead.
# DEMO_DATA_DIR relocates dev state (and therefore the watched filesystem).
DATA_DIR_ARGS=()
PRESSURE_PATH="$WORK_DIR"
if [[ -n "${DEMO_DATA_DIR:-}" ]]; then
  mkdir -p "$DEMO_DATA_DIR"
  DATA_DIR_ARGS=(--data-dir "$DEMO_DATA_DIR")
  PRESSURE_PATH="$DEMO_DATA_DIR"
fi
used_pct="$(df -P "$PRESSURE_PATH" | awk 'NR == 2 { gsub(/%/, "", $5); print $5 }')"
if [[ -n "$used_pct" && "$used_pct" -ge 95 ]]; then
  echo "error: the filesystem backing $PRESSURE_PATH is ${used_pct}% used." >&2
  echo "       the agent refuses container starts at >=95% (critical disk pressure)," >&2
  echo "       so every attempt would fail to start. Free space, or point the demo at" >&2
  echo "       a roomier filesystem with DEMO_DATA_DIR=/path/to/dir." >&2
  exit 1
fi

say "Building the coppice binary"
run cargo build --quiet --bin coppice --manifest-path "$REPO_ROOT/Cargo.toml"
COPPICE="$REPO_ROOT/target/debug/coppice"

# --- 1. Spawn the dev cluster ----------------------------------------------

say "Starting the dev cluster (coppice dev --executor docker)"
# The `[@]+` guard keeps an empty DATA_DIR_ARGS from tripping `set -u` on
# bash 3.2, which macOS still ships as /bin/bash.
"$COPPICE" dev --executor docker ${DATA_DIR_ARGS[@]+"${DATA_DIR_ARGS[@]}"} >"$DEV_LOG" 2>&1 &
DEV_PID=$!

# The banner is printed only after the in-process agent's registration has
# landed in applied state, so it *is* the readiness signal.
for _ in $(seq 1 600); do
  grep -q "Coppice dev is ready" "$DEV_LOG" && break
  kill -0 "$DEV_PID" 2>/dev/null || { echo "dev exited early:"; cat "$DEV_LOG"; exit 1; }
  sleep 0.5
done
grep -q "Coppice dev is ready" "$DEV_LOG" || { echo "timed out waiting for dev"; tail -30 "$DEV_LOG"; exit 1; }

sed -n '/Coppice dev is ready/,/Press Ctrl-C/p' "$DEV_LOG"

API="$(sed -n 's#.* API  *\(http://[^ ]*\)/api/v1.*#\1#p' "$DEV_LOG" | head -1)"
QUOTA_ENTITY="$(sed -n 's#.* Quota entity  *\(quota-[0-9a-f-]*\).*#\1#p' "$DEV_LOG" | head -1)"
[[ -n "$API" && -n "$QUOTA_ENTITY" ]] || { echo "could not parse the dev banner"; exit 1; }
export COPPICE_API="$API"

# --- 2. Submit a dummy job --------------------------------------------------

cat >"$SPEC" <<EOF
image = "$IMAGE"
command = ["sh", "-c", "i=1; while [ \$i -le $ITERATIONS ]; do echo \"tick \$i/$ITERATIONS from \$(hostname)\"; [ \$((i % 4)) -eq 0 ] && echo \"checkpoint \$i\" >&2; i=\$((i + 1)); sleep 1; done; echo done"]
quota_entity = "$QUOTA_ENTITY"
priority = 0
max_runtime = "2m"

[resources]
cpu_millis = 500
memory = "128MiB"
disk = "512MiB"
EOF

say "Job spec"
cat "$SPEC"

say "Submitting the job"
SUBMIT_OUT="$(run "$COPPICE" job submit "$SPEC")"
echo "$SUBMIT_OUT"
JOB_ID="$(printf '%s' "$SUBMIT_OUT" | sed -n 's#^submitted \(job-[0-9a-f-]*\).*#\1#p' | head -1)"
[[ -n "$JOB_ID" ]] || { echo "could not parse the submitted job id"; exit 1; }

# --- 3. Status while it runs ------------------------------------------------

say "Waiting for the job to start running"
for _ in $(seq 1 240); do
  state="$(job_state "$JOB_ID" || true)"
  case "$state" in
    attempting|succeeded|failed|aborted) break ;;
  esac
  sleep 0.5
done

say "coppice job status (mid-flight)"
run "$COPPICE" job status "$JOB_ID"

# --- 4. Logs ----------------------------------------------------------------

# Give the container a few seconds of output before the first read, so the
# page is non-empty rather than racing the first line.
sleep 5

say "coppice job logs (first read, chronological)"
run "$COPPICE" job logs "$JOB_ID"

say "coppice job logs --stream stderr"
run "$COPPICE" job logs "$JOB_ID" --stream stderr

# --- 5. Wait for completion -------------------------------------------------

say "Following the log until the job is terminal (coppice job logs --follow)"
run "$COPPICE" job logs "$JOB_ID" --follow

say "Waiting for the terminal state to be applied"
FINAL=""
for _ in $(seq 1 240); do
  FINAL="$(job_state "$JOB_ID" || true)"
  case "$FINAL" in
    succeeded|failed|aborted) break ;;
  esac
  sleep 0.5
done

say "coppice job status (final)"
run "$COPPICE" job status "$JOB_ID"

if [[ "$FINAL" != "succeeded" ]]; then
  echo "job ended in state '$FINAL' (expected succeeded)" >&2
  exit 1
fi
say "Job $JOB_ID succeeded"

# Teardown happens in the EXIT trap.
