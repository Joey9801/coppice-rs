#!/usr/bin/env bash
#
# Shared helpers for the AWS demo scripts (docs/roadmap/aws-demo-plan.md).
#
# Sourced, never executed:
#
#   source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
#
# Every script here must work from any cwd, so paths are derived from this
# file's own location rather than from $PWD.

# Constants below are consumed by the scripts that source this file, not here.
# shellcheck disable=SC2034

# Repo root: this file lives at <repo>/scripts/aws-demo/lib.sh.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# The demo lives in one account, one region, one bucket. They are overridable
# for a fork, but nothing in these scripts should ever have to think about it.
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-426363836540}"
REGION="${REGION:-eu-west-2}"
STATE_BUCKET="${STATE_BUCKET:-coppice-terraform-state-${AWS_ACCOUNT_ID}}"
DOMAIN="${DOMAIN:-coppice.jwjr.uk}"

# The two Terraform stacks. `bootstrap` is applied once per account by
# scripts/aws-demo/bootstrap.sh; `env` is applied once per demo environment.
ENV_DIR="$REPO_ROOT/deploy/aws/env"
BOOTSTRAP_DIR="$REPO_ROOT/deploy/aws/bootstrap"

# Colour only when stdout is a terminal — these scripts are routinely run from
# CI logs and from `tee`, where escape codes are noise.
if [ -t 1 ]; then
  C_RESET=$'\033[0m'
  C_BLUE=$'\033[34m'
  C_YELLOW=$'\033[33m'
  C_RED=$'\033[31m'
else
  C_RESET=""
  C_BLUE=""
  C_YELLOW=""
  C_RED=""
fi

say() {
  printf '%s==>%s %s\n' "$C_BLUE" "$C_RESET" "$*"
}

warn() {
  printf '%swarning:%s %s\n' "$C_YELLOW" "$C_RESET" "$*" >&2
}

die() {
  printf '%serror:%s %s\n' "$C_RED" "$C_RESET" "$*" >&2
  exit 1
}

# Fail early and in one place, rather than half way through an apply.
require_cmd() {
  local missing=() cmd
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    die "missing required command(s): ${missing[*]}"
  fi
}

# `env_name` is a prefix for every AWS resource name and for the SSM path, so
# it must match what the Terraform variable validates.
validate_env_name() {
  local name="${1:-}"
  [ -n "$name" ] || die "environment name is required"
  if ! printf '%s' "$name" | grep -Eq '^[a-z][a-z0-9-]{0,19}$'; then
    die "invalid environment name '$name' (must match ^[a-z][a-z0-9-]{0,19}\$)"
  fi
}

# Point the env stack's S3 backend at this environment's own state key.
# `-reconfigure` because the working directory is shared between environments:
# without it Terraform offers to migrate the previous env's state into the new
# key. `use_lockfile` is S3-native locking (no DynamoDB table).
tf_init_env() {
  local env_name="$1"
  terraform -chdir="$ENV_DIR" init \
    -reconfigure \
    -input=false \
    -backend-config="bucket=$STATE_BUCKET" \
    -backend-config="key=env/$env_name/terraform.tfstate" \
    -backend-config="region=$REGION" \
    -backend-config="use_lockfile=true"
}

# One output, raw, for capture into a shell variable.
tf_output() {
  terraform -chdir="$ENV_DIR" output -raw "$1"
}

# retry_until <deadline_secs> <interval_secs> <command...>
#
# Run the command until it exits 0 or the budget runs out. Returns the last
# exit status. The command is responsible for its own output; this only
# controls the clock.
retry_until() {
  local budget="$1" interval="$2"
  shift 2
  local deadline=$((SECONDS + budget)) rc=1
  while :; do
    # The status has to be taken in the `else` branch: after an `if` whose
    # condition failed, `$?` is the (zero) status of the `if` itself.
    if "$@"; then
      return 0
    else
      rc=$?
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      return "$rc"
    fi
    sleep "$interval"
  done
}
