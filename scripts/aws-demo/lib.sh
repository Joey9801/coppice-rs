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

# --- secrets on disk ---------------------------------------------------------

# Neither a password nor a bearer token may appear in any process's argv, where
# `ps` shows it to every user on the machine. Anything secret therefore travels
# through a 0600 file in a 0700 directory that a trap removes on exit. Call
# init_secrets_dir once, at top level: a trap set inside a `$(...)` subshell
# would fire when that subshell exits and remove the directory before it is
# ever used.
secrets_dir=""
init_secrets_dir() {
  [ -z "$secrets_dir" ] || return 0
  secrets_dir="$(mktemp -d "${TMPDIR:-/tmp}/coppice-aws-demo.XXXXXX")"
  chmod 0700 "$secrets_dir"
  # shellcheck disable=SC2064
  trap "rm -rf '$secrets_dir'" EXIT
}

secret_file() {
  local f
  f="$(mktemp "$secrets_dir/$1.XXXXXX")"
  chmod 0600 "$f"
  printf '%s\n' "$f"
}

# --- Cognito ------------------------------------------------------------------

# mint_id_token <ssm_prefix> <client_id> <username>
#
# A Cognito ID token for the demo user, obtained non-interactively with
# USER_PASSWORD_AUTH — CI has no browser, and the CLI has no login flow of its
# own: it consumes a bearer token through COPPICE_TOKEN. The coordinator
# validates `aud`, and Cognito puts an `aud` claim only in the ID token (the
# access token carries `client_id` instead), so this is deliberately not
# AccessToken. The password reaches jq through its environment (readable only
# by the same user) and the AWS CLI through a 0600 file.
mint_id_token() {
  local ssm_prefix="$1" client_id="$2" username="$3" input token
  input="$(secret_file initiate-auth)"
  PASSWORD="$(aws ssm get-parameter --region "$REGION" \
    --name "$ssm_prefix/demo-user/password" --with-decryption \
    --query Parameter.Value --output text)" \
    jq -n --arg c "$client_id" --arg u "$username" \
    '{AuthFlow: "USER_PASSWORD_AUTH", ClientId: $c,
      AuthParameters: {USERNAME: $u, PASSWORD: env.PASSWORD}}' >"$input"
  token="$(aws cognito-idp initiate-auth --region "$REGION" \
    --cli-input-json "file://$input" \
    --query 'AuthenticationResult.IdToken' --output text)"
  rm -f "$input"
  [ -n "$token" ] && [ "$token" != "None" ] || return 1
  printf '%s\n' "$token"
}

# --- the client API -----------------------------------------------------------

# The bearer token for api_get, written once as a curl config file (`-K`) so it
# never appears on a command line.
auth_config=""
set_api_token() {
  auth_config="$(secret_file curl-auth)"
  printf 'header = "Authorization: Bearer %s"\n' "$1" >"$auth_config"
}

# api_get <fqdn> <path>: the body, or empty on any transport failure.
api_get() {
  local fqdn="$1" path="$2"
  curl -s --max-time 15 -K "$auth_config" "https://$fqdn$path" 2>/dev/null || true
}

# http_code <url> [curl args...]: the status code, or empty when nothing answered.
http_code() {
  local url="$1"
  shift
  curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$@" "$url" 2>/dev/null || true
}

# --- SSM run-command ----------------------------------------------------------

# ssm_run <instance_id> <timeout_secs> <script>
#
# Run a script on an instance through AWS-RunShellScript and wait for it. The
# whole script travels as ONE element of the `commands` array (the document
# joins the elements with newlines, which would otherwise split heredocs and
# multi-line constructs) and reaches the API through --cli-input-json, so it
# never touches argv. Results land in the globals below; the return status is
# 0 only for a `Success` invocation. Remember that AWS retains every
# invocation's output: nothing secret may be echoed by the script.
ssm_status=""
ssm_stdout=""
ssm_stderr=""
ssm_run() {
  local instance_id="$1" timeout_secs="$2" script="$3" input command_id
  input="$(mktemp "${TMPDIR:-/tmp}/coppice-ssm.XXXXXX")"
  jq -n --arg id "$instance_id" --arg s "$script" --arg t "$timeout_secs" \
    '{DocumentName: "AWS-RunShellScript", InstanceIds: [$id],
      TimeoutSeconds: ($t | tonumber),
      Parameters: {commands: [$s], executionTimeout: [$t]}}' >"$input"
  command_id="$(aws ssm send-command --region "$REGION" \
    --cli-input-json "file://$input" \
    --query 'Command.CommandId' --output text)"
  rm -f "$input"

  # The invocation's own timeout covers a running script; this deadline covers
  # an instance that never picks the command up at all (terminated, or its SSM
  # agent not yet online), which would otherwise poll forever.
  local deadline=$((SECONDS + timeout_secs + 120))
  while :; do
    ssm_status="$(aws ssm get-command-invocation --region "$REGION" \
      --command-id "$command_id" --instance-id "$instance_id" \
      --query Status --output text 2>/dev/null || echo Pending)"
    case "$ssm_status" in
    Pending | InProgress | Delayed)
      if [ "$SECONDS" -ge "$deadline" ]; then
        ssm_status="Undelivered"
        ssm_stdout=""
        ssm_stderr="no result from $instance_id within $((timeout_secs + 120))s (last status: pending)"
        return 1
      fi
      sleep 3
      ;;
    *) break ;;
    esac
  done
  ssm_stdout="$(aws ssm get-command-invocation --region "$REGION" \
    --command-id "$command_id" --instance-id "$instance_id" \
    --query StandardOutputContent --output text 2>/dev/null || true)"
  ssm_stderr="$(aws ssm get-command-invocation --region "$REGION" \
    --command-id "$command_id" --instance-id "$instance_id" \
    --query StandardErrorContent --output text 2>/dev/null || true)"
  [ "$ssm_stderr" != "None" ] || ssm_stderr=""
  [ "$ssm_status" = "Success" ]
}
