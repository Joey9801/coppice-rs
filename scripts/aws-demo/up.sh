#!/usr/bin/env bash
#
# Bring one AWS demo environment up, end to end: apply the env Terraform
# stack, form the cluster on a coordinator over SSM, and wait until the
# cluster answers as healthy. See docs/roadmap/aws-demo-plan.md.
#
# Safe to re-run: Terraform converges, formation is idempotent, and the waits
# are pure reads.
set -euo pipefail

# shellcheck source=scripts/aws-demo/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

usage() {
  cat <<'EOF'
usage: up.sh ENV_NAME --tarball PATH [options]
       up.sh --status-only ENV_NAME
       up.sh --token-only ENV_NAME

Bring up (or re-converge) one AWS demo environment.

  --tarball PATH        the release tarball to deploy, named
                        coppice-<version>-aarch64-unknown-linux-gnu.tar.gz.
                        Required unless --status-only/--token-only.
  --on-demand-agents    run the agent ASG on-demand instead of spot.
  --skip-apply          skip `terraform apply`; still form and wait.
  --status-only         only wait for readiness and print the summary.
  --token-only          print a Cognito ID token for the demo user and exit.
                        Nothing else is printed, so it is safe to capture.
  --timeout SECS        readiness budget, default 1200.
  -h, --help            this message.
EOF
}

env_name=""
tarball=""
on_demand_agents=false
skip_apply=false
status_only=false
token_only=false
timeout_secs=1200

while [ $# -gt 0 ]; do
  case "$1" in
  --tarball)
    [ $# -ge 2 ] || die "--tarball needs a value"
    tarball="$2"
    shift 2
    ;;
  --tarball=*)
    tarball="${1#--tarball=}"
    shift
    ;;
  --timeout)
    [ $# -ge 2 ] || die "--timeout needs a value"
    timeout_secs="$2"
    shift 2
    ;;
  --timeout=*)
    timeout_secs="${1#--timeout=}"
    shift
    ;;
  --on-demand-agents)
    on_demand_agents=true
    shift
    ;;
  --skip-apply)
    skip_apply=true
    shift
    ;;
  --status-only)
    status_only=true
    shift
    ;;
  --token-only)
    token_only=true
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  -*)
    die "unknown option: $1"
    ;;
  *)
    [ -z "$env_name" ] || die "unexpected argument: $1"
    env_name="$1"
    shift
    ;;
  esac
done

validate_env_name "$env_name"
require_cmd terraform aws jq curl
printf '%s' "$timeout_secs" | grep -Eq '^[0-9]+$' || die "--timeout must be a whole number of seconds"

fqdn=""
api_url=""
cognito_client_id=""
demo_user_email=""
ssm_prefix="/coppice/$env_name"

read_outputs() {
  fqdn="$(tf_output fqdn)"
  api_url="$(tf_output api_url)"
  cognito_client_id="$(tf_output cognito_client_id)"
  demo_user_email="$(tf_output demo_user_email)"
}

# --- preflight ---------------------------------------------------------------

preflight_identity() {
  local ident
  ident="$(aws sts get-caller-identity --output json)" ||
    die "no usable AWS credentials (aws sts get-caller-identity failed)"
  say "AWS account $(jq -r .Account <<<"$ident") as $(jq -r .Arn <<<"$ident")"
}

preflight_tarball() {
  [ -n "$tarball" ] || {
    usage >&2
    die "--tarball is required"
  }
  [ -f "$tarball" ] || die "release tarball not found: $tarball"
  case "$(basename "$tarball")" in
  coppice-*-aarch64-unknown-linux-gnu.tar.gz) ;;
  *)
    # The instances are arm64 t4g; a host-built x86_64 tarball would install
    # cleanly and then fail to exec, hundreds of seconds later.
    die "tarball must be a coppice-<version>-aarch64-unknown-linux-gnu.tar.gz build: $tarball"
    ;;
  esac
}

# A launch-template change only shapes instances launched *after* it: the six
# that already exist keep running whatever they booted with, and a rolling
# instance refresh that is safe for a raft voter set needs readiness-aware
# hooks this stack does not have yet. So a different release on an existing
# environment is refused rather than silently deployed to nobody — the fleet
# is disposable, and `down.sh` then `up.sh` is the supported way to change it.
preflight_release_unchanged() {
  local deployed local_sha
  # `output -raw` prints its diagnostics to stdout, so only a value that looks
  # like a digest counts; anything else means nothing is deployed yet.
  deployed="$(terraform -chdir="$ENV_DIR" output -no-color -raw artefact_sha256 2>/dev/null || true)"
  printf '%s' "$deployed" | grep -Eq '^[0-9a-f]{64}$' || return 0
  local_sha="$(shasum -a 256 "$tarball" | cut -d' ' -f1)"
  [ "$deployed" = "$local_sha" ] ||
    die "environment '$env_name' already runs release sha256 $deployed; a new tarball ($local_sha) does not reach existing instances — run scripts/aws-demo/down.sh $env_name first, then up.sh again"
}

preflight_bootstrap_state() {
  aws s3api head-object --bucket "$STATE_BUCKET" --key bootstrap/terraform.tfstate \
    --region "$REGION" >/dev/null 2>&1 ||
    die "no bootstrap state at s3://$STATE_BUCKET/bootstrap/terraform.tfstate — run scripts/aws-demo/bootstrap.sh first"
}

# --- terraform ---------------------------------------------------------------

apply_env_stack() {
  local args=(-input=false -auto-approve
    -var "env_name=$env_name"
    -var "release_tarball=$tarball"
    -var "region=$REGION"
    -var "domain=$DOMAIN"
    -var "state_bucket=$STATE_BUCKET")
  if [ "$on_demand_agents" = true ]; then
    args+=(-var "agents_on_demand=true")
  fi
  say "applying the env stack for '$env_name'"
  terraform -chdir="$ENV_DIR" apply "${args[@]}"
}

print_outputs() {
  say "stack outputs"
  terraform -chdir="$ENV_DIR" output
}

# --- formation ---------------------------------------------------------------

# The first (by sorted instance id, so the choice is stable across re-runs)
# coordinator that the ASG reports InService.
first_inservice_coordinator() {
  local asg id
  asg="$(tf_output coordinator_asg_name)"
  id="$(aws autoscaling describe-auto-scaling-groups \
    --region "$REGION" --auto-scaling-group-names "$asg" \
    --query "sort(AutoScalingGroups[0].Instances[?LifecycleState=='InService'].InstanceId)|[0]" \
    --output text 2>/dev/null || true)"
  [ -n "$id" ] && [ "$id" != "None" ] || return 1
  printf '%s\n' "$id"
}

ssm_agent_online() {
  local id="$1" status
  status="$(aws ssm describe-instance-information --region "$REGION" \
    --filters "Key=InstanceIds,Values=$id" \
    --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null || true)"
  [ "$status" = "Online" ]
}

# Ship formation.sh to the instance and wait for it; the exports the script
# needs are prepended so they survive its re-exec under bash.
run_formation() {
  local instance_id="$1" script rc=0
  script="$(printf 'export ENV_NAME=%q SSM_PREFIX=%q REGION=%q\n' \
    "$env_name" "$ssm_prefix" "$REGION")
$(cat "$REPO_ROOT/scripts/aws-demo/formation.sh")"

  say "running formation on $instance_id"
  ssm_run "$instance_id" 900 "$script" || rc=$?
  say "formation output (tail)"
  printf '%s\n' "$ssm_stdout" | tail -n 40
  if [ -n "$ssm_stderr" ]; then
    printf '%s\n' "--- stderr ---" >&2
    printf '%s\n' "$ssm_stderr" >&2
  fi
  [ "$rc" -eq 0 ] || die "formation failed on $instance_id (status: $ssm_status)"
}

form_cluster() {
  local instance_id
  say "waiting for a coordinator to reach InService"
  retry_until 900 15 first_inservice_coordinator >/dev/null ||
    die "no coordinator reached InService within 15 minutes"
  instance_id="$(first_inservice_coordinator)"
  say "chose coordinator $instance_id"

  say "waiting for its SSM agent to report Online"
  retry_until 900 10 ssm_agent_online "$instance_id" ||
    die "instance $instance_id never came Online in SSM"

  run_formation "$instance_id"
}

# --- readiness and cluster shape ---------------------------------------------

wait_for_readyz() {
  local url="https://$fqdn/readyz?require=healthy" code deadline
  say "waiting for $url (up to ${timeout_secs}s)"
  deadline=$((SECONDS + timeout_secs))
  while :; do
    # Everything short of a 200 is "not yet": the Route53 record may not have
    # propagated, the NLB may have no healthy target, and the leader answers
    # ?require=healthy only once the cluster has its redundancy.
    code="$(http_code "$url")"
    if [ "$code" = "200" ]; then
      say "readyz is 200"
      curl -s --max-time 10 "$url" | jq . || true
      return 0
    fi
    [ "$SECONDS" -lt "$deadline" ] || die "readyz never returned 200 (last: ${code:-none})"
    printf '  readyz: %s (%ss elapsed)\n' "${code:-no answer}" "$SECONDS"
    sleep 10
  done
}

wait_for_voters() {
  local body count deadline
  say "waiting for three coordinator voters"
  deadline=$((SECONDS + timeout_secs))
  while :; do
    body="$(api_get "$fqdn" /api/v1/coordinators)"
    count="$(jq -r '[.members[]? | select(.voter)] | length' <<<"$body" 2>/dev/null || echo 0)"
    if [ "${count:-0}" -ge 3 ]; then
      say "coordinators: $count voters"
      return 0
    fi
    [ "$SECONDS" -lt "$deadline" ] || die "only ${count:-0} voters after ${timeout_secs}s"
    printf '  voters: %s/3\n' "${count:-0}"
    sleep 10
  done
}

wait_for_nodes() {
  local body count deadline
  say "waiting for three schedulable compute nodes"
  deadline=$((SECONDS + timeout_secs))
  while :; do
    body="$(api_get "$fqdn" /api/v1/nodes)"
    count="$(jq -r '[.nodes[]? | select(.schedulable and .health != "lost")] | length' \
      <<<"$body" 2>/dev/null || echo 0)"
    if [ "${count:-0}" -ge 3 ]; then
      say "nodes: $count schedulable"
      return 0
    fi
    [ "$SECONDS" -lt "$deadline" ] || die "only ${count:-0} schedulable nodes after ${timeout_secs}s"
    printf '  nodes: %s/3\n' "${count:-0}"
    sleep 10
  done
}

print_summary() {
  cat <<EOF

$(say "environment '$env_name' is up")

  API and web UI : $api_url
  readiness      : https://$fqdn/readyz?require=healthy
  demo user      : $demo_user_email (password in SSM at $ssm_prefix/demo-user/password)

To drive it from the CLI:

  export COPPICE_API=$api_url
  export COPPICE_TOKEN="\$(scripts/aws-demo/up.sh --token-only $env_name)"
  coppice cluster status

To prove it end to end (cluster shape, a real Docker job, Prometheus, OIDC):

  scripts/aws-demo/smoke.sh $env_name

EOF
}

# --- main --------------------------------------------------------------------

if [ "$token_only" = true ]; then
  # Quiet path: the only thing on stdout is the token, so `COPPICE_TOKEN=$(…)`
  # captures exactly the token. Everything else goes to stderr.
  preflight_identity >&2
  tf_init_env "$env_name" >&2
  read_outputs
  init_secrets_dir
  mint_id_token "$ssm_prefix" "$cognito_client_id" "$demo_user_email" ||
    die "could not obtain an ID token for $demo_user_email"
  exit 0
fi

preflight_identity

if [ "$status_only" = false ]; then
  if [ "$skip_apply" = false ]; then
    preflight_tarball
  fi
  preflight_bootstrap_state
fi

tf_init_env "$env_name"

if [ "$status_only" = false ] && [ "$skip_apply" = false ]; then
  preflight_release_unchanged
  apply_env_stack
fi

read_outputs
print_outputs

if [ "$status_only" = false ]; then
  form_cluster
fi

wait_for_readyz
say "obtaining a demo-user ID token"
init_secrets_dir
id_token="$(mint_id_token "$ssm_prefix" "$cognito_client_id" "$demo_user_email")" ||
  die "could not obtain an ID token for $demo_user_email"
set_api_token "$id_token"
unset id_token
wait_for_voters
wait_for_nodes
print_summary
