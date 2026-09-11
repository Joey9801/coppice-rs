#!/usr/bin/env bash
#
# Prove one AWS demo environment works, end to end, from the outside
# (docs/roadmap/aws-demo-plan.md): the cluster has its shape, a real Docker
# job runs to completion and is queryable afterwards, Prometheus is scraping
# every process, and OIDC actually guards the client API.
#
# Runs from a workstation or a CI runner and touches only the public surface
# plus, for the Prometheus check, the ops host over SSM run-command (Prometheus
# is loopback-only there by design). Every check prints PASS or FAIL with the
# evidence; the exit status is non-zero if any check failed.
#
# What this does NOT prove, stated once here and again in the output where it
# matters: job logs and usage are served from per-attempt segments on the
# agent that ran the work, retained for about an hour after the attempt ends
# and less under disk pressure; terminal jobs leave replicated state on the
# `terminal_retention` TTL; and `[history] mode = "none"` is the only history
# mode, so nothing durable is written. The job assertions are made within
# minutes of the job finishing. They are a "query it now" guarantee, not a
# "query it tomorrow" one.
set -euo pipefail

# shellcheck source=scripts/aws-demo/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

usage() {
  cat <<'EOT'
usage: smoke.sh ENV_NAME [options]

Run the end-to-end checks against an environment brought up by up.sh.

  --job-spec PATH       job spec to submit; default examples/jobs/stress-demo.toml
                        (about five minutes of work, shaped so the usage
                        samples are worth reading).
  --quota-entity ID     the quota entity the job charges, substituted into a
                        copy of the spec; default is the `demo` entity that
                        deploy/examples/policy.toml seeds at formation.
  --coppice PATH        the `coppice` CLI to drive (default: $COPPICE_BIN, else
                        `coppice` on PATH). Build one with
                        `cargo build --release --bin coppice`.
  --skip-job            run every check except the job one.
  --timeout SECS        budget for each wait (Prometheus targets, job terminal
                        state); default 900.
  -h, --help            this message.
EOT
}

env_name=""
job_spec="$REPO_ROOT/examples/jobs/stress-demo.toml"
quota_entity="quota-00000000-0000-0000-0000-0000000000d1"
coppice_bin="${COPPICE_BIN:-coppice}"
skip_job=false
timeout_secs=900

while [ $# -gt 0 ]; do
  case "$1" in
  --job-spec)
    [ $# -ge 2 ] || die "--job-spec needs a value"
    job_spec="$2"
    shift 2
    ;;
  --job-spec=*)
    job_spec="${1#--job-spec=}"
    shift
    ;;
  --quota-entity)
    [ $# -ge 2 ] || die "--quota-entity needs a value"
    quota_entity="$2"
    shift 2
    ;;
  --quota-entity=*)
    quota_entity="${1#--quota-entity=}"
    shift
    ;;
  --coppice)
    [ $# -ge 2 ] || die "--coppice needs a value"
    coppice_bin="$2"
    shift 2
    ;;
  --coppice=*)
    coppice_bin="${1#--coppice=}"
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
  --skip-job)
    skip_job=true
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
if [ "$skip_job" = false ]; then
  [ -f "$job_spec" ] || die "job spec not found: $job_spec"
  command -v "$coppice_bin" >/dev/null 2>&1 ||
    die "coppice CLI not found ($coppice_bin); pass --coppice or build one with: cargo build --release --bin coppice"
fi

ssm_prefix="/coppice/$env_name"
fqdn=""
cognito_client_id=""
demo_user_email=""
ops_instance_id=""

# --- the report --------------------------------------------------------------

# Every check runs regardless of the others, so one run reports everything
# that is wrong. `fail` inside a check records the reason and returns
# non-zero; `evidence` prints an indented line under the check's heading.
checks_run=0
checks_failed=0
failed_names=""

evidence() {
  printf '      %s\n' "$*"
}

fail() {
  printf '      %sFAIL:%s %s\n' "$C_RED" "$C_RESET" "$*"
  return 1
}

# run_check <name> <description> <function>
run_check() {
  local name="$1" description="$2" fn="$3"
  checks_run=$((checks_run + 1))
  printf '\n%s[%s]%s %s\n' "$C_BLUE" "$name" "$C_RESET" "$description"
  if "$fn"; then
    printf '  %sPASS%s  %s\n' "$C_BLUE" "$C_RESET" "$name"
  else
    checks_failed=$((checks_failed + 1))
    failed_names="$failed_names $name"
    printf '  %sFAIL%s  %s\n' "$C_RED" "$C_RESET" "$name"
  fi
}

# --- preflight ---------------------------------------------------------------

ident="$(aws sts get-caller-identity --output json)" ||
  die "no usable AWS credentials (aws sts get-caller-identity failed)"
say "AWS account $(jq -r .Account <<<"$ident") as $(jq -r .Arn <<<"$ident")"

tf_init_env "$env_name" >/dev/null
fqdn="$(tf_output fqdn)"
cognito_client_id="$(tf_output cognito_client_id)"
demo_user_email="$(tf_output demo_user_email)"
ops_instance_id="$(tf_output ops_instance_id)"
say "environment '$env_name' at https://$fqdn (ops host $ops_instance_id)"

init_secrets_dir

# The ID token is minted lazily and re-minted when it is more than ten minutes
# old: Cognito issues them for fifteen, and the job check alone runs for five.
id_token=""
token_minted_at=0
ensure_token() {
  if [ -z "$id_token" ] || [ $((SECONDS - token_minted_at)) -gt 600 ]; then
    id_token="$(mint_id_token "$ssm_prefix" "$cognito_client_id" "$demo_user_email")" ||
      die "could not obtain a Cognito ID token for $demo_user_email"
    token_minted_at=$SECONDS
    set_api_token "$id_token"
  fi
}

api() {
  ensure_token
  api_get "$fqdn" "$1"
}

# Run the CLI against this environment. The token reaches it through the
# environment (COPPICE_TOKEN is how the CLI takes a bearer token; it has no
# login flow), never through argv.
cli() {
  ensure_token
  COPPICE_API="https://$fqdn" COPPICE_TOKEN="$id_token" "$coppice_bin" "$@"
}

# --- check: OIDC guards the client API ----------------------------------------

check_authn() {
  local url="https://$fqdn/api/v1/overview" code
  code="$(http_code "$url")"
  evidence "GET /api/v1/overview without a token: HTTP ${code:-no answer}"
  [ "$code" = "401" ] || fail "expected 401 for a tokenless request" || return 1

  code="$(http_code "$url" -H "Authorization: Bearer not-a-token")"
  evidence "GET /api/v1/overview with a garbage token: HTTP ${code:-no answer}"
  [ "$code" = "401" ] || fail "expected 401 for an invalid token" || return 1

  ensure_token
  code="$(http_code "$url" -K "$auth_config")"
  evidence "GET /api/v1/overview with a Cognito ID token for $demo_user_email: HTTP ${code:-no answer}"
  [ "$code" = "200" ] || fail "expected 200 with a valid Cognito token" || return 1

  local principal
  principal="$(api /api/v1/session | jq -r '"\(.principal) via \(.auth_method)"' 2>/dev/null || true)"
  [ -z "$principal" ] || evidence "GET /api/v1/session: $principal"
  return 0
}

# --- check: three coordinator voters -----------------------------------------

check_coordinators() {
  local body voters members
  body="$(api /api/v1/coordinators)"
  members="$(jq -r '[.members[]?] | length' <<<"$body" 2>/dev/null || echo 0)"
  voters="$(jq -r '[.members[]? | select(.voter)] | length' <<<"$body" 2>/dev/null || echo 0)"
  evidence "GET /api/v1/coordinators: $members members, $voters voters"
  jq -r '.members[]? | "  member \(.id) \(.addr) voter=\(.voter)"' <<<"$body" 2>/dev/null |
    while IFS= read -r line; do evidence "$line"; done
  [ "$voters" = "3" ] || fail "expected exactly 3 voters, found ${voters:-0}"
}

# --- check: three schedulable agents -----------------------------------------

# The health verdict comes from the leader's in-memory liveness marks, so a
# read the load balancer routes to a follower answers `unknown` for every node
# (issue #133). Schedulability is replicated and is what this check asserts;
# a leader-served read is sought for the evidence but is not required.
nodes_all_healthy() {
  local body="$1"
  [ "$(jq -r '[.nodes[]? | select(.health == "healthy")] | length' <<<"$body" 2>/dev/null || echo 0)" = "3" ]
}

check_nodes() {
  local body total ready healthy attempt
  body="$(api /api/v1/nodes)"
  total="$(jq -r '[.nodes[]?] | length' <<<"$body" 2>/dev/null || echo 0)"
  ready="$(jq -r '[.nodes[]? | select(.schedulable and .health != "lost")] | length' \
    <<<"$body" 2>/dev/null || echo 0)"
  evidence "GET /api/v1/nodes: $total nodes, $ready schedulable and not lost"
  jq -r '.nodes[]? | "  \(.id) health=\(.health) schedulable=\(.schedulable) cpu=\(.capacity.cpu_millis)m mem=\(.capacity.memory_bytes) role=\(.labels.role // "-")"' \
    <<<"$body" 2>/dev/null | while IFS= read -r line; do evidence "$line"; done
  [ "$ready" = "3" ] || fail "expected exactly 3 schedulable nodes, found ${ready:-0}" || return 1

  # Each request is a new connection, so a handful of tries usually reaches
  # the leader once.
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    nodes_all_healthy "$body" && break
    body="$(api /api/v1/nodes)"
  done
  healthy="$(jq -r '[.nodes[]? | select(.health == "healthy")] | length' <<<"$body" 2>/dev/null || echo 0)"
  if [ "$healthy" = "3" ]; then
    evidence "a leader-served read reports all 3 nodes healthy (attempt $attempt)"
  else
    evidence "note: no leader-served read in $attempt attempts; health stays 'unknown' from a follower (issue #133), schedulability is the replicated fact"
  fi
  return 0
}

# --- check: Prometheus scrapes all six ---------------------------------------

# One instant query against the ops host's loopback Prometheus, through SSM
# run-command (no port-forward, no session-manager plugin, nothing listening
# publicly). Prints the scalar result, or nothing.
prom_query() {
  local query="$1" script
  script="$(printf "curl -sS --max-time 10 http://127.0.0.1:9090/api/v1/query --data-urlencode 'query=%s'" "$query")"
  ssm_run "$ops_instance_id" 60 "$script" || return 1
  printf '%s\n' "$ssm_stdout"
}

# The scalar value of a query that returns one sample, or empty.
prom_scalar() {
  prom_query "$1" | jq -r '.data.result[0].value[1] // empty' 2>/dev/null || true
}

prom_targets_healthy() {
  local n
  n="$(prom_scalar 'count(up{job="coppice"} == 1)')"
  [ "$n" = "6" ]
}

check_prometheus() {
  # The service unit must be up before anything else is worth asking.
  ssm_run "$ops_instance_id" 60 'systemctl is-active prometheus && prometheus --version 2>&1 | head -n 1' ||
    fail "prometheus is not active on the ops host: $ssm_stdout $ssm_stderr" || return 1
  evidence "ops host: prometheus $(printf '%s' "$ssm_stdout" | tail -n 1 | cut -d' ' -f3) is active"

  # Targets appear one discovery refresh (30 s) plus one scrape (15 s) after
  # the instances are running; on a fresh environment this can lag readiness.
  say "waiting for Prometheus to report six healthy targets (up to ${timeout_secs}s)"
  retry_until "$timeout_secs" 15 prom_targets_healthy ||
    fail "count(up{job=\"coppice\"} == 1) never reached 6 (last: $(prom_scalar 'count(up{job="coppice"} == 1)'))" || return 1
  evidence 'count(up{job="coppice"} == 1) = 6'

  local by_role coordinators agents
  by_role="$(prom_query 'count by (role) (up{job="coppice"} == 1)')"
  coordinators="$(jq -r '.data.result[] | select(.metric.role == "coordinator") | .value[1]' <<<"$by_role" 2>/dev/null || true)"
  agents="$(jq -r '.data.result[] | select(.metric.role == "agent") | .value[1]' <<<"$by_role" 2>/dev/null || true)"
  evidence "healthy targets by role: coordinator=${coordinators:-0} agent=${agents:-0}"
  [ "$coordinators" = "3" ] && [ "$agents" = "3" ] ||
    fail "expected 3 coordinator and 3 agent targets" || return 1

  prom_query 'up{job="coppice"}' |
    jq -r '.data.result[] | "  \(.metric.role) \(.metric.instance) (\(.metric.instance_id)) up=\(.value[1])"' 2>/dev/null |
    while IFS= read -r line; do evidence "$line"; done

  # Families that actually exist (verified against the describe_metrics call
  # sites), not the aspirational list in docs/operations/observability.md.
  # Every coordinator replica applies the same state, so all three report
  # the node count and agree on it; the count includes nodes the cluster has
  # declared lost (a reclaimed spot agent stays in state until the liveness
  # sweep evicts it), so it is at least three, not exactly three. Every agent
  # exposes its running-job gauge from startup.
  local reporting lo hi running
  reporting="$(prom_scalar 'count(coordinator_state_nodes)')"
  lo="$(prom_scalar 'min(coordinator_state_nodes)')"
  hi="$(prom_scalar 'max(coordinator_state_nodes)')"
  evidence "coordinator_state_nodes: reported by ${reporting:-0} coordinators, min ${lo:-absent}, max ${hi:-absent}"
  [ "$reporting" = "3" ] || fail "expected coordinator_state_nodes from all 3 coordinators" || return 1
  [ -n "$lo" ] && [ "$lo" = "$hi" ] && [ "$lo" -ge 3 ] ||
    fail "expected every coordinator to agree on at least 3 nodes in replicated state" || return 1
  running="$(prom_scalar 'count(agent_running_jobs)')"
  evidence "count(agent_running_jobs) = ${running:-absent}"
  [ "$running" = "3" ] || fail "expected agent_running_jobs from all 3 agents" || return 1
  evidence "note: node utilisation is a short in-memory window on the coordinator; Prometheus is the only longer view, and it lives on this ops host only"
  return 0
}

# --- check: a real Docker job runs and is queryable -------------------------

job_id=""

# The job's current state kind, or empty when the read fails.
job_state() {
  api "/api/v1/jobs/$job_id" | jq -r '.state // empty' 2>/dev/null || true
}


check_job() {
  local rendered submit_out state
  # The checked-in spec charges the entity `coppice dev` seeds; a copy carries
  # the one this cluster's formation policy seeded instead.
  rendered="$(mktemp "$secrets_dir/job-spec.XXXXXX.toml")"
  sed -e "s|^quota_entity = \".*\"|quota_entity = \"$quota_entity\"|" "$job_spec" >"$rendered"
  grep -q "^quota_entity = \"$quota_entity\"" "$rendered" ||
    fail "could not substitute quota_entity in $job_spec" || return 1
  evidence "spec: $job_spec (quota entity $quota_entity)"

  submit_out="$(cli job submit "$rendered" 2>&1)" ||
    fail "coppice job submit failed: $submit_out" || return 1
  job_id="$(printf '%s\n' "$submit_out" | sed -n 's/^submitted \(job-[0-9a-f-]*\).*/\1/p')"
  [ -n "$job_id" ] || fail "could not parse a job id from: $submit_out" || return 1
  evidence "$submit_out"

  say "waiting for $job_id to reach a terminal state (up to ${timeout_secs}s)"
  local started=$SECONDS last=""
  while :; do
    # One read per iteration, and the decision is made on that same read: a
    # second fetch could observe the terminal state while `state` still
    # holds the previous one and fail the assertion below for nothing.
    state="$(job_state)"
    if [ "$state" != "$last" ]; then
      evidence "state: ${state:-unknown} (+$((SECONDS - started))s)"
      last="$state"
    fi
    case "$state" in
    succeeded | failed | aborted) break ;;
    esac
    [ $((SECONDS - started)) -lt "$timeout_secs" ] ||
      fail "job still ${state:-unknown} after ${timeout_secs}s" || return 1
    sleep 10
  done
  [ "$state" = "succeeded" ] || fail "job ended $state, expected succeeded" || return 1

  local detail attempts exit_code node
  detail="$(api "/api/v1/jobs/$job_id")"
  attempts="$(jq -r '.attempts | length' <<<"$detail")"
  node="$(jq -r '.attempts[-1].node // "-"' <<<"$detail")"
  exit_code="$(jq -r '.attempts[-1].outcome | "\(.kind // "-") exit_code=\(.exit_code // "-") class=\(.class // "-")"' <<<"$detail")"
  evidence "attempts: $attempts, last on $node, outcome: $exit_code, terminal at $(jq -r '.terminal_at' <<<"$detail")"

  # Transitions: the replicated timeline, read through the HTTP API.
  local timeline chain submitted
  timeline="$(api "/api/v1/jobs/$job_id/timeline")"
  submitted="$(jq -r '[.events[] | select(.kind == "job_submitted")] | length' <<<"$timeline" 2>/dev/null || echo 0)"
  chain="$(jq -r '[.events[] | select(.kind == "job_state_changed") | "\(.from)->\(.to)"] | join(", ")' <<<"$timeline" 2>/dev/null || true)"
  evidence "GET /jobs/$job_id/timeline: job_submitted=$submitted, transitions: ${chain:-none}"
  [ "$submitted" = "1" ] || fail "timeline does not start from the submission" || return 1
  printf '%s' "$chain" | grep -q -- '->succeeded$' ||
    fail "timeline has no transition into succeeded" || return 1

  # Logs: the CLI must print real output, and the API must say the source was
  # available rather than expired or unreachable.
  local logs_out logs_err lines logs_api availability
  logs_err="$(mktemp "$secrets_dir/logs-err.XXXXXX")"
  logs_out="$(cli job logs "$job_id" 2>"$logs_err")" ||
    fail "coppice job logs failed: $(cat "$logs_err")" || return 1
  lines="$(printf '%s\n' "$logs_out" | sed '/^$/d' | wc -l | tr -d ' ')"
  evidence "coppice job logs: $lines lines; first: $(printf '%s\n' "$logs_out" | head -n 1 | cut -c1-100)"
  evidence "coppice job logs: last: $(printf '%s\n' "$logs_out" | tail -n 1 | cut -c1-100)"
  if [ -s "$logs_err" ]; then
    evidence "coppice job logs stderr: $(tr '\n' ' ' <"$logs_err")"
  fi
  [ "$lines" -ge 1 ] || fail "coppice job logs printed nothing" || return 1
  logs_api="$(api "/api/v1/jobs/$job_id/logs")"
  availability="$(jq -r '[.sources[].availability] | unique | join(",")' <<<"$logs_api" 2>/dev/null || true)"
  evidence "GET /jobs/$job_id/logs: $(jq -r '.entries | length' <<<"$logs_api") entries, source availability: ${availability:-none}"
  [ "$availability" = "available" ] || fail "log source is not 'available'" || return 1

  # Usage: the samples the agent took while the container ran.
  local usage_out usage_err usage_api samples peak
  usage_err="$(mktemp "$secrets_dir/usage-err.XXXXXX")"
  usage_out="$(cli job usage "$job_id" 2>"$usage_err")" ||
    fail "coppice job usage failed: $(cat "$usage_err")" || return 1
  if printf '%s' "$usage_out" | grep -q '(no samples)'; then
    fail "coppice job usage reported no samples"
    return 1
  fi
  evidence "coppice job usage: $(printf '%s\n' "$usage_out" | sed '/^$/d' | wc -l | tr -d ' ') lines; header: $(printf '%s\n' "$usage_out" | head -n 1 | cut -c1-100)"
  if [ -s "$usage_err" ]; then
    evidence "coppice job usage stderr: $(tr '\n' ' ' <"$usage_err")"
  fi
  usage_api="$(api "/api/v1/jobs/$job_id/usage")"
  samples="$(jq -r '.samples | length' <<<"$usage_api" 2>/dev/null || echo 0)"
  peak="$(jq -r '[.samples[].memory_peak_bytes] | max // 0' <<<"$usage_api" 2>/dev/null || echo 0)"
  availability="$(jq -r '[.sources[].availability] | unique | join(",")' <<<"$usage_api" 2>/dev/null || true)"
  evidence "GET /jobs/$job_id/usage: $samples samples on the first page, peak memory $peak bytes, source availability: ${availability:-none}"
  [ "${samples:-0}" -ge 1 ] || fail "no usage samples" || return 1
  [ "$availability" = "available" ] || fail "usage source is not 'available'" || return 1
  [ "${peak:-0}" -gt 0 ] || fail "usage samples carry no memory measurement" || return 1

  evidence "note: logs and usage were read minutes after the attempt ended. They live in per-attempt segments on the agent that ran the job, kept for about an hour and less under disk pressure; the job itself leaves replicated state on the terminal_retention TTL and [history] mode = \"none\" writes no durable copy. This is a query-it-now result, not a query-it-tomorrow one."
  return 0
}

# --- main --------------------------------------------------------------------

run_check authn "OIDC guards the client API: tokenless 401, Cognito token 200" check_authn
run_check coordinators "three coordinators are raft voters" check_coordinators
run_check nodes "three agents are schedulable" check_nodes
run_check prometheus "Prometheus on the ops host scrapes all six processes" check_prometheus
if [ "$skip_job" = true ]; then
  say "skipping the job check (--skip-job)"
else
  run_check job "a real Docker job runs to completion and its logs, usage and transitions are queryable now" check_job
fi

printf '\n'
if [ "$checks_failed" -eq 0 ]; then
  say "all $checks_run checks passed against '$env_name' (https://$fqdn)"
  exit 0
fi
printf '%serror:%s %d of %d checks failed:%s\n' "$C_RED" "$C_RESET" "$checks_failed" "$checks_run" "$failed_names" >&2
exit 1
