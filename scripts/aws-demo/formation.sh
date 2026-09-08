#!/usr/bin/env bash
#
# Day-0 formation for one AWS demo environment (docs/roadmap/aws-demo-plan.md).
#
# This does NOT run on your laptop: up.sh ships it to one coordinator instance
# with `aws ssm send-command` (AWS-RunShellScript), which runs it as root with
# these variables exported ahead of it:
#
#   ENV_NAME    the environment name, e.g. "demo"
#   SSM_PREFIX  /coppice/<env>
#   REGION      eu-west-2
#
# Idempotent by design: up.sh runs it on every bring-up, including against an
# already-formed cluster, and it must be a no-op then. Nothing here scrapes
# logs — convergence state comes from the daemon's own /readyz document
# (ADR 0037 §9).
#
# Secrets pass through this script (the two enrollment secrets and the
# operator private key). Nothing is ever echoed, and the two files that hold
# secret material live in a 0700 directory at mode 0600 and are removed by a
# trap. The whole SSM command output is retained by AWS, so a single stray
# `echo` would publish a cluster credential.

# AWS-RunShellScript writes the commands to a file and runs it with `sh`
# (dash on Ubuntu), ignoring the shebang, and dash has no `pipefail`. Re-exec
# under bash first; the exports the caller prepended survive the exec.
[ -z "${BASH_VERSION:-}" ] && exec bash "$0" "$@"
set -euo pipefail

ENV_NAME="${ENV_NAME:?ENV_NAME must be exported by the caller}"
SSM_PREFIX="${SSM_PREFIX:?SSM_PREFIX must be exported by the caller}"
REGION="${REGION:?REGION must be exported by the caller}"

DAY0_DIR=/root/coppice-day0
POLICY_TEMPLATE=/opt/coppice-release/deploy/examples/policy.toml
READYZ_URL=http://127.0.0.1:7070/readyz
ADMIN_SOCK=/run/coppice/admin.sock
CONFIG=/etc/coppice/coordinator.toml

# Files that will hold plaintext secrets, cleaned up on every exit path.
policy_file=""
sed_file=""
cleanup() {
  [ -n "$policy_file" ] && rm -f "$policy_file"
  [ -n "$sed_file" ] && rm -f "$sed_file"
  return 0
}
trap cleanup EXIT

mkdir -p "$DAY0_DIR"
chmod 0700 "$DAY0_DIR"

# The phase string from /readyz, or empty if the daemon is not answering yet.
# ADR 0037 §1 spells the phases kebab-case on the wire (waiting,
# formation-failed, history-superseded, joining, learner, voter).
readyz_body() {
  # No `-f`: a parked daemon answers /readyz with 503 and a body, and the body
  # (its `phase`) is exactly what this needs.
  curl -sS --max-time 5 "$READYZ_URL" 2>/dev/null || true
}

readyz_phase() {
  readyz_body | jq -r '.phase // empty' 2>/dev/null || true
}

# 1. Wait for the daemon to be up enough to say what it is waiting for. A
#    parked daemon serves /readyz and the admin socket and nothing else, so
#    this is the earliest point at which anything can be decided.
echo "waiting for the coordinator daemon to answer /readyz (up to 10 minutes)"
phase=""
deadline=$((SECONDS + 600))
while :; do
  if [ -S "$ADMIN_SOCK" ]; then
    phase="$(readyz_phase)"
    [ -n "$phase" ] && break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out waiting for $ADMIN_SOCK and $READYZ_URL" >&2
    exit 1
  fi
  sleep 5
done
echo "phase: $phase"

case "$phase" in
formation-failed | formation_failed)
  # Fail-stop with no resume path (ADR 0037 §3). Wiping a data directory is
  # never something automation should decide, so this stops and says so.
  echo "this replica is in the formation-failed phase; its readiness document:" >&2
  readyz_body >&2
  echo >&2
  echo "there is no resume path: wipe /var/lib/coppice on this instance, restart" >&2
  echo "coppice-coordinator, then re-run scripts/aws-demo/up.sh $ENV_NAME" >&2
  exit 1
  ;;
history-superseded | history_superseded)
  echo "this replica's raft history has been superseded; its readiness document:" >&2
  readyz_body >&2
  echo >&2
  echo "wipe /var/lib/coppice on this instance so it re-enrols into the live" >&2
  echo "history, then re-run scripts/aws-demo/up.sh $ENV_NAME" >&2
  exit 1
  ;;
esac

# 2/3. Form the cluster, unless it is already formed.
if [ "$phase" = "waiting" ]; then
  echo "cluster is unformed; rendering the bootstrap policy and running init"
  [ -f "$POLICY_TEMPLATE" ] || {
    echo "missing $POLICY_TEMPLATE (is the release tarball unpacked?)" >&2
    exit 1
  }

  coordinator_secret="$(aws ssm get-parameter --region "$REGION" \
    --name "$SSM_PREFIX/enroll/coordinator" --with-decryption \
    --query Parameter.Value --output text)"
  agent_secret="$(aws ssm get-parameter --region "$REGION" \
    --name "$SSM_PREFIX/enroll/agent" --with-decryption \
    --query Parameter.Value --output text)"

  policy_file="$(mktemp "$DAY0_DIR/policy.XXXXXX.toml")"
  sed_file="$(mktemp "$DAY0_DIR/render.XXXXXX.sed")"
  chmod 0600 "$policy_file" "$sed_file"

  # The substitutions go through a sed *script file* rather than `-e`, so the
  # secrets never appear in this process's argv. Both secrets are
  # Terraform-minted 48-char alphanumerics, which is why a bare `s|…|…|` is
  # safe: no delimiter, backslash or `&` can appear in the replacement.
  {
    printf 's|COORDINATOR_ENROLL_SECRET|%s|g\n' "$coordinator_secret"
    printf 's|AGENT_ENROLL_SECRET|%s|g\n' "$agent_secret"
  } >"$sed_file"
  unset coordinator_secret agent_secret
  sed -f "$sed_file" "$POLICY_TEMPLATE" >"$policy_file"
  rm -f "$sed_file"
  sed_file=""

  if grep -q 'ENROLL_SECRET' "$policy_file"; then
    echo "policy rendering left a placeholder behind" >&2
    exit 1
  fi

  # `init` is idempotent and reports already-initialized, but another
  # coordinator may also have formed between our phase read and this call —
  # so a failure is only a failure if we are still parked afterwards.
  if coppice coordinator init \
    --config "$CONFIG" \
    --policy "$policy_file" \
    --out-dir "$DAY0_DIR"; then
    echo "init completed"
  else
    phase_now="$(readyz_phase)"
    case "$phase_now" in
    joining | learner | voter)
      echo "init failed but the cluster is now formed (phase: $phase_now); continuing"
      ;;
    *)
      echo "coppice coordinator init failed (phase: ${phase_now:-unknown})" >&2
      exit 1
      ;;
    esac
  fi

  rm -f "$policy_file"
  policy_file=""
else
  echo "cluster is already formed (phase: $phase); skipping init"
fi

# 4. Make sure the operator break-glass material is in SSM. Terraform created
#    the three parameters with the placeholder value "unset" and ignores
#    changes to them, so it still owns and destroys them while formation
#    supplies the real values (docs/operations/security.md § Token custody).
#
#    All three are checked, not just the certificate: a run interrupted between
#    the first and third put would otherwise leave a certificate with no key
#    behind it that every later run treats as complete. A partial set is
#    republished as one matching cert/key/CA trio.
operator_param() {
  aws ssm get-parameter --region "$REGION" --name "$SSM_PREFIX/operator/$1" \
    --with-decryption --query Parameter.Value --output text 2>/dev/null || echo unset
}

operator_complete=true
for part in cert key ca; do
  value="$(operator_param "$part")"
  if [ -z "$value" ] || [ "$value" = "unset" ] || [ "$value" = "None" ]; then
    operator_complete=false
  fi
done
unset value

if [ "$operator_complete" = false ]; then
  if [ ! -s "$DAY0_DIR/operator.crt" ] || [ ! -s "$DAY0_DIR/operator.key" ] ||
    [ ! -s "$DAY0_DIR/ca.crt" ]; then
    # The `init` output was not ours to keep (another coordinator formed the
    # cluster, or a previous run cleaned up). Minting a fresh operator
    # certificate over the local admin socket is the documented day-0
    # recovery, and works on any voter since the CA key is on its disk. A
    # fresh trio also supersedes whatever partial set was in SSM.
    echo "no complete init material on disk; minting an operator certificate locally"
    rm -f "$DAY0_DIR/operator.crt" "$DAY0_DIR/operator.key" "$DAY0_DIR/ca.crt"
    coppice coordinator admin --config "$CONFIG" issue-operator-cert \
      --operator-cn demo-operator --out-dir "$DAY0_DIR"
  fi

  put_secure() {
    local name="$1" file="$2" tier=()
    # SSM standard parameters cap at 4 KB. These are a leaf certificate, its
    # key and the CA certificate — comfortably inside that — so Advanced tier
    # (which is billed per parameter per month) is only reached if a value
    # actually overflows.
    if [ "$(wc -c <"$file")" -gt 4096 ]; then
      tier=(--tier Advanced)
    fi
    aws ssm put-parameter --region "$REGION" \
      --name "$name" --type SecureString --overwrite \
      "${tier[@]}" --value "file://$file" >/dev/null
    echo "stored $name"
  }

  # Key and CA first, certificate last: the certificate is the part a reader
  # is most likely to check first, so it is the last to become non-placeholder.
  put_secure "$SSM_PREFIX/operator/key" "$DAY0_DIR/operator.key"
  put_secure "$SSM_PREFIX/operator/ca" "$DAY0_DIR/ca.crt"
  put_secure "$SSM_PREFIX/operator/cert" "$DAY0_DIR/operator.crt"
else
  echo "operator material already complete in $SSM_PREFIX/operator/*; leaving it alone"
fi

# 5. Leave the readiness document in the command output — it is the whole
#    convergence picture, and up.sh prints this tail.
echo "final readiness document:"
readyz_body
echo
