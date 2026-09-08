#!/usr/bin/env bash
#
# Tear one AWS demo environment down and prove nothing is left running
# (docs/roadmap/aws-demo-plan.md). The demo is billed by the hour, so the
# check at the end matters as much as the destroy.
set -euo pipefail

# shellcheck source=scripts/aws-demo/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

usage() {
  cat <<'EOF'
usage: down.sh ENV_NAME [--yes]

Destroy the env Terraform stack for ENV_NAME and sweep for leftovers.

  --yes       do not prompt for confirmation.
  -h, --help  this message.

The environment's Terraform state object is left in the state bucket; its key
is printed at the end so you can remove it yourself if you want to.
EOF
}

env_name=""
assume_yes=false

while [ $# -gt 0 ]; do
  case "$1" in
  --yes | -y)
    assume_yes=true
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

state_key="env/$env_name/terraform.tfstate"
ssm_prefix="/coppice/$env_name"

ident="$(aws sts get-caller-identity --output json)" ||
  die "no usable AWS credentials (aws sts get-caller-identity failed)"
say "AWS account $(jq -r .Account <<<"$ident") as $(jq -r .Arn <<<"$ident")"

if [ "$assume_yes" = false ]; then
  printf "destroy the '%s' environment in %s? [y/N] " "$env_name" "$REGION"
  read -r reply
  case "$reply" in
  y | Y | yes | YES) ;;
  *) die "aborted" ;;
  esac
fi

tf_init_env "$env_name"

if aws s3api head-object --bucket "$STATE_BUCKET" --key "$state_key" \
  --region "$REGION" >/dev/null 2>&1; then
  # `release_tarball` is a required variable and the artefact object computes
  # filemd5() of it, which is evaluated at plan time even for a destroy — a
  # missing path is an error before anything can be destroyed. An empty temp
  # file satisfies it; destroy uploads nothing.
  placeholder="$(mktemp -t coppice-destroy-tarball.XXXXXX)"
  trap 'rm -f "$placeholder"' EXIT

  say "destroying the env stack for '$env_name'"
  terraform -chdir="$ENV_DIR" destroy \
    -input=false -auto-approve \
    -var "env_name=$env_name" \
    -var "release_tarball=$placeholder" \
    -var "region=$REGION" \
    -var "domain=$DOMAIN" \
    -var "state_bucket=$STATE_BUCKET"
else
  warn "no state object at s3://$STATE_BUCKET/$state_key — nothing to destroy; sweeping anyway"
fi

# --- sweep -------------------------------------------------------------------

say "sweeping SSM parameters under $ssm_prefix"
# No `mapfile`: this has to run under the bash 3.2 that ships with macOS.
leftover_params="$(
  aws ssm get-parameters-by-path --region "$REGION" \
    --path "$ssm_prefix" --recursive \
    --query 'Parameters[].Name' --output text 2>/dev/null | tr '\t' '\n' | sed '/^$/d'
)"
if [ -n "$leftover_params" ]; then
  warn "deleting $(printf '%s\n' "$leftover_params" | wc -l | tr -d ' ') leftover parameter(s)"
  # delete-parameters takes at most 10 names per call.
  printf '%s\n' "$leftover_params" | xargs -n 10 \
    aws ssm delete-parameters --region "$REGION" --names >/dev/null
else
  say "no parameters left under $ssm_prefix"
fi

# The authoritative leak check asks each service directly, by tag or by the
# `coppice-<env>-` name prefix every resource carries. The Resource Groups
# Tagging API is NOT authoritative here: it kept listing terminated instances,
# deleted volumes and the rules of deleted security groups for well over an
# hour after the first real teardown, so it is shown afterwards as advice only.
tag_filter="Name=tag:coppice:env,Values=$env_name"
prefix="coppice-$env_name-"

# Each check prints one line per live resource, and nothing when clean.
live_resources() {
  aws ec2 describe-instances --region "$REGION" --filters "$tag_filter" \
    "Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped" \
    --query 'Reservations[].Instances[].InstanceId' --output text | tr '\t' '\n' | sed 's/^./instance &/'
  aws ec2 describe-volumes --region "$REGION" --filters "$tag_filter" \
    --query 'Volumes[].VolumeId' --output text | tr '\t' '\n' | sed 's/^./volume &/'
  aws ec2 describe-addresses --region "$REGION" --filters "$tag_filter" \
    --query 'Addresses[].AllocationId' --output text | tr '\t' '\n' | sed 's/^./elastic-ip &/'
  aws ec2 describe-nat-gateways --region "$REGION" --filter "$tag_filter" \
    "Name=state,Values=pending,available,deleting" \
    --query 'NatGateways[].NatGatewayId' --output text | tr '\t' '\n' | sed 's/^./nat-gateway &/'
  aws ec2 describe-security-groups --region "$REGION" --filters "$tag_filter" \
    --query 'SecurityGroups[].GroupId' --output text | tr '\t' '\n' | sed 's/^./security-group &/'
  aws ec2 describe-vpcs --region "$REGION" --filters "$tag_filter" \
    --query 'Vpcs[].VpcId' --output text | tr '\t' '\n' | sed 's/^./vpc &/'
  aws ec2 describe-launch-templates --region "$REGION" --filters "$tag_filter" \
    --query 'LaunchTemplates[].LaunchTemplateName' --output text | tr '\t' '\n' | sed 's/^./launch-template &/'
  aws autoscaling describe-auto-scaling-groups --region "$REGION" \
    --query "AutoScalingGroups[?starts_with(AutoScalingGroupName, '$prefix')].AutoScalingGroupName" \
    --output text | tr '\t' '\n' | sed 's/^./auto-scaling-group &/'
  aws elbv2 describe-load-balancers --region "$REGION" \
    --query "LoadBalancers[?starts_with(LoadBalancerName, '$prefix')].LoadBalancerName" \
    --output text | tr '\t' '\n' | sed 's/^./load-balancer &/'
  aws elbv2 describe-target-groups --region "$REGION" \
    --query "TargetGroups[?starts_with(TargetGroupName, '$prefix')].TargetGroupName" \
    --output text | tr '\t' '\n' | sed 's/^./target-group &/'
  aws cognito-idp list-user-pools --region "$REGION" --max-results 60 \
    --query "UserPools[?Name=='coppice-$env_name'].Id" --output text | tr '\t' '\n' | sed 's/^./cognito-user-pool &/'
  aws s3api list-buckets --query "Buckets[?starts_with(Name, '$prefix')].Name" \
    --output text | tr '\t' '\n' | sed 's/^./s3-bucket &/'
  aws iam list-roles --query "Roles[?starts_with(RoleName, '$prefix')].RoleName" \
    --output text | tr '\t' '\n' | sed 's/^./iam-role &/'
  aws iam list-instance-profiles --query "InstanceProfiles[?starts_with(InstanceProfileName, '$prefix')].InstanceProfileName" \
    --output text | tr '\t' '\n' | sed 's/^./iam-instance-profile &/'
}

say "checking every service for resources of environment '$env_name'"
remaining="$(live_resources 2>&1 | sed '/^$/d' || true)"
if [ -n "$remaining" ]; then
  warn "resources of environment '$env_name' still exist:"
  printf '%s\n' "$remaining" | sed 's/^/  /'
  die "teardown incomplete — remove the above by hand, they are still billable"
fi
say "no instances, volumes, addresses, NAT gateways, security groups, VPCs, launch templates, ASGs, load balancers, target groups, user pools, buckets or IAM roles remain"

# Advisory only, for the reason above; it usually still lists terminated
# instances and deleted volumes for a while.
stale="$(aws resourcegroupstaggingapi get-resources --region "$REGION" \
  --tag-filters "Key=coppice:env,Values=$env_name" \
  --query 'length(ResourceTagMappingList)' --output text 2>/dev/null || echo 0)"
if [ "${stale:-0}" != "0" ]; then
  say "the tagging API still lists $stale ARN(s) for coppice:env=$env_name; those are deleted resources it has not yet forgotten"
fi
say "Terraform state left in place at s3://$STATE_BUCKET/$state_key"
