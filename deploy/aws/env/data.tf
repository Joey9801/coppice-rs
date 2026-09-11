# The once-per-account bootstrap stack owns the hosted zone and the wildcard
# ACM certificate for *.<domain>; every environment reads them rather than
# creating its own (a certificate per environment would burn DNS validation
# records and ACM quota for no gain).
data "terraform_remote_state" "bootstrap" {
  backend = "s3"

  # The same bucket this root's own state lives in (passed to `init` by the
  # scripts as -backend-config, and here as a variable because a backend
  # block cannot be referenced). A fork with its own account sets both.
  config = {
    bucket       = var.state_bucket
    key          = var.bootstrap_state_key
    region       = var.region
    use_lockfile = true
  }
}

# Canonical's public SSM parameter always names the current Ubuntu 24.04 LTS
# arm64 gp3 AMI, so there is no AMI id to hardcode and no image to bake — the
# plan's "no AMI baking in v1" decision.
data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id"
}

# Used to build the SSM parameter ARNs the instance policies are scoped to.
data "aws_caller_identity" "current" {}

locals {
  name_prefix = "coppice-${var.env_name}"

  # The public name of this environment: what the certificate covers, what
  # agents dial for the agent plane, and what enrollment posts to.
  fqdn = "${var.env_name}.${var.domain}"

  # Every secret and every piece of operator material for this environment
  # lives under this prefix, which is also what the instance policies scope to.
  ssm_prefix = "/coppice/${var.env_name}"

  ssm_parameter_arn_prefix = "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter${local.ssm_prefix}"

  # The variables every cloud-init template is rendered with. One map for all
  # three roles so a template can start using a value without a plumbing
  # change; unused entries are harmless.
  cloud_init_vars = {
    env_name        = var.env_name
    fqdn            = local.fqdn
    region          = var.region
    artefact_bucket = aws_s3_bucket.artefacts.bucket
    artefact_key    = aws_s3_object.release.key
    # Stamped into the rendered user-data so a tarball whose contents change
    # under the same name still produces a new launch-template version (and a
    # visible plan diff), instead of nothing at all.
    artefact_sha256   = filesha256(var.release_tarball)
    ssm_prefix        = local.ssm_prefix
    cluster_id        = random_uuid.cluster.result
    cognito_pool_id   = aws_cognito_user_pool.this.id
    cognito_client_id = aws_cognito_user_pool_client.web.id
    # The ops host's Prometheus; unused by the other two templates.
    prometheus_version = var.prometheus_version
    prometheus_sha256  = var.prometheus_sha256
  }
}

# The logical cluster name every replica in this environment shares (ADR
# 0020/0037). Generated once per environment and kept in state: it survives a
# wipe-and-re-form of the cluster, which the per-formation history_id does not.
resource "random_uuid" "cluster" {}
