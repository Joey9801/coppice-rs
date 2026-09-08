# Everything an instance must learn at boot, and everything formation produces,
# passes through Parameter Store under /coppice/<env>. Nothing secret is ever
# baked into user-data, which is readable by anything that can reach IMDS.
#
# All three SecureStrings use the AWS-managed `alias/aws/ssm` key, whose key
# policy already permits any principal in this account that is allowed the SSM
# API call — so the instance policies need no explicit KMS grant.

# The coordinator-role enrollment secret. Alphanumeric only: it travels through
# `sed` into the formation policy and through a shell variable on the way to
# the token file, and a shell metacharacter in it would be a landmine for no
# added entropy at this length.
resource "random_password" "enroll_coordinator" {
  length  = 48
  special = false
}

# Each role gets its own secret: verification keeps the last token whose hash
# matches, so one shared value would leave one role unable to enroll.
resource "random_password" "enroll_agent" {
  length  = 48
  special = false
}

resource "aws_ssm_parameter" "enroll_coordinator" {
  name  = "${local.ssm_prefix}/enroll/coordinator"
  type  = "SecureString"
  value = random_password.enroll_coordinator.result
}

resource "aws_ssm_parameter" "enroll_agent" {
  name  = "${local.ssm_prefix}/enroll/agent"
  type  = "SecureString"
  value = random_password.enroll_agent.result
}

# The seeded Cognito user's permanent password. The character-class minima
# match the pool's password policy exactly; `override_special` is the set
# Cognito accepts, minus the characters that would need quoting in the CLI
# invocations the smoke test makes.
resource "random_password" "demo_user" {
  length           = 24
  min_upper        = 1
  min_lower        = 1
  min_numeric      = 1
  min_special      = 1
  override_special = "!@#%^*-_+="
}

resource "aws_ssm_parameter" "demo_user_password" {
  name  = "${local.ssm_prefix}/demo-user/password"
  type  = "SecureString"
  value = random_password.demo_user.result
}

# The operator certificate, its key and the cluster CA bundle. Formation mints
# them on a coordinator and writes them here with `PutParameter --overwrite`,
# which is why they are created as placeholders and their values ignored:
# Terraform still owns the parameters and destroys them with the environment,
# but never fights the value formation put there.
resource "aws_ssm_parameter" "operator_cert" {
  name  = "${local.ssm_prefix}/operator/cert"
  type  = "SecureString"
  value = "unset"

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "operator_key" {
  name  = "${local.ssm_prefix}/operator/key"
  type  = "SecureString"
  value = "unset"

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "operator_ca" {
  name  = "${local.ssm_prefix}/operator/ca"
  type  = "SecureString"
  value = "unset"

  lifecycle {
    ignore_changes = [value]
  }
}
