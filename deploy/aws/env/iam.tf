# One instance role per node role, each holding only what a code path on that
# node actually calls. Every statement below names the caller.
#
# There is no SSH key anywhere in this environment, so the
# AmazonSSMManagedInstanceCore attachment on all three roles is also the only
# way in: Session Manager is how formation runs and how a host is debugged.
# The Ubuntu AMI ships the SSM agent as a snap, so nothing installs it.

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

# --- coordinator -------------------------------------------------------------

resource "aws_iam_role" "coordinator" {
  name               = "${local.name_prefix}-coordinator"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

data "aws_iam_policy_document" "coordinator" {
  statement {
    # The `ec2-asg` discovery backend
    # (crates/coppice-discovery/src/ec2_asg.rs): find this instance's auto
    # scaling group, list its members, resolve each to a private IP. None of
    # these three APIs supports resource-level scoping, so `*` is the only
    # possible resource.
    sid = "DiscoverOwnAutoScalingGroup"
    actions = [
      "autoscaling:DescribeAutoScalingInstances",
      "autoscaling:DescribeAutoScalingGroups",
      "ec2:DescribeInstances",
    ]
    resources = ["*"]
  }

  statement {
    # cloud-init step 3: download the release tarball.
    sid       = "DownloadRelease"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.artefacts.arn}/*"]
  }

  statement {
    # Without ListBucket, S3 answers a *missing* key with 403 rather than 404,
    # which would make the download retry loop's log indistinguishable from a
    # genuine permissions problem.
    sid       = "ListArtefactBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.artefacts.arn]
  }

  statement {
    # cloud-init step 5 reads this node's own enrollment secret, and formation
    # (scripts/aws-demo/formation.sh, run on a coordinator) reads BOTH secrets
    # to render the [[enroll_token]] entries of the init policy — hence the
    # prefix rather than the single coordinator parameter.
    sid     = "ReadEnrollmentSecrets"
    actions = ["ssm:GetParameter", "ssm:GetParameters"]
    resources = [
      "${local.ssm_parameter_arn_prefix}/enroll/*",
    ]
  }

  statement {
    # Formation stores the operator certificate, its key and the CA bundle it
    # minted back into Parameter Store for the bring-up script to collect.
    sid     = "PublishOperatorMaterial"
    actions = ["ssm:PutParameter"]
    resources = [
      "${local.ssm_parameter_arn_prefix}/operator/*",
    ]
  }
}

resource "aws_iam_role_policy" "coordinator" {
  name   = "${local.name_prefix}-coordinator"
  role   = aws_iam_role.coordinator.id
  policy = data.aws_iam_policy_document.coordinator.json
}

resource "aws_iam_role_policy_attachment" "coordinator_ssm" {
  role       = aws_iam_role.coordinator.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "coordinator" {
  name = "${local.name_prefix}-coordinator"
  role = aws_iam_role.coordinator.name
}

# --- agent -------------------------------------------------------------------

resource "aws_iam_role" "agent" {
  name               = "${local.name_prefix}-agent"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

data "aws_iam_policy_document" "agent" {
  statement {
    # cloud-init step 3: download the release tarball.
    sid       = "DownloadRelease"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.artefacts.arn}/*"]
  }

  statement {
    # Same reason as on the coordinator role: a 404 rather than a 403 for a key
    # that is not there yet.
    sid       = "ListArtefactBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.artefacts.arn]
  }

  statement {
    # cloud-init step 5. An agent runs no formation, so it reads exactly its
    # own secret and not the prefix.
    sid       = "ReadEnrollmentSecret"
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.enroll_agent.arn]
  }
}

resource "aws_iam_role_policy" "agent" {
  name   = "${local.name_prefix}-agent"
  role   = aws_iam_role.agent.id
  policy = data.aws_iam_policy_document.agent.json
}

resource "aws_iam_role_policy_attachment" "agent_ssm" {
  role       = aws_iam_role.agent.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "agent" {
  name = "${local.name_prefix}-agent"
  role = aws_iam_role.agent.name
}

# --- ops ---------------------------------------------------------------------

resource "aws_iam_role" "ops" {
  name               = "${local.name_prefix}-ops"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

data "aws_iam_policy_document" "ops" {
  statement {
    # Prometheus EC2 service discovery, for when this host starts scraping the
    # fleet by instance tag. Supports no resource-level scoping.
    sid       = "PrometheusEc2ServiceDiscovery"
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "ops" {
  name   = "${local.name_prefix}-ops"
  role   = aws_iam_role.ops.id
  policy = data.aws_iam_policy_document.ops.json
}

resource "aws_iam_role_policy_attachment" "ops_ssm" {
  role       = aws_iam_role.ops.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ops" {
  name = "${local.name_prefix}-ops"
  role = aws_iam_role.ops.name
}
