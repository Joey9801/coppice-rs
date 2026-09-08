# GitHub Actions federation. Nothing in the current bring-up path uses this
# role: it exists so a later CI workflow can apply and destroy env stacks from
# the repository without a long-lived access key.

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # AWS verifies GitHub's certificate against its own trust store and ignores
  # these, but the argument is still required; these are the two well-known
  # GitHub Actions thumbprints.
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
  ]
}

data "aws_iam_policy_document" "github_actions_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    # Only tokens minted for this repository may assume the role; any branch,
    # tag or environment within it is allowed (the workflow itself is the
    # narrower gate).
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:*"]
    }

    # Without an audience check any OIDC token from the provider would do.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name               = "coppice-github-actions"
  description        = "Federated role for GitHub Actions to apply and destroy AWS demo environments."
  assume_role_policy = data.aws_iam_policy_document.github_actions_assume.json
  # A demo bring-up plus smoke test plus teardown fits well inside two hours.
  max_session_duration = 7200
}

# Everything the env stack touches outside IAM: VPC, EC2, ELB, S3, SSM,
# Cognito, Route53. PowerUserAccess is deliberately broad — this is a
# single-purpose demo account.
resource "aws_iam_role_policy_attachment" "github_actions_power_user" {
  role       = aws_iam_role.github_actions.name
  policy_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
}

data "aws_iam_policy_document" "github_actions_iam" {
  # PowerUserAccess excludes IAM, but the env stack creates the three instance
  # roles, their policies and their instance profiles. Scoped by name so the
  # role can never touch itself or anything outside the demo.
  statement {
    sid    = "ManageCoppiceInstanceIdentities"
    effect = "Allow"
    actions = [
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:GetRole",
      "iam:TagRole",
      "iam:UntagRole",
      "iam:UpdateRole",
      "iam:UpdateAssumeRolePolicy",
      "iam:PassRole",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy",
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:GetRolePolicy",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:ListInstanceProfilesForRole",
      "iam:CreateInstanceProfile",
      "iam:DeleteInstanceProfile",
      "iam:GetInstanceProfile",
      "iam:AddRoleToInstanceProfile",
      "iam:RemoveRoleFromInstanceProfile",
      "iam:TagInstanceProfile",
      "iam:CreatePolicy",
      "iam:DeletePolicy",
      "iam:GetPolicy",
      "iam:GetPolicyVersion",
      "iam:ListPolicyVersions",
      "iam:CreatePolicyVersion",
      "iam:DeletePolicyVersion",
      "iam:TagPolicy",
    ]
    resources = [
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/coppice-*",
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:instance-profile/coppice-*",
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/coppice-*",
    ]
  }

  # Spot and auto-scaling create their service-linked roles on first use in a
  # fresh account; those role names are AWS's and cannot be name-scoped.
  statement {
    sid       = "CreateServiceLinkedRoles"
    effect    = "Allow"
    actions   = ["iam:CreateServiceLinkedRole"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "github_actions_iam" {
  name   = "coppice-env-iam"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.github_actions_iam.json
}
