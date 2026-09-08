# Bootstrap stack for the AWS demo (docs/roadmap/aws-demo-plan.md): the
# once-per-account things every environment shares — the Terraform state
# bucket, GitHub's OIDC trust, the `coppice.jwjr.uk` hosted zone and the
# wildcard certificate the load balancers terminate.

terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Deliberately empty: this stack creates the bucket it stores its own state
  # in, so bucket/key/region cannot be literals here on the very first apply.
  # scripts/aws-demo/bootstrap.sh applies the bucket against a local backend,
  # then migrates the state in with `-backend-config`. Locking is the S3
  # conditional-write lockfile (Terraform >= 1.10), so there is no DynamoDB
  # table to create or pay for.
  backend "s3" {}
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      "coppice:env"        = "bootstrap"
      "coppice:managed-by" = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
