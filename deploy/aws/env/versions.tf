# The per-environment root for the AWS demo (docs/roadmap/aws-demo-plan.md).
# One apply of this root is one whole environment: network, load balancer,
# six instances, Cognito, secrets and DNS name. N of them coexist in the
# account because every name, tag and SSM key is keyed on `env_name`.

terraform {
  # `use_lockfile = true` on the S3 backend (S3-native locking, no DynamoDB
  # table) is only available from 1.10.
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Deliberately empty: the bucket, key, region and `use_lockfile` are passed
  # by `-backend-config` from the bring-up script, because the state key is
  # `env/<env_name>/terraform.tfstate` and `env_name` is not knowable here
  # (backend blocks take no variables).
  backend "s3" {}
}

provider "aws" {
  region = var.region

  # Everything this root creates is disposable and is destroyed by env name,
  # so every resource that can carry provider tags does. The resources these
  # cannot reach — ASG-launched instances and their volumes — repeat them
  # explicitly in the launch templates and ASG tag blocks.
  default_tags {
    tags = {
      "coppice:env"        = var.env_name
      "coppice:managed-by" = "terraform"
    }
  }
}
