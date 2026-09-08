# The Terraform state bucket, shared by this stack (key bootstrap/) and every
# env stack (key env/<env_name>/). Locking uses S3 conditional writes
# (`use_lockfile = true` in the backend config), so no DynamoDB table exists.

locals {
  state_bucket = "coppice-terraform-state-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket" "state" {
  bucket = local.state_bucket

  # Losing this bucket loses the record of every live environment, and the env
  # stacks would then leak their instances. Destroying it has to be a
  # deliberate, manual act.
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id

  # State history is the only recovery path from a bad apply or a truncated
  # write, and the backend's own rollback advice assumes it.
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    # State files are written and read constantly; the bucket key collapses
    # per-object encryption calls. Free with SSE-S3, harmless either way.
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket = aws_s3_bucket.state.id

  # State contains the enrollment secrets and the demo user's password in
  # clear; nothing about it may ever become reachable anonymously.
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    id     = "expire-noncurrent-state-versions"
    status = "Enabled"

    # Versioning is for recovery from the last few applies, not an archive:
    # ephemeral demo environments churn state constantly.
    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }

  depends_on = [aws_s3_bucket_versioning.state]
}
