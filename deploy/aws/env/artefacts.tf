# One bucket per environment holding exactly one object: the release tarball
# every instance downloads at boot. Per-environment rather than shared so that
# destroying the environment destroys its artefacts, and so a release-gate run
# cannot be perturbed by another environment replacing the tarball underneath
# it.

resource "aws_s3_bucket" "artefacts" {
  bucket = "${local.name_prefix}-artefacts-${data.aws_caller_identity.current.account_id}"

  # The environment is disposable and the only object in here is a build
  # artefact that exists elsewhere; `terraform destroy` must not need a manual
  # empty-the-bucket step first.
  force_destroy = true
}

# Versioning stays off: the bucket holds one immutable object per apply, and
# noncurrent versions would only make force_destroy slower.

resource "aws_s3_bucket_server_side_encryption_configuration" "artefacts" {
  bucket = aws_s3_bucket.artefacts.id

  # SSE-S3 rather than SSE-KMS: nothing here is secret (it is the same tarball
  # published on the releases page), and a KMS key would need a grant on every
  # instance role for no benefit.
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "artefacts" {
  bucket = aws_s3_bucket.artefacts.id

  # Instances read it with their instance-role credentials; nothing anonymous
  # ever should.
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "release" {
  bucket = aws_s3_bucket.artefacts.id
  key    = basename(var.release_tarball)
  source = var.release_tarball

  # `source_hash` rather than `etag`: it compares a local digest and is not
  # confused by multipart uploads, so replacing the tarball with a new build
  # of the same name still triggers a re-upload (and, through user-data, an
  # instance refresh on the next launch).
  source_hash = filemd5(var.release_tarball)
}
