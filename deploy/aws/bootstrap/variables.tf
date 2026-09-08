variable "region" {
  description = "Region everything in the demo lives in; the ACM certificate must share it with the load balancers that use it."
  type        = string
  default     = "eu-west-2"
}

variable "domain" {
  description = "Public domain delegated to this account's Route53 zone. Environments get <env_name>.<domain>."
  type        = string
  default     = "coppice.jwjr.uk"
}

variable "github_repo" {
  description = "GitHub repository allowed to assume the federated CI role, as owner/name."
  type        = string
  default     = "Joey9801/coppice-rs"
}

variable "validate_certificate" {
  description = <<-EOT
    Whether to wait for ACM to validate the wildcard certificate.

    ACM cannot validate until `coppice` is delegated to this zone's
    nameservers, which is a manual edit in Linode's DNS manager for jwjr.uk
    (the parent zone stays at Linode). So the first apply runs with this
    false: it creates the zone, the certificate and the validation CNAMEs but
    never blocks. Add the four NS records Linode-side, then apply again with
    `-var validate_certificate=true` (or `bootstrap.sh --validate`) to have
    Terraform wait for the certificate to become ISSUED.
  EOT
  type        = bool
  default     = false
}
