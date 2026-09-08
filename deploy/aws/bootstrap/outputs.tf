# The env stack reads these through `terraform_remote_state`; the names are
# part of that contract.

output "state_bucket" {
  description = "Terraform state bucket shared by this stack and every env stack."
  value       = aws_s3_bucket.state.id
}

output "zone_id" {
  description = "Route53 hosted zone for the demo domain."
  value       = aws_route53_zone.coppice.zone_id
}

output "zone_name" {
  description = "Demo domain, without the trailing dot Route53 stores."
  value       = trimsuffix(aws_route53_zone.coppice.name, ".")
}

output "name_servers" {
  description = "Nameservers to delegate to from the parent zone at Linode."
  value       = aws_route53_zone.coppice.name_servers
}

output "certificate_arn" {
  description = "Wildcard certificate for *.<domain>, used by every environment's TLS listener."
  value       = aws_acm_certificate.wildcard.arn
}

output "certificate_validated" {
  description = "Whether this stack waited for the certificate to be issued. Env stacks fail at listener creation if it is false and the certificate is still pending."
  value       = var.validate_certificate
}

output "github_actions_role_arn" {
  description = "Federated role for GitHub Actions; unused until the CI workflow exists."
  value       = aws_iam_role.github_actions.arn
}

output "github_oidc_provider_arn" {
  description = "GitHub Actions OIDC provider in this account."
  value       = aws_iam_openid_connect_provider.github.arn
}

output "delegation_instructions" {
  description = "The one manual step: delegate the demo domain from the parent zone."
  value       = <<-EOT
    Delegate ${trimsuffix(aws_route53_zone.coppice.name, ".")} to this zone:

      1. In Linode's DNS manager, open the jwjr.uk domain and add four NS
         records with hostname "coppice", one per nameserver:

    ${join("\n", [for ns in aws_route53_zone.coppice.name_servers : "           ${ns}"])}

      2. Wait for the delegation to be visible (a minute or two, then cached):

           dig +short NS ${trimsuffix(aws_route53_zone.coppice.name, ".")} @1.1.1.1

      3. Re-run the bootstrap with certificate validation enabled, which waits
         for ACM to issue the wildcard certificate:

           scripts/aws-demo/bootstrap.sh --validate
  EOT
}
