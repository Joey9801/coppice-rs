# Everything the bring-up, formation and smoke-test scripts need. They read
# these with `terraform output -raw`, so the names are a contract.

output "fqdn" {
  description = "The environment's public name."
  value       = local.fqdn
}

output "api_url" {
  description = "Base URL of the client plane: API, web UI and the enrollment endpoint."
  value       = "https://${local.fqdn}"
}

output "nlb_dns_name" {
  description = "The load balancer's own name, for diagnosing a DNS or certificate problem without the alias in the way."
  value       = aws_lb.this.dns_name
}

output "coordinator_asg_name" {
  description = "Coordinator auto scaling group, which formation picks an instance out of to run `coppice coordinator init` on over SSM."
  value       = aws_autoscaling_group.coordinator.name
}

output "agent_asg_name" {
  description = "Agent auto scaling group."
  value       = aws_autoscaling_group.agent.name
}

output "ops_instance_id" {
  description = "Ops host instance id: the Prometheus host, reached with `aws ssm start-session` (port-forward 9090) or `aws ssm send-command`; the smoke test's Prometheus check goes through the latter."
  value       = aws_instance.ops.id
}

output "artefact_bucket" {
  description = "Bucket holding the release tarball the instances boot from."
  value       = aws_s3_bucket.artefacts.bucket
}

output "artefact_key" {
  description = "Object key of that tarball."
  value       = aws_s3_object.release.key
}

output "cognito_user_pool_id" {
  description = "Cognito user pool id."
  value       = aws_cognito_user_pool.this.id
}

output "cognito_client_id" {
  description = "App client id. The coordinator's [sso] client_id and audience, and what a non-interactive USER_PASSWORD_AUTH token request is made against."
  value       = aws_cognito_user_pool_client.web.id
}

output "cognito_issuer" {
  description = "OIDC issuer URL, matching the coordinator's [sso] issuer."
  value       = "https://cognito-idp.${var.region}.amazonaws.com/${aws_cognito_user_pool.this.id}"
}

output "demo_user_email" {
  description = "The seeded user's username. Its password is at <ssm_prefix>/demo-user/password."
  value       = var.demo_user_email
}

output "ssm_prefix" {
  description = "Parameter Store prefix holding this environment's enrollment secrets, demo password and operator certificate material."
  value       = local.ssm_prefix
}

output "cluster_id" {
  description = "The [cluster_id] every replica shares, without the `cluster-` prefix the config carries."
  value       = random_uuid.cluster.result
}

output "vpc_id" {
  description = "VPC id."
  value       = aws_vpc.this.id
}

output "artefact_sha256" {
  description = "SHA-256 of the deployed release tarball. up.sh refuses to apply a different tarball onto an existing environment, because a launch-template change never reaches instances that are already running."
  value       = filesha256(var.release_tarball)
}
