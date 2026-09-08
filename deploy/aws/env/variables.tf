variable "env_name" {
  description = "Environment name. Every resource name, tag and SSM key is keyed on it, and it is the DNS label of <env_name>.<domain>."
  type        = string

  validation {
    # A DNS label that is also safe in resource names and SSM paths. The
    # 20-character ceiling keeps the derived names (longest is the Cognito
    # hosted-UI domain prefix, `coppice-<env>-<6 hex>`) inside their limits.
    condition     = can(regex("^[a-z][a-z0-9-]{0,19}$", var.env_name))
    error_message = "env_name must match ^[a-z][a-z0-9-]{0,19}$."
  }
}

variable "release_tarball" {
  description = "Local path to the release tarball to deploy, e.g. coppice-<version>-aarch64-unknown-linux-gnu.tar.gz. Uploaded to the environment's artefact bucket and fetched by cloud-init on every instance."
  type        = string
}

variable "region" {
  description = "AWS region. The ACM certificate, the load balancer and the instances all live in it, and the ec2-asg discovery backend reads its own region from IMDS, so nothing else is region-specific."
  type        = string
  default     = "eu-west-2"
}

variable "domain" {
  description = "Parent hosted zone, created by the bootstrap stack. The environment is served at <env_name>.<domain>."
  type        = string
  default     = "coppice.jwjr.uk"
}

variable "agents_on_demand" {
  description = "Run the agent ASG on on-demand instances instead of spot. Spot is the default (cheapest, and an interruption is exactly the worker-replacement scenario the demo wants to show); set true when a CI run needs determinism."
  type        = bool
  default     = false
}

variable "coordinator_count" {
  description = "Coordinator ASG size, and the [discovery] cluster_size the coordinators are configured with. Changing it means changing deploy/examples/coordinator.toml too."
  type        = number
  default     = 3
}

variable "agent_count" {
  description = "Agent ASG size."
  type        = number
  default     = 3
}

variable "coordinator_instance_type" {
  description = "Coordinator instance type (arm64/Graviton)."
  type        = string
  default     = "t4g.small"
}

variable "agent_instance_types" {
  description = "Agent instance types, as spot capacity-pool overrides. The first entry is also the on-demand type when agents_on_demand is set; more entries mean more spot pools to draw from. All 2-vCPU Graviton shapes, so deploy/examples/agent.toml's reservation fits any of them. A single type was unfulfillable on the first real bring-up (t4g.medium spot in two zones), which is why the default is a list."
  type        = list(string)
  default     = ["t4g.medium", "t4g.large", "c7g.large", "c6g.large", "m7g.large", "m6g.large"]

  validation {
    condition     = length(var.agent_instance_types) > 0
    error_message = "agent_instance_types must list at least one instance type."
  }
}

variable "ops_instance_type" {
  description = "Ops instance type. It runs nothing yet (Prometheus is a later addition), so the smallest Graviton size is right."
  type        = string
  default     = "t4g.nano"
}

variable "coordinator_root_gb" {
  description = "Coordinator root volume size. ADR 0016 makes 'replace the instance' the recovery story, so the raft data directory lives on the root volume and there is no separate EBS volume to attach."
  type        = number
  default     = 20
}

variable "agent_root_gb" {
  description = "Agent root volume size. It has to hold the OS, the Docker image cache and the [reservation] disk figure in deploy/examples/agent.toml (8 GiB)."
  type        = number
  default     = 30
}

variable "demo_user_email" {
  description = "Username (an email address) of the single seeded Cognito user. Its password is generated here and stored as an SSM SecureString; no mail is ever sent."
  type        = string
  default     = "demo@coppice.jwjr.uk"
}

variable "availability_zones" {
  description = "Availability zones to spread the subnets, the load balancer and both ASGs across."
  type        = list(string)
  # Three rather than the two the load balancer needs: each zone is another
  # spot capacity pool for the agents.
  default = ["eu-west-2a", "eu-west-2b", "eu-west-2c"]

  validation {
    condition     = length(var.availability_zones) >= 2
    error_message = "availability_zones must list at least two zones (the load balancer requires two subnets)."
  }
}

variable "vpc_cidr" {
  description = "VPC CIDR. Only ever reached from inside the VPC and from the load balancer, so the range is arbitrary; it is a variable so that two environments can be peered later if anything ever needs it."
  type        = string
  default     = "10.42.0.0/16"
}

variable "state_bucket" {
  description = "The Terraform state bucket the bootstrap stack created, read here for its outputs (zone id, certificate ARN). The scripts pass the same value they hand to `terraform init` for this root's own state."
  type        = string
}

variable "bootstrap_state_key" {
  description = "Object key of the bootstrap stack's state in state_bucket."
  type        = string
  default     = "bootstrap/terraform.tfstate"
}
