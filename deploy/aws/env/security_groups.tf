# Four groups, one per plane. Nothing opens port 22 anywhere: there are no key
# pairs in this environment and shell access is SSM Session Manager, which the
# agent reaches outbound over the egress rules below.

resource "aws_security_group" "nlb" {
  name        = "${local.name_prefix}-nlb"
  description = "Coppice ${var.env_name} network load balancer"
  vpc_id      = aws_vpc.this.id

  tags = {
    Name = "${local.name_prefix}-nlb"
  }
}

# The public client plane: HTTPS API, web UI and the /enroll endpoint that a
# fresh coordinator or agent posts its CSR to. TLS terminates here on the ACM
# certificate.
resource "aws_vpc_security_group_ingress_rule" "nlb_client" {
  security_group_id = aws_security_group.nlb.id
  description       = "Public HTTPS client plane (API, web UI, enrollment)"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

# The agent plane, passed through untouched: it is mTLS gRPC end to end and the
# balancer must not terminate it. Public because the agents dial the balancer's
# public name; authorization is the mTLS handshake, not the network.
resource "aws_vpc_security_group_ingress_rule" "nlb_agent" {
  security_group_id = aws_security_group.nlb.id
  description       = "Agent plane, mTLS pass-through to the coordinators"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 7072
  to_port           = 7072
}

# The balancer forwards to the coordinator targets; without egress the target
# groups never turn healthy.
resource "aws_vpc_security_group_egress_rule" "nlb_all" {
  security_group_id = aws_security_group.nlb.id
  description       = "Forward to the coordinator target groups"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_security_group" "coordinator" {
  name        = "${local.name_prefix}-coordinator"
  description = "Coppice ${var.env_name} coordinators"
  vpc_id      = aws_vpc.this.id

  tags = {
    Name = "${local.name_prefix}-coordinator"
  }
}

# Client listener from the balancer. TLS has already been terminated, so this
# hop carries plain HTTP inside the VPC — the posture deploy/examples/
# coordinator.toml sets `[client_tls] insecure = true` for.
resource "aws_vpc_security_group_ingress_rule" "coordinator_client_from_nlb" {
  security_group_id            = aws_security_group.coordinator.id
  description                  = "Client plane from the load balancer"
  referenced_security_group_id = aws_security_group.nlb.id
  ip_protocol                  = "tcp"
  from_port                    = 7070
  to_port                      = 7070
}

# The same port from anywhere in the VPC: the target group's HTTP /readyz
# health check arrives from the balancer's own subnet addresses rather than
# from its security group, and the ops instance scrapes /metrics on this port.
resource "aws_vpc_security_group_ingress_rule" "coordinator_client_from_vpc" {
  security_group_id = aws_security_group.coordinator.id
  description       = "Health checks, /metrics scrapes and intra-VPC clients"
  cidr_ipv4         = var.vpc_cidr
  ip_protocol       = "tcp"
  from_port         = 7070
  to_port           = 7070
}

# Raft peer traffic, coordinator to coordinator only. `ec2-asg` discovery hands
# peers private IPs inside this group, so self-reference is exactly the set.
resource "aws_vpc_security_group_ingress_rule" "coordinator_raft" {
  security_group_id            = aws_security_group.coordinator.id
  description                  = "Raft peer traffic between coordinators"
  referenced_security_group_id = aws_security_group.coordinator.id
  ip_protocol                  = "tcp"
  from_port                    = 7071
  to_port                      = 7071
}

# Agent plane from the balancer's own addresses.
resource "aws_vpc_security_group_ingress_rule" "coordinator_agent_from_nlb" {
  security_group_id            = aws_security_group.coordinator.id
  description                  = "Agent plane from the load balancer"
  referenced_security_group_id = aws_security_group.nlb.id
  ip_protocol                  = "tcp"
  from_port                    = 7072
  to_port                      = 7072
}

# And from the VPC: an NLB with a TCP pass-through listener preserves the
# client's source IP, so agent connections arrive on the coordinator carrying
# the *agent's* private address, not the balancer's, and would otherwise be
# dropped by the rule above.
resource "aws_vpc_security_group_ingress_rule" "coordinator_agent_from_vpc" {
  security_group_id = aws_security_group.coordinator.id
  description       = "Agent plane with the source IP preserved through the NLB"
  cidr_ipv4         = var.vpc_cidr
  ip_protocol       = "tcp"
  from_port         = 7072
  to_port           = 7072
}

# apt, the S3 artefact download, the SSM agent's outbound session and the
# enrollment POST to the balancer's public name.
resource "aws_vpc_security_group_egress_rule" "coordinator_all" {
  security_group_id = aws_security_group.coordinator.id
  description       = "apt, S3, SSM and the public enrollment endpoint"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_security_group" "agent" {
  name        = "${local.name_prefix}-agent"
  description = "Coppice ${var.env_name} node agents"
  vpc_id      = aws_vpc.this.id

  tags = {
    Name = "${local.name_prefix}-agent"
  }
}

# The agent-hosted NodeService (ADR 0034): how a coordinator fetches this
# node's job logs and usage detail after a job goes terminal. Coordinators are
# the only callers.
resource "aws_vpc_security_group_ingress_rule" "agent_node_service" {
  security_group_id            = aws_security_group.agent.id
  description                  = "NodeService log and usage reads from coordinators"
  referenced_security_group_id = aws_security_group.coordinator.id
  ip_protocol                  = "tcp"
  from_port                    = 7073
  to_port                      = 7073
}

# The agent's `metrics_addr` is unauthenticated by design, so it is closed to
# everything but the ops instance that will scrape it.
resource "aws_vpc_security_group_ingress_rule" "agent_metrics" {
  security_group_id            = aws_security_group.agent.id
  description                  = "Prometheus scrape from the ops instance"
  referenced_security_group_id = aws_security_group.ops.id
  ip_protocol                  = "tcp"
  from_port                    = 9464
  to_port                      = 9464
}

# apt, the S3 artefact download, Docker image pulls for the jobs it runs, SSM,
# and the agent plane out to the balancer.
resource "aws_vpc_security_group_egress_rule" "agent_all" {
  security_group_id = aws_security_group.agent.id
  description       = "apt, S3, Docker image pulls, SSM and the agent plane"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_security_group" "ops" {
  name = "${local.name_prefix}-ops"
  # No ingress rule at all: everything on this host is reached by SSM port
  # forwarding, which is an outbound session from the instance.
  description = "Coppice ${var.env_name} ops host"
  vpc_id      = aws_vpc.this.id

  tags = {
    Name = "${local.name_prefix}-ops"
  }
}

# apt, SSM, and the scrapes it will make of the fleet.
resource "aws_vpc_security_group_egress_rule" "ops_all" {
  security_group_id = aws_security_group.ops.id
  description       = "apt, SSM and outbound scrapes"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
