# Public subnets and no NAT gateway, on purpose: a NAT gateway costs more per
# month than the whole six-instance fleet, and the instances need egress only
# for apt, the S3 artefact download and Docker image pulls. Nothing is reachable
# from outside except through the load balancer's security group, and there is
# no SSH anywhere — access is SSM Session Manager.

resource "aws_vpc" "this" {
  cidr_block = var.vpc_cidr

  # Instances resolve each other's private DNS names; the agent's
  # `[listen] advertise_host` is the private DNS name from IMDS, so this must
  # be on.
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = local.name_prefix
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = local.name_prefix
  }
}

# One /24 per availability zone, carved out of the VPC's /16.
resource "aws_subnet" "public" {
  for_each = { for idx, az in var.availability_zones : az => idx }

  vpc_id            = aws_vpc.this.id
  availability_zone = each.key
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, each.value)

  # Instances are launched with no NAT behind them, so a public IPv4 address
  # is how they reach apt, S3 and Docker Hub.
  map_public_ip_on_launch = true

  tags = {
    Name = "${local.name_prefix}-public-${each.key}"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${local.name_prefix}-public"
  }
}

resource "aws_route" "default" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  for_each = aws_subnet.public

  subnet_id      = each.value.id
  route_table_id = aws_route_table.public.id
}
