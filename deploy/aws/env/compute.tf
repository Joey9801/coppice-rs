# Six instances and an ops host, all from the stock Ubuntu 24.04 arm64 image
# with cloud-init doing the work — no baked AMI in this iteration. Boot to
# ready is a few minutes, which an on-demand environment can afford.
#
# Neither ASG-launched instances nor their volumes are reachable by the
# provider's `default_tags`, so every tag they need is repeated explicitly in
# `tag_specifications` here and in the ASG `tag` blocks below.

locals {
  # The one thing every instance must be able to do before it has any coppice
  # identity: reach IMDS. Hop limit 1 keeps a container on the host from
  # reaching it (the agents run untrusted job containers), and IMDSv2 is
  # required so a request-forgery bug in anything on the host cannot mint
  # credentials with a plain GET.
  metadata_options = {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }
}

resource "aws_launch_template" "coordinator" {
  name          = "${local.name_prefix}-coordinator"
  image_id      = data.aws_ssm_parameter.ubuntu.value
  instance_type = var.coordinator_instance_type

  iam_instance_profile {
    arn = aws_iam_instance_profile.coordinator.arn
  }

  vpc_security_group_ids = [aws_security_group.coordinator.id]

  metadata_options {
    http_endpoint               = local.metadata_options.http_endpoint
    http_tokens                 = local.metadata_options.http_tokens
    http_put_response_hop_limit = local.metadata_options.http_put_response_hop_limit
  }

  block_device_mappings {
    # Canonical's images root on /dev/sda1.
    device_name = "/dev/sda1"

    ebs {
      volume_size = var.coordinator_root_gb
      volume_type = "gp3"
      # The raft log, the manifest stamp and the node's private key all live on
      # this volume (ADR 0016); it is not worth having them unencrypted for the
      # zero cost of gp3 encryption.
      encrypted             = true
      delete_on_termination = true
    }
  }

  user_data = base64encode(templatefile("${path.module}/cloud-init/coordinator.sh.tftpl", local.cloud_init_vars))

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name                 = "${local.name_prefix}-coordinator"
      "coppice:env"        = var.env_name
      "coppice:role"       = "coordinator"
      "coppice:managed-by" = "terraform"
    }
  }

  tag_specifications {
    resource_type = "volume"
    tags = {
      Name                 = "${local.name_prefix}-coordinator"
      "coppice:env"        = var.env_name
      "coppice:role"       = "coordinator"
      "coppice:managed-by" = "terraform"
    }
  }

  # The ASG below references this template by version, so a user-data or AMI
  # change must produce a new version rather than fail on an in-place update.
  update_default_version = true
}

resource "aws_launch_template" "agent" {
  name     = "${local.name_prefix}-agent"
  image_id = data.aws_ssm_parameter.ubuntu.value
  # The mixed-instances policy below supplies the type per capacity pool; this
  # is the fallback for the on-demand portion.
  instance_type = var.agent_instance_types[0]

  iam_instance_profile {
    arn = aws_iam_instance_profile.agent.arn
  }

  vpc_security_group_ids = [aws_security_group.agent.id]

  metadata_options {
    http_endpoint               = local.metadata_options.http_endpoint
    http_tokens                 = local.metadata_options.http_tokens
    http_put_response_hop_limit = local.metadata_options.http_put_response_hop_limit
  }

  block_device_mappings {
    device_name = "/dev/sda1"

    ebs {
      # Holds the OS, the Docker image cache and the telemetry segments that
      # back `coppice job logs`, on top of the 8 GiB the agent config reserves.
      volume_size           = var.agent_root_gb
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }

  user_data = base64encode(templatefile("${path.module}/cloud-init/agent.sh.tftpl", local.cloud_init_vars))

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name                 = "${local.name_prefix}-agent"
      "coppice:env"        = var.env_name
      "coppice:role"       = "agent"
      "coppice:managed-by" = "terraform"
    }
  }

  tag_specifications {
    resource_type = "volume"
    tags = {
      Name                 = "${local.name_prefix}-agent"
      "coppice:env"        = var.env_name
      "coppice:role"       = "agent"
      "coppice:managed-by" = "terraform"
    }
  }

  update_default_version = true
}

resource "aws_autoscaling_group" "coordinator" {
  name                = "${local.name_prefix}-coordinator"
  vpc_zone_identifier = [for subnet in aws_subnet.public : subnet.id]

  # Fixed size, not a scaling target: the voter count is a replicated
  # consensus property (`[discovery] cluster_size`), so growing the group
  # without changing the config would be wrong rather than useful.
  min_size         = var.coordinator_count
  max_size         = var.coordinator_count
  desired_capacity = var.coordinator_count

  # `EC2` rather than `ELB`: a parked coordinator waiting for formation is
  # deliberately unhealthy to the target group's /readyz check, and an ELB
  # health check would terminate the whole fleet in a loop before formation
  # could ever run.
  health_check_type = "EC2"

  # Keep waiting for capacity through failed scaling activities rather than
  # failing the apply on the first one. The account's first ever ASG creates
  # the AWSServiceRoleForAutoScaling service-linked role on the fly and the
  # first launches fail with "Access denied when attempting to assume role"
  # until IAM catches up; the group retries and succeeds a minute later.
  ignore_failed_scaling_activities = true

  launch_template {
    id      = aws_launch_template.coordinator.id
    version = aws_launch_template.coordinator.latest_version
  }

  # Both planes are served by the same instances, so the group registers with
  # both target groups.
  target_group_arns = [
    aws_lb_target_group.client.arn,
    aws_lb_target_group.agent.arn,
  ]

  dynamic "tag" {
    for_each = {
      Name                 = "${local.name_prefix}-coordinator"
      "coppice:env"        = var.env_name
      "coppice:role"       = "coordinator"
      "coppice:managed-by" = "terraform"
    }

    content {
      key   = tag.key
      value = tag.value
      # `coppice:role` in particular has to reach the instance: it is what
      # Prometheus EC2 service discovery will select on.
      propagate_at_launch = true
    }
  }
}

resource "aws_autoscaling_group" "agent" {
  name                = "${local.name_prefix}-agent"
  vpc_zone_identifier = [for subnet in aws_subnet.public : subnet.id]

  min_size         = var.agent_count
  max_size         = var.agent_count
  desired_capacity = var.agent_count

  health_check_type = "EC2"

  # Same reason as the coordinator group, plus spot: an unfulfillable pool is
  # retried against the others rather than failing the apply.
  ignore_failed_scaling_activities = true

  # Agents are not load-balancer targets: they dial out to the coordinators.
  mixed_instances_policy {
    instances_distribution {
      # Spot for everything by default — it is the cheapest compute and a spot
      # interruption is precisely the "replace a worker" scenario the demo
      # wants to exercise. `agents_on_demand` flips the whole group to
      # on-demand for a CI run that needs determinism.
      on_demand_base_capacity                  = 0
      on_demand_percentage_above_base_capacity = var.agents_on_demand ? 100 : 0
      spot_allocation_strategy                 = "price-capacity-optimized"
    }

    launch_template {
      launch_template_specification {
        launch_template_id = aws_launch_template.agent.id
        version            = aws_launch_template.agent.latest_version
      }

      # One override per type: more entries mean more spot capacity pools to
      # draw from and fewer interruptions.
      dynamic "override" {
        for_each = var.agent_instance_types

        content {
          instance_type = override.value
        }
      }
    }
  }

  dynamic "tag" {
    for_each = {
      Name                 = "${local.name_prefix}-agent"
      "coppice:env"        = var.env_name
      "coppice:role"       = "agent"
      "coppice:managed-by" = "terraform"
    }

    content {
      key                 = tag.key
      value               = tag.value
      propagate_at_launch = true
    }
  }
}

# A plain instance, not a group: it is a singleton whose only job is to be
# somewhere to run Prometheus from later, and it holds nothing worth replacing
# automatically.
resource "aws_instance" "ops" {
  ami                    = data.aws_ssm_parameter.ubuntu.value
  instance_type          = var.ops_instance_type
  subnet_id              = values(aws_subnet.public)[0].id
  vpc_security_group_ids = [aws_security_group.ops.id]
  iam_instance_profile   = aws_iam_instance_profile.ops.name

  metadata_options {
    http_endpoint               = local.metadata_options.http_endpoint
    http_tokens                 = local.metadata_options.http_tokens
    http_put_response_hop_limit = local.metadata_options.http_put_response_hop_limit
  }

  root_block_device {
    volume_size           = 8
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true

    # Tagged here rather than through `volume_tags`, which the provider cannot
    # combine with per-device tags.
    tags = {
      Name           = "${local.name_prefix}-ops"
      "coppice:role" = "ops"
    }
  }

  user_data_base64 = base64encode(templatefile("${path.module}/cloud-init/ops.sh.tftpl", local.cloud_init_vars))

  tags = {
    Name           = "${local.name_prefix}-ops"
    "coppice:role" = "ops"
  }
}
