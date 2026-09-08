# One network load balancer carries both planes: a TLS listener holding the
# ACM certificate for the client plane, and a plain TCP listener that passes
# the agent plane's mTLS through untouched. It is also the stable name agents
# discover, so replacing a coordinator needs no agent restart.

resource "aws_lb" "this" {
  # Load balancer and target group names are capped at 32 characters by the
  # ELB API, and `env_name` may be 20; substr keeps a long environment name
  # from failing the apply, and the role suffix stays distinguishing because
  # only its tail is trimmed.
  name               = substr("${local.name_prefix}-nlb", 0, 32)
  load_balancer_type = "network"
  internal           = false
  subnets            = [for subnet in aws_subnet.public : subnet.id]
  security_groups    = [aws_security_group.nlb.id]

  # Coordinators are spread over both zones and any of them can serve any
  # request, so a zone with a briefly unhealthy coordinator should borrow the
  # other's rather than fail.
  enable_cross_zone_load_balancing = true

  tags = {
    Name = "${local.name_prefix}-nlb"
  }
}

# Both target groups carry the same instances and the same health check. The
# check is HTTP `/readyz` on the client port rather than a TCP connect on the
# target port, because a *parked* coordinator — one whose listeners serve but
# which has not joined the cluster — accepts TCP on both ports while being
# useless to a client and to an enrolling agent. Plain `/readyz` answers 200
# only from a formed replica (voter, learner or caught-up joiner), so the
# balancer never routes enrollment or agent traffic to a parked node.
resource "aws_lb_target_group" "client" {
  name        = substr("${local.name_prefix}-client", 0, 32)
  vpc_id      = aws_vpc.this.id
  target_type = "instance"
  protocol    = "TCP"
  port        = 7070

  # Instances are replaced, not drained, so there is nothing to wait for
  # beyond letting in-flight requests finish.
  deregistration_delay = 30

  health_check {
    protocol            = "HTTP"
    port                = "7070"
    path                = "/readyz"
    matcher             = "200"
    interval            = 10
    healthy_threshold   = 2
    unhealthy_threshold = 2
  }

  tags = {
    Name = "${local.name_prefix}-client"
  }
}

resource "aws_lb_target_group" "agent" {
  name        = substr("${local.name_prefix}-agent", 0, 32)
  vpc_id      = aws_vpc.this.id
  target_type = "instance"
  protocol    = "TCP"
  port        = 7072

  deregistration_delay = 30

  # Same gate as the client group, and deliberately not a TCP check on 7072:
  # the agent listener is up on a parked coordinator too.
  health_check {
    protocol            = "HTTP"
    port                = "7070"
    path                = "/readyz"
    matcher             = "200"
    interval            = 10
    healthy_threshold   = 2
    unhealthy_threshold = 2
  }

  tags = {
    Name = "${local.name_prefix}-agent"
  }
}

# The public client plane. The certificate is the bootstrap stack's wildcard
# for *.<domain>, so enrollment verifies it under ordinary system roots and
# there is no private CA to distribute before a node can enroll.
resource "aws_lb_listener" "client" {
  load_balancer_arn = aws_lb.this.arn
  port              = 443
  protocol          = "TLS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = data.terraform_remote_state.bootstrap.outputs.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.client.arn
  }
}

# The agent plane: protocol TCP, not TLS, so the handshake reaches the
# coordinator. The coordinator's leaf therefore has to serve the balancer's
# public name, which is what `[listen] extra_sans` in the rendered
# coordinator.toml is for.
resource "aws_lb_listener" "agent" {
  load_balancer_arn = aws_lb.this.arn
  port              = 7072
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.agent.arn
  }
}
