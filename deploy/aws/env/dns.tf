# The environment's one public name. An alias rather than a CNAME so the apex
# form stays available and there is no TTL to wait out on teardown.
#
# IPv4 only: the load balancer is `ip_address_type = "ipv4"` (the default), so
# an AAAA record would resolve to nothing.
resource "aws_route53_record" "this" {
  zone_id = data.terraform_remote_state.bootstrap.outputs.zone_id
  name    = local.fqdn
  type    = "A"

  alias {
    name    = aws_lb.this.dns_name
    zone_id = aws_lb.this.zone_id

    # The balancer's own target-group health decides whether the name answers;
    # there is nothing else behind it to fail over to.
    evaluate_target_health = true
  }
}
