# One wildcard certificate for every environment's NLB TLS listener. Regional
# (not us-east-1) because it is consumed by load balancers in var.region.
resource "aws_acm_certificate" "wildcard" {
  domain_name               = "*.${var.domain}"
  subject_alternative_names = [var.domain]
  validation_method         = "DNS"

  # A replacement certificate has to exist and be attached before the old one
  # can go, or every environment's listener breaks mid-apply.
  lifecycle {
    create_before_destroy = true
  }
}

# The validation records themselves never block on anything, so they are
# created unconditionally: they must be in the zone before delegation lands
# for ACM to see them on its first check.
resource "aws_route53_record" "validation" {
  for_each = {
    for option in aws_acm_certificate.wildcard.domain_validation_options :
    option.domain_name => option
  }

  zone_id = aws_route53_zone.coppice.zone_id
  name    = each.value.resource_record_name
  type    = each.value.resource_record_type
  records = [each.value.resource_record_value]
  ttl     = 60

  # ACM emits the same record for `*.<domain>` and `<domain>`; whichever the
  # for_each writes second must be allowed to win rather than fail the apply.
  allow_overwrite = true
}

# Gated: waiting for ISSUED can only succeed once `coppice` is delegated from
# Linode, so the first apply of a fresh account must not include it. See the
# `validate_certificate` variable.
resource "aws_acm_certificate_validation" "wildcard" {
  count = var.validate_certificate ? 1 : 0

  certificate_arn         = aws_acm_certificate.wildcard.arn
  validation_record_fqdns = [for record in aws_route53_record.validation : record.fqdn]

  # The default is 75 minutes; if delegation is right ACM answers in a couple
  # of minutes, and failing fast is friendlier than an hour-long hang.
  timeouts {
    create = "10m"
  }
}
