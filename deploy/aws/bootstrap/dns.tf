# `jwjr.uk` is served by Linode's nameservers and stays there. This zone owns
# only the `coppice` subtree; the operator adds one NS record set for the
# `coppice` label in Linode's DNS manager pointing at the four nameservers
# below (see the `delegation_instructions` output). That is the only manual
# step in the demo, and it is done once per account.
resource "aws_route53_zone" "coppice" {
  name    = var.domain
  comment = "Coppice AWS demo environments: <env>.${var.domain}"
}
