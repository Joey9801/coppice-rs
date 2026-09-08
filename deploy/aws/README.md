# AWS demo deployment

Terraform for the AWS demo environments described in
[docs/roadmap/aws-demo-plan.md](../../docs/roadmap/aws-demo-plan.md): a
six-node Coppice cluster (three coordinators, three Docker agents) plus the
external services it needs, reachable at `https://<env>.coppice.jwjr.uk`.
There are two Terraform roots — `bootstrap/`, applied once per AWS account,
and `env/`, applied once per environment.

## Bootstrap stack

`bootstrap/` owns the things every environment shares:

- the Terraform state bucket `coppice-terraform-state-<account id>`
  (versioned, SSE-S3, private). Locking is the S3 conditional-write lockfile
  Terraform 1.10 added, so there is no DynamoDB table;
- the GitHub Actions OIDC provider and the `coppice-github-actions` role a
  future CI workflow assumes to apply and destroy environments;
- the Route53 hosted zone for `coppice.jwjr.uk`;
- a wildcard ACM certificate for `*.coppice.jwjr.uk` in `eu-west-2`, which
  every environment's load balancer terminates.

Apply it with `scripts/aws-demo/bootstrap.sh`, using credentials that can
create IAM roles. The script handles the chicken-and-egg of a stack that
creates its own state bucket: on a fresh account it applies the bucket
against a temporary local backend and then migrates the state into it.

`coppice.jwjr.uk` is delegated out of `jwjr.uk`, which stays on Linode's
nameservers, and that delegation is the one manual step in the whole design:

```
scripts/aws-demo/bootstrap.sh            # creates the zone, prints its
                                         # four nameservers and what to do
# add four NS records for the `coppice` label in Linode's DNS manager
dig +short NS coppice.jwjr.uk @1.1.1.1   # confirm the delegation is live
scripts/aws-demo/bootstrap.sh --validate # waits for ACM to issue
```

Until the delegation exists ACM cannot validate, so the first run leaves the
certificate pending on purpose (`validate_certificate = false`) rather than
blocking for an hour. `--plan` shows the plan and changes nothing.

The stack's outputs (`state_bucket`, `zone_id`, `zone_name`, `name_servers`,
`certificate_arn`, `certificate_validated`, `github_actions_role_arn`,
`github_oidc_provider_arn`) are read by the env stack through
`terraform_remote_state`, so they are a contract: do not rename them.


