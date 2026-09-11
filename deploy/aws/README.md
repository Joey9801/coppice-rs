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


## Environment stack

`env/` is one whole environment per apply, keyed on `env_name` (a DNS label,
`^[a-z][a-z0-9-]{0,19}$`). Everything it creates is named `coppice-<env>-…`
and tagged `coppice:env = <env>`, so N environments coexist in the account
with nothing mutable shared between them. `scripts/aws-demo/up.sh` and
`down.sh` are what drive it.

What it builds:

- a VPC with one public subnet per availability zone and **no NAT gateway** —
  a NAT gateway alone costs more than the whole fleet, and the instances need
  egress only for apt, the artefact download and Docker image pulls;
- one network load balancer carrying both planes: a `TLS` listener on 443
  holding the bootstrap stack's wildcard certificate for the client plane
  (API, web UI, `/enroll`), and a plain `TCP` listener on 7072 that passes the
  agent plane's mTLS through untouched. `<env>.coppice.jwjr.uk` is an alias to
  it;
- a coordinator ASG (fixed size, on-demand) registered with both target
  groups, an agent ASG (spot by default, `agents_on_demand = true` for a
  deterministic CI run), and one small ops instance running Prometheus
  (below);
- a Cognito user pool, app client and one seeded demo user — the OIDC issuer
  the coordinators' `[sso]` block points at;
- an artefact bucket holding the release tarball named by `release_tarball`,
  and the environment's SSM parameters under `/coppice/<env>`.

Two things about it are worth knowing before reading the code:

- **Both target groups health-check `HTTP /readyz` on port 7070**, not TCP on
  the target port. A coordinator that is *parked* — listeners serving, cluster
  not yet formed — accepts TCP on both ports while being useless to a client
  and to an enrolling agent. Plain `/readyz` answers 200 only from a formed
  replica, so the balancer never routes to a parked node. The ASG's own health
  check is `EC2` for the same reason: an `ELB` health check would terminate
  the fleet in a loop before formation could ever run.
- **The coordinator security group admits 7072 from the whole VPC**, not just
  from the balancer's group: a TCP pass-through listener preserves the source
  IP, so agent connections arrive carrying the agent's own private address.

### Secrets and cloud-init

Nothing secret is in user-data, which is readable by anything that can reach
IMDS. Terraform mints the two enrollment secrets and the demo user's password
as SSM SecureStrings; `cloud-init/{coordinator,agent}.sh.tftpl` fetch the
one secret their role is entitled to at boot, with a retry loop because a
fresh instance profile's permissions take up to a minute to propagate, and
write it to `/etc/coppice/enroll-token` 0600-owned by the role's user.

Those templates render `/etc/coppice/<role>.toml` from the *checked-in*
`deploy/examples/<role>.toml` with `sed`, so the comments explaining every key
travel to the host, and then refuse to start the unit if any ALL-CAPS
placeholder survived the substitution. The operator certificate parameters are
created here as `"unset"` placeholders with `ignore_changes = [value]`:
formation overwrites them on a coordinator, and Terraform still owns and
destroys them.

The three instance roles hold only what a named code path calls — the
coordinator's `autoscaling:Describe*` is `ec2-asg` discovery, its
`ssm:PutParameter` is formation storing operator material — and every
statement in `iam.tf` carries the caller in a comment. There is no SSH: no key
pair, no port 22, and `AmazonSSMManagedInstanceCore` on all three roles is the
only way onto a host.

The stack's outputs are a contract too; `up.sh`, `formation.sh` and `smoke.sh`
read them by name.

### Prometheus

The ops instance runs one upstream Prometheus (`prometheus_version`, pinned
and checksum-verified in `cloud-init/ops.sh.tftpl`) with **EC2 service
discovery** on the tags the launch templates propagate: `coppice:env` selects
this environment, `coppice:role` selects coordinators and agents, and there is
no static target list, so an ASG replacement is scraped as soon as it is
running. Relabelling picks the port by role:

| role | target | why |
|---|---|---|
| coordinator | `<private ip>:7070` `/metrics` | the client listener; the same one as the API, there is no separate coordinator metrics port |
| agent | `<private ip>:9464` `/metrics` | the agent's dedicated `metrics_addr`, unauthenticated, so the agent security group admits it from the ops instance only |

Every target carries `role`, `instance` (the private DNS name, which is also
the agent's `advertise_host`) and `instance_id`.

Prometheus listens on loopback only and the ops security group has no
ingress rule at all. Reaching it is an SSM port-forward (needs the
[Session Manager plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html)):

```
aws ssm start-session --region eu-west-2 \
  --target "$(terraform -chdir=deploy/aws/env output -raw ops_instance_id)" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["9090"],"localPortNumber":["9090"]}'
# then http://localhost:9090
```

`smoke.sh` instead runs its queries on the host with `aws ssm send-command`,
which needs no plugin and no background process on a CI runner.

Node utilisation is a short in-memory window on the coordinator, so this
Prometheus is the only longer view of it that exists, and it lives on this one
instance with two days of retention: the environment is disposable and so is
its metric history. Grafana is out of scope.

## Bring-up and teardown

Three scripts drive an environment; all of them work from any cwd and take the
environment name as their first argument (`^[a-z][a-z0-9-]{0,19}$`).

```
scripts/aws-demo/up.sh demo --tarball ./coppice-0.1.0-aarch64-unknown-linux-gnu.tar.gz
scripts/aws-demo/down.sh demo --yes
```

`up.sh` applies the env stack, then waits — it does not return until the
cluster is actually usable:

1. preflight: caller identity, an arm64 release tarball, and bootstrap state
   in the state bucket (otherwise it tells you to run `bootstrap.sh` first);
2. `terraform apply` for `env/`, with per-environment state at
   `env/<name>/terraform.tfstate`;
3. picks the first coordinator the ASG reports `InService` whose SSM agent is
   `Online`;
4. runs `scripts/aws-demo/formation.sh` on it via SSM `send-command`. That
   script forms the cluster (`coppice coordinator init` with the rendered
   `deploy/examples/policy.toml`) and stores the operator break-glass
   certificate, key and CA in SSM under `/coppice/<env>/operator/*`. It is
   idempotent — it reads the daemon's `/readyz` phase and skips `init` on an
   already-formed cluster — so `up.sh` always runs it;
5. polls `https://<env>.coppice.jwjr.uk/readyz?require=healthy` for a 200,
   then the API for three coordinator voters and three schedulable nodes.

**Changing the release means a new environment.** A launch-template change
only shapes instances launched after it; the six that already exist keep
running what they booted with, and an instance refresh that is safe for a raft
voter set needs readiness-aware lifecycle hooks this stack does not have yet.
`up.sh` therefore refuses a tarball whose SHA-256 differs from the deployed
one (`artefact_sha256` output) and points at `down.sh`. The hash is also
stamped into the user-data, so a changed tarball under an unchanged name still
shows up as a launch-template diff rather than as nothing.

`--on-demand-agents` swaps the agent ASG off spot, `--skip-apply` re-runs
formation and the waits against an existing stack, `--status-only` runs only
the waits and the summary, and `--timeout` (default 1200 s) is the budget for
each wait.

There is no SSH anywhere: reaching an instance is
`aws ssm start-session --target <instance id>`.

Getting a token for the API or the CLI:

```
export COPPICE_API=https://demo.coppice.jwjr.uk
export COPPICE_TOKEN="$(scripts/aws-demo/up.sh --token-only demo)"
coppice cluster status
```

`--token-only` reads the demo user's password from SSM and exchanges it for a
Cognito **ID** token — the coordinator validates the `aud` claim, which
Cognito puts only in the ID token, never the access token. Neither the
password nor the token is echoed, and neither is ever passed on a command
line.

### The smoke test

`scripts/aws-demo/smoke.sh <env>` proves an environment works from the
outside and is what a CI run executes between `up.sh` and `down.sh`. It
needs a `coppice` CLI (`cargo build --release --bin coppice`; pass
`--coppice PATH` or set `COPPICE_BIN`), touches only the public surface plus
the ops host over SSM for the Prometheus check, prints `PASS`/`FAIL` with
evidence per check, runs every check even after one fails, and exits
non-zero if any did.

| check | what it proves |
|---|---|
| `authn` | a tokenless `GET /api/v1/overview` is 401, a garbage token is 401, and a Cognito ID token minted with `USER_PASSWORD_AUTH` for the seeded user is 200 |
| `coordinators` | `GET /api/v1/coordinators` shows exactly three voters |
| `nodes` | `GET /api/v1/nodes` shows exactly three schedulable agents that are not lost (issue #51's criterion). Health is a leader-only read (#133): a leader-served `healthy` verdict is reported as evidence when one is obtained, not asserted |
| `prometheus` | on the ops host, `count(up{job="coppice"} == 1)` is 6 with three of each role; every coordinator reports `coordinator_state_nodes` = 3 and all three agents expose `agent_running_jobs` |
| `job` | `coppice job submit examples/jobs/stress-demo.toml` (with the quota entity swapped for the one formation seeded) reaches `succeeded`; the timeline from `GET /jobs/{id}/timeline` starts at `job_submitted` and ends in a transition to `succeeded`; `coppice job logs` prints real lines and `coppice job usage` real samples, and the API reports both sources as `available` |

The Prometheus families asserted on are ones that exist in the code
(`describe_metrics` call sites), not the aspirational list in
[docs/operations/observability.md](../../docs/operations/observability.md).

**The job check is time-bounded, and says so in its output.** Logs and usage
are served from per-attempt segments on the agent that ran the work, kept
for about an hour after the attempt ends (`[telemetry]` filesystem sink
retention) and less under disk pressure; terminal jobs are evicted from
replicated state on the `terminal_retention` TTL; and `[history] mode =
"none"` is the only history mode (issue #43), so nothing durable is written
first. The assertions are made within minutes of the job finishing. They are
a "query it now" guarantee, not a "query it tomorrow" one, and the test does
not pretend otherwise.

`--skip-job` runs the four fast checks only; `--job-spec` submits a different
spec; `--timeout` (default 900 s) bounds each wait.

**Spot agents can fail a run honestly.** The agent ASG is spot by default,
and a spot reclaim during the run is reported as exactly what it is: the
Prometheus check sees five healthy targets until the replacement boots,
and a job on the reclaimed node ends `node_lost` (the example spec has
`max_retries = 0`, so the job fails rather than retrying). Two reclaims
hit the first evening's runs in `eu-west-2`. Bring the environment up
with `--on-demand-agents` when the result has to be deterministic, as a
CI run does.

`down.sh` destroys the stack and then proves the environment is gone: it
deletes any SSM parameters left under `/coppice/<env>`, then asks each
service directly — EC2 instances, volumes, addresses, NAT gateways, security
groups, VPCs, launch templates, ASGs, load balancers, target groups, Cognito
pools, buckets and IAM roles — for anything tagged `coppice:env=<env>` or
named `coppice-<env>-…`. It exits non-zero and lists what it found if anything
remains — the demo is billed by the hour. The Resource Groups Tagging API is
deliberately only advisory here: after the first real teardown it went on
listing terminated instances, deleted volumes and the rules of deleted
security groups for over an hour. The environment's Terraform state object is
left in the bucket; its key is printed at the end.
