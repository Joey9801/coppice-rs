# AWS multi-node demo and release-gate environment: plan

Status: **plan**, written 2026-09-06 for issue #51. Nothing here is
implemented yet. The purpose is to size the work, fix the shape of the
deployment before any Terraform is written, and record which acceptance
criteria of #51 the codebase can honour today.

## Goals

1. `up` brings a six-node cluster (three coordinators, three Docker agents)
   plus the demo's external services up from nothing on AWS, with **no
   interactive step**; `down` destroys everything.
2. Cost is low enough that the environment is created on demand and
   destroyed after use, and cheap enough to forget about if it is not.
3. Every environment is fully keyed on an `env_name`, so N of them can
   coexist in one account with no shared mutable state, which is what a
   release-gate CI job needs to silo test groups.
4. A smoke test exercises the public client surface end to end and is the
   artefact CI runs. The demo *is* the test.

## What already exists (surveyed 2026-09-06)

Everything a zero-touch deployment needs from the product side is built,
with two documented gaps:

- **One binary, one flagless command per role.** `coppice coordinator
  --config …` and `coppice agent --config …` in every situation; the
  coordinator parks until formation and self-joins after
  ([cluster-lifecycle](../operations/cluster-lifecycle.md), ADR 0037).
- **Formation is a single local act**: `coppice coordinator init --config …
  --policy policy.toml` over the daemon's root-owned Unix socket, exactly
  once per cluster lifetime, on any one parked daemon. It is idempotent
  against a `formation_complete` marker. ADR 0037 blesses SSH/SSM/cloud-init
  as the caller.
- **Enrollment tokens are chosen before boot.** `[[enroll_token]]` entries
  in the init policy carry an operator-supplied secret and label; the policy
  seeds their salted hashes. So Terraform can mint the secrets, bake them
  into user-data, and hand the hashes to `init`. Nothing formation emits
  needs distributing except the operator certificate, which `init
  --out-dir` writes for collection.
- **Discovery backends**: `ec2-asg` (IMDSv2 + DescribeAutoScalingInstances /
  DescribeAutoScalingGroups / DescribeInstances) for coordinators finding
  their own group; `dns` and `static` for agents.
- **Enrollment endpoint trust** is ordinary system-root verification of the
  public client listener's certificate. The config docs explicitly allow
  that listener to sit behind a TLS-terminating load balancer with
  `[client_tls] insecure = true` inside the VPC.
- **Web UI** is embedded into the coordinator from `web/dist` at build time
  and served on the same client listener. No separate static tier.
- **Prometheus**: coordinator `/metrics` rides the client listener (7070);
  the agent has an optional `metrics_addr` that must be set explicitly.
- **OIDC**: `[sso] issuer / client_id / audience`, offline JWKS validation;
  web authorization-code + PKCE login landed 2026-09-03; the CLI consumes a
  bearer token through `COPPICE_TOKEN` and has no login flow (issue #101).
- **Failover**: openraft election, client-minted idempotent job ids
  (ADR 0026), so submit-retry across a leader kill is a no-op. Dead-voter
  removal on a terminate-before-launch replacement is fully hands-off.
- **Job logs and usage** are queryable after terminal via `coppice job logs`
  / `job usage` for ~60 min (agent-local segments, ADR 0034/0036).

Gaps, each of which shapes the acceptance criteria below:

- **No release pipeline.** CI builds nothing shippable: no release job, no
  cross-compilation, no Dockerfile, no systemd units, no example TOMLs
  checked in. The plan's first chunk is that pipeline.
- **No ClickHouse sink** (issue #42) and no durable history store (issue
  #43, `[history] mode = "none"` is the only mode). The demo cannot include
  ClickHouse; it must say so.
- **No drain verb** (OD-15b, issue #49). Worker replacement today is the
  90 s liveness timeout, `DeclareNodeLost`, and attempt retry. The demo
  documents that as current behaviour, which #51 allows.
- **The in-repo fleet tests are in-process.** The `Fleet` harness in
  `crates/coppice-coordinator/tests/common` runs every member as a tokio
  task with real mTLS. This deployment is the first time the daemons run as
  separate processes on separate hosts across a real network. Expect
  process-boundary and network findings, and budget for product PRs.

## Topology

One Terraform root module per environment, parameterised by `env_name`.

```
Route53 zone coppice.jwjr.uk           GitHub Actions (OIDC-federated role)
   └─ <env>.coppice.jwjr.uk ──► NLB ──┬─ :443 TLS (ACM) ──► coordinators :7070  (API, web UI, /metrics, /enroll)
                                    └─ :7072 TCP pass ──► coordinators :7072  (agent plane, mTLS end to end)

VPC 10.x/16, two public subnets, no NAT gateway
   coordinator ASG  (3 × on-demand, [discovery] ec2-asg, raft :7071 intra-SG)
   agent ASG        (3 × spot, [discovery] static → <env>.coppice.jwjr.uk:7072, metrics :9464)
   ops instance     (1 × nano: Prometheus with ec2_sd, reachable via SSM port-forward)
   Cognito user pool + app client (OIDC issuer for [sso], one seeded demo user)
   S3 artefact bucket (release tarball by git sha), SSM parameters (tokens, operator cert)
   IAM instance roles: coordinators (ASG/EC2 describe, S3 get, SSM), agents (S3 get, SSM), ops (EC2 describe)
```

Decisions and why:

- **One NLB, not ALB + something.** The agent plane is mTLS gRPC and must be
  passed through untouched; the client plane needs an externally-signed
  certificate for enrollment. One NLB with a TLS listener (ACM) and a TCP
  passthrough listener covers both, and gives agents a stable discovery
  name that survives coordinator replacement. Pass-through means the
  coordinator itself terminates TLS for the balancer's name, and the agent
  verifies the name it dialled, so coordinator leaves carry that name via
  `[listen] extra_sans` (added in chunk 1) while Raft membership still
  advertises only the private address. Relatedly, `ec2-asg` discovery hands
  peers private IPs, so `advertise_host` must be the instance's private IP
  (rendered from IMDS), not a hostname. Alternative if the pass-through
  proves awkward: coordinators self-register in a Route53 private zone from
  cloud-init and agents use `dns` discovery. Discovery is advisory, so stale
  records are harmless.
- **Public subnets, no NAT.** A NAT gateway alone costs more than the whole
  fleet. Instances get public IPv4 for image pulls and artefact download;
  security groups close everything except the load-balancer paths and
  intra-VPC planes. No SSH: SSM Session Manager for the formation step and
  for debugging.
- **Cognito as the OIDC provider.** Fully Terraform-managed, free at this
  scale, standard discovery document and JWKS, and CI can obtain a token
  non-interactively with `USER_PASSWORD_AUTH`. Risk to verify in a spike:
  Cognito access tokens carry `client_id` rather than `aud`; ID tokens
  carry `aud`. The web PKCE flow and `coppice-authn` must agree on which
  token is sent. If they don't, that is a small product PR, not a design
  change.
- **Prometheus on a nano instance**, EC2 service discovery over instance
  tags, scraping six targets. Amazon Managed Prometheus is per-sample
  billing and needs a sigv4 remote-write path; not worth it here. Grafana
  is out of scope; the smoke test queries the Prometheus HTTP API.
- **No AMI baking in v1.** Ubuntu 24.04 LTS AMI, cloud-init installs Docker
  from apt, downloads the tarball from S3, writes the TOML from a template,
  starts the unit. Boot to ready is a few minutes, acceptable for an
  on-demand environment. Packer is a later optimisation if CI turnaround
  matters. Building on `ubuntu-24.04` runners keeps glibc matched.
- **arm64 (Graviton) from the start.** The repository is public, so the
  `ubuntu-24.04-arm` GitHub runners are free; the release job builds
  natively on one and the instances are `t4g`. About 10–15% cheaper than
  the x86 equivalents, and the demo workload images are multi-arch.
- **Coordinator data on the root volume.** ADR 0016 makes "replace the
  instance" the recovery story, so a detached persistent volume adds
  ceremony without adding a demo. `RequiresMountsFor=` in the unit still
  points at the data path so the unit shape matches production guidance.
  Issue #51 lists persistent volumes; this is a deliberate deviation to
  record in the runbook.
- **Agents on spot.** Cheapest compute, and a spot interruption is exactly
  the "replace a worker" scenario. Spot is optional through a variable for
  CI determinism.

Two Terraform roots: a once-per-account **bootstrap** stack (state bucket
with lockfile, GitHub OIDC provider and role, the `coppice.jwjr.uk` hosted
zone, and an ACM wildcard certificate for `*.coppice.jwjr.uk` validated by
DNS in that zone) and the per-environment **env** stack. The env stack is
what CI applies and destroys.

**Region: `eu-west-2` (London)**, for the owner's latency. Prices there are
a few percent above `us-east-1`; the table below uses London prices. The
`ec2-asg` backend takes the region from IMDS and the ACM certificate lives
in the load balancer's region, so nothing else is region-specific.

**DNS delegation.** `jwjr.uk` is served by Linode's nameservers and stays
there. The bootstrap stack creates the `coppice.jwjr.uk` hosted zone and
outputs its four NS records; the owner adds one `NS` record set for the
`coppice` label in Linode's DNS manager pointing at them. That is the only
manual step in the whole design, done once. Every environment then gets
`<env>.coppice.jwjr.uk` from Terraform alone.

## Bring-up sequence (`scripts/aws-demo/up.sh`)

1. `terraform apply` the env stack. Terraform generates the enrollment
   secrets (one coordinator role, one agent role) and the Cognito demo user
   password, and stores them as SSM SecureStrings under `/coppice/<env>/`.
   User-data on both roles fetches its token from SSM at boot with retry,
   writes `token_path`, and starts the unit. All six instances boot in
   parallel; coordinators park, agents fail enrollment and retry under
   `Restart=always` until the cluster exists.
2. Wait for one coordinator to be InService, then run formation over SSM
   `send-command`: render `policy.toml` with the two `[[enroll_token]]`
   entries (secrets pulled from SSM on the instance, never through the
   operator's shell history) and the demo quota entity, run
   `coppice coordinator init --policy … --out-dir …`, push the operator
   certificate and key to SSM. Idempotent: rerunning on a formed cluster is
   a refusal, not a second cluster.
3. Poll `https://<env>.demo.<domain>/readyz?require=healthy` until 200,
   then `coppice cluster status` until three voters, then `GET /nodes`
   until three schedulable agents.
4. Print the URLs, the demo user, and an exported `COPPICE_API` /
   `COPPICE_TOKEN` snippet.

Teardown is `terraform destroy` with `force_destroy` on the artefact bucket
and no resources that resist deletion. `down.sh` also deletes the SSM
parameters, which Terraform owns anyway.

## Smoke test (`scripts/aws-demo/smoke.sh`)

Runs from a workstation or a CI runner against the public surface only.
Maps onto #51's acceptance criteria:

| # | Criterion | Check |
|---|---|---|
| 1 | one command creates six nodes + services | `up.sh` exits 0 |
| 2 | three voters, three schedulable agents | `cluster status`, `GET /nodes` |
| 3 | real Docker job, queryable afterwards | submit `examples/jobs/…`, wait terminal, `job logs` and `job usage` non-empty, `GET /jobs/{id}` shows transitions |
| 4 | Prometheus scrapes all six | Prometheus API `count(up==1)` is 6, via SSM port-forward |
| 4 | OAuth protects the client API | tokenless `GET /api/v1/overview` is 401; Cognito token succeeds |
| 5 | leader kill, no duplicate submission | find leader, terminate its instance through the ASG, submit the same job id in a retry loop during election, assert one job; assert a new leader and, after the replacement joins, three voters again |
| 6 | worker replacement | terminate an agent mid-job, assert the attempt ends `NodeLost`, the retry lands on another node, and the ASG replacement enrols; runbook states this is the timeout path, not drain |
| 7 | CI exercises it | the workflow below |

ClickHouse is recorded as not demonstrable until issue #42 lands.

## CI workflow (`aws-demo.yml`)

`workflow_dispatch` only: every run costs real money, so nothing runs it
unattended. A release-gate invocation is a manual dispatch on the release
candidate; a schedule can be added later if drift becomes a problem and the
cost is judged worth it. Steps: assume the federated role, build or reuse the
release tarball for the commit, `env_name = ci-<run id>`, `up.sh`,
`smoke.sh`, `down.sh` under `if: always()`. Concurrency is unbounded by
design; siloed test groups are separate env names in a matrix. A budget
alarm and a nightly tag-based sweeper for environments older than a day
protect against leaks from cancelled runs.

## Cost

Per environment-hour, eu-west-2, September 2026 list prices, rounded:

| Item | $/h |
|---|---|
| 3 × t4g.small coordinators, on-demand | 0.053 |
| 3 × t4g.medium agents, spot (on-demand 0.106) | ~0.04 |
| NLB + minimal LCU | ~0.03 |
| ops t4g.nano + 7 public IPv4 + ~150 GB gp3 | ~0.06 |
| Cognito, SSM, S3, Route53 records | ~0 |
| **Total** | **~0.18 (≈0.25 with on-demand agents)** |

Roughly $5 a day left running, $140 a month if never destroyed. A CI run
of ~30 minutes is about $0.10. The bootstrap stack is ~$0.50 a month for
the hosted zone records and state bucket.

## Work breakdown

Sized in PRs. Each is a session or less on its own; the whole is three to
four sessions because chunk 2 needs real-AWS iteration.

1. **Release pipeline and host packaging** (small) — *landed in PR (pending)*.
   A manually dispatched `release-build` workflow (a two-architecture
   release build is too expensive to run per commit): `npm run build` in
   `web/`, then `cargo build --release --bin coppice` natively on
   `ubuntu-24.04-arm` and `ubuntu-24.04`, tarball plus checksum as a
   workflow artefact, attached to the GitHub release when dispatched on a
   `v*` tag. It is `workflow_call`-able, so chunk 5's
   `aws-demo.yml` reuses the very tarball it deploys instead of building its
   own (the S3 copy is that workflow's job, not this one's). Under `deploy/`:
   hardened `coppice-coordinator.service` and `coppice-agent.service`
   (`RequiresMountsFor`, `Restart=always`, `RuntimeDirectoryMode=0700` for the
   admin socket, sandboxing directives), `install.sh`, and templated example
   TOMLs for both roles plus the formation policy — each parse-tested in CI
   against the real config structs. The coordinator unit is `Type=notify`; the
   **agent unit is `Type=exec`**, because the agent daemon has no `sd_notify`
   caller and no SIGTERM handler (a product gap worth its own PR, not folded
   into the packaging one). Verified by booting both units under systemd in a
   container: formation, `/readyz?require=healthy` 200, a restart that resumes
   from the stamp, and an agent that enrols, reaches Docker and registers.
2. **Terraform env stack and `up.sh` / `down.sh`** (large, the risky one).
   Bootstrap stack, env stack, cloud-init templates, SSM-driven formation,
   Cognito. Done when `up.sh` reaches three voters and three schedulable
   agents from nothing, twice in a row with different `env_name`s, and
   `down.sh` leaves no resources. Product findings from the first real
   multi-host run are spun out as their own PRs against `main`, not folded
   into this one.
3. **Observability and the happy-path smoke test** (medium) — *landed*.
   Prometheus on the ops instance with EC2 service discovery, agent
   `metrics_addr` wired, `smoke.sh` covering criteria 2 through 4, Cognito
   token acquisition for the CLI. The Prometheus check queries the ops host
   over SSM run-command rather than a port-forward, so a CI runner needs no
   Session Manager plugin; the port-forward remains the human route.
4. **Chaos scenarios and the runbook** (medium). Coordinator kill and
   worker replacement in `smoke.sh`, `docs/operations/aws-demo.md` with
   cost, sizing, teardown, the ClickHouse and drain caveats, and the
   persistent-volume deviation. Update `deployment-story.md` status.
5. **CI workflow and leak protection** (small). `aws-demo.yml`, federated
   role, budget alarm, sweeper, and the multi-environment matrix shape.

## Spikes before chunk 2

Cheap checks that de-risk the large chunk, done in a session's first hour:

- Cognito token claim compatibility with `coppice-authn` and the web PKCE
  flow (which token is sent, `aud` versus `client_id`).
- Behaviour when `[enrollment].token_path` does not yet exist at daemon
  start: fail-stop and rely on `Restart=always`, or park. Either is fine;
  cloud-init just needs to match it.
- NLB TCP pass-through on 7072 with the agent-plane mTLS and the
  coordinator's `advertise_host` inside a VPC with default private DNS.
- The agent's default 20 GiB disk reservation against the chosen root
  volume size and the 95% pressure gate.

## Settled with the owner (2026-09-06)

- DNS: delegate `coppice.jwjr.uk` from Linode to a Route53 zone the
  bootstrap stack owns; wildcard ACM certificate on it.
- Architecture: arm64, because the public repo gets free arm64 runners.
- Region: `eu-west-2`.
