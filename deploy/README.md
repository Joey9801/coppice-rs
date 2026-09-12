# Host packaging

What `.github/workflows/release-build.yml` ships, and how a host consumes it.
This is the packaging layer only; the rules it packages are documented in
[configuration.md](../docs/operations/configuration.md),
[cluster-lifecycle.md](../docs/operations/cluster-lifecycle.md) and
[security.md](../docs/operations/security.md), which this file links rather
than repeats. The deployment it exists for is
[the AWS demo plan](../docs/roadmap/aws-demo-plan.md).

## The tarball

`coppice-<version>-<target>.tar.gz`, built natively on `ubuntu-24.04` and
`ubuntu-24.04-arm`, with a `.sha256` beside it:

```
coppice                                   # one static-enough binary, both roles
deploy/install.sh
deploy/systemd/coppice-coordinator.service
deploy/systemd/coppice-agent.service
deploy/examples/coordinator.toml
deploy/examples/agent.toml
deploy/examples/policy.toml
deploy/README.md
```

The web UI is compiled *into* the binary (`crates/coppice-api/build.rs` embeds
`web/dist`), so there is no static tier to deploy. The release workflow builds
`web/dist` before `cargo build` and fails the job if `web/dist/index.html` is
missing — without it the coordinator silently serves a "run npm build" stub.

## Install

```sh
sudo mkdir -p /opt/coppice-release
sudo tar xzf coppice-<version>-<target>.tar.gz -C /opt/coppice-release
sudo /opt/coppice-release/deploy/install.sh --role coordinator   # or --role agent
```

`install.sh` creates the `coppice` and `coppice-agent` system users, installs
the binary at `/usr/local/bin/coppice`, installs both units, creates
`/etc/coppice` (root-owned, 0755), and runs `daemon-reload`. It writes no
configuration and starts nothing: cloud-init or an operator does that.
Re-running it is safe.

Configuration then goes at:

| path | what |
|---|---|
| `/etc/coppice/coordinator.toml` | coordinator node config (`deploy/examples/coordinator.toml`) |
| `/etc/coppice/agent.toml` | agent node config (`deploy/examples/agent.toml`) |
| `/etc/coppice/enroll-token` | the enrollment secret, 0600, owned by the role's user |
| `/var/lib/coppice/pki/` or `/var/lib/coppice-agent/pki/` | leaf, key and CA bundle under `[tls] source = "cluster"` — **written by the daemon**, not by you (see `docs/operations/configuration.md`) |

Then `systemctl enable --now coppice-coordinator` (or `coppice-agent`).

An agent host must have Docker installed **before** the agent unit starts: the
unit's `SupplementaryGroups=docker` is resolved by systemd, not by the daemon,
so on a host with no `docker` group the unit dies with `status=216/GROUP`
before `ExecStart` runs. `Restart=always` makes that self-healing — the unit
retries until the group exists — but cloud-init should install Docker first
either way.

## Forming the cluster

Formation is one local act on any one parked coordinator, exactly once per
cluster lifetime (ADR 0037 §3):

```sh
sudo coppice coordinator init \
  --config /etc/coppice/coordinator.toml \
  --policy policy.toml \
  --out-dir /root/coppice-day0
```

Run it as **root**: it speaks to the daemon's admin socket at
`/run/coppice/admin.sock`, which is owner-only by design — being able to open
that socket *is* the authorization for the verbs it carries, so there is no
`--target` and no credential to pass. `--out-dir` collects the operator
certificate, its key and the CA bundle; they are printed to the terminal and
not stored otherwise. Re-running against an already-formed cluster is a
refusal, and the policy's own seeds are idempotent (see the comments in
`deploy/examples/policy.toml`).

## Checking it came up

```sh
systemctl status coppice-coordinator          # Type=notify: active (running) means listeners are serving
curl -s -o /dev/null -w '%{http_code}\n' localhost:7070/readyz?require=healthy
coppice cluster status --api http://localhost:7070
curl -s localhost:7070/api/v1/nodes           # agents that have registered
```

Systemd readiness and cluster readiness are different questions. `READY=1` is
sent as soon as the listeners serve, which includes a *parked* daemon waiting
for formation — that daemon is `active (running)` and answers
`/readyz?require=healthy` with 503. The 200 is the real gate for bringup
automation, and it requires a full, caught-up voter set held continuously for
`[raft] health_stability_interval`.

## Two things the units say out loud

- **The agent unit is `Type=exec`, not `Type=notify`.** The agent daemon has no
  `sd_notify` caller today (only the coordinator does, in
  `crates/coppice-coordinator/src/systemd.rs`), so `Type=notify` would sit out
  the notify timeout and then fail the unit. It also installs no SIGTERM
  handler, so `systemctl stop` is the kernel's default termination: running
  containers are cleaned up by the reap janitor on the next start and by the
  coordinator's liveness timeout, not by a drain. There is no drain verb yet
  (issue #49).
- **`Restart=always` is the whole recovery story.** Both daemons' startup paths
  are idempotent and resume from any interruption, so "not formed yet", "the
  enrollment token has not landed yet", "no coordinator is up yet" and "the
  process crashed" are all the same event: start again.
