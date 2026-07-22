# Deployment story: plan

Status: **plan**, written 2026-07-10; Part 1 updated 2026-07-22. This is
the working plan for making both control-plane and compute deployment
operable with minimal human ceremony. The coordinator half is now decided:
[OD-14](open-decisions.md) is resolved by
[ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md),
and Part 1 below describes that settled intent awaiting implementation.
The agent half ([OD-15](open-decisions.md)) remains open.

Two goals, one per audience:

1. **Coordinators**: a rolling replace/upgrade should be a templated,
   near-zero-thought operation — identical config apart from addresses,
   no hand-allocated identities, no hand-carried certificates.
2. **Agents**: a cloud autoscaling group whose launch script brings a
   fresh compute node from cloud-init to servicing workloads with **no
   human in the loop**, and whose scale-in drains gracefully instead of
   getting shot after a liveness timeout.

## Where we already are (2026-07-10)

- Coordinator raft identity is self-minted at init and stamped in the data
  directory ([ADR 0025](../decisions/0025-self-minted-coordinator-identity.md));
  `node_id` is gone from config. Per-replica config now differs only in
  addresses.
- All entity ids are typed self-describing strings
  ([ADR 0024](../decisions/0024-typed-self-describing-ids.md)).
- One `coppice` binary runs every role (`coordinator`, `agent`, `dev`,
  `job`): a deployment ships a single artifact.
- `coppice dev` demonstrates the fully-autonomous bootstrap pattern in
  miniature: identities minted and persisted on first boot, TLS material
  generated rather than provisioned, restart resumes from the stamp.
- Agent **registration is already self-service**: the coordinator accepts
  any register from a session whose CA-signed client cert's CN matches the
  claimed node id (ADR 0011 binding, ADR 0009 epochs). There is no
  allowlist to maintain — the trust anchor is solely cert issuance.

## Part 1 — Coordinator deployment (decided: ADR 0037)

The C1–C4 friction items this section originally tracked are all resolved
by [ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md);
the operational workflow is described in
[cluster-lifecycle](../operations/cluster-lifecycle.md). In brief:

- **C3, join choreography** → the daemon runs one flagless command in
  every situation (`--bootstrap`/`--join` are gone). A new instance
  discovers and probes for the cluster, self-joins (learner admission →
  catch-up → promotion) through an idempotent convergence loop, or
  *parks* if no cluster exists; first-ever formation is an explicit,
  formation-token-keyed `coppice-cli cluster init`, never emergent. The
  authorization question is settled as a machine self-service grant
  amending ADRs 0022/0023: one vote per CA-attested installation
  identity, self-scoped verbs only; removal/init/set-address require the
  operator-profile certificate.
- **C2, finding the cluster** → a seed-only `Discovery` trait behind a
  uniform `[discovery]` config section: `static`, `dns`, `file` (local
  multi-process clusters), `ec2-asg`. Consul remains a possible future
  backend, deliberately unbuilt. Membership stays the sole authority.
- **C1, PKI** → as planned: issuance stays external (Vault-style
  short-lived leaves or config-managed certs), the coordinator gains
  cert reload without restart, and ADR 0037 adds one hard requirement —
  a stable, unique certificate subject per coordinator installation,
  since the subject now anchors the membership grant.
- **C4, rolling upgrade** → replacement is "start the new machine";
  removal rides the promotion joint change. Serial fleet replacement is
  gated on the new machine-readable `/readyz` (an ASG instance refresh
  with a launch lifecycle hook polling it implements the loop; no
  further coordinator code needed).

The coordinator half of this (issue #47) has landed: the flagless daemon
with derived intent; the seed-only `Discovery` trait with the `static`,
`dns`, `file`, and `ec2-asg` backends; explicit token-keyed `cluster init`
with durable, resumable formation; the self-converging membership loop with
idempotent verbs and promotion-coupled removal; the machine self-service
authorization grant with TLS reload; and the `/readyz` readiness surface.
The `ec2-asg` backend reads this instance's id and region from IMDSv2, lists
its Auto Scaling group's members (lifecycle states Pending/Pending:Wait/
Pending:Proceed/InService), and resolves their private IPs; its
`LivenessAttestor` lets discovery absence strengthen an overflow removal
(ADR 0037 §5). The pre-0037 admin verbs are retained as the break-glass
surface.

Deliberately **not** in this landing, and still deferred:

- **platform-CAS auto-formation** — the opt-in, per-platform
  `BootstrapAuthority` behind the same `InitializeCluster` seam;
- **agent-side enrollment and drain** — Part 2 below, still OD-15.

## Part 2 — Agent lifecycle (autoscaling to zero-touch)

### The gaps, ranked by how hard they block the ASG goal

| # | Gap | Today |
| --- | --- | --- |
| A1 | Node identity + config are hand-authored | `node_id` written into `agent.toml` by a human, must match the cert CN |
| A2 | Cert issuance is out-of-band | ADR 0011's enrollment-token → CSR → per-node-cert flow is documented, not built; `configuration.md` lists an `enrollment token path` field no code reads |
| A3 | Capacity is static config | the advertised vector is typed in by hand; nothing detects cores/memory/disk |
| A4 | No graceful scale-in | `SetNodeSchedulable` exists in the state machine but **nothing calls it** (no API, no CLI); there is no compute-node removal/GC command at all; scale-in today = instance dies → 90 s liveness timeout → `DeclareNodeLost` → running attempts killed as `NodeLost` |
| A5 | The Docker executor is a stub | every `start` fails; the agent cannot run real workloads (tracked as the top critical-path item) |

### Plan, in shipping order

**A1 — self-minted agent identity (mirrors ADR 0025).** On first boot with
no journal, the agent mints `NodeId::new()` and persists it in its data
dir (`coppice dev` already does exactly this); `node_id` leaves
`agent.toml`. The CN↔NodeId binding then inverts: identity exists first,
the cert is issued *for* it — which is the enrollment flow's shape anyway.

**A2 — enrollment (the keystone).** Implement ADR 0011's flow:

1. The launch template carries one secret: a scoped, expiring
   **enrollment token** (minted by `coppice node enroll-token`, stored in
   the ASG's secret manager / instance metadata).
2. First boot: agent mints its NodeId, generates a keypair, sends
   CSR + token to a coordinator **enrollment endpoint** (server-TLS-only;
   it is the one endpoint a certless client may call).
3. The coordinator validates the token, signs a leaf with CN = the typed
   node id, and returns it with the cluster CA bundle. The agent persists
   both and from then on speaks ordinary mTLS.
4. Renewal uses the same endpoint authenticated by the *current* cert
   (re-issue before expiry), giving short-lived agent certs for free.

Where the signing key lives is the OD-15 decision: a coordinator-held CA
(replicated policy holds the CA cert, the leader holds the key — simplest,
no new infra) versus delegating signing to Vault (better key hygiene,
external dependency). The protocol above is identical either way, so the
endpoint can ship with the built-in signer and grow a Vault backend.

**A3 — capacity autodetect.** Detect cpu/memory/disk at startup
(`available_parallelism`, cgroup/sysinfo limits, statvfs on the workdir)
with `[capacity]` becoming an optional override. Advertised capacity is
already re-sent on every heartbeat, so nothing downstream changes.

**A4 — graceful drain and decommission.** Three pieces:

1. **A drain verb**: `coppice node drain <node-id> [--wait]` (admin CLI
   now, API later) proposing `SetNodeSchedulable{false}` — the apply and
   scheduler sides already honor it; `--wait` polls until the node's live
   allocations reach zero.
2. **Agent-initiated drain on shutdown**: SIGTERM → agent reports
   draining, waits for running work up to a deadline, then exits. Cloud
   wiring: an ASG lifecycle hook (or spot-interruption watcher) runs the
   drain before the instance is reaped, holding scale-in protection until
   the wait completes.
3. **Node record GC**: a `RemoveNode`-for-compute-nodes command (records
   are currently immortal — `DeclareNodeLost` only marks unschedulable),
   proposed by housekeeping for nodes unschedulable and empty past a
   retention window (ADR 0012 style), so an ASG that churns instances
   daily does not grow state forever.

`DeclareNodeLost` stays as the backstop for ungraceful death; the point of
A4 is that *planned* scale-in never rides the 90 s timeout or kills work.

**A5 — Docker executor** is unchanged in scope and remains the top
critical-path item ahead of everything here except possibly A1 (trivial).

### The resulting launch script

With A1–A3 shipped, the entire ASG user-data is:

```sh
curl -o /usr/local/bin/coppice …   # or baked into the AMI
cat > /etc/coppice/agent.toml <<EOF
data_dir = "/var/lib/coppice-agent"
coordinators = ["coord.batch.example.com:7072"]   # DNS, per Part 1 C2
[tls]
enrollment_token_path = "/run/coppice/enroll-token"   # from secrets manager
EOF
systemctl start coppice-agent   # ExecStart=coppice agent --config …
```

No ids, no certs, no capacity numbers, no coordinator-side pre-registration.

## Sequencing against the critical path

The MVP critical path landed 2026-07-20 (Docker executor → API server →
CLI), so nothing here competes with it any more. Remaining sequencing:
the Part 1 coordinator implementation (issue #47) has landed per ADR 0037
(platform-CAS auto-formation remains the deferred follow-up above); on the
agent side,
A1 (hours, removes ceremony) can land any time, while A2/A4 still wait on
the OD-15 signer/decommission decisions — ADRs 0022/0023 are written, so
authorization is no longer the blocker there.
