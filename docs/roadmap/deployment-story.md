# Deployment story: plan

Status: **plan**, written 2026-07-10. This is the working plan for making
both control-plane and compute deployment operable with minimal human
ceremony. Decisions that need settling before implementation are registered
as [OD-14 and OD-15](open-decisions.md); once settled they become ADRs and
this doc is updated to describe the then-current intent.

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

## Part 1 — Coordinator deployment

### Remaining friction

| # | Friction | Today |
| --- | --- | --- |
| C1 | Certificate provisioning | out-of-band PKI; the ADR 0011 enrollment/CSR flow is an unimplemented stub |
| C2 | Finding the cluster | static `peers` seed list in each config file |
| C3 | Join choreography | operator runs `--join`, reads the minted id from the log, then `admin add-learner` + `admin promote` by hand |
| C4 | Rolling upgrade | the serial replace loop in [cluster-lifecycle](../operations/cluster-lifecycle.md) is documented but manual |

### Plan

**C3 first — join automation (no new dependencies).** Add
`coppice coordinator join --config …` (or extend `--join`) so the new
replica, after stamping its identity, *itself* drives the ADR 0016 dance
against the seed list: request add-learner for its own id/address, wait for
catch-up, request promotion (optionally `--replace <old-id>` for rebuilds).
The admin RPCs already exist; what is new is the client loop running inside
the joining node and an authorization question — today any CA-signed cert
may drive membership RPCs, which is exactly the "possession of a cert is
admin" posture ADRs 0022/0023 (authn/z, settled but unwritten) must
formalize before self-join ships. `coppice coordinator replace` then
becomes a one-command rolling-upgrade primitive, and a systemd unit that
runs `join-or-restart` makes coordinator ASGs/instance-groups viable.

**C2 — discovery feeds the seed list, never membership.** Authoritative
addressing already lives in replicated membership (`BasicNode.addr`); only
the *seed list* (whom to dial first) needs discovery. Plan: make `peers`
resolvable from pluggable sources — static list (today), DNS (SRV or
round-robin A records in front of the coordinators), and Consul as an
optional backend. Resolution happens at process start and on admin-CLI
invocation only; no runtime dependency on the discovery system. DNS is the
default recommendation (every environment has it; an internal LB or
headless-service record is enough). Consul adds health-checked entries but
also an operational dependency — see OD-14.

**C1 — PKI.** Intra-cluster mTLS stays mandatory (ADR 0011). The plan is
*not* to build a bespoke CA into the coordinator for coordinator↔coordinator
trust; control-plane nodes are few and provisioned deliberately. Instead:
document and template the two mainstream paths — (a) Vault PKI engine (or
cert-manager in k8s) issuing short-lived leaves at boot via the machine's
cloud identity (IAM auth method), with a config-file `tls` section pointing
at the Vault-agent-rendered paths; (b) plain long-lived certs from
config management for small static clusters. What the coordinator itself
must gain is **cert reload without restart** (tonic listener rebind or
connection-time reload) so short-lived certs are usable; that is the only
code change this item needs. Agent-facing PKI is different — see Part 2,
because agents are numerous and automatic.

**C4 — rolling upgrade** falls out of C3: serially, per replica, `replace`
(spot instance) or drain-restart (in-place upgrade), gated on `admin
status` convergence. Ship it as a documented loop first, automation later.

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

The MVP critical path (Docker executor → API server → CLI) is not displaced
by any of this. Recommended interleaving: A1 (hours, unblocks nothing but
removes ceremony) and C3 (small, pure client-side loop) can land any time;
A2/A4 want ADRs 0022/0023 written first since both are authorization
surfaces; C2/C1 are documentation-plus-small-code and can trail. A5 stays
first among everything.
