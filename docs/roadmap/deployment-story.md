# Deployment story: plan

Status: **plan**, written 2026-07-10; Part 1 updated 2026-07-22. This is
the working plan for making both control-plane and compute deployment
operable with minimal human ceremony. The coordinator half is now decided:
[OD-14](open-decisions.md) is resolved by
[ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md),
and Part 1 below describes that settled intent awaiting implementation.
ADR 0037 also settles the *signer* half of [OD-15](open-decisions.md)
(the cluster-owned CA signs agent leaves); the decommission/scale-in half
remains open.

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
  enrolls for its certificate, discovers and probes for the cluster,
  self-joins (learner admission → catch-up → promotion) through an
  idempotent convergence loop, or *parks* if no cluster exists.
  First-ever formation is `coppice coordinator init`, a **local-only**
  verb on the daemon's root-owned Unix socket (SSH/SSM/cloud-init run
  it), never emergent and never a network RPC; a `formation_complete`
  marker bounds a fail-stop covering every partial-formation state, and
  the forming daemon serves nothing until it exists. The authorization
  question is settled as a machine self-service grant amending
  ADRs 0022/0023: one seat per cluster-minted installation identity,
  self-scoped verbs only; removal/replacement/set-address require the
  operator-profile certificate.
- **C2, finding the cluster** → a seed-only `Discovery` trait behind a
  uniform `[discovery]` config section: `static`, `dns`, `file` (local
  multi-process clusters), `ec2-asg`. Consul remains a possible future
  backend, deliberately unbuilt. Membership stays the sole authority.
- **C1, PKI** → decided the other way from the original plan: **the
  cluster owns its CA**. The root is minted at formation; the CA key
  never enters replicated state (it normally resides on voter disks,
  plus a promotion candidate past the key-transfer gate, under an
  explicit custody invariant and a root-equivalence threat model);
  coordinators and agents obtain leaves through one token-authenticated
  public endpoint (`POST /api/v1/enroll` on the client listener, whose
  externally-signed certificate certless machines verify via ordinary
  system roots — never TOFU, no CA-pin distribution, single-phase
  bringup; renewal rides the internal mTLS services). Subjects are
  cluster-minted, so stability and uniqueness hold by construction. The
  coordinator gains cert reload without restart; external PKI
  (Vault-style leaves, config-managed certs) remains a supported
  substitution, never a requirement.
- **C4, rolling upgrade** → replacement is "start the new machine".
  A dead predecessor's removal rides the newcomer's promotion joint
  change on the leader's own replication evidence
  (terminate-before-launch: fully hands-off); replacing a *live* voter
  is the explicit, operator-authenticated `ReplaceVoter{old,new}`
  (launch-before-terminate rollouts drive it). Serial fleet replacement
  is gated on the new machine-readable `/readyz` (an ASG instance
  refresh with a launch lifecycle hook polling it implements the loop;
  no further coordinator code needed).

Implementation is tracked as issue #47 and has landed in chunks: the
substrate and hot-reload TLS store, the cluster-owned PKI primitives and
replicated identity facts, formation (chunk 01, PR #70), PKI core
(chunk 02, PR #71), the local admin socket and `coppice coordinator init`
(chunk 03, PR #72), enrollment — `POST /api/v1/enroll` and the token
lifecycle (chunk 04, PR #73), the convergence loop that turns a parked
daemon into a learner and then a voter, plus `/readyz` and its
`?require=healthy` gate (chunk 05, PR #74), and evidence-gated removal
plus `ReplaceVoter` and CA-key custody (chunk 06, PR #76). The pre-0037
manual sequence and the `--bootstrap`/`--join` flags are gone; every
coordinator runs the one flagless `coppice coordinator --config …`
command described above. What remains is chunk 07 — completing the test
matrix and writing the re-root runbook (this PR) — which closes out
issue #47.

## Part 2 — Agent lifecycle (autoscaling to zero-touch)

### The gaps, ranked by how hard they block the ASG goal

| # | Gap | Today |
| --- | --- | --- |
| A1 | ~~Node identity + config are hand-authored~~ — landed | the agent mints `NodeId::new()` on first boot and persists it at `<data_dir>/node-identity`; `node_id` is gone from `agent.toml` (a tombstone key rejects it with the migration), so a fleet ships one byte-identical file |
| A2 | ~~Cert issuance is out-of-band~~ — landed | ADR 0011's enrollment-token → CSR → per-node-cert flow is built (`POST /api/v1/enroll`, `[enrollment]` in `configuration.md`, agent-side renewal), resolving OD-15a; see the plan below for the shape as shipped |
| A3 | ~~Capacity is static config~~ — landed | cpu/memory/disk are detected at startup (`available_parallelism` ∩ cgroup `cpu.max`, `MemTotal` ∩ `memory.max`, `statvfs` on `data_dir`); `[capacity]` survives only as a per-dimension override |
| A4 | No graceful scale-in | `SetNodeSchedulable` exists in the state machine but **nothing calls it** (no API, no CLI); there is no compute-node removal/GC command at all; scale-in today = instance dies → 90 s liveness timeout → `DeclareNodeLost` → running attempts killed as `NodeLost` |
| A5 | The Docker executor is a stub | every `start` fails; the agent cannot run real workloads (tracked as the top critical-path item) |

### Plan, in shipping order

**A1 — self-minted agent identity (mirrors ADR 0025), landed.** On first
boot with no journal, the agent mints `NodeId::new()` and persists it at
`<data_dir>/node-identity`; `node_id` left `agent.toml` (the key is a
tombstone that fail-stops with the migration, which for a pre-A1 node is
"seed the identity file once with your existing id"). A data dir holding
a journal but no identity file is a hard error, never a re-mint — fencing
history and the leaf's CN both hang off the id. The CN↔NodeId binding
inverted as planned: identity exists first, enrollment issues the cert
*for* it, and `coppice dev` now goes through this same production path.

**A2 — enrollment (the keystone), landed.** ADR 0011's flow, whose
signer ADR 0037 decided (resolving OD-15a), is built as follows:

1. The launch template carries a **role-scoped enrollment token**
   (agent role; reusable and long-lived is the supported default — no
   secrets manager or minting service required; short-lived per-refresh
   minting via `coppice node enroll-token` is the recommended stronger
   posture where automation exists).
2. First boot: agent mints its NodeId, generates a keypair, sends
   CSR + token to `POST /api/v1/enroll` on the cluster's public client
   listener, verifying its externally-signed certificate via ordinary
   system roots (plain HTTP requires the client-side
   `[enrollment].insecure` opt-in — dev/test only, and distinct from
   the coordinator's serving-side `[client_tls].insecure` — else
   startup fails).
3. The coordinator validates the token, signs a leaf with CN = the typed
   node id, and returns it with the cluster CA bundle. The agent persists
   both and from then on speaks ordinary mTLS.
4. Renewal rides the agent's existing mTLS session, authenticated by
   the *current* cert (re-issue before expiry), giving short-lived
   agent certs for free — and renewal refusal for operator-revoked
   identities is v1's certificate revocation. The public route serves
   certless first contact only.

The signer is the **cluster-owned CA** (ADR 0037 §4): the CA certificate
is replicated; the key never enters replicated state — it normally
resides on voter disks and may also reside on a promotion candidate past
the key-transfer gate, with every disk that has ever received it
accounted root-equivalent — and the leader signs. Tokens are salted
hashes in replicated
policy — listable and revocable with a policy write, with the stated
caveat that token revocation stops future enrollments but does not
recall already-issued leaves. Vault-style external issuance remains a
substitution behind the same `[tls]` paths, not a dependency.

**A3 — capacity autodetect, landed.** cpu/memory/disk are detected at
startup (`available_parallelism` ∩ the cgroup v2 `cpu.max` quota,
`MemTotal` ∩ `memory.max`, `statvfs` on `data_dir`), and `[capacity]`
became an optional per-dimension override: a set field wins over its
reading, a dimension with neither reading nor override fails startup, and
the reservation is checked against the *settled* vector. Advertised
capacity is re-sent on every heartbeat as before, so nothing downstream
changed. The agent also gained the coordinator's `[discovery]` section
(Part 1 C2, same spelling) in place of the old `coordinators` list —
consulted on every reconnect, so a DNS/ASG change reaches a running agent
without a restart — and an empty static seed list is now a config-load
error rather than a runtime panic.

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

With A1–A3 shipped (all three now landed, issue #48), the entire ASG
user-data is:

```sh
curl -o /usr/local/bin/coppice …   # or baked into the AMI
cat > /etc/coppice/agent.toml <<EOF
data_dir = "/var/lib/coppice-agent"
[discovery]                                       # ADR 0037 §2, per Part 1 C2
backend = "dns"
[discovery.dns]
name = "coord.batch.example.com"                  # re-resolved on every reconnect
port = 7072
[enrollment]
token_path = "/etc/coppice/enroll-token"          # baked into user-data
endpoint = "https://coord.batch.example.com:7070" # system-root verified
EOF
systemctl start coppice-agent   # ExecStart=coppice agent --config …
```

The coordinator unit is the same shape and the same single `ExecStart`
line — `coppice coordinator --config /etc/coppice/coordinator.toml`, with
no startup-intent flag in any situation (ADR 0037 §1). Two unit
directives carry the rest of the operational story:
`RequiresMountsFor=/var/lib/coppice`, because the empty-directory
fail-stop is gone and a failed mount must fail unit ordering rather than
present a coordinator with a blank disk; and `Restart=always`, which is
the *entire* recovery story — the convergence loop re-enters from the top
on every restart and the membership verbs are idempotent by contract
(ADR 0037 §6), so a process killed at any step converges after respawn
with no cleanup.

No ids, no certs, no capacity numbers, no coordinator-side
pre-registration, no secrets manager, no CA distribution — the token is
the only secret, and the endpoint is verified like any HTTPS service.

## Sequencing against the critical path

The MVP critical path landed 2026-07-20 (Docker executor → API server →
CLI), so nothing here competes with it any more. Remaining sequencing:
the Part 1 implementation (issue #47) landed in full, and the agent side
followed: A1, A2, and A3 are all built (issue #48), so the zero-touch ASG
launch script above is real. Only A4 still waits on the OD-15
decommission/scale-in decision — ADRs 0022/0023 are written, so
authorization is no longer the blocker there.
