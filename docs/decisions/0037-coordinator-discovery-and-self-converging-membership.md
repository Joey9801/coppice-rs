# 37. Coordinator discovery, self-converging membership, and cluster-owned PKI

- **Status:** Accepted
- **Date:** 2026-07-22
- **Resolves:** [OD-14](../roadmap/open-decisions.md#od-14-coordinator-discovery-and-control-plane-pki),
  and the signer half (a) of
  [OD-15](../roadmap/open-decisions.md#od-15-agent-enrollment-and-scale-in)
  (agent-leaf issuance; the drain/scale-in half (b) remains open).
- **Amends:** the startup-intent halves of
  [ADR 0016](0016-coordinator-rebuild-learner-join.md) (`--bootstrap`/`--join`
  as operator-supplied flags),
  [ADR 0020](0020-node-config-vs-replicated-policy.md) (the CLI flag set, the
  static `peers` list, and `cluster init`'s duties),
  [ADR 0022](0022-oidc-identity-and-authentication.md) /
  [ADR 0023](0023-scoped-role-bindings.md) (a narrowly-scoped machine
  self-service grant for membership, §7; CA provenance — left open by
  ADR 0022 — is now decided as a cluster-owned root, §4; the first
  operator-profile certificate is minted at formation, §3), and
  [ADR 0011](0011-container-security-posture.md) (its enrollment-token → CSR
  flow is adopted as specified, and the signer is decided: the cluster CA).
- **Builds on:** [ADR 0025](0025-self-minted-coordinator-identity.md)
  (self-minted raft identity), [ADR 0011](0011-container-security-posture.md)
  (mandatory intra-cluster mTLS).

## Context

Coordinator formation works but is choreographed by a human. The operator
decides per invocation whether a process is `--bootstrap`, `--join`, or a
restart; reads the minted node id out of the new replica's log; runs
`admin add-learner` and `admin promote` by hand; and maintains a static
`peers` seed list that differs per environment. Certificates are provisioned
out of band and require a process restart to rotate. This is the C1–C4
friction table of the [deployment story](../roadmap/deployment-story.md).

The first production deployment is a three-replica EC2 auto-scaling group
with immutable instances: upgrades replace coordinators one at a time, each
replacement waiting until the new node has synchronized raft state. That
model is incompatible with per-invocation operator intent — every instance
runs the *same* user-data and the *same* systemd unit, whether it is a
fresh replica joining or a rebooted instance resuming its volume. The
design must not be EC2-shaped, though: the same binary must also bring up
several coordinator processes on one dev machine for integration tests, and
later fit other platforms.

### Design principles

Several technically sound alternatives — an automatic recovery machine for
bootstrap-time faults, externally-issued certificates, extra evidence
sources for membership decisions, implicit trust-on-first-use transport —
were considered during review and rejected on three grounds that govern
the rest of this document:

- **Rare one-time failures need identification, not automatic recovery.**
  Formation happens once per cluster lifetime. A formation interrupted by a
  crash, disk error, or partition is minutes old and holds no data; the
  correct recovery is *wipe and re-run*, provided the failure is loudly and
  unambiguously identified. Machinery whose only purpose is to auto-heal an
  improbable state during a restartable one-time operation is deleted in
  favor of a clear fail-stop.
- **Every external dependency is a deployment cost.** A minimal production
  cluster should need a DNS name and an OIDC issuer, and nothing else — no
  external PKI, no Vault, no secrets manager on the critical path. External
  issuance remains a supported *substitution*, never a requirement.
- **Reasonably secure, not maximally engineered — and honest about it.**
  The invariants that bound blast radius are kept in full: no automatic
  formation ever, no amnesiac voters, one membership seat per machine
  credential, mTLS on every internal plane, and no credential class that
  silently escalates past its stated scope. What goes is edge-case
  choreography that defends against states the immutable-infrastructure
  model makes into operator errors — those are refused and surfaced, not
  arbitrated. Where v1 accepts a limitation (reusable enrollment tokens,
  no certificate revocation list), the limitation is stated, not papered
  over.

Two invariants from earlier ADRs are load-bearing and must survive:

- **Raft membership is the sole authority on who is in the cluster and at
  what address** (ADR 0016, restated by OD-14). Any discovery mechanism may
  only ever answer "whom might I dial first?", never "who are the voters?".
- **An empty disk joining an existing cluster may only ever enter as a new
  learner** (ADR 0016's amnesiac-voter defense). First-ever formation is the
  sole exception: it requires the explicit operator act in §3. No automatic
  or discovery-driven path can seed an empty disk as a voter.

One approach was considered and rejected in review: making *first-ever
cluster formation* emergent from discovery (a deterministic leaderless
"bootstrap election" among uninitialized candidates). It is unsound for two
reasons. Discovery backends are explicitly allowed to be stale, partial,
or wrong — so two stable divergent discovery views can each satisfy any
local election condition and form two clusters. And no amount of probing
can distinguish first-ever formation from a previously formed cluster
whose members have all vanished from discovery — an empty replacement
fleet would silently re-create an empty history under the same
`cluster_id`. Formation therefore needs a one-time authority outside the
discovery system. We make it an explicit operator act (§3); the daemons
themselves never form a cluster on their own initiative.

Self-join was gated on the authorization model (ADR 0022/0023): membership
RPCs are unscoped-admin cluster verbs. Automating them from the joining
machine itself means machine credentials can now reach membership, so the
grant must be scoped tightly enough that one compromised or misissued
coordinator certificate cannot rewrite arbitrary membership (§7).

## Decision

### 1. One command, derived intent

The coordinator daemon is started the same way in every situation:

```
coppice coordinator --config /etc/coppice/coordinator.toml
```

The `--bootstrap` and `--join` flags are removed from the daemon. Startup
intent is *derived*, not declared:

- **Manifest present** → resume the instance on this disk, exactly as
  today's flagless restart (identity read from the stamp, history-id
  cross-check unchanged). Then run the convergence loop (§6), which no-ops
  when this identity is already a caught-up voter.
- **Manifest present but formation incomplete** (a formation intent
  stamped by `init` without the `formation_complete` marker, §3 — the
  crash may have landed on either side of `raft.initialize`) →
  **fail-stop** in phase `formation-failed`, visible in `/readyz` and
  the exit diagnostics. Recovery is documented and manual: wipe the
  data directory and re-run `cluster init` (§3). There is no resume
  path.
- **Manifest absent** → this is a new instance. If it holds no usable leaf
  certificate, enroll first (§5); then run discovery (§2) and probe the
  candidates (§3):
  - an initialized cluster with a matching `cluster_id` is found →
    self-join (§6);
  - no initialized cluster is found → **park**: serve the certless surface
    (§4), report the `waiting` phase through `/readyz` (§9), and keep
    re-running discovery, enrollment attempts, and probing. A parked daemon
    leaves this state only when an initialized cluster appears in discovery
    (→ enroll if needed, then join) or a local `cluster init` command
    arrives on its admin socket (§3). It never bootstraps on its own.

One systemd unit with one `ExecStart` line therefore covers scale-out
join, instance replacement, and plain restart; first-ever formation is the
one additional, deliberate act at cluster birth (§3) — matching the shape
"one bootstrap intent, N−1 join intents". The hidden
`coppice coordinator admin` verbs are retained as the manual surface, with
their semantics tightened for idempotency (§6); they stop being part of
any routine procedure.

The failure-mode table in
[cluster-lifecycle](../operations/cluster-lifecycle.md) changes accordingly:
"unexpectedly empty directory" stops being a fail-stop and becomes "new
instance: converge". The failed-mount case that fail-stop guarded against
moves to the mount layer (the systemd unit declares
`RequiresMountsFor=/var/lib/coppice`; cloud-init orders volume attach before
service start) — and the blast radius if that guard is missed is bounded by
the amnesiac-voter defense: the worst case is a spurious learner join and a
later removal of the orphaned old identity, never data loss or a second
cluster. A whole fleet that loses its volumes parks and alarms; it can
never re-form an empty history. The identity-stamp mismatch fail-stop
(wrong volume, wrong cluster) is unchanged.

### 2. Discovery: pluggable, seed-only, uniform config

A `Discovery` trait answers one question: *the current set of coordinator
candidates*, as dialable raft addresses. It is consulted by a converging
process (to find someone to enroll with, probe, and join through) and never
on the raft hot path; its output is never authoritative — candidates are
addresses to dial, and the probe protocol decides what they mean. Discovery
being stale, partial, or down can delay convergence; it can never change
membership by itself.

Config gains a `[discovery]` section; the top-level `peers` field is
subsumed by the `static` backend and removed. Backends at this stage:

```toml
[discovery]
backend = "dns"            # "static" | "dns" | "file" | "ec2-asg"
cluster_size = 3           # expected voter count; used by removal (§7)
                           # and the `formed` status field (§9)

[discovery.dns]            # exactly one backend table, matching `backend`
name = "coord.batch.example.com"   # A/AAAA or SRV; SRV supplies ports
port = 7071                        # used when the record carries none
```

- **`static`** — the literal list, today's `peers` under a new roof:
  `addrs = ["coord-1:7071", …]`.
- **`dns`** — resolve one name at each consultation; every distinct address
  is a candidate. TTL staleness is tolerable because discovery only seeds
  dialing.
- **`file`** — enumerate a well-known directory; each file is one
  candidate whose first line is the raft address. Registrations are
  **run-scoped**: each process, on binding its listeners, writes
  `<dir>/<run-id>` where the run id is minted fresh per process launch
  (it is *not* the instance UUID — registration happens before any
  identity exists, and needs no identity: it is advisory dialing
  information, nothing more). The file is removed on graceful shutdown; a
  stale file from a crash costs only a failed dial, because no protocol
  step requires every discovered candidate to respond (§3). This backend
  is what makes port-0 multi-process clusters on one dev machine work
  with no harness coordination beyond a shared directory.
- **`ec2-asg`** — from EC2 instance metadata, find this instance's ASG and
  list its instances. The listing includes lifecycle states `Pending`,
  `Pending:Wait`, `Pending:Proceed`, and `InService`, and excludes
  `Terminating*`: launch lifecycle hooks hold new instances in
  `Pending:Wait` until their hook completes, and a fleet whose instances
  gate their hooks on readiness (§9) would otherwise be invisible to each
  other precisely while converging. This is the only platform-specific
  backend and is a thin adapter over the same trait; Consul or other
  registries are future backends of the same shape, deliberately not
  built now.

`cluster_size` is node-local config rather than replicated policy only
because convergence consults it before replicated state is reachable (the
same reason `cluster_id` is config); replicas disagreeing on it degrade
convergence behavior but never safety, which passes ADR 0020's litmus for
node config.

Everything in the config file is now identical across replicas of a
production cluster: `advertise_host` — the one remaining per-replica field —
defaults to the machine's hostname resolution (explicit value ▸ system FQDN
▸ the local address of the default route), so a fleet ships one literal
config artifact. Several processes on one *host* (dev, integration tests)
still need distinct `data_dir` and ports; that is a host-level concern the
test harness handles by generating per-process files, exactly as
`coppice dev` already does. The invariant this ADR guarantees is that no
config field encodes cluster *role* or *identity* — and with cluster-minted
certificates (§4), no per-replica certificate artifact needs provisioning
either.

### 3. Explicit formation with local authority, no recovery machinery

A converging process probes discovered candidates:

```
ProbeCluster() → { cluster_id, history_id, initialized, node_id?,
                   leader_hint?, voters: [{node_id, addr}]? }
```

Two cluster identifiers appear here, and they are deliberately distinct:

- **`cluster_id`** is the operator-chosen logical name in config,
  identical across the fleet (ADR 0020). It answers *"is this the
  cluster I intend to join?"* — a fresh daemon, which has no stamp yet,
  matches on it when probing.
- **`history_id`** is minted by formation (step 2 below) and stamped
  into every data directory at initialization — joiners learn it at
  admission. It names one raft *history*: a wiped-and-re-formed cluster
  keeps its `cluster_id` but carries a new `history_id`, which is
  exactly what lets volumes from the old history fail-stop (ADR 0016's
  cross-check) instead of merging into the new one. Daemons resuming a
  manifest, and peers contacting each other, compare stamped history
  ids. (Earlier ADRs and the storage manifest call this value the
  *cluster UUID*; this ADR renames the design surface deliberately —
  `cluster_id` next to `cluster_uuid` was a confusion waiting to
  happen, and the new name says what the value distinguishes.)

Any answer reporting `initialized` with a matching `cluster_id` means the
cluster exists: enroll if needed (§5) and self-join (§6). A stamped
`history_id` disagreeing with the contacted cluster's is the existing
ADR 0016 cross-cluster refusal. No answers, or only uninitialized
answers, means park (§1). Unreachable candidates are simply skipped —
probing is a search for the cluster, not a census, so stale discovery
entries can slow the search but never wedge it.

**First-ever formation is explicit, and its authority is local.** Cluster
formation is not a network RPC. Every coordinator daemon serves a small
admin surface on a **Unix domain socket** in its runtime directory,
accessible only to root and the daemon's user — local socket access is the
authority, which is the honest one: whoever holds root on a coordinator
host already holds everything the host will ever store. The formation
verb is driven by the operator (or the provisioning automation, via SSH,
SSM, cloud-init, or a test harness — all of which execute local commands
without exposing formation to the network) exactly once per cluster
lifetime, on any one parked daemon:

```
coppice coordinator init [--policy policy.toml] [--operator-csr csr.pem]
```

This is the same `cluster init` verb ADR 0020 already reserved for
bootstrap policy: formation and policy seeding are one act. On a genuinely
parked daemon, it:

1. runs one round of discovery+probe and refuses if any candidate already
   reports an initialized cluster with this `cluster_id` — a guard against
   accidental double-init, not a safety proof;
2. mints and stamps the node identity and the history id (via the
   existing `storage::init`);
3. mints the **cluster root CA** (§4) and issues its own coordinator leaf;
4. calls `raft.initialize` with itself as the single voter;
5. signs the supplied operator CSR into the first operator-profile
   certificate (ADR 0022's break-glass credential; day-0 admin authority)
   and prints it with the CA bundle and the CA fingerprint — with no CSR
   supplied it mints the keypair locally and prints both, for the
   operator's SSH session to collect;
6. applies the supplied policy (idempotent puts);
7. stamps a local **`formation_complete` marker** in the manifest.
   Formation has happened only when the marker exists: it is the
   boundary of the fail-stop below, deliberately placed *after* the
   operator certificate and the bootstrap policy, so that every partial
   formation state — not just one that died before `raft.initialize` —
   is identified as failed rather than mistaken for a healthy cluster.

   **Until the marker exists, the forming daemon's external surface
   stays closed**: `ProbeCluster` does not report `initialized`,
   enrollment and every membership verb are refused, and no client API
   is served. Parked peers can discover a forming node but cannot join
   it, so a partially formed cluster can never acquire a second member
   — the marker's guarantee is cluster-wide precisely because there is
   never a cluster to protect until it is total.

Re-running `init` returns the distinct `AlreadyInitialized` outcome,
carrying the cluster status, **only when the `formation_complete` marker
exists**; automation treats it as success. Against a daemon whose
manifest records formation intent without the marker, `init` reports the
`formation-failed` state instead — there is no path on which a partial
formation is reported as a completed one, and no path that resumes it.
The day-0 recovery verb lives on the same socket:
`coppice coordinator admin issue-operator-cert` signs a new operator CSR
at any point in the cluster's life — the documented recovery for "the
init output was lost" and for "all operator certificates lost", granting
nothing that local disk access did not already confer. Routine operator
cert issuance beyond day 0 remains ADR 0022's network path, authorized by
an existing operator certificate.

**Failure handling is identification, then restart-from-scratch.** A crash
at *any* point during formation — before or after `raft.initialize`, the
marker draws the line — fail-stops the daemon in phase `formation-failed`
(§1). The data directory is minutes old and empty of value; the operator
wipes it, restarts the daemon (which parks), and re-runs `init`. Because
the external surface is closed until the marker exists (step 7), no
other replica can have enrolled or joined: a failed formation is
confined to the forming node by construction, the wipe touches exactly
one data directory, and the re-init's probe guard finds no surviving
partial cluster to refuse against. A durable formation token with
a resumable formation state machine — the daemon completing formation
itself on restart from a recorded intent — was considered and rejected in
review: it existed solely to auto-heal this window, and the window is
rare, occurs during a restartable one-time operation, and is loudly
identified instead.

Running `init` concurrently on two disjoint hosts is the same class of
operator error as running `--bootstrap` twice today; the probe guard
narrows the window and the status surface (§9) makes the result
immediately visible. Daemons never initialize themselves, so discovery
churn, replacement fleets, and partitions can never re-trigger formation.
If a fleet loses all its volumes, the replacements park indefinitely and
`/readyz` says so; recovery is a deliberate restore-from-snapshot or a
deliberate re-init, never automatic amnesia. Two network-facing formation
variants are deferred, not decided: a request signed by a bootstrap public
key pinned in config, and platform-native one-shot CAS objects (an S3
`If-None-Match` marker, a DynamoDB conditional write) as an opt-in
per-platform `BootstrapAuthority`.

### 4. Cluster-owned PKI

ADR 0022 fixed certificate *semantics* and left the trust root's
provenance open. It is now decided: **the cluster owns its CA by default.**
A minimal deployment provisions no certificates at all.

- **Root.** Minted at formation (§3), long-lived. The CA *certificate* is
  replicated state — it is public material every node needs. The CA
  *private key* never enters replicated state: it is created in the
  forming voter's data directory (owner-only permissions), normally
  resides on voter disks, and may also reside on a promotion candidate
  that has passed the key-transfer gate below — no other disk ever
  holds it. Signing verbs execute on the leader, which is always a
  voter and therefore always holds the key. Keeping the key out of
  replicated state is what makes the blast-radius claims of §7 true:
  learners receive snapshots and log replay, so a key in replicated
  state would let anyone who reaches *any* learner seat mint arbitrary
  certificates — escalating "one stolen machine cert = one seat's read
  access" to total cluster authority. Instead the key reaches a disk
  only through formation or the deliberate, gated transfer below, and
  every disk it has ever reached is accounted root-equivalent.
- **Key custody is an invariant, ordered before membership.** Every
  voter holds the key, and the ordering that maintains this is explicit:
  a learner about to become a voter first receives the key over the
  mutually-authenticated admin channel, persists it durably, and
  acknowledges receipt; the leader records that confirmation as a
  replicated fact (the *fact of possession* is replicated — the key
  never is), and **confirmed durable key receipt is a precondition of
  the joint change** in both `PromoteVoter` and `ReplaceVoter` (§7).
  Key transfer as a side effect of an already-committed promotion was
  considered and rejected: if membership committed first and the
  remaining key holder then vanished, a replacement could end as sole
  voter without the signing key — clearest in single-voter replacement,
  possible in correlated failures. With key-before-membership ordering
  the joint change stays atomic and every membership change trivially
  satisfies the postcondition, which is nonetheless enforced explicitly:
  **no change may leave the continuing voter set without a confirmed
  key holder**, and any verb that would (a lost confirmation, a
  detected corrupt key file) is refused for operator repair. A crash
  between key receipt and the joint change leaves a caught-up learner
  holding the key for the promotion it was already gated into —
  accepted, and covered by the custody statement below.
- **What key custody honestly means.** Removal cannot make a disk
  forget: a disk, a snapshot of one, or a compromised instance retains
  signing authority indefinitely, and identity revocation plus seat
  removal does not recall it. The threat model therefore states
  plainly: **every disk that has ever received the key is
  root-equivalent** — every current and former voter, and every
  promotion candidate keyed ahead of a joint change, *including one
  whose promotion never committed* (a leader crash in that window
  abandons the candidate as a learner, but abandons it holding the key,
  exactly as a removed voter does). Equally root-equivalent is any
  credential path that can reach the key transfer — in particular a
  coordinator enrollment token (§5), which through a legitimate vacancy
  or an evidence-gated removal window can place a learner that is
  subsequently staged for promotion and keyed. Suspected
  compromise of any of these is answered by **re-rooting**, and the
  re-root/root-rotation runbook is accordingly a *required* deliverable
  before the first production deployment, not a deferred nicety —
  losing every voter disk (learner disks and backups notwithstanding)
  lands in the same runbook. If operational experience shows re-rooting
  too costly to be the compromise response, the designed upgrade path
  is a cluster-held rotating *intermediate* under a longer-lived root —
  bounding the authority a voter disk retains to the intermediate's
  lifetime — chosen over threshold signing, which is out of proportion
  to this system's needs.
- **Leaf profiles.** Three, per ADR 0022's framework: *coordinator*
  (subject = a cluster-minted machine identity, §7), *agent*
  (CN = node id, per ADR 0011), *operator* (`OU=coppice-operators`,
  break-glass and day-0). Because the cluster mints every subject, the
  discipline external issuance would have to be trusted for — one
  installation, one subject, unique across the fleet, stable while the
  installation's state persists — holds by construction instead of by
  lint.
- **Issuance.** One **enrollment endpoint** on the existing cluster —
  with `ProbeCluster`, the entire network surface a certless client may
  call; every other network verb requires mTLS, and formation is not a
  network verb at all (§3). A caller presents a role-scoped enrollment
  token (§5) plus a CSR and receives a leaf and the CA bundle. This is
  exactly ADR 0011's flow, now with a decided signer, and it applies to
  **coordinators and agents alike**: at cluster birth, coordinators 2..N
  enroll against the freshly formed cluster before joining — the same two
  steps (get a leaf, run the convergence loop) as a routine instance
  refresh years later. There is exactly one privileged ceremony in the
  system's lifetime, and it is §3.
- **Enrollment transport is explicitly secure or explicitly insecure —
  never implicit.** An enrollment client is about to transmit a bearer
  token, so it must know whom it is talking to. The enrollment endpoint
  is served beside the machine-plane listeners (never on the client
  HTTP listener), and client config (the same `[enrollment]` table that
  names the token path) must name exactly one server trust anchor:
  - `trust = "system"` — an externally-signed certificate on the
    listener that actually serves enrollment, verified against system
    roots. For deployments that already front the fleet with a public
    or corporate cert; a certificate on some other listener
    authenticates nothing here.
  - `trust = { ca_bundle = <path> }` or `trust = { fingerprint_path =
    <path> }` — the cluster CA, pinned. The bundle or fingerprint is
    emitted by `init` (§3) and travels to instances by the same channel
    as the enrollment token itself (launch template, user-data, config
    management) — anyone who can tamper with that channel already holds
    the token, so the pin adds no new trust assumption and requires no
    external PKI.

    **This option imposes a two-phase bringup, and that is accepted
    explicitly** rather than worked around: the pin cannot exist before
    formation, so cluster birth is *form first, publish second* — run
    `init` on one node, then place the emitted fingerprint (with the
    tokens) into the fleet's provisioning channel, then launch or
    refresh the rest. Because both anchors are read from paths, a fleet
    launched all at once is also fine: daemons park (§1) and re-attempt
    enrollment as they poll, so trust material that arrives on disk
    after boot — dropped by config management or fetched by a user-data
    loop once published — is picked up without a relaunch, and
    `/readyz` shows the parked daemons waiting on it. Steady-state
    autoscaling is unaffected: the template carries the pin from then
    on. This is the honest price of never sending a bearer token to an
    unverified endpoint, paid once per cluster lifetime.
  - `insecure = true` — no server verification (and, for `coppice dev`
    and single-host integration tests, optionally no TLS at all).
    Conspicuous opt-in, mutually exclusive with `trust`, documented as
    suitable only for development and isolated test environments that
    accept exposing enrollment tokens on the wire.

  A daemon or agent whose config names an enrollment token but neither a
  trust anchor nor the insecure flag **fails at startup** with a config
  error; there is no fallback. Trust-on-first-use as a default was
  considered and rejected in review: it is a silent middle posture that
  verifies nothing while looking as if it does, and both honest postures
  are cheap to configure.
- **Renewal, and revocation's honest shape.** Leaves are re-issued over
  the same endpoint, authenticated by the current leaf, preserving the
  subject — short-lived leaves are therefore free, and renewal is a
  *policy decision*: the leader refuses renewal for identities an
  operator has marked revoked. With short leaf lifetimes this is v1's
  certificate revocation mechanism — there is no CRL or OCSP
  distribution, and an already-issued leaf remains valid until it
  expires. The `[tls]` paths remain hot-reloaded (mtime watch, plus
  SIGHUP to force), served via a connection-time certificate resolver on
  all listeners and picked up by outbound channels on reconnect;
  in-flight connections finish on the old leaf.
- **External PKI stays a substitution, not a requirement.** Issuance was
  always behind file paths plus reload; a deployment that supplies its own
  leaves bypasses enrollment entirely and takes on the subject
  requirements above itself (worth a startup-time lint where detectable).
  Vault and friends are an option for organizations that already run
  them, never a prerequisite.
- **Client (HTTP API) listener: externally-signed TLS or explicit
  insecure — the cluster CA never serves here.** The user-facing
  listener has its **own certificate configuration** (`[client_tls]`,
  the same path-plus-hot-reload convention as `[tls]` but a separate
  table — the machine-plane leaf that enrollment writes into `[tls]`
  must never be conflated with this listener's serving cert): either a
  public/corporate certificate (or a TLS-terminating load balancer in
  front), or plain HTTP under the same conspicuous `insecure = true`
  opt-in as above. A cluster-issued leaf as a client-listener default
  was considered and rejected: browsers will never trust a private
  root, so it would create a third, half-verified posture for exactly
  the audience least able to pin — the two honest modes suffice, and
  machine-plane traffic (raft, agent gateway, enrollment) is where the
  cluster CA belongs. One role the cluster CA does keep on this
  listener, independent of the serving certificate: it remains the
  *client-certificate* trust anchor against which ADR 0022's
  operator-profile certificates are verified. User authentication
  otherwise remains OIDC bearer (ADR 0022); the listener never
  *requires* client certificates.

### 5. Enrollment tokens

Enrollment is authorized by bearer tokens with deliberately simple
semantics:

- A token is minted *for a role* — `coordinator` or `agent`, never both —
  with an optional TTL and a label. Tokens are stored in replicated
  policy as salted hashes: listable and revocable with a normal policy
  write (`enroll-token list` / `revoke`).
- **The supported default is a long-lived, reusable token baked into the
  launch template** (user-data, AMI, or config management). This is the
  entire hands-off autoscaling story: no secrets manager, no minting
  service, no lifecycle-hook glue is required.
- **The limitations of that default are part of the decision, stated
  precisely.** A reusable bearer token authorizes unlimited enrollments
  until revoked or expired; v1 provides no per-instance, single-use
  enrollment credential. Token revocation and credential revocation are
  distinct: revoking a token stops *future* enrollments, but does not
  invalidate leaves already issued from it, and those leaves continue to
  renew (§4) unless the operator also marks the issued identities
  revoked. If an attacker redeemed a token before it was revoked, token
  revocation alone does not evict them — the response is token
  revocation *plus* identity revocation of the illegitimately enrolled
  subjects (visible in `admin status`'s binding list and the audit log),
  after which their short-lived leaves age out. The trust weights differ
  by role and the documentation says so plainly. An agent token admits
  workload hosts. A coordinator token admits learners — immediately a
  read-everything credential, since learners replicate all cluster
  state — and its ceiling is higher than that: whenever a voter seat is
  legitimately open (a vacancy, or a voter past the evidence-gated
  removal grace), a learner it placed can be staged for promotion and
  thereby receive the CA key (§4), making the **coordinator token
  root-equivalent** in the threat model, and identity revocation plus
  seat removal insufficient once that has happened (§4's custody
  statement: the answer is re-rooting). The operational position is
  unambiguous:
  - the **long-lived coordinator launch-template token is the supported
    v1 default**, consistent with the no-minting-service story above,
    and it is root-equivalent — an *explicitly accepted risk*, on par
    with the provisioning channel that delivers it, which can already
    reach coordinator hosts;
  - **short-lived, per-refresh-cycle coordinator tokens are the
    recommended stronger posture** wherever the deployment has the
    delivery automation to mint them (the verb below); they are not
    required, and choosing them is what requires minting glue — the
    default requires none;
  - long-lived *agent* tokens are the reasonable steady state with no
    such caveat;
  - per-instance single-use tokens and richer issued-identity
    revocation are named future improvements, not implied features.
- **On-demand minting is a verb, not a service.**
  `coppice node enroll-token --role agent --ttl 15m` is an ordinary
  admin-plane command, callable with an operator certificate or by an
  OIDC principal whose role binding includes the new, narrow
  `mint-enroll-token` verb (one row in ADR 0023's table; grantable to a
  CI or lifecycle-hook principal without any other authority).
  Deployments that want short-lived per-instance tokens build the
  delivery glue (SSM, lifecycle hooks) out of this verb on their own
  platform; Coppice's contract ends at "the token arrives at the path
  named in the config".
- **Platform attestation is the named future seam.** Accepting a
  platform-signed instance-identity document in place of a token (the
  Vault `aws-auth` pattern) would eliminate secret distribution entirely;
  it is per-cloud verification code, shaped like a second enrollment
  authenticator next to `token`, and is deferred exactly as Consul
  discovery is.

### 6. Self-join: the convergence loop

Joining stops being an operator dance and becomes a loop the new replica
runs against the cluster itself, using the existing admin RPCs as a
client:

1. **Enroll** if no usable leaf is on disk (§5): token + CSR → coordinator
   leaf bearing this installation's machine identity (§7).
2. **Commit to an identity**: mint and stamp the node id and instance UUID
   via the existing `storage::init`. A crash after stamping resumes with
   the same identity: restart re-enters this loop, not a fresh mint.
3. `AddLearner(self_id, self_advertised_addr)` against the leader (probe
   answers carry a leader hint; a follower's refusal names the leader, as
   today). The caller's machine identity — the CA-attested subject of the
   mTLS certificate the request arrives under — is bound to `self_id` in
   the membership record at admission (§7). Before admitting, the leader
   verifies the claimed endpoint: it dials the advertised address over
   mTLS and requires both that the serving certificate presents the
   requester's machine identity and that `ProbeCluster` there reports the
   claimed stamped node id.
4. Wait for catch-up, polling `ClusterStatus` until replication lag is
   inside the promotion threshold.
5. `PromoteVoter(self_id)` — authorized only from the machine identity
   bound at step 3 (§7); the leader applies its existing lag gate, the
   confirmed-key-receipt precondition (§4: the key is transferred and
   durably acknowledged *before* the joint change), and the voter-count
   rules of §7. Where the promotion cannot proceed because
   the voter set is full, the node simply remains a caught-up learner,
   polling — it is then either the `new_node_id` of a pending
   `ReplaceVoter` (§7) or waiting on the evidence-gated removal of a dead
   predecessor (§7).

The loop is re-entered from the top on any retryable failure and on every
restart, which requires the membership verbs to be **idempotent by
contract**, not by accident. Each verb short-circuits on current membership
state *before* any other gate:

- `AddLearner(id, addr)`: id already a learner or voter at `addr` →
  success, no-op. Same id at a *different* address → refused (there is no
  silent repointing; see below). Otherwise → admit.
- `PromoteVoter(id)`: id already a voter → success, no-op, checked before
  the replication-lag gate (a voter has no learner replication entry to
  measure, and must not be bounced with `LearnerNotCaughtUp`). Unknown
  id → refused.
- `RemoveNode(id)`: id absent from membership → success, no-op.
- `ReplaceVoter(old, new)` (§7): `new` already a voter and `old` absent →
  success, no-op. Either precondition unmet in any other way → refused
  with the specific reason.

This tightens, and is a deliberate amendment to, the current verb
semantics; the multi-node integration test asserts each no-op case
explicitly. With that contract, a process killed at any step converges
after respawn with no cleanup, and the systemd unit's `Restart=always` is
the entire recovery story.

**There is no self-service address repair.** An earlier draft let a
resumed instance rewrite its own membership address via
`ChangeMembers::SetNodes`; review rejected it — openraft warns that a
wrong `SetNodes` address can split-brain, and no machine credential should
be able to repoint a voter (§7). An instance whose address changed is,
under the immutable model, simply a new instance (EC2 private addresses
are stable for an instance's lifetime, so in-place restarts keep theirs).
For the rare pet deployment, `admin set-address` exists as an
operator-credential break-glass verb, and even then the leader commits it
only after dialing the *new* address and verifying by probe that the
endpoint's TLS certificate subject matches the machine-identity binding
already stored for the target and `ProbeCluster` reports the target's
stamped node id. A claimed node id without the matching CA-attested subject
is not sufficient proof of endpoint ownership.

Progress and terminal states are reported through the status surface (§9),
not log prose; nothing in the join path requires reading logs, and the
minted node id never needs human handling.

### 7. Membership authority: machine identity, one seat, explicit replacement

Self-join makes a coordinator's *machine* credential a routine caller of
membership verbs, which ADR 0023 classifies as unscoped-admin cluster
verbs. We amend ADRs 0022/0023 with a deliberately narrow **machine
self-service grant** built on the certificate's CA-attested identity.

**Machine identity is a cluster-minted opaque identifier**, issued at
enrollment (§4), carried in the certificate subject, and persisted in the
installation's data directory alongside the manifest. Its required
properties are exactly these:

- unique among coordinator installations in the cluster — guaranteed
  because the cluster mints it;
- stable across restarts that retain the installation's persistent state
  (the leaf and key are disposable and renew freely; the subject rides
  along);
- distinct for multiple coordinator processes on one physical host (an
  N-process development cluster is N installations: N data directories,
  N identities);
- **newly minted when an installation starts with fresh persistent
  state** — a replacement instance is a new installation with a new
  identity, full stop.

Hostname, IP address, cloud instance id, or data-directory path may
appear as issuance context or SANs, but none of them *is* the identity:
production fleets share `/var/lib/coppice` as a path, test processes
share a hostname and IP, and DHCP reassigns addresses. The opaque
identifier in the data directory is the identity; everything else is
metadata.

**The grant: one identity, one seat, self-scope only.**

- Admission creates a **replicated binding**: the membership record stores
  the machine identity taken from the mTLS session `AddLearner` arrived
  under (verified by the TLS layer, never claimed in the request body).
  Formation (§3) creates the same binding for the initial voter, so no
  seat, including the first, is ever unbound.
- A machine identity binds to at most one node id, ever — identity and
  raft node id are minted by and stamped into the same installation, so
  the pairing is one-to-one by construction. An `AddLearner` presenting a
  machine identity already bound to a *different* node id, or to the same
  node id at a different address, is refused and surfaced (§9): under
  this identity model that request is a duplicated or stolen credential,
  or a misissuance — an operator problem to see, not a race to arbitrate.
- **Self-scope is the whole machine grant.** A coordinator machine
  certificate may call `ProbeCluster`, `ClusterStatus`, `AddLearner` for
  its own machine identity, and `PromoteVoter` for the one node id its
  machine identity is bound to. Everything else — `RemoveNode`,
  `ReplaceVoter`, `set-address`, formation, admission or promotion of an
  id bound to another machine — is refused for machine certs. Agent
  certificates can call none of the membership surface. The integration
  tests assert the full refusal matrix.
- Operator-authenticated verbs use the operator-profile certificate
  (ADR 0022), presented over the same mTLS admin listener. OIDC-driven
  membership administration via the client API may layer on later.

**Replacement is an explicit operation, not an inference.** Because a
replacement installation carries a *new* identity, the cluster cannot —
and deliberately does not — guess which old voter a new learner
supersedes. Inferring replacement from a shared machine identity was
considered and rejected in review: it contradicts fresh-state-fresh-
identity, and in a launch-before-terminate rolling replacement it
deadlocks — the new node cannot take the old node's seat while the old
node is alive and holding it, and the platform will not terminate the old
node until the new one is ready. Instead:

```
ReplaceVoter { old_node_id, new_node_id }
```

is an **operator-authenticated** verb. The new installation enrolls,
joins, and catches up as a learner through its own convergence loop (§6);
`ReplaceVoter` then commits a single joint-consensus change that promotes
`new_node_id` (subject to the same replication-lag and confirmed-key-
receipt gates as promotion, §4) and removes `old_node_id` atomically —
the voter count never overshoots, quorum among survivors holds
throughout, and because the incoming voter confirmed durable key
possession before the change, the continuing voter set is never left
without the signing key even if the departing voter was the last other
holder (the single-voter replacement case). Every removal path —
`ReplaceVoter`, evidence-gated removal, `admin remove` — additionally
refuses any change whose continuing voters would include no confirmed
key holder (§4). Identifying the pair is the
caller's job: a human for pet deployments, the refresh automation (via
the `mint-enroll-token`-style narrow grant pattern, one more row scoped
to `replace-voter`) for orchestrated launch-before-terminate rollouts.
Idempotency per §6 makes retries safe.

**The hands-off path needs no caller at all.** A terminate-before-launch
replacement — an instance dies or is refreshed with capacity headroom —
requires nobody to name the pair: the replacement joins and calls plain
`PromoteVoter`, and if the candidate voter set exceeds `cluster_size`,
the leader folds into the promotion's joint change the removal of at most
one voter whose replication has been failing for longer than
`removal_grace` (default 120s). The evidence is the leader's own
replication observation, full stop. Consulting discovery-backend liveness
as a second evidence source was considered and rejected: only one backend
(`ec2-asg`) could ever supply it, the design must tolerate its absence
anyway, and a generous grace period buys the same confidence without a
second mechanism. The postconditions are explicit: the joint change must
leave at most `cluster_size` voters and a live majority from the leader's
vantage. If no candidate qualifies, or one removal is not enough,
promotion is refused with a machine-readable reason — the learner keeps
polling and the situation is visible in status output. A *live*
predecessor never qualifies as evidence-dead, which is precisely why
launch-before-terminate rollouts use `ReplaceVoter`; the deployment
documentation states the pairing plainly (ASG instance refresh with
capacity headroom → set the refresh to terminate first, or drive
`ReplaceVoter` from the rollout automation).

There is no background *voter* reaper: voter membership only shrinks
inside `ReplaceVoter`, inside an evidence-gated promotion, or by explicit
`admin remove`. Stale *learners* — installations that died permanently
before promotion; their successors carry new identities — never affect
quorum and are garbage-collected by the leader after an unambiguous
period **without successful replication contact** (`learner_expiry`,
default 1h): the criterion is failed heartbeat/append acknowledgement,
never lack of log advancement, because an idle cluster's fully caught-up
learner — say one waiting as the `new_node_id` of a pending
`ReplaceVoter` — may legitimately see no new entries for hours and must
not expire. This keeps membership records from accumulating under
instance churn.

The net effect: possession of one coordinator machine credential admits
exactly one learner — the single seat its cluster-minted identity names,
at an endpoint verified to hold both that identity and the claimed raft
identity — and promotes exactly that learner, only when the voter-count
rules allow it. It can never remove, replace, repoint, or initialize.
The blast-radius statement is two-tiered, honestly: a stolen machine
*certificate* is bounded to its one seat (and cannot even occupy it
without the matching stamped data directory, per the endpoint
verification above) — learner-grade read of all replicated state is its
worst case, and eviction is identity revocation (§5) plus seat removal.
But any credential whose seat reaches promotion's *key-transfer stage*
— whether or not the joint change ultimately commits — receives the CA
key and crosses into §4's root-equivalence, from which the only
eviction is re-rooting; which is why the coordinator enrollment token,
the credential that mints fresh promotable identities, carries the
root-equivalent classification in §5 rather than this bounded one.

### 8. What agents inherit

The agent half of the enrollment design (OD-15a) is resolved by §§4–5 with
no agent-specific machinery: an agent boots with a role-scoped token,
enrolls for an agent leaf (CN = node id, ADR 0011) under the same
explicit transport-trust rules, and renews over the same endpoint —
short-lived agent certificates, and renewal-refusal as their revocation
lever, for free. Registration remains what it is today: the coordinator
accepts any register whose CA-signed certificate CN matches the claimed
node id; the trust anchor is solely issuance, and there is no allowlist
to maintain. Drain, scale-in, and node-record GC remain OD-15(b),
untouched here.

### 9. Machine-readable status and readiness

Log scraping is removed from every workflow:

- **`GET /readyz`** on the client listener (beside the existing
  `/metrics`): returns the convergence state as JSON — `cluster_id`,
  `history_id`,
  `node_id`, `instance_uuid`, phase (`waiting` | `formation-failed` |
  `joining` | `learner` | `voter`), leadership, applied index,
  replication lag, plus `voters`, `cluster_size`, `formed` (membership
  cardinality reached — desired shape, saying nothing about health), the
  cluster CA fingerprint once formed, and any admission refusal the
  daemon last received (a duplicated-identity refusal per §7 surfaces
  here). Two gates, two questions:
  - `GET /readyz` → 200 **iff** this replica is an initialized voter whose
    applied index is within the promotion threshold of the leader. This is
    *node* readiness: the ASG lifecycle-hook and load-balancer gate, and
    "wait for the new node to finish synchronizing before replacing the
    next" is exactly "wait for 200".
  - `GET /readyz?require=healthy` → 200 additionally requires the leader
    to observe at least `cluster_size` voters within the promotion-lag
    threshold, sustained for a stability interval (default 10s). This is
    the *cluster-redundancy* gate for bringup automation and anything
    that assumes the cluster can lose a node. It is answered
    authoritatively **only by the leader** (openraft replication metrics
    exist only there); a non-leader returns 503 with a machine-readable
    `health_unknown` reason and a leader hint. Automation either targets
    the leader or uses `admin status --json`. Having followers answer
    from a freshness-bounded health snapshot fetched from the leader was
    considered and rejected: it buys load-balancer convenience at the
    cost of a cache, a staleness bound, and a new failure mode — unknown
    health is not health, and saying so plainly is cheaper.
- **`admin status --json`** emits the cluster-wide view (membership,
  per-follower lag, machine-identity bindings, health) in stable JSON for
  scripting; the human table remains the default.
- The systemd unit runs `Type=notify`: the daemon signals `READY=1` when
  listeners are serving, so unit ordering works, while node and cluster
  readiness remain `/readyz`'s job. A parked daemon is `READY=1`, phase
  `waiting`, HTTP 503 — visible, alive, and deliberately not "ready"; the
  same holds for `formation-failed`.

## Consequences

- **A minimal production deployment needs a DNS name and an OIDC issuer.**
  No external PKI, no Vault, no secrets manager, no per-replica
  certificate provisioning: one artifact, one byte-identical config file,
  one command line for every daemon, two enrollment tokens plus the
  cluster-CA pin in launch templates, and one local `cluster init` at
  cluster birth. The pin's two-phase bringup (form first, publish the
  fingerprint and tokens second) is accepted explicitly as the price of
  never sending a bearer token to an unverified endpoint; deployments
  that prefer single-phase bringup use an externally-signed certificate
  on the enrollment listener instead. The user-facing HTTP listener is
  externally-signed TLS or explicit insecure — the cluster CA serves
  only the machine planes. Integration tests get N-process local
  clusters from the `file` backend plus one harness-driven local init
  call. External PKI and per-instance short-lived-token delivery remain
  documented substitutions for organizations that want them.
- **Formation cannot be forked or repeated by infrastructure behavior**:
  daemons never initialize on their own, and formation is not reachable
  from the network at all — its authority is local root on a coordinator
  host, which subsumes nothing beyond what local root already holds.
  Divergent discovery views, partitions, and volume-less replacement
  fleets all converge to "parked and alarming", never to a second
  history. The cost is one deliberate human/automation act per cluster
  lifetime, and that a wiped fleet stays down until someone decides
  between restore and re-init — the correct posture for a data-loss
  event.
- **Rare bootstrap failures are identified, not auto-recovered.** The
  `formation_complete` marker makes the identification total: a crash
  anywhere in formation — before `raft.initialize` or after it but
  before the operator certificate and bootstrap policy landed — restarts
  into `formation-failed`, never into a plausible-looking cluster
  missing its day-0 state; and because the forming daemon serves no
  enrollment, membership, or client traffic until the marker exists, a
  failed formation is confined to the forming node — no peer can have
  joined a partial cluster. Recovery is wipe and re-run of exactly one
  data directory in every case.
  The price of rejecting the formation-token state machine is that this
  window takes a manual wipe-and-rerun, and a lost init output takes a
  local `issue-operator-cert`, instead of resuming automatically — an
  accepted trade, because that machinery would have defended a
  restartable one-time operation.
- **The cluster is its own CA; the key never enters replicated state,
  under an explicit custody invariant and an honest threat model.** The
  CA certificate is replicated; the signing key normally resides on
  voter disks — plus any promotion candidate past the key-transfer gate
  — and reaches a disk only through formation or that gated transfer.
  Transfer to an incoming voter happens *before* its joint change
  commits (confirmed durable receipt is a promotion precondition), and
  every membership change must leave a confirmed key holder among the
  continuing voters — so a replacement can never end as a keyless sole
  voter, and no credential short of a staged promotion can mint
  certificates.
  The custody corollary is stated, not hidden: removal cannot make a
  disk forget, so every disk that has ever received the key — current
  and former voters, and promotion candidates whose joint change never
  committed — and the coordinator enrollment token are root-equivalent,
  with re-rooting as the compromise response. The re-root/root-rotation runbook is
  therefore a required pre-production deliverable, and a cluster-held
  rotating intermediate is the designed upgrade if re-rooting proves
  too costly in practice. Subjects are cluster-minted opaque
  identities, so one-seat-per-credential no longer depends on external
  PKI discipline; coordinators and agents share one enrollment flow,
  one token model, one renewal path; ADR 0022's operator certificate is
  minted at formation instead of provisioned out of band.
- **Replacement is explicit where the predecessor is alive, automatic
  where it is dead.** `ReplaceVoter` gives launch-before-terminate
  rollouts an atomic, operator-authorized promote-and-remove;
  evidence-gated removal inside plain promotion keeps
  terminate-before-launch and crash replacement fully hands-off. Neither
  path lets a machine credential remove anyone, and voter membership
  never shrinks outside a joint change the leader commits.
- **Token limitations are stated, not discovered.** The default
  enrollment token is reusable until revoked; token revocation does not
  recall issued leaves; eviction of an attacker who redeemed a token is
  token revocation plus identity revocation, with short-lived leaves
  aging out the rest — except past the key transfer, where the answer
  is re-rooting. The agent token's long-lived launch-template posture
  is the reasonable steady state; the long-lived coordinator
  launch-template token is the supported v1 default and an explicitly
  accepted root-equivalent risk, with short-lived per-refresh minting
  the recommended stronger posture where automation exists. Single-use
  per-instance tokens and richer identity revocation are named future
  work.
- The empty-directory fail-stop from ADR 0016 is traded away: an empty
  data dir now means "new instance" rather than "operator error until
  proven otherwise". The failed-mount guard moves to unit/mount ordering,
  and the residual risk is bounded to a spurious learner join plus later
  expiry of the orphaned identity — the amnesiac-voter defense, the
  identity stamp, and the cross-cluster refusal all survive intact.
- Discovery remains strictly advisory and non-blocking: it can delay a
  join but can neither wedge convergence, nor block or cause a removal
  (removal evidence is exclusively the leader's replication
  observation), nor change membership.
- The membership verbs acquire an explicit server-side idempotency
  contract (state short-circuit before gates) — notably `PromoteVoter` on
  an existing voter becomes a no-op success instead of a spurious lag
  error, and `ReplaceVoter` re-runs are no-ops once applied. The
  convergence loop's restart-safety rests on this contract, so it is
  tested directly.
- New failure modes to test explicitly: interrupted join at every step;
  formation crashed on each side of `raft.initialize` (both restart into
  `formation-failed`, never a serving cluster; wipe + re-init of the one
  forming node recovers); a parked peer probing a forming daemon
  (`initialized` not reported, enrollment and membership refused, until
  the marker exists); `AlreadyInitialized` returned only with the
  `formation_complete` marker present, and `init` against a marker-less
  formation reporting `formation-failed`; a promotion candidate keyed
  and then abandoned by a leader crash (never a voter, flagged in the
  key-custody accounting as a holder); enrollment with a revoked,
  expired, or
  wrong-role token; enrollment client with no trust anchor and no
  insecure flag (startup failure); enrollment against a server whose
  certificate fails the pinned fingerprint (refusal, token not sent);
  trust material arriving on disk after boot (parked daemon proceeds
  without relaunch); `ReplaceVoter` with a lagging `new_node_id`
  (refused), with a live `old_node_id` (succeeds — that is its purpose),
  re-run after success (no-op), and raced against the predecessor's
  crash (evidence-gated path completes it); single-voter replacement
  where the old voter vanishes immediately after the joint change (new
  voter provably holds the key); crash between key receipt and the
  joint change (loop re-entry converges, no duplicate transfer);
  any removal that would leave no confirmed key holder among continuing
  voters (refused); evidence-gated removal with a live predecessor
  (never fires); promotion refused with the voter set full and no dead
  candidate; a duplicated machine identity presented from a second
  installation (refused, surfaced in status); stale learner expiry after
  `learner_expiry` on failed replication contact — and its converse, an
  idle fully-caught-up learner surviving indefinitely on heartbeats
  alone; concurrent joiners racing for one vacancy; leader change
  mid-join; a parked fleet resuming when its cluster reappears; `formed`
  true with unreachable voters (must fail `?require=healthy`);
  `?require=healthy` on a non-leader (503 `health_unknown`); CA-key
  absence from snapshots and learner state asserted directly; and cert
  renewal under load, including renewal refusal for a revoked identity.
  The multi-node integration test stops driving admin RPCs by hand and
  instead asserts convergence from N identical configs plus one init
  call.
- Deferred, unchanged in scope: signed-request and platform-CAS network
  formation behind the same seam; platform-attestation enrollment (per
  platform, opt-in) beside the token authenticator; the cluster-held
  rotating-intermediate signing design (the re-root runbook itself is
  *required*, not deferred); single-use per-instance enrollment tokens;
  Consul (or any registry) as another `Discovery` impl; agent drain and
  scale-in (OD-15b); rolling *upgrade automation* beyond the readiness
  gate stays operational tooling on top of these primitives, not
  coordinator code.
