# 37. Coordinator discovery and self-converging membership

- **Status:** Accepted
- **Date:** 2026-07-22
- **Resolves:** [OD-14](../roadmap/open-decisions.md#od-14-coordinator-discovery-and-control-plane-pki)
- **Amends:** the startup-intent halves of
  [ADR 0016](0016-coordinator-rebuild-learner-join.md) (`--bootstrap`/`--join`
  as operator-supplied flags),
  [ADR 0020](0020-node-config-vs-replicated-policy.md) (the CLI flag set and
  the static `peers` list), and
  [ADR 0022](0022-oidc-identity-and-authentication.md) /
  [ADR 0023](0023-scoped-role-bindings.md) (a new, narrowly-scoped machine
  self-service grant for coordinator certificates, §6).
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

Two invariants from earlier ADRs are load-bearing and must survive:

- **Raft membership is the sole authority on who is in the cluster and at
  what address** (ADR 0016, restated by OD-14). Any discovery mechanism may
  only ever answer "whom might I dial first?", never "who are the voters?".
- **An empty disk joining an existing cluster may only ever enter as a new
  learner** (ADR 0016's amnesiac-voter defense). First-ever formation is the
  sole exception: it requires the explicit, operator-authenticated,
  durably-recorded intent in §3. No automatic or discovery-driven path can
  seed an empty disk as a voter.

One approach was considered and rejected in review: making *first-ever
cluster formation* emergent from discovery (a deterministic leaderless
"bootstrap election" among uninitialized candidates). It is unsound for two
reasons. Discovery backends are explicitly allowed to be stale, partial,
or wrong — so two stable divergent discovery views can each satisfy any
local election condition and form two clusters. And no amount of probing
can distinguish first-ever formation from a previously formed cluster
whose members have all vanished from discovery — an empty replacement
fleet would silently re-create an empty history under the same
`cluster_id`. Formation therefore needs a durable, one-time authority
outside the discovery system. We make it an explicit operator act (§3);
the daemons themselves never form a cluster on their own initiative.

Self-join was gated on the authorization model (ADR 0022/0023): membership
RPCs are unscoped-admin cluster verbs. Automating them from the joining
machine itself means machine credentials can now reach membership, so the
grant must be scoped tightly enough that one compromised or misissued
coordinator certificate cannot rewrite arbitrary membership (§6).

## Decision

### 1. One command, derived intent

The coordinator daemon is started the same way in every situation:

```
coppice coordinator --config /etc/coppice/coordinator.toml
```

The `--bootstrap` and `--join` flags are removed from the daemon. Startup
intent is *derived*, not declared:

- **Manifest present** → resume the instance on this disk, exactly as
  today's flagless restart (identity read from the stamp, cluster-UUID
  cross-check unchanged). Then run the convergence loop (§4), which no-ops
  when this identity is already a caught-up voter.
- **Manifest absent** → this is a new instance. Run discovery (§2) and
  probe the candidates (§3):
  - an initialized cluster with a matching `cluster_id` is found →
    self-join (§4);
  - no initialized cluster is found → **park**: serve `ProbeCluster` and
    the admin listener, report the `waiting` phase through `/readyz` (§7),
    and keep re-running discovery and probing. A parked daemon leaves this
    state only when an initialized cluster appears in discovery (→ join)
    or an explicit `InitializeCluster` command arrives (§3). It never
    bootstraps on its own.

One systemd unit with one `ExecStart` line therefore covers scale-out
join, instance replacement, and plain restart; first-ever formation is the
one additional, deliberate act at cluster birth (§3) — matching the shape
"one bootstrap intent, N−1 join intents". The hidden
`coppice coordinator admin` verbs are retained as the manual surface, with
their semantics tightened for idempotency (§4); they stop being part of
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
process (to find someone to probe and a leader to join through) and by the
leader when reconciling membership (§5). It is never consulted on the raft
hot path and its output is never authoritative — candidates are addresses
to dial, and the probe protocol decides what they mean. Discovery being
stale, partial, or down can delay convergence; it can never change
membership by itself.

Config gains a `[discovery]` section; the top-level `peers` field is
subsumed by the `static` backend and removed. Backends at this stage:

```toml
[discovery]
backend = "dns"            # "static" | "dns" | "file" | "ec2-asg"
cluster_size = 3           # expected voter count; used by removal (§5)
                           # and the formation-complete signal (§7)

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
  gate their hooks on readiness (§7) would otherwise be invisible to each
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
`coppice dev` already does. It also issues each process a distinct
coordinator certificate subject (§6): the authorization identity names a
coordinator installation, not a physical host. The invariant this ADR
guarantees is that no config field encodes cluster *role* or *identity*.

### 3. The probe protocol and explicit formation

A new RPC joins the admin surface on the raft listener, callable the moment
the mTLS server is up — before raft initialization:

```
ProbeCluster() → { cluster_uuid, initialized, node_id?, leader_hint?,
                   voters: [{node_id, addr}]? }
```

A converging process probes discovered candidates. Any answer reporting
`initialized` with a matching `cluster_uuid` means the cluster exists:
proceed to self-join (§4). A mismatched `cluster_uuid` is the existing
ADR 0016 cross-cluster refusal. No answers, or only uninitialized answers,
means park (§1). Unreachable candidates are simply skipped — probing is a
search for the cluster, not a census, so stale discovery entries can slow
the search but never wedge it.

**First-ever formation is explicit.** A new admin RPC,
`InitializeCluster`, forms the cluster; it is driven by the operator (or
the provisioning automation) exactly once per cluster lifetime:

```
coppice-cli cluster init --target coord-1:7071 [--policy policy.toml]
```

This is the same `cluster init` verb ADR 0020 already reserved for
bootstrap policy: formation and policy seeding become one act, against any
one parked daemon.

The RPC carries a client-supplied **formation token**, and the token must
be durable *outside* the CLI process so that a rerun — a fresh
`coppice-cli` invocation, a retried cloud-init, a re-executed provisioning
step — presents the same token rather than manufacturing a conflict:

- `--formation-token <string>`: automation passes a value that is already
  stable in the provisioning system (a stack or deployment id, a value
  stored in SSM — any stable opaque string);
- `--formation-token-file <path>`: the CLI creates the file exclusively
  on first use and re-reads it on subsequent runs;
- neither given (interactive use): the CLI mints a token, prints it
  prominently, and the operator re-supplies it on retry — the
  conflicting-token refusal also names the recorded token, so recovery
  never requires forensics.

The receiving daemon treats formation as a durable, resumable state
machine rather than a one-shot call:

1. If already initialized: a request bearing the recorded formation token
   returns success with the completed result (and idempotently re-applies
   any supplied policy — policy seeding is expressed as idempotent puts);
   any other request is refused as "already initialized".
2. If the manifest exists with a recorded formation token but raft is not
   yet initialized (a crash between stamp and `raft.initialize`): a
   request with the same token resumes formation; a different token is
   refused, naming the recorded one.
3. Otherwise (genuinely parked): run one round of discovery+probe and
   refuse if any candidate already reports an initialized cluster with
   this `cluster_id` — a guard against accidental double-init, not a
   safety proof. Then durably record the formation token in the manifest
   stamp together with the freshly minted identity (§4), call
   `raft.initialize` with itself as the single voter, and apply the
   supplied policy.

Because the formation intent is durable before any irreversible step, a
daemon that crashes mid-formation **completes formation itself on
restart**: the recorded token is an authorized operator instruction that
survives the process. Interrupted `cluster init`, timeouts with ambiguous
outcomes, and partial policy application are all repaired by re-running
the same command with the same token; there is no state from which the
verb can neither proceed nor report completion.

Every other parked daemon observes `initialized` on a later probe and
joins. The durable one-time authority is thus the operator's action plus
the initialized data directories it creates: discovery churn, replacement
fleets, and partitions can never re-trigger formation, because no
automatic path calls `InitializeCluster`. Running it twice concurrently
against two disjoint targets is the same class of operator error as
running `--bootstrap` twice today; the probe guard narrows the window and
the status surface (§7) makes the result immediately visible, but the
verb is an operator credential's to misuse — machine certificates cannot
call it (§6).

If a fleet loses all its volumes, the replacements park indefinitely and
`/readyz` says so; recovery is a deliberate restore-from-snapshot or a
deliberate re-init, never an automatic amnesia. Platform-native one-shot
CAS objects (an S3 `If-None-Match` marker, a DynamoDB conditional write)
could later make formation automatic *per platform* as an opt-in
`BootstrapAuthority` behind the same RPC; that is deferred, not decided.

### 4. Self-join: the convergence loop

Joining stops being an operator dance and becomes a loop the new replica
runs against the cluster itself, using the existing admin RPCs as a
client:

1. **Commit to an identity**: on first entering the join path (or on
   `InitializeCluster`), mint and stamp the node id and instance UUID via
   the existing `storage::init`. A crash after stamping resumes with the
   same identity: restart re-enters this loop, not a fresh mint.
2. `AddLearner(self_id, self_advertised_addr)` against the leader (probe
   answers carry a leader hint; a follower's refusal names the leader, as
   today). The caller's machine identity — the CA-attested subject of the
   mTLS certificate the request arrives under — is bound to `self_id` in
   the membership record at admission (§6).
3. Wait for catch-up, polling `ClusterStatus` until replication lag is
   inside the promotion threshold.
4. `PromoteVoter(self_id)` — authorized only from the machine identity
   bound at step 2 (§6); the leader applies its existing lag gate and,
   when a replacement is in progress, folds the departed voter's removal
   into the same joint change (§5).

The loop is re-entered from the top on any *retryable* failure and on every
restart, which requires the membership verbs to be **idempotent by
contract**, not by accident. A terminal seat conflict follows the
watch-without-resubmitting behavior in §6 instead of blindly retrying. Each
verb short-circuits on current membership state *before* any other gate:

- `AddLearner(id, addr)`: id already a learner or voter at `addr` →
  success, no-op. Same id at a *different* address → refused (there is no
  silent repointing; see below). Otherwise → admit.
- `PromoteVoter(id)`: id already a voter → success, no-op, checked before
  the replication-lag gate (a voter has no learner replication entry to
  measure, and must not be bounced with `LearnerNotCaughtUp`). Unknown
  id → refused.
- `RemoveNode(id)`: id absent from membership → success, no-op.

This tightens, and is a deliberate amendment to, the current verb
semantics; the multi-node integration test asserts each no-op case
explicitly. With that contract, a process killed at any step converges
after respawn with no cleanup, and the systemd unit's `Restart=always` is
the entire recovery story.

**There is no self-service address repair.** An earlier draft let a
resumed instance rewrite its own membership address via
`ChangeMembers::SetNodes`; review rejected it — openraft warns that a
wrong `SetNodes` address can split-brain, and no machine credential should
be able to repoint a voter (§6). An instance whose address changed is,
under the immutable model, simply a new instance (EC2 private addresses
are stable for an instance's lifetime, so in-place restarts keep theirs).
For the rare pet deployment, `admin set-address` exists as an
operator-credential break-glass verb, and even then the leader commits it
only after dialing the *new* address and verifying by probe that the
endpoint's TLS certificate subject matches the machine-identity binding
already stored for the target and `ProbeCluster` reports the target's
stamped node id. A claimed node id without the matching CA-attested subject
is not sufficient proof of endpoint ownership.

Progress and terminal states are reported through the status surface (§7),
not log prose; nothing in the join path requires reading logs, and the
minted node id never needs human handling.

### 5. Replacement: promotion-coupled removal

Replacing a coordinator (instance refresh, dead volume) is: the platform
starts a fresh instance, which self-joins per §4. What is new is who
removes the departed identity. That authority is the **leader's**, and it
is exercised conservatively inside the promotion that grants the new vote:

On `PromoteVoter`, removal is folded into the promotion's joint-consensus
change (today's `promote --remove`, chosen automatically) under two
distinct rules:

**Replacement removal — unconditional.** If the promoting node's machine
identity has a superseded predecessor still in membership (§6), that
predecessor is removed in the same joint change that grants the new vote,
*regardless of the current voter count*. The one-seat-per-machine
invariant is enforced by the promotion itself, not by cardinality: an
underfilled cluster (say two voters of `cluster_size = 3`, one being
replaced) must not end the promotion with two voters bound to one
machine. "Superseded" is a marking, never a mechanism — only this joint
change actually retires the old vote.

**Overflow removal — evidence-gated and evaluated second.** The leader first
constructs the candidate voter set after applying the mandatory replacement
rule above: remove the superseded predecessor, when present, and add the
promoting node. Only if *that candidate set* still has
`voters > cluster_size` may it select at most one additional *dead* voter
for removal. A voter qualifies only if **all** hold:

- the leader's replication to it has been failing for longer than a grace
  period (`removal_grace`, default 60s) — this is the primary evidence,
  and it is the leader's own observation, not discovery's;
- where the discovery backend can attest instance liveness (`ec2-asg`:
  the instance is genuinely gone from the group), the departed voter is
  additionally absent from the leader's discovery view. Backends with no
  liveness semantics (`static`, `dns`, `file`) contribute nothing here — a
  stale registration file or an unedited static list must be unable to
  block a legitimate removal, so for those backends replication evidence
  plus the grace period stands alone.

The final-set postconditions are explicit: the joint change must leave at
most `cluster_size` voters and a live majority of the resulting voters from
the leader's replication vantage. If no overflow candidate qualifies, or
removing the one candidate permitted by this ADR would still leave
`voters > cluster_size`, promotion is refused for operator cleanup rather
than committing an unsafe or still-oversized configuration. Replacement
retirement therefore satisfies overflow before the overflow rule is
considered; a three-voter cluster replacing one voter removes exactly the
predecessor, never the predecessor plus a second voter.

If an overflow removal is needed and no candidate qualifies, the
promotion is refused with a
machine-readable reason (voter set full, no removable peer) rather than
growing the cluster — the new instance keeps polling, and the situation is
visible in status output. There is no background voter reaper: voter
membership only shrinks inside a promotion or by explicit `admin remove`.
The admission-time replacement of a stale *learner* in §6 never touches the
voter set. This keeps the serial ASG rolling replacement correct by
construction: at any moment at most one voter is in flight, quorum among
survivors holds, and the lifecycle hook's readiness gate (§7) prevents the
ASG from proceeding to the next instance before convergence.

### 6. Authorization: a scoped machine self-service grant, and cert reload

Self-join makes a coordinator's *machine* credential a routine caller of
membership verbs, which ADR 0023 classifies as unscoped-admin cluster
verbs. Granting machine certs unscoped admin would mean any coordinator
leaf — or anyone who obtains one — could rewrite arbitrary membership.
We amend ADRs 0022/0023 with a deliberately narrow **machine self-service
grant** instead, built on the certificate's own CA-attested identity:

- Every coordinator leaf carries a **machine identity**: a stable name for
  one coordinator installation in its subject (alongside the coordinator
  role marker that distinguishes it from agent leaves; concrete SAN/OU form
  settled at implementation within ADR 0022's framework). "Machine" is the
  authorization term, not necessarily a physical host: production normally
  runs one installation per VM, while an N-process development cluster
  issues N distinct subjects on one host. This imposes a requirement on the
  issuance process, whichever external path a deployment uses: one
  installation, one subject, unique across the fleet and **stable across
  certificate rotation** — the subject is the identity, the leaf and its
  key are disposable. An earlier draft used a self-generated per-instance
  keypair here; review rejected it, correctly: a key introduced by the
  very request it authenticates attests nothing, and one stolen
  certificate could have minted identities without bound.
- Admission creates a **replicated binding**: the membership node record
  stores the machine identity taken from the mTLS session `AddLearner`
  arrived under (verified by the TLS layer, never claimed in the request
  body). `InitializeCluster` creates the same binding for the initial
  voter — the forming daemon binds the subject of its own configured
  `[tls]` leaf — so no seat, including the first, is ever unbound. At
  most one *voter* and one *pending learner* may be bound to a given
  machine identity. An `AddLearner` from a machine with a bound voter is
  admitted as a *replacement* (the voter is marked superseded, and the
  promotion that follows retires it atomically, §5). If a *different*
  pending learner already holds that machine identity's replacement slot,
  the first committed admission remains the deterministic winner; the
  newcomer is refused with non-retryable-while-current
  `MachineSeatPending { incumbent_id }` rather than evicting it. The losing
  daemon enters `seat-conflict`, watches status, and does not resubmit while
  that incumbent is reachable and making replication progress, preventing
  two restart loops from replacing each other forever. If the incumbent
  disappears, or is unreachable or makes no progress for
  `replacement_grace` (default 60s), the loser may retry once; the leader
  atomically removes the stale learner and admits the newcomer in log order.
  A racing request then observes the new, non-stale incumbent and returns to
  `seat-conflict`. Learner replacement never touches quorum and requires no
  background reaper. One credential can therefore hold at most one vote and
  one stable in-flight replacement, in formation and forever after; a
  stolen certificate cannot fill a quorum.
- Before admitting, the leader also verifies the claimed endpoint: it
  dials the advertised address over mTLS and requires both that the
  serving certificate presents the requester's machine identity and that
  `ProbeCluster` there reports the claimed stamped node id.
- **Self-scope is the whole machine grant.** A coordinator machine
  certificate may call `ProbeCluster`, `ClusterStatus`, `AddLearner` for
  its own machine identity as above, and `PromoteVoter` for the one node
  id its machine identity is bound to. Everything else — `RemoveNode`,
  `InitializeCluster`, `set-address`, promotion or admission of an id
  bound to another machine — is refused for machine certs. Agent
  certificates can call none of the membership surface. The integration
  tests assert the full refusal matrix.
- Operator-authenticated verbs use the **operator-profile certificate**
  ADR 0022 already defines for break-glass: a distinct cert profile,
  presented over the same mTLS admin listener the machine certs use
  (there is no plaintext or token path to the raft listener).
  `InitializeCluster` in particular *cannot* be authorized by an OIDC
  unscoped admin: at day zero the replicated role bindings that would
  confer that authority are part of the very policy being created, and
  the RPC is reachable before raft exists. ADR 0022 already requires the
  operator-profile certificate for `cluster init`; this ADR inherits that
  requirement unchanged, and extends it to the other operator-only
  membership verbs. OIDC-principal-driven membership administration via
  the client API is out of scope here and may layer on later.

The net effect: possession of one coordinator machine credential admits
exactly one learner — the single seat its CA-attested subject names, at an
endpoint verified to hold both that subject and the claimed raft identity
— and promotes exactly that learner. It can never remove, repoint,
initialize, or occupy a second seat. The blast radius of a stolen machine
cert is one membership slot, which is the same blast radius as physical
possession of the machine it was issued to.

Certificate issuance stays external (the two documented paths from the
deployment story: platform-issued short-lived leaves, or long-lived certs
from config management). What the coordinator gains is **reload without
restart**: the TLS paths in `[tls]` are re-read on change (mtime watch,
plus SIGHUP to force), served via a connection-time certificate resolver on
all three listeners, and picked up by outbound peer/admin channels on
reconnect. Short-lived externally-rotated certificates then require no
process choreography at all; in-flight connections finish on the old leaf.

### 7. Machine-readable status and readiness

Log scraping is removed from every workflow:

- **`GET /readyz`** on the client listener (beside the existing
  `/metrics`): returns the convergence state as JSON — `cluster_uuid`,
  `node_id`, `instance_uuid`, phase (`waiting` | `joining` | `learner` |
  `seat-conflict` | `voter`), leadership, applied index, replication lag,
  plus `voters`, `voters_live` (voters the leader currently observes within the
  promotion-lag threshold), `cluster_size`, and `formed`. `formed` is
  strictly **membership cardinality** (`voters ≥ cluster_size`) — desired
  membership reached, saying nothing about health. The status code
  distinguishes three questions:
  - `GET /readyz` → 200 **iff** this replica is an initialized voter whose
    applied index is within the promotion threshold of the leader. This is
    *node* readiness: the ASG lifecycle-hook and load-balancer gate, and
    "wait for the new node to finish synchronizing before replacing the
    next" is exactly "wait for 200".
  - `GET /readyz?require=formed` → 200 additionally requires `formed`:
    the membership shape is as intended. Suitable for gates that care
    about configuration completeness, not liveness.
  - `GET /readyz?require=healthy` → 200 additionally requires
    `voters_live ≥ cluster_size` sustained for a stability interval
    (default 10s). This is the *cluster-redundancy* gate: initial-bringup
    automation, policy seeding beyond what `cluster init --policy`
    applied, and anything that assumes the cluster can lose a node gates
    here — a fully-enumerated membership with unreachable voters must not
    pass it.

  `voters_live` is a leader observation (openraft replication metrics
  exist only there), but `/readyz` is served by every replica and may sit
  behind a load balancer, so followers must not guess: a non-leader
  answers `?require=healthy` (and populates `voters_live`) from a
  **freshness-bounded health snapshot** fetched from the leader over the
  existing admin channel and cached briefly (bound ≈ 2s). A follower
  whose snapshot is stale or whose leader is unreachable returns 503 with
  a machine-readable `health_unknown` reason — unknown health is not
  health, and within the freshness bound every replica gives the same
  answer rather than alternating between truth and false failure.
- **`admin status --json`** emits the cluster-wide view (membership,
  per-follower lag, machine-identity bindings) in stable JSON for
  scripting; the human table remains the default.
- The systemd unit runs `Type=notify`: the daemon signals `READY=1` when
  listeners are serving, so unit ordering works, while cluster and node
  readiness remain `/readyz`'s job. A parked daemon (§1) is `READY=1`,
  phase `waiting`, HTTP 503 — visible, alive, and deliberately not
  "ready". A daemon in `seat-conflict` is likewise alive but returns 503
  until its incumbent disappears and it can retry admission.

## Consequences

- One artifact, one config file, one command line for the daemon, plus one
  explicit `cluster init` at cluster birth. A production cluster's
  coordinator config is byte-identical across replicas; an EC2 ASG needs
  only a launch template with static user-data, and integration tests get
  N-process local clusters from the `file` backend plus generated
  per-process `data_dir`/ports and one harness-driven init call.
- Formation cannot be forked or repeated by infrastructure behavior:
  daemons never initialize on their own, so divergent discovery views,
  partitions, and volume-less replacement fleets all converge to "parked
  and alarming", never to a second history. The cost is one deliberate
  human/automation act per cluster lifetime, and that a wiped fleet stays
  down until someone decides between restore and re-init — which is the
  correct posture for a data-loss event.
- The empty-directory fail-stop from ADR 0016 is traded away: an empty
  data dir now means "new instance" rather than "operator error until
  proven otherwise". The failed-mount guard moves to unit/mount ordering,
  and the residual risk is bounded to a spurious learner join plus later
  removal of the orphaned identity — the amnesiac-voter defense, the
  identity stamp, and the cross-cluster refusal all survive intact.
- Discovery remains strictly advisory and non-blocking: it can delay a
  join but can neither wedge convergence (no step requires all candidates
  to answer) nor block a removal (backends without liveness semantics
  contribute nothing to removal decisions) nor change membership.
- Machine credentials gain exactly one capability: occupying the single
  membership seat their CA-attested subject names, at an endpoint the
  leader has verified holds both that subject and the claimed raft
  identity. Removal, repointing, initialization, and second seats stay
  out of reach. This is a real amendment to ADRs 0022/0023 and the
  integration tests must cover the refusal matrix (agent cert, machine
  cert on a foreign id, machine cert on operator verbs, machine cert
  attempting a second concurrent seat).
- The membership verbs acquire an explicit server-side idempotency
  contract (state short-circuit before gates), amending their current
  semantics — notably `PromoteVoter` on an existing voter becomes a no-op
  success instead of a spurious lag error. The convergence loop's
  restart-safety rests on this contract, so it is tested directly.
- The external PKI acquires a hard requirement it did not have: one
  coordinator installation, one subject, unique across the fleet and
  stable across rotation. Both documented issuance paths must be checked
  against it, and a deployment that violates it (shared wildcard subjects,
  per-leaf random names) loses the one-seat-per-credential property — this
  is worth a startup-time lint where detectable.
- New failure modes to test explicitly: interrupted join at every step,
  formation crashed between stamp and initialize (resumed on restart) and
  `cluster init` retried with same and different tokens, replacement
  promotion in an *underfilled* cluster (predecessor's vote retired in
  the same joint change), replacement in a full cluster (predecessor
  retirement satisfies cardinality without a second overflow removal),
  a pre-existing overflow too large for one removal (promotion refused),
  two concurrent replacements for one machine identity choosing one stable
  pending learner while the loser enters `seat-conflict`, stale pending
  learner cleanup after `replacement_grace`, promotion refused with the
  voter set full, concurrent joiners racing for one vacancy, leader change
  mid-join, double-init attempts, a parked fleet resuming when its
  cluster reappears, `formed` true with unreachable voters (must fail
  `?require=healthy`), `?require=healthy` answered by a follower with and
  without a fresh leader snapshot, and cert rotation under load.
  The multi-node integration test stops driving admin RPCs by hand and
  instead asserts convergence from N identical configs plus one init call.
- Deferred, unchanged in scope: platform-CAS auto-formation (opt-in, per
  platform) behind the same `InitializeCluster` seam; Consul (or any
  registry) as another `Discovery` impl; agent-side enrollment and drain
  remain OD-15; rolling *upgrade automation* beyond the readiness gate
  stays operational tooling on top of these primitives, not coordinator
  code.
