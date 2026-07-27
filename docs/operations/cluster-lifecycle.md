# Cluster Lifecycle

How to bring up a coordinator cluster, add a coordinator, and replace one.
The rules being operated here are decided in
[ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md)
(one flagless daemon command with derived intent; pluggable seed-only
discovery; explicit local-only formation; a cluster-owned CA with
token-based enrollment; self-join; explicit `ReplaceVoter` plus
evidence-gated removal), layered on
[ADR 0016](../decisions/0016-coordinator-rebuild-learner-join.md) (node IDs
are single-use instance identities; disks carry an identity stamp; an
empty disk only ever enters an existing cluster as a learner),
[ADR 0025](../decisions/0025-self-minted-coordinator-identity.md)
(identities are minted, never configured), and
[ADR 0020](../decisions/0020-node-config-vs-replicated-policy.md) (one TOML
file per process; the daemon's only flag is `--config`). All
coordinator↔coordinator traffic — Raft and the membership admin surface
alike — is mutual TLS
([ADR 0011](../decisions/0011-container-security-posture.md)).

## The one command

```
coppice coordinator --config /etc/coppice/coordinator.toml
```

This is the entire daemon command line in every situation — first node,
joining node, restarted node. The process derives its intent from the data
directory:

- **Manifest present** → resume the replica stamped on this disk, then
  converge (a caught-up voter no-ops; an interrupted join continues).
- **Manifest recording an incomplete formation** (formation intent
  without the `formation_complete` marker) → fail-stop in phase
  `formation-failed`. Wipe the data directory and re-run `init`; there
  is no resume path, and no peer can have joined the partial cluster.
- **Manifest absent** → new instance: enroll for a certificate if none
  is on disk (see below), then discover and probe for the cluster. If an
  initialized cluster with the configured `cluster_id` is found, it
  self-joins (mints its identity, requests learner admission, catches
  up, requests promotion). If none is found, it **parks** — alive,
  probing, visible in `/readyz` as phase `waiting` — until a cluster
  appears or an operator forms one locally. A daemon never bootstraps on
  its own initiative.

The systemd unit should set `Restart=always` (the join loop is idempotent
and resumes from any interruption) and `RequiresMountsFor=` the data
volume (an unmounted volume must fail unit ordering, not present an empty
directory).

## Identities and certificates, once

Four identities exist around any coordinator:

- **Cluster id** — the operator-chosen *logical* name of the cluster
  (`uuidgen` output is a convenient choice), written once as
  `cluster_id` into the shared config, identical in every replica's
  file. It answers "which cluster do I intend to find": probing and
  joining match on it.
- **History id** — minted by `init` at formation, stamped into every
  data directory at initialization (joiners learn it at admission), and
  cross-checked on every start and on peer contact. It names one raft
  *history*: a wiped-and-re-formed cluster keeps its `cluster_id` but
  carries a new history id, so volumes from the old history fail-stop
  instead of merging into the new one. A mismatch fail-stops: wrong
  volume, cross-cluster mixup, or a pre-re-formation volume. (Earlier
  ADRs call this the cluster UUID; ADR 0037 renames the design surface —
  code, proto field names, and the manifest — to keep it visibly
  distinct from `cluster_id`.)
- **Node ID** — minted automatically when the data directory is first
  initialized, stamped into the manifest, read back on every restart
  (ADR 0025). It never appears in config, in logs-you-must-scrape, or in
  any operator workflow. A rebuilt machine mints a *new* node ID; the old
  one is retired during replacement. Do not think of node IDs as slots.
- **Machine identity** — a cluster-minted opaque identifier carried in
  the coordinator's TLS certificate subject and persisted in the data
  directory, naming one coordinator *installation* (one per VM in
  production; the dev harness issues one per process — data directories,
  not hosts, are installations). It is stable across restarts that keep
  the installation's state; a fresh data directory is a new installation
  with a new identity. Membership binds each seat to the machine
  identity that admitted it, one identity to at most one node id, ever
  (ADR 0037 §7).

Certificates come from the **cluster's own CA**, minted at formation:
a new coordinator presents a role-scoped enrollment token plus a CSR to
`POST /api/v1/enroll` on the cluster's **public client listener** and
receives its leaf and the CA bundle. The endpoint sits on the public
listener deliberately (ADR 0037 §4): a certless machine cannot verify
cluster-PKI TLS yet, and that listener already has the solved bootstrap
trust posture — enrollment clients perform ordinary **system-root
hostname verification** against its externally-signed certificate. A
plain-HTTP endpoint requires the conspicuous `insecure = true` opt-in
(dev/test only — it exposes the token); a token configured against an
unverified endpoint without it is a startup error. Renewal happens
later over the machine-plane mTLS services, authenticated by the
current leaf, preserving the subject — short-lived leaves are free.
Externally-issued certificates remain a supported substitution via the
same `[tls]` paths. Either way, changed files under `[tls]` reload
without restart (or force with SIGHUP); rotation never requires
recycling processes. Operators additionally hold an **operator-profile
certificate** (ADR 0022) for the verbs machines may not call — the
first one is minted by `init` at formation.

Config is byte-identical across replicas — addresses come from
`advertise_host` defaulting per machine, seeds come from the `[discovery]`
section (`static`, `dns`, `file`, or `ec2-asg` backend), and
`cluster_size` states the intended voter count. Discovery only ever feeds
first-dial; membership itself is replicated raft state.

## Bootstrap: forming a new cluster

Formation is a **local** act: it happens on the coordinator host, over
the daemon's root-owned Unix admin socket — there is no network formation
verb. Exactly once per cluster lifetime, on any one parked daemon (via
SSH, SSM, cloud-init, or a test harness):

```
uuidgen        # once; becomes the logical cluster_id in the shared config
               # (init separately mints the history id)
coppice coordinator init --config /etc/coppice/coordinator.toml \
  [--policy policy.toml] [--operator-csr csr.pem] [--operator-cn NAME] \
  [--out-dir /root/coppice-day0]
```

`--config` is read only to find the socket — `<data_dir>/admin.sock`, or
the explicit `[listen] admin_socket` — and never for a target or TLS
material: there is nothing to authenticate to. Without `--operator-csr`
the cluster mints the operator keypair and prints **both halves plus the
CA bundle** to this terminal; that private key exists nowhere else, so
collect it (or pass `--out-dir`, which writes `operator.crt`,
`operator.key`, and `ca.crt` owner-only).

The daemon runs a probe round as a double-init guard (its own address
excluded), stamps itself,
mints the cluster root CA, forms a single-voter cluster, applies the
supplied bootstrap policy, signs (or mints) the first operator-profile
certificate, prints it with the CA bundle, and stamps the
`formation_complete` marker. Until the marker exists the daemon serves no
enrollment, membership, or client traffic and does not answer probes as
initialized — a partial formation can never acquire a second member.

The guard **fails closed**: a daemon holding no TLS material cannot ask
discovered candidates anything, and `init` refuses rather than treating
"cannot probe" as "no cluster exists". A certless first formation is
still one command — run it with an empty discovery seed set — and once
enrollment lands, a parked daemon acquires a leaf before formation, so
the guard runs as designed in every configuration.

A crash anywhere in formation restarts into phase `formation-failed`:
wipe that one data directory, restart the daemon (it parks), and re-run
`init`. Re-running against a *completed* formation returns
`AlreadyInitialized` with the cluster status (and never mints
credentials); a lost init output is recovered with the local
`admin issue-operator-cert` verb on the same socket.

Bringup is single-phase: the enrollment tokens are chosen *before*
anything boots (baked into the launch template, seeded as hashes via
`init --policy`), and nothing formation emits needs distributing —
enrollment clients verify the public listener's externally-signed
certificate via system roots. Every other parked daemon enrolls,
observes the formed cluster on its next probe, and joins automatically.
Wait for `GET /readyz?require=healthy` to return 200 against the leader
(all `cluster_size` voters live and caught up): the cluster is formed
and redundant.

Daemons never form a cluster themselves, and formation is unreachable
from the network — which is why discovery churn, replacement fleets, and
partitions can never re-form an empty history. A fleet that has lost all
its volumes parks and alarms; recovery is a deliberate restore or
re-init, never automatic amnesia.

## Restart

Run the same command. No flags, no decisions. The startup refuses to run
only if the identity stamp names a different history than the cluster it
reaches — a stamped history id that doesn't match (wrong volume,
cross-cluster mixup, or a volume from before a re-formation); an *empty*
directory is treated as a new instance and converges as above — which is
why the mount guard belongs in the unit file, not in operator memory.

## Join and replace

Adding capacity and replacing an instance start the same way: a new
machine with an empty data directory, an enrollment token, and the
standard command. It enrolls, self-joins as a learner, catches up
(streaming snapshot install for rebuilds, per
[ADR 0017](../decisions/0017-log-manifest-truncation-and-purge.md)/
[ADR 0018](../decisions/0018-protobuf-records-in-parallel-containers.md)),
and requests promotion. A fresh installation always carries a *new*
machine identity, so the cluster never guesses which voter a newcomer
replaces (ADR 0037 §7); who retires the departed voter depends on
whether it is still alive:

- **Dead predecessor — hands-off.** The newcomer's plain promotion folds
  in the removal of at most one voter whose replication has been failing
  past `removal_grace` (default 120s) — the leader's own evidence, and
  nothing else. This is the terminate-before-launch ASG refresh and the
  crash-replacement path: nobody names the pair.
- **Live predecessor — explicit.** `ReplaceVoter { old, new }`
  (operator-authenticated; also exposed as `admin replace-voter`)
  commits one joint-consensus change that promotes the caught-up
  learner and removes the named predecessor atomically. This is the
  launch-before-terminate path — a live voter never qualifies as
  evidence-dead, so a rollout that starts the new instance first must
  drive this verb (or configure the refresh to terminate first).

Every change must leave at most `cluster_size` voters, a live majority,
and a confirmed CA-key holder among the continuing voters (the incoming
voter receives and durably confirms the key *before* the joint change
commits — ADR 0037 §4); otherwise promotion is refused for operator
cleanup. Stale learners that died before promotion expire after
`learner_expiry` (default 1h) without successful replication contact —
an idle but caught-up learner stays indefinitely.

Gate rolling replacement on the new node's readiness: replace one
instance, wait for its `GET /readyz` to return 200 (caught-up voter),
proceed to the next. An EC2 ASG instance refresh with a launch lifecycle
hook that polls `/readyz` implements exactly this; quorum among survivors
holds throughout because at most one voter is ever in flight.

A second installation presenting an already-bound machine identity is
refused outright and surfaced in status: under fresh-state-fresh-identity
that request is a duplicated or stolen credential, or a misissuance — an
operator problem to see, not a race to arbitrate.

## Observing convergence

No workflow reads logs. Per node:

```
curl -s https://coord-1:7070/readyz          # 200 iff caught-up voter
curl -s "https://coord-1:7070/readyz?require=healthy"   # 200 iff formed AND redundant
```

The same document is available on the local admin socket via
`admin local-status`, which matters when the client listener is not
being served at all — a parked daemon, or one in `formation-failed`:

```
coppice coordinator admin --config coordinator.toml local-status
```

The JSON body (`local-status --json`) carries `phase` (`waiting` |
`formation-failed` | `joining` | `learner` | `voter`),
node/cluster/instance ids, applied index and lag, `voters`,
`cluster_size`, `formed`, and any admission refusal the daemon last
received. Note that `?require=healthy` is answered authoritatively only
by the leader; a follower returns 503 `health_unknown` with a leader
hint — unknown health is not health, so point automation at the leader
or use `admin status --json`, the cluster-wide scripting surface, which
resolves the leader itself:

```
coppice coordinator admin --config coordinator.toml status --json
```

`admin status` always emits the cluster-wide document with one stable
JSON schema, with or without `--target` (without one, the target is the
first candidate from the config's `[discovery]` backend, as for every
other network verb). If the replica that answers is a follower, the CLI
re-dials the leader the answer names — once, at the address the
follower's own membership view carries — and renders the leader's
document. The JSON carries membership with roles and machine-identity
bindings, per-follower replication lag (leader only), leadership, and
`health` — the same leader-only redundancy verdict `?require=healthy`
gates on, with `null` values only when no leader could be reached
(health is never fabricated).

## Break-glass: the manual verbs

The `admin` verbs from the pre-0037 workflow remain for surgery, now
requiring the operator-profile certificate for anything beyond status:

```
coppice coordinator admin --config c.toml --target coord-1:7071 add-learner …
coppice coordinator admin --config c.toml --target coord-1:7071 promote …
coppice coordinator admin --config c.toml --target coord-1:7071 replace-voter --old <id> --new <id>
coppice coordinator admin --config c.toml --target coord-1:7071 remove --node-id <id>
coppice coordinator admin --config c.toml --target coord-1:7071 set-address …
coppice coordinator admin issue-operator-cert        # local socket only
```

All verbs are idempotent by contract (re-running a completed step is a
no-op success; a completed `replace-voter` — new a voter, old absent —
is likewise a no-op). `set-address` commits only after the leader
verifies the new endpoint presents the target's bound machine identity
and stamped node id. `issue-operator-cert` runs only over the local
admin socket — filesystem access is its authority — and is the recovery
for lost or expired operator certificates at any point in the cluster's
life. Enrollment tokens are managed with `coppice node enroll-token`
(mint, with `--role`, optional `--ttl`) and `enroll-token list`/`revoke`
as policy verbs. Admin requests against a follower are refused with the
current leader named in the error.

## Failure modes worth recognizing

| Signal | Meaning | Action |
| --- | --- | --- |
| phase `waiting`, indefinitely | no initialized cluster found: genuinely new fleet awaiting `init`, discovery misconfiguration, failing enrollment (bad token, unreachable endpoint), or a fleet whose volumes are all gone | form the cluster deliberately, fix `[discovery]`/`[enrollment]`, or begin restore — parking is deliberate, never auto-resolved |
| phase `formation-failed` | a formation crashed before its `formation_complete` marker (either side of `raft.initialize`) | wipe that one data directory, restart the daemon, re-run `init` — no peer can have joined |
| "identity stamp mismatch" at startup | volume belongs to another cluster or another instance | attach the right volume; never edit the stamp |
| "cross-cluster contact refused" (from a peer) | a coordinator from a different cluster dialed this one | fix the peer's config/addressing |
| startup error: enrollment endpoint unverifiable | `[enrollment]` names a token against a plain-HTTP endpoint without `insecure = true` | give the endpoint an externally-verified certificate, or opt into insecure for dev/test; never fall back silently |
| promotion refused: voter set full, no removable peer | a joiner wants a vote, no seat is free, and the departing voter is still alive (live voters never qualify as evidence-dead) | drive `admin replace-voter --old … --new …`, terminate the departure first, or raise `cluster_size` |
| admission refused: machine identity already bound | a second installation presented an identity the membership already binds | treat as duplicated/stolen credential or misissuance — investigate, revoke the identity, re-enroll the legitimate node |
| `readyz?require=healthy` → 503 `health_unknown` | queried replica is not the leader | point the probe at the leader or use `admin status --json` |
