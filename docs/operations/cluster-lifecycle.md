# Cluster Lifecycle

How to bring up a coordinator cluster, add a coordinator, and replace one.
The rules being operated here are decided in
[ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md)
(one flagless daemon command with derived intent; pluggable seed-only
discovery; explicit one-shot formation; self-join; promotion-coupled
removal), layered on
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
- **Manifest absent** → new instance: discover and probe for the cluster.
  If an initialized cluster with the configured `cluster_id` is found, it
  self-joins (mints its identity, requests learner admission, catches up,
  requests promotion). If none is found, it **parks** — alive, probing,
  visible in `/readyz` as phase `waiting` — until a cluster appears or an
  operator forms one. A daemon never bootstraps on its own initiative.

The systemd unit should set `Restart=always` (the join loop is idempotent
and resumes from any interruption) and `RequiresMountsFor=` the data
volume (an unmounted volume must fail unit ordering, not present an empty
directory).

## Identities and certificates, once

Three identities exist around any coordinator:

- **Cluster UUID** — generated once per cluster (`uuidgen`), written as
  `cluster_id` into the shared config, stamped into every data directory
  at initialization, and cross-checked on every start. A mismatch
  fail-stops: wrong volume or cross-cluster mixup.
- **Node ID** — minted automatically when the data directory is first
  initialized, stamped into the manifest, read back on every restart
  (ADR 0025). It never appears in config, in logs-you-must-scrape, or in
  any operator workflow. A rebuilt machine mints a *new* node ID; the old
  one is retired during replacement. Do not think of node IDs as slots.
- **Machine identity** — the stable subject of the coordinator's TLS
  certificate, naming one coordinator *installation* (one per VM in
  production; the dev harness issues one per process). The PKI must keep
  it unique across the fleet and stable across certificate rotation: the
  subject is the identity, leaves and keys are disposable. Membership
  binds each seat to the machine identity that admitted it, and a machine
  identity can hold at most one vote (ADR 0037 §6).

Certificates are issued externally (platform PKI or config management) and
reload without restart: changed files under `[tls]` are picked up
automatically (or force with SIGHUP). Rotation never requires recycling
processes. Operators additionally hold an **operator-profile certificate**
(ADR 0022) for the verbs machines may not call.

Config is byte-identical across replicas — addresses come from
`advertise_host` defaulting per machine, seeds come from the `[discovery]`
section (`static`, `dns`, `file`, or `ec2-asg` backend), and
`cluster_size` states the intended voter count. Discovery only ever feeds
first-dial; membership itself is replicated raft state.

## Bootstrap: forming a new cluster

Start all the replicas (they park), then, exactly once per cluster
lifetime, from any machine holding the operator certificate:

```
uuidgen        # once; becomes cluster_id in the shared config
coppice-cli cluster init --target coord-1:7071 \
    --formation-token <stable-token> [--policy policy.toml]
```

The targeted daemon stamps itself, forms a single-voter cluster, and
applies the supplied bootstrap policy; every other parked daemon observes
the formed cluster on its next probe and joins automatically. Wait for
`GET /readyz?require=healthy` to return 200 (all `cluster_size` voters
live and caught up): the cluster is formed and redundant.

The formation token makes the command safe to retry from automation:
supply a value that is already durable in your provisioning system (a
stack id, an SSM value) or use `--formation-token-file`. Re-running with
the same token resumes or reports the completed formation; a different
token is refused, naming the recorded one. A daemon that crashes
mid-formation completes it on restart.

`InitializeCluster` requires the operator-profile certificate — machine
certificates cannot call it, which is why discovery churn, replacement
fleets, and partitions can never re-form an empty history. A fleet that
has lost all its volumes parks and alarms; recovery is a deliberate
restore or re-init, never automatic amnesia.

## Restart

Run the same command. No flags, no decisions. The startup refuses to run
only if the directory's cluster UUID doesn't match `cluster_id` (wrong
volume or cross-cluster mixup); an *empty* directory is treated as a new
instance and converges as above — which is why the mount guard belongs in
the unit file, not in operator memory.

## Join and replace

Adding capacity and replacing an instance are the same operator action:
start a new machine with an empty data directory and the standard command.
It self-joins as a learner, catches up (streaming snapshot install for
rebuilds, per [ADR 0017](../decisions/0017-log-manifest-truncation-and-purge.md)/
[ADR 0018](../decisions/0018-protobuf-records-in-parallel-containers.md)),
and requests promotion. The leader folds removal into the promotion's
single joint-consensus change (ADR 0037 §5):

- a predecessor seat held by the *same machine identity* is retired
  unconditionally in that change — replacement never leaves two votes
  bound to one machine;
- a *dead* voter is additionally removed only when the post-promotion set
  would exceed `cluster_size`, and only on the leader's own replication
  evidence (unreachable past `removal_grace`; discovery absence counts
  only for backends that attest liveness, i.e. `ec2-asg`);
- every change must leave at most `cluster_size` voters and a live
  majority, else promotion is refused for operator cleanup.

Gate rolling replacement on the new node's readiness: replace one
instance, wait for its `GET /readyz` to return 200 (caught-up voter),
proceed to the next. An EC2 ASG instance refresh with a launch lifecycle
hook that polls `/readyz` implements exactly this; quorum among survivors
holds throughout because at most one voter is ever in flight.

Two replacements racing for the same machine identity resolve
deterministically: the first admission wins; the loser parks in phase
`seat-conflict` and retries only once the incumbent is gone or stale past
`replacement_grace`.

## Observing convergence

No workflow reads logs. Per node:

```
curl -s https://coord-1:7070/readyz          # 200 iff caught-up voter
curl -s "https://coord-1:7070/readyz?require=healthy"   # 200 iff formed AND redundant
```

The JSON body carries `phase` (`waiting` | `joining` | `learner` |
`seat-conflict` | `voter`), node/cluster/instance ids, applied index and
lag, `voters`, `voters_live`, `cluster_size`, and `formed`. Note that
`?require=healthy` on a follower is answered from a freshness-bounded
leader snapshot and returns 503 `health_unknown` when the leader is
unreachable — unknown health is not health. Cluster-wide:

```
coppice coordinator admin --config coordinator.toml --target coord-1:7071 \
    status --json
```

## Break-glass: the manual verbs

The `admin` verbs from the pre-0037 workflow remain for surgery, now
requiring the operator-profile certificate for anything beyond status:

```
coppice coordinator admin --config c.toml --target coord-1:7071 add-learner …
coppice coordinator admin --config c.toml --target coord-1:7071 promote …
coppice coordinator admin --config c.toml --target coord-1:7071 remove --node-id <id>
coppice coordinator admin --config c.toml --target coord-1:7071 set-address …
```

All verbs are idempotent by contract (re-running a completed step is a
no-op success). `set-address` commits only after the leader verifies the
new endpoint presents the target's bound machine identity and stamped
node id. Admin requests against a follower are refused with the current
leader named in the error.

## Failure modes worth recognizing

| Signal | Meaning | Action |
| --- | --- | --- |
| phase `waiting`, indefinitely | no initialized cluster found: genuinely new fleet awaiting `cluster init`, discovery misconfiguration, or a fleet whose volumes are all gone | form the cluster deliberately, fix `[discovery]`, or begin restore — parking is deliberate, never auto-resolved |
| "identity stamp mismatch" at startup | volume belongs to another cluster or another instance | attach the right volume; never edit the stamp |
| "cross-cluster contact refused" (from a peer) | a coordinator from a different cluster dialed this one | fix the peer's config/addressing |
| promotion refused: voter set full, no removable peer | a joiner wants a vote but no seat is free and no voter qualifies as dead | remove the intended departure (`admin remove`) or raise `cluster_size` |
| phase `seat-conflict` | two replacements raced for one machine identity | none if the incumbent is progressing; the loser resolves itself once the incumbent completes or goes stale |
| `readyz?require=healthy` → 503 `health_unknown` on a follower | that follower's leader snapshot is stale or the leader is unreachable | check leader liveness; query another replica |
| formation-token refusal naming another token | `cluster init` retried with a different token than the recorded one | re-run with the recorded token |
