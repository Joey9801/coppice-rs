# Cluster Lifecycle

How to bring up a coordinator cluster, add a coordinator, and replace one.
The rules being operated here are decided in
[ADR 0016](../decisions/0016-coordinator-rebuild-learner-join.md) (node IDs
are single-use instance identities; disks carry an identity stamp; empty
data directories require an explicit intent flag) and
[ADR 0020](../decisions/0020-node-config-vs-replicated-policy.md) (one TOML
file per process; the only CLI flags are `--config`, `--bootstrap`, and
`--join`). All coordinator↔coordinator traffic — Raft and the membership
admin surface alike — is mutual TLS
([ADR 0011](../decisions/0011-container-security-posture.md)): every command
below presents the node certificate from the config file's `[tls]` section.

## Identities, once

Two identities exist before any process starts:

- **Cluster UUID** — generated once per cluster (`uuidgen`), written as
  `cluster_id` into *every* coordinator's config file, and stamped into every
  data directory at initialization. A directory whose stamp disagrees with
  the config fail-stops at startup: that is the wrong-volume /
  cross-cluster-mixup guard.
- **Node ID** — a small integer allocated once per *instance*, never reused.
  A rebuilt machine gets a *new* node ID; the old one is removed from
  membership during replace. Do not think of node IDs as slots.

The data directory additionally receives an **instance UUID** at
initialization, minted fresh on every `--bootstrap`/`--join`, so "same node
ID, different life" is distinguishable in logs and forensics.

## Bootstrap: the first node of a new cluster

```
uuidgen                       # once; becomes cluster_id in every config
coppice-coordinator --config /etc/coppice/coordinator.toml --bootstrap
```

`--bootstrap` is legal only on an **empty** data directory. It stamps the
directory with the configured cluster UUID and node ID plus a fresh instance
UUID, then forms a single-voter Raft cluster with this node as leader. On a
non-empty directory it fail-stops ("already initialized"); restarting the
node afterwards uses no flag at all.

## Restart: the default

```
coppice-coordinator --config /etc/coppice/coordinator.toml
```

No intent flag means "resume the instance on this disk". The startup
refuses to run if:

- the directory is **empty** — an unexpectedly empty directory is treated as
  a failed mount, never as permission to start clean; the error names the
  directory and both intent flags;
- the directory's **cluster UUID** doesn't match `cluster_id` — wrong volume
  or cross-cluster mixup;
- the directory's **node ID** doesn't match `node_id` — volume attached to
  the wrong instance.

These refusals are the whole ADR 0016 amnesiac-voter defense: an empty disk
can only ever enter the cluster as a new learner.

## Join: adding a coordinator

On the new machine, with a fresh node ID in its config and an empty data
directory:

```
coppice-coordinator --config /etc/coppice/coordinator.toml --join
```

`--join` stamps a fresh instance identity and starts the replica idle — it
holds no vote and triggers no election; it waits to be added. From any
machine with a coordinator config (the admin client dials `--target`, or the
first entry of the config's `peers` list, and authenticates with the config's
TLS material):

```
coppice-coordinator admin --config coordinator.toml --target coord-1:7071 \
    add-learner --node-id 4 --addr coord-4:7071
```

The leader begins replicating to the learner — snapshot install plus log
replay, with no quorum impact while it syncs. Watch progress:

```
coppice-coordinator admin --config coordinator.toml --target coord-1:7071 status
```

Once the learner's replication lag is inside the promotion threshold,
promote it to voter:

```
coppice-coordinator admin --config coordinator.toml --target coord-1:7071 \
    promote --node-id 4
```

`promote` polls until the learner is caught up (bounded by `--wait`, default
60s) and then commits the membership change; a learner still too far behind
after the deadline is reported, not force-promoted.

## Replace: rebuilding a coordinator

Replacing a coordinator — dead disk or healthy-but-recycled during an
upgrade — is join plus a joint removal (ADR 0016's `coordinator replace`
verb; the polished `coppice-cli` wrapper drives exactly this sequence):

1. Provision the new machine with an **empty** data directory and a **new
   node ID** in its config; start it with `--join`.
2. `admin add-learner --node-id <new> --addr <new-addr>` against the leader.
   Because sealed log segments are purged once a snapshot covers them
   ([ADR 0017](../decisions/0017-log-manifest-truncation-and-purge.md)), a
   rebuild catches up primarily via streaming snapshot install
   ([ADR 0018](../decisions/0018-protobuf-records-in-parallel-containers.md)).
3. Promote the new instance and remove the departed identity **in one
   joint-consensus change**:

   ```
   coppice-coordinator admin --config coordinator.toml --target coord-1:7071 \
       promote --node-id <new> --remove <departed>
   ```

4. Confirm with `admin status`: the new node ID is a voter, the departed ID
   is gone, and the applied index advances.

Rolling rebuild of a fleet is this procedure repeated serially — at most one
voter in flight, so quorum among survivors holds throughout. The departed
node ID is never reused.

If only membership cleanup is needed (a learner added by mistake, a node
already removed from the voter set):

```
coppice-coordinator admin --config coordinator.toml --target coord-1:7071 \
    remove --node-id <id>
```

## Failure modes worth recognizing

| Startup message contains | Meaning | Action |
| --- | --- | --- |
| "has no manifest: refusing to start on an unexpectedly empty directory" | failed mount, wrong volume, or a genuinely fresh machine started without an intent flag | fix the mount, or pass `--bootstrap`/`--join` if the empty disk is deliberate |
| "already initialized (manifest present)" | `--bootstrap`/`--join` on a live data directory | drop the flag to restart, or wipe the directory only if this machine is truly being reborn (then also allocate a new node ID) |
| "identity stamp mismatch" | volume belongs to another cluster or another instance | attach the right volume; never edit the stamp |
| "cross-cluster contact refused (ADR 0016)" (in logs, from a peer) | a coordinator from a different cluster dialed this one | fix the peer's config/addressing |

Admin requests against a follower are refused with the current leader named
in the error — re-target the command at the leader.
