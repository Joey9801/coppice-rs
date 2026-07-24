# Configuration

Coppice is configured through two mechanisms with a bright line between them,
decided in [ADR 0020](../decisions/0020-node-config-vs-replicated-policy.md):

1. **A node configuration file** — one TOML file per process, read once at
   startup. Holds everything that is legitimately per-node: addresses, paths,
   tuning, telemetry.
2. **Replicated cluster policy** — held in the Raft state machine, changed
   via `coppice-cli policy …` commands, identical on every replica by
   construction. Holds everything that replicas must agree on.

The litmus test for any new knob: *would replicated state, scheduling, or
fencing diverge if two replicas disagreed on it?* → policy. *Could a canary
node want a different value than its peers?* → config file.

## Which mechanism owns what

| Setting | Where |
| --- | --- |
| Listen/advertise addresses, ports | config file |
| Data directory | config file (the raft node id is *not* config: minted at init and read from the disk stamp, [ADR 0025](../decisions/0025-self-minted-coordinator-identity.md)) |
| Machine-plane TLS paths (`[tls]`); client-listener TLS paths (`[client_tls]`); enrollment token path and trust anchor | config file |
| Discovery backend and `cluster_size` | config file (seed-only; consulted before replicated state is reachable — [ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md)) |
| Enrollment tokens (hashes), issued-identity revocations | replicated policy (ADR 0037) |
| SSO issuer, client id, client-secret path | config file |
| Log level/format, OTLP endpoint, metrics address | config file |
| Raft election timeout, Raft heartbeat interval, snapshot thresholds | config file (liveness-only; safe to vary per node) |
| Quota-entity tree, soft quotas, cost weights | replicated policy |
| Decay policy (tick, λ — the "half-life"), penalty exponent | replicated policy ([ADR 0019](../decisions/0019-deterministic-quota-arithmetic.md)) |
| Priority-multiplier table | replicated policy |
| Data-retention windows | replicated policy ([ADR 0012](../decisions/0012-data-retention.md)) |
| Agent-liveness / allocation-lost deadlines (fencing inputs) | replicated policy |
| SSO role/group → authorization mappings (role bindings) | replicated policy ([ADR 0023](../decisions/0023-scoped-role-bindings.md)) |
| OIDC groups-claim name | replicated policy (interpretation of a token decides who is an admin — [ADR 0022](../decisions/0022-oidc-identity-and-authentication.md)) |

If a setting seems to belong to both, it probably splits the way SSO does:
the *connection* parameters are node config, the *meaning* (who is an admin)
is policy.

## The config file

One file per binary, passed explicitly:

```
coppice coordinator --config /etc/coppice/coordinator.toml
coppice agent       --config /etc/coppice/agent.toml
```

Conventions (all from ADR 0020):

- **Unknown keys are startup errors.** A typo fail-stops with the key named.
- **Durations and sizes are strings** — `"1500ms"`, `"24h"`, `"512MiB"`.
  Unlabelled numbers are rejected for both: a size needs a unit suffix, and
  an operator who writes `memory = 34359738368` gets a startup error naming
  the key rather than a value nobody can check by eye. Sizes accept IEC
  (`KiB`/`MiB`/`GiB`/`TiB`/`PiB`/`EiB`, powers of 1024) and SI (`KB`/`MB`/
  `GB`/`TB`/`PB`/`EB`, powers of 1000) suffixes, case-insensitively, with an optional
  fraction — `"1.5GiB"`, rounded up to the next whole byte. Bit units
  (`"10Mbit"`) are refused rather than converted. Sizes are always *reported*
  back in IEC.
- **No inline secrets, ever** — the file holds *paths* to key material, so
  the file itself is safe to commit to config management, diff, and attach
  to support bundles.
- **Precedence:** CLI flags > file > built-in defaults. The daemon's only
  flag is `--config` — startup intent is derived from the data directory
  ([ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md)).
  There is no environment-variable layer.
- **No hot reload of the config file.** Changes take effect on restart;
  coordinator restarts are designed to be cheap (rolling restart with
  learner catch-up). The one deliberate exception is the key material the
  file *points to*: files under `[tls]` paths reload without restart
  (mtime watch, or SIGHUP to force — ADR 0037), so certificate rotation
  never recycles processes.
- The effective configuration is logged in full at startup.

### Annotated coordinator example

```toml
# /etc/coppice/coordinator.toml
#
# Node-local configuration only. Cluster-wide behaviour — quotas, decay,
# retention, authorization — is replicated policy: see `coppice-cli policy`.

# Operator-chosen *logical* cluster name (a uuid is a convenient choice),
# identical in every replica's file; probing and joining match on it.
# Deliberately distinct from the history id, which `init` mints at
# formation and stamps into data directories to name one raft history —
# a wiped-and-re-formed cluster keeps this cluster_id but gets a new
# history id, so stale volumes fail-stop (ADRs 0016/0037). Typed string
# form per ADR 0024.
cluster_id = "cluster-6fa1e2c4-9b0d-4c1e-8f6a-2d3b5a7c9e01"
data_dir = "/var/lib/coppice"

[discovery]
# Seed-only: feeds first-dial and admin tooling; authoritative addresses
# live in replicated membership (ADR 0037). Backends: static | dns |
# file | ec2-asg — exactly one backend table, matching `backend`.
backend = "dns"
cluster_size = 3            # intended voter count (removal + `formed` gates)
[discovery.dns]
name = "coord.batch.example.com"
port = 7071

[listen]
client_addr = "0.0.0.0:7070"    # user/CLI API
raft_addr   = "0.0.0.0:7071"    # coordinator peer traffic
agent_addr  = "0.0.0.0:7072"    # agent heartbeats and reports
advertise_host = "coord-3.batch.example.com"   # what peers and agents dial

[raft]
# Liveness tuning only — never affects safety. The defaults are right for
# ordinary datacenter networks; you almost certainly should not touch this.
election_timeout   = "1500ms"
heartbeat_interval = "300ms"
rpc_timeout        = "1s"       # per-request timeout for peer RPCs
snapshot_log_entries = 50_000
# Post-snapshot log entries kept before purge (ADR 0017); a fresh learner
# beyond this window resyncs via streaming snapshot install (ADR 0016).
snapshot_keep_log_entries = 1_000

[tls]
# MACHINE PLANE ONLY: the leaf served on the raft, agent-gateway, and
# enrollment listeners, and this node's client identity toward peers.
# The trust root is the cluster-owned CA minted at formation (ADR 0037);
# these files are written by enrollment, hot-reloaded on change, and
# externally-issued certs are a supported substitution at the same
# paths. This cert is never served on the user-facing listener below.
cert_path = "/etc/coppice/pki/node.crt"
key_path  = "/etc/coppice/pki/node.key"
ca_path   = "/etc/coppice/pki/ca.crt"

[client_tls]
# USER-FACING HTTP API listener (ADR 0037 §4): an externally signed
# serving certificate (browsers won't trust the cluster's private root),
# or sit behind a TLS-terminating LB, or `insecure = true` (conspicuous
# dev-only opt-in; mutually exclusive with the paths). Independent of
# this serving cert, operator-profile *client* certificates presented
# here are verified against the cluster CA from [tls] (ADR 0022
# break-glass).
cert_path = "/etc/coppice/pki/api.example.com.crt"
key_path  = "/etc/coppice/pki/api.example.com.key"

[enrollment]
# How a certless new installation obtains its leaf (ADR 0037 §§4-5).
# Exactly one trust anchor, or a conspicuous `insecure = true` (dev
# only); a token with no anchor is a startup error.
token_path = "/etc/coppice/enroll-token"
trust = { fingerprint_path = "/etc/coppice/ca-fingerprint" }

[sso]
# Connection identity only — the groups-claim name and all role bindings
# are replicated policy (`coppice-cli policy`), per ADRs 0022/0023.
issuer = "https://sso.example.com/oidc"
client_id = "coppice"
audience = "coppice"
client_secret_path = "/etc/coppice/oidc-secret"

[observability]
log_level  = "info"
log_format = "json"
otlp_endpoint = "https://otel-collector.example.com:4317"
```

The coordinator's Prometheus `/metrics` endpoint has no address of its own:
it is served on the client API listener at `/metrics` (issue #46), alongside
`/api/v1`, so there is no coordinator `metrics_addr` knob. (The agent daemon,
which has no such listener, keeps its own optional `metrics_addr`.)

The agent's file follows the same conventions with its own schema
(coordinator endpoints, the same `[enrollment]` table, image-cache and
workdir paths, resource-advertisement overrides).

## Replicated policy

Policy is inspected and changed through the CLI, which converts
human-friendly forms into the exact replicated representations:

- `coppice coordinator init --policy policy.toml` — supplies *initial*
  policy exactly once, at cluster formation, over the daemon's local
  admin socket (ADR 0037: formation is local-only). The node config file
  never seeds policy, even on first boot.
- `coppice-cli policy …` — reads and updates policy at runtime; every change
  is a committed Raft command, ordered in the log and applied identically on
  every replica.
- Conversions like decay half-life → Q0.64 per-tick factor and quota rate →
  quota stock happen here, in tooling — never inside the state machine
  ([ADR 0019](../decisions/0019-deterministic-quota-arithmetic.md)).
