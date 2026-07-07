# 20. Node configuration file vs. replicated policy

- **Status:** Accepted
- **Date:** 2026-07-07

## Context

The coordinator (and agent) need configuration: hostnames, ports, data
directories, Raft tuning, TLS key paths, SSO parameters, observability
endpoints. The operator experience we want is a single, human-readable file
per process.

But Coppice already has a second, stronger configuration mechanism: the
Raft-replicated state machine. Quota trees, cost weights, the decay factor
(ADR 0019), retention windows — these are *policy*, replicated precisely so
that no two replicas can ever disagree about them. The decay half-life that
prompted this decision is the canonical example: ADR 0019 deliberately moved
its derived fixed-point factor **into** replicated state. If the same value
also appeared in each node's config file there would be two authorities, and
the failure mode writes itself: an operator edits the file on one
coordinator, restarts it, and either nothing changes (the file is a lie) or
one replica starts behaving differently (divergence — the exact bug the
replication design exists to exclude).

So the real decision is not "which format" — it is where the line sits
between the config file and the state machine, and how to keep every future
knob on the correct side of it.

## Decision

### The litmus test

Every configuration value is classified by two questions, applied in order:

1. **Would replicated state, scheduling decisions, or the fencing protocol
   diverge if two replicas disagreed on this value?** Then it is
   **cluster policy**: it lives in the replicated state machine, is changed
   via committed commands through the API/CLI, and **never appears in the
   config file**. Examples: the quota-entity tree, cost weights, decay
   policy, penalty exponent, priority-multiplier table, data-retention
   windows (ADR 0012), allocation-lost and heartbeat *deadlines* that agents
   and all coordinators must judge identically.
2. **Could a canary node legitimately want a different value than its
   peers?** Then it is **node configuration**: the file. Examples: listen
   and advertise addresses, ports, data directory, TLS/PKI paths, enrollment
   token path, OTLP endpoint, log level and format, snapshot and compaction
   thresholds, Raft *timing* (election timeout, Raft heartbeat interval —
   per-node-safe because Raft timing affects liveness, never safety).

Split cases split: for SSO, the OIDC issuer, client id, and client-secret
*path* are node config (each coordinator needs them to serve the API);
anything authorization-shaped — role mappings, admin groups — is policy,
because two coordinators must not enforce different admin lists.

### Bootstrap policy is a CLI input, not a file section

Initial cluster policy is supplied to `coppice-cli cluster init --policy
policy.toml` (and adjusted later via `coppice-cli policy …`). The node config
file never seeds policy, not even on first boot: a file section that is live
exactly once and ignored forever after trains operators to edit dead
configuration. The CLI is also where human-friendly forms are converted to
the replicated fixed-point representations (half-life → Q0.64 λ, quota rate
→ stock; ADR 0019), keeping transcendental math out of every replica.

### One TOML file per process

- **Format: TOML.** The config is small, shallow, and comment-hungry;
  TOML's serde integration is first-class; and unlike YAML there are no
  type-coercion surprises (`no` is a string, not a boolean).
- One file per binary — `coordinator.toml`, `agent.toml` — passed as
  `--config <path>`. Same conventions, different schemas.
- **Unknown keys are startup errors** (`deny_unknown_fields`): a typo'd knob
  fail-stops with the offending key named, never silently defaults. This
  extends ADR 0016's fail-stop posture from data directories to
  configuration.
- **Humane scalar forms**, parsed at load into the internal types:
  durations as strings (`"1500ms"`, `"24h"`), sizes as `"512MiB"`.
  Raw-integer durations are rejected — an unlabelled `1500` is a bug
  generator.
- **Secrets by path reference only.** Private keys, client secrets, and
  enrollment tokens live in their own files; the config holds paths. The
  config file itself therefore contains no secret material and can be
  world-readable, checked into config management, diffed in PRs, and
  included verbatim in support bundles.
- **`node_id` is in the file** and is cross-checked against the data
  directory's stamped identity at startup — this is the "volume attached to
  the wrong instance" check ADR 0016 requires.

### Precedence and overrides

`CLI flags > file > built-in defaults`, and the flag set stays deliberately
tiny: `--config`, the ADR 0016 intent flags (`--bootstrap`, `--join`), and
nothing else without an ADR-worthy reason. There is **no environment-variable
layer** in v1: three override mechanisms is the point at which "why is it
using that port" stops having a checkable answer. The fully-resolved
effective configuration is logged at startup (it contains no secrets by
construction).

### No hot reload

Configuration is immutable for the life of the process; changes require a
restart. Partial reload — some subsystems rebound, some not — is an
operational trap, and the system is explicitly designed so coordinator
restarts are cheap (rolling restart with learner catch-up, ADR 0016). If a
carve-out ever proves necessary (log level via SIGHUP is the plausible
candidate), it will be a documented exception, not a general mechanism.

### Raft timing stays in the file

Considered removing it (openraft's defaults are sane, and mistuned elections
are a classic self-inflicted outage). Kept, with defaults and a
"you almost certainly should not touch this" comment: operators genuinely do
need to tune election timeouts on unusual networks, per-node variation is
safe by Raft's design, and a visible knob with a warning beats a magic
constant found by reading source.

### Implementation shape

`toml` (and a duration/size parsing helper such as `humantime-serde`) pinned
in the workspace; a plain-serde `config.rs` per binary crate. No shared
`coppice-config` crate until the coordinator and agent schemas actually
overlap enough to earn it; pure value-parsing helpers can live in
`coppice-core` (which stays I/O-free — file reading belongs to the
binaries).

## Consequences

- The litmus test gives every future knob a home before it is written, and
  reviewers a one-line question to ask of any PR that adds one. The failure
  mode it exists to prevent — policy drifting into per-node files —
  is the config-system equivalent of the float-in-replicated-state bug
  ADR 0019 closed.
- Operators get one greppable, commentable, secret-free file per process,
  safe to manage with any config-management tool, plus a CLI for everything
  cluster-wide. The corresponding cost: two places to look, so the living
  doc ([../operations/configuration.md](../operations/configuration.md))
  must always say which mechanism owns which knob, and the file carries a
  pointer comment for policy-shaped settings.
- `deny_unknown_fields` means renaming a config key is a breaking change for
  operators; renames need deprecation aliases and release notes.
- No env-var layer means container deployments template the file (or mount
  it) rather than injecting `COPPICE_*` variables. Accepted; revisit only
  with concrete demand.
- Fail-stop on unknown keys plus no hot reload makes configuration errors
  loud and early rather than latent — consistent with how the rest of the
  system treats ambiguity (ADR 0016).
