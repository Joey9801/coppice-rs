# Agent playbook — coppice

Coppice is a distributed batch job scheduler (Rust, Cargo workspace) with a
Raft-replicated control plane. See [README.md](README.md) for the workspace
layout and [docs/](docs/) for architecture and design docs. This file is for
coding-agent conventions and hard-earned operational knowledge that isn't
already written down elsewhere in the repo.

A separate [web/AGENTS.md](web/AGENTS.md) covers the React/Vite UI in
`web/`. Before working under `web/`, read and follow `web/AGENTS.md` — it
is not loaded automatically for a session started at the repo root.

## Build, test, lint

Run from the repo root. These are baseline Rust checks — run whichever are
relevant to what you changed; you don't need the whole list for a docs-only
change, and if you touched `web/` you need the web checks in
`web/AGENTS.md` too, not these.

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
scripts/sqlx-prepare.sh --check   # only relevant if you added/changed a sqlx query
```

The toolchain is pinned by `rust-toolchain.toml`; rustup picks it up
automatically. No system `protoc` is needed — the proto schema corpus is
compiled in-process by `protox`.

Tests run under nextest, not `cargo test`, and are split into two CI jobs
matching `.config/nextest.toml`'s profiles:

```sh
cargo nextest run --workspace --all-features --locked --profile ci          # everything except the fleet suites
cargo nextest run --workspace --all-features --locked --profile ci-fleet-1  # one fleet shard (1-4); slow, spins up real Raft clusters
```

For a change confined to one or two crates, running nextest scoped to those
packages (`-p <crate>`) is normally enough; reach for the full `ci` profile
run before opening a PR that touches shared/core crates. The fleet suites
(`ci-fleet-1..4`) are sharded by `scripts/check-fleet-shards.sh` and are
usually only worth running locally if you touched consensus, membership, or
coordinator startup/shutdown — see
[docs/agent-notes/ci-flakes.md](docs/agent-notes/ci-flakes.md) before
concluding a fleet failure is real.

CI itself (`.github/workflows/ci.yml`) runs `fmt`, `clippy`, the `ci`
nextest profile, the four `ci-fleet-*` shards plus the shard-partition
check, `sqlx-prepare.sh --check`, and a separate `web` job (lint, format
check, test, build) — that's the authoritative list if this section drifts
out of date.

## Conventions specific to this repo

- **SQL always goes through sqlx**, using compile-time checked queries. Do
  not add rusqlite or any other raw driver. If you add or change a query,
  regenerate the query cache with `scripts/sqlx-prepare.sh` (or `cargo sqlx
  prepare`) and commit the updated cache — CI's `--check` step fails
  otherwise.
- **Don't add derived or memoized fields to the replicated `StateMachine`
  struct** (`crates/coppice-state`). It is the thing that gets snapshotted
  and replicated; derived data belongs in a handler-scoped throwaway memo,
  computed where it's needed, not stored on the state machine itself.
- **No back-compat shims.** The project is early-stage (see `README.md`
  status line) and has no external users yet. Don't add migration code,
  compatibility layers, or deprecated-but-kept fields — just change the
  thing.
- **Write an ADR (`docs/decisions/NNNN-*.md`) only for changes with real
  architecture, contract, or wire-format impact.** A localized change,
  perf fix, or internal refactor doesn't need one — follow the existing
  numbering and format in `docs/decisions/` when one is warranted.

## Process

- **Never merge a PR or push to `main` on your own initiative.** Open the
  PR, get CI green, and hand the merge command back to a human — this repo
  requires human review before anything lands on `main`, regardless of how
  routine the change looks.
- **Don't take a CI failure on an unrelated PR at face value.** It may be a
  known flake (see `docs/agent-notes/ci-flakes.md`) or a first-time one.
  Verify with a fresh `workflow_dispatch` run on a control branch off
  `main` before concluding it's real — a *rerun* of the same failed
  workflow run is not sufficient evidence either way, since reruns can
  fail independently of the underlying tree. If it turns out to be a new,
  unrelated flake, file a GitHub issue with the run IDs, the relevant
  pasted log lines, a cause hypothesis, and a fix sketch — don't just
  note it and move on.

## Deeper notes

`docs/agent-notes/` has more detail on recurring operational issues:

- [ci-flakes.md](docs/agent-notes/ci-flakes.md) — known flaky tests/areas,
  how to bisect them, and the Docker Hub pull-limit hazard in CI.
- [ci-performance.md](docs/agent-notes/ci-performance.md) — what made the
  fleet test suite fast, and debug-build crypto perf pitfalls.
- [architecture-gotchas.md](docs/agent-notes/architecture-gotchas.md) —
  postmortem-derived design constraints (e.g. shutdown ordering vs.
  consensus) worth knowing before touching those areas.
