# 33. Aligned limit-breach outcome names and the disk flavour

- **Status:** Accepted
- **Date:** 2026-07-17
- **Amends:** [ADR 0013](0013-job-attempt-allocation-state-machines.md)
  (terminal-outcome table)

## Context

The Docker executor design
([docker-executor.md](../architecture/docker-executor.md) §4) adds a
third hard-limit kill: the disk enforcer terminates a container that
exceeds its disk request, exactly as the kernel's OOM killer enforces the
memory limit and the session enforces `max_runtime`. All three are the
same underlying event — the job used more of a hard-limited resource than
it asked for, and policy terminated it — but ADR 0013 names the existing
two incoherently (`OomKilled`, a mechanism; `MaxRuntimeExceeded`, a
resource), and the disk variant would have made it three naming schemes.

## Decision

The limit-breach family becomes three aligned flat variants of
`AttemptOutcome`, named for the resource whose limit was breached:

| Outcome | Classification | Retried by default? |
| --- | --- | --- |
| `MemoryLimitExceeded` (was `OomKilled`) | User error (policy kill) | No — deterministic recurrence |
| `RuntimeLimitExceeded` (was `MaxRuntimeExceeded`) | User error (policy kill) | No — deterministic recurrence |
| `DiskLimitExceeded` (new) | User error (policy kill) | No — deterministic recurrence |

Classification is identical across the three; the rest of ADR 0013's
outcome table is unchanged. This is a pre-release rename: the proto enum
values are renamed in place (same tags for the two existing variants,
one new tag for disk) and the descriptor breaking-gate snapshot is
regenerated, per the established pre-release-rename practice.

## Consequences

- Evidence-to-outcome mapping stays above the executor trait (ADR 0013):
  the executor reports `ExitCause::{OomKilled, DiskKilled}` evidence,
  `classify_exit` assigns `MemoryLimitExceeded` / `DiskLimitExceeded`,
  and the session assigns `RuntimeLimitExceeded` on max-runtime kills.
- Coordinator `Finalizing` resolution, journal encoding, and the web UI
  outcome strings carry the new names; no behavioral change anywhere —
  renames only, plus the new disk variant, which no code path emits
  until the Docker executor's disk enforcer lands.
