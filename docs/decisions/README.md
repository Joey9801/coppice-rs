# Architecture Decision Records

This directory holds Architecture Decision Records (ADRs): short documents that
capture a single significant decision, the context that forced it, and its
consequences.

## Why ADRs

The design documents elsewhere in `docs/` describe the system as it is *intended
to be right now*, and are edited freely as that intention changes. ADRs are
different: they are an append-only history of *why* the design is the way it is.
Once accepted, an ADR is not edited — if a decision is reversed, a new ADR is
written that supersedes the old one.

## Format

Each ADR is a file named `NNNN-short-title.md`, numbered sequentially. A record
has:

- **Status** — Proposed, Accepted, Superseded (by `NNNN`), or Deprecated.
- **Context** — the forces at play; what makes this a decision.
- **Decision** — what was chosen.
- **Consequences** — what becomes easier or harder as a result.

See [0001-record-architecture-decisions.md](0001-record-architecture-decisions.md)
for the first record. The list of decisions still to be made lives in
[../roadmap/open-decisions.md](../roadmap/open-decisions.md).
