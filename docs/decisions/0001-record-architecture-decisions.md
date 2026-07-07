# 1. Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-07-07

## Context

Coppice is a large system with many consequential and interdependent design
decisions — the Raft library, the schema versioning strategy, the scheduler's
quota policy, and more (see
[../roadmap/open-decisions.md](../roadmap/open-decisions.md)). As the design
evolves, it is easy to lose track of *why* a choice was made, which leads to
re-litigating settled questions and accidentally violating the constraints that
motivated earlier decisions.

## Decision

We will keep Architecture Decision Records, as described by Michael Nygard, in
`docs/decisions/`. Each significant decision gets a numbered, append-only record
capturing its context, the decision itself, and its consequences. Records are
not edited after acceptance; a reversal is a new record that supersedes the old
one.

## Consequences

- The reasoning behind the architecture is preserved and discoverable.
- Contributors can see which questions are settled and which remain open.
- There is a small, ongoing cost to writing a record for each real decision —
  accepted deliberately, because the alternative is losing the rationale.
