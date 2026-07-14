# Coppice Documentation

This folder holds Coppice's living design and architecture documentation. It is
meant to be iterated on continuously: as decisions are made and the
implementation evolves, update the relevant document rather than letting the
design drift away from the code.

The content here began life as a single high-level design dump
(`initial_design.md` at the repository root) and has been split into topical
documents so that each concern can grow independently.

## How this folder is organised

| Area | Purpose |
| --- | --- |
| [overview.md](overview.md) | Purpose, target scale, and core responsibilities. |
| [architecture/](architecture/) | Components, state model, high availability, versioning, and storage boundaries. |
| [lifecycle/](lifecycle/) | The job lifecycle state machine. |
| [scheduling/](scheduling/) | Scheduling model, the v1 scheduler algorithm, quotas and priorities, image-cache policy. |
| [protocols/](protocols/) | The agent–coordinator protocol. |
| [operations/](operations/) | Configuration, observability, failure handling, and security. |
| [testing/](testing/) | The [end-to-end test register](testing/end-to-end.md): behaviours only ever checked by driving a real cluster, pending an automated suite. |
| [roadmap/](roadmap/) | Initial scope, the design-decision register, and the [known open-issues register](roadmap/known-open-issues.md). |
| [decisions/](decisions/) | Architecture Decision Records (ADRs). |
| [design-principles.md](design-principles.md) | The principles that guide the whole design. |

## Conventions

- **Design docs** (everything outside `decisions/`) describe the current
  intended design. They are edited in place as the design changes.
- **Decision records** (`decisions/`) are immutable once accepted. To reverse a
  decision, add a new ADR that supersedes the old one.
- When a design doc reflects a decision worth recording permanently, capture the
  decision as an ADR and link to it.
