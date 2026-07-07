# Job Lifecycle

A representative job lifecycle is:

- Submitted.
- Accepted.
- Queued.
- Reserved, if future capacity is being earmarked.
- Assigned.
- Dispatching.
- Running.
- Completing.
- Succeeded or failed.
- Retrying, if policy allows.
- Cancelled, if requested before completion.

The exact lifecycle should be carefully defined so that every transition has a
clear owner.

## Transition ownership

- **User-facing commands** may request transitions, such as submit, cancel, or
  retry.
- **The scheduler** owns transitions from queued to reserved or assigned.
- **The coordinator** owns commitment of those transitions.
- **The agent** owns local execution and reports observed transitions such as
  started, exited, failed to pull image, or lost container.
- **The reconciler** resolves discrepancies between desired state and observed
  state.

## Status

Formalising the exact transition table — the legal edges and their owners — is
an [open design decision](../roadmap/open-decisions.md). The lifecycle enum in
`coppice-core` (`JobState`) mirrors the states listed above and is the code-side
anchor for this document.
