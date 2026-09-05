# Log viewer

All log panels use the same controller and view. Tail is selected initially;
entries read chronologically from top to bottom. Head requests the first page,
Tail requests the last page, and the page size is configurable (50–1000, default
200). The unit is a captured entry: job stdout/stderr chunks can contain multiple
lines and are preserved verbatim. Page byte and RPC budgets can return fewer
entries; a continuation token, not page length, determines whether more remain.

| Source | Real deployment | Demo mode |
| --- | --- | --- |
| Job | Best-effort agent stdout/stderr, directional history and live polling | Synthetic raw output with the same paging controls |
| Agent/node | Collection unavailable; Head, Tail, Play and verbosity disabled | Synthetic structured logs with inclusive thresholds |
| Coordinator | Collection unavailable; Head, Tail, Play and verbosity disabled | Synthetic structured logs with inclusive thresholds |

Timestamps toggle only UI prefixes; text already containing timestamps is
unchanged. Job output never has inferred log levels (stderr is a stream, not an
error severity). Structured verbosity uses inclusive thresholds: warn includes
warn/error, info includes info/warn/error, debug includes debug/info/warn/error.
Trace is below the offered thresholds. Filtering is local to fetched structured
entries, so empty filtered pages can still have more history.

Pause preserves the current entries and stops automatic fetching. An in-flight
poll is ignored after pausing. Manual boundary buttons remain available. A paused
running tail can have both older and newer output. Source/window changes ignore
stale responses; only one request per controller is active at a time, every poll
waits two seconds, and an error stops polling until Retry. Retry repeats the failed
direction. Terminal sources stop polling after outstanding forward pages drain.
The viewport follows only in Tail while playing and already at the bottom;
prepending retains the reader's scroll position. Maximize uses a native modal
dialog for keyboard focus containment, Escape and focus restoration.

## Contract

`GET /api/v1/jobs/{job}/logs` retains `order=asc|desc`, `limit`, `cursor` and the
inclusive `from` bound. The web adapter normalizes both directions to chronological
entries and preserves attempt, stream, per-entry truncation, and per-attempt
availability/retention metadata. Unreachable and expired sources are shown rather
than treated as an empty successful log. History is best effort, not a snapshot:
retention, delayed/backdated writes, agent failures and job retries can change
what is retrievable. New writes older than a consumed high-water mark are not
promised by live polling; revisit Head/Tail to refresh retained history.

The additive response fields are:

- `entries[].id`: attempt plus opaque segment/row identity supplied by the agent,
  stable across direction, overlapping pages and payload truncation. Two identical
  writes at the same timestamp remain distinct. Older agents without identities
  cannot drive this viewer and produce an explicit upgrade error.
- `live`: whether the replicated job state can still produce output.
- `resume_cursor`: ascending exclusive high-water mark even when `next_cursor`
  is null. It retains accumulated same-microsecond skips, allowing bounded polling
  without repeatedly downloading the final timestamp's entire run.

The controller maintains an independent descending history cursor and ascending
forward cursor. Tail initializes the forward walk from its newest entry's exact
microsecond timestamp, inclusively. That overlap is merged by identity. The `from`
filter stays attached to every forward continuation. On reaching the current end,
`resume_cursor` is retained for the next poll; `next_cursor` still means that a
page is truncated and immediate history remains. An empty initial tail probes
from the start, which also handles jobs that have not started their first attempt.
A new attempt is reachable after the previous attempt's high-water mark.

The protobuf identity addition is backward compatible. No node/coordinator
collection endpoint is invented, and real mode no longer silently delegates those
reads to the demo world. Production polling requires the updated agent and
coordinator. Existing CLI consumers can ignore the additive HTTP fields.
