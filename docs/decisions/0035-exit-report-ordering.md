# 35. Exit reports precede reaps; heartbeats claim unreported exits

- **Status:** Accepted
- **Date:** 2026-07-20
- **Amends:** [ADR 0009](0009-agent-restart-reconciliation.md) (the
  heartbeat running set becomes an accountability set), the session reap
  ordering in [docker-executor.md](../architecture/docker-executor.md) §5

## Context

A confirmed race misclassified naturally-exited containers as
`AgentError` losses and burned a retry. Two windows compose it, both
between "the container exits" and "the coordinator ingests the terminal
report":

1. **The drain window (~1–2 s, dominant).** `Session::handle_observed_exit`
   journaled the exit, then *awaited* the reap — which waits on the S6b
   telemetry drain barrier — and only then returned the terminal report.
   The drain regularly consumes most of its 2 s budget on a Colima
   daemon: dockerd itself is slow to close the `follow=true` logs HTTP
   stream after a container dies (nothing in coppice or bollard polls or
   delays; the `die` event, `stop`, and `inspect` all confirm the exit
   promptly while the log-follow response stays open ~1–2 s longer).
2. **The events window (~100s of ms on slow daemons).** The heartbeat's
   `observe()` can see the container exited *before* the docker events
   task has delivered the `die` event to the session at all. No
   agent-side report ordering can beat that heartbeat onto the stream.

In both windows the heartbeat's running set — previously a raw filter of
`observe()` to `Running` — omitted the allocation, and the coordinator's
reverse reconciliation ("`Running` attempts absent from the running set
are lost") committed `LostAttempt { AgentError }`. The real terminal
report arrived later and was ignored (already terminal). Measured on
`coppice dev --executor docker` (2 s heartbeat, ~2.3 s jobs): 4/5 first
attempts misclassified before the fix; 2/10 with window 1 alone closed;
0/30 with both closed.

## Decision

**Report before reap.** Exit paths never reap inline. The session queues
the reap (`pending_reaps`, drained by the live loop via
`take_pending_reaps`, mirroring the armed-watchdog pattern) and returns
the terminal report immediately — the exit is already durably journaled,
so the container is spent evidence and its removal can trail the report.
The runner hands queued reaps to a dedicated reaper task (bounded
concurrency — each reap can wait seconds on the drain barrier and hits
the daemon and the telemetry store, so a burst of exits must not fan out
into unbounded requests; queue overflow is dropped to the janitor), so
the drain barrier no longer delays reports, heartbeats, or command
processing (retry dispatch included). The janitor sweep remains the
backstop for reaps that never complete; recovery reaps queue the same
way behind the ObservedSet.

**Heartbeats claim unreported exits.** The heartbeat running set is the
agent's *accountability* set, not the raw runtime state: a container
observed `Exited` under a **journaled intent** whose exit is **not yet
journaled** is still claimed as running — its exit event is in flight
and the terminal report will follow within the cycle. Once the exit is
journaled (terminal report produced, and always queued on the stream
ahead of any later heartbeat), the claim drops. A genuinely lost
container is absent from `observe()` entirely and is never claimed, so
loss-detection latency is unchanged. Runtime-only recovery survivors
(exited, no intent record) are never claimed either: they are reported
terminal once in the registration ObservedSet, and claiming them would
draw a `StopJob` the stop path cannot resolve — a permanent loop. An
intent-holding survivor whose exit went unjournaled *is* claimed and
self-heals in one cycle (the stop lands `AlreadyExited`, journaling the
exit).

Ingestion needs no change: it is serial and `propose().await` completes
apply, so a terminal report sent before a heartbeat is applied before
that heartbeat is normalized; apply already treats a `LostAttempt` on a
terminal attempt as a benign no-op, backstopping any residual view lag.

## Alternatives considered

- **Coordinator-side debounce** (require two consecutive absent
  heartbeats before `LostAttempt`): also closes window 2, but doubles
  genuine-loss detection latency and adds leader-local suspicion state
  to ingestion. The accountability-set claim achieves the same coverage
  with neither cost, using evidence the agent already holds. Debounce
  remains available if some future interleaving surfaces that the agent
  cannot see.
- **Reordering alone**: insufficient — measured 2/10 misclassifications
  from window 2 on Colima, where events delivery is hundreds of ms.

## Consequences

- A stop/abort racing an unclaimed exit is unaffected: stop paths queue
  their reaps the same way and report terminal first.
- If the events stream stalls entirely, a claimed exited container keeps
  its attempt `Running` until the events resync (60 s backstop)
  synthesizes the exit — a hang bounded by resync, judged better than a
  spurious `AgentError`.
- The drain latency itself (dockerd's slow close of the log-follow
  stream) still delays reap/finalization by ~1–2 s per exit on slow
  daemons. Follow-up (separate change): race the already-prompt `die`
  signal into the log follower and finish with a one-shot `follow=false`
  catch-up fetch, collapsing the wait to a single fast GET.
