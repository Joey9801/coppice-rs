# Architecture gotchas

Design constraints learned from real bugs, not (yet) obvious from reading
the code cold.

## Shutdown must drain serving surfaces *after* consensus stops accepting new work, not before

Root cause of a real deadlock (fixed in the coordinator shutdown path):
graceful shutdown was draining serving surfaces (the API, agent-facing RPC)
*before* stopping consensus. If a handler was parked waiting on a consensus
write when the drain started, it could never complete, and the daemon
deadlocked instead of shutting down. There's also a boot-time-deadline trap
in the same area: a shutdown that starts before the daemon has finished
booting can hit the same ordering problem from the other direction.

When touching the coordinator's startup/shutdown sequencing, preserve the
invariant: stop accepting new consensus-dependent work *before* you start
draining the surfaces that generate it, not after.

## `StateMachine` holds no derived data

`crates/coppice-state`'s `StateMachine` struct is the thing that gets
snapshotted and replicated via Raft. Every field on it has a real
replication and snapshot-size cost. Derived or memoized values (e.g. "how
much capacity is currently in use on this node") should be computed in a
handler-scoped throwaway memo where they're needed, not stored as a field
on the state machine — even when a derived field would be locally
convenient, it doesn't belong there. This has come up more than once as an
easy-looking shortcut that gets rejected in review.

## SQL access is sqlx-only

Every SQL usage in the repo goes through sqlx with compile-time checked
queries, backed by the checked-in query cache (see
`scripts/sqlx-prepare.sh`). There's no rusqlite or other raw driver
anywhere in the tree — don't introduce one, even for something that looks
like a small, isolated use case.
