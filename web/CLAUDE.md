# Coppice web UI — agent playbook

Observability/debugging UI for the Coppice cluster, currently running
entirely on **mock data** (`src/api/mock/`). It is a Vite + React 19 +
TypeScript SPA; see README.md for the human-facing overview and the
eventual serve-from-coordinator story.

## Commands

All run from `web/`:

- `npm run dev` — dev server; proxies `/api/v1` to a coordinator at
  `COPPICE_API_ADDR` (default `http://127.0.0.1:7070`). Set
  `VITE_COPPICE_MOCK=1` to force the mock client instead (no coordinator
  needed) — see README.md.
- `npm run typecheck` / `npm run lint` / `npm test` / `npm run build`
- `npm run format` — Prettier; run it before committing

## Architecture rules (do not break these)

1. **Components never fetch.** UI code reads data only via the hooks in
   `src/api/queries.ts`. Never import `src/api/index.ts`, `client.ts`, or
   anything under `src/api/mock/` from a component or route.
2. **One method per endpoint.** Every piece of server data is a method on
   the `CoppiceApi` interface (`src/api/client.ts`), implemented by the
   mock client, exposed through exactly one hook in `queries.ts` with its
   query key in `queryKeys`.
3. **Types live in `src/api/types.ts`** and mirror the Rust domain model
   (crates/coppice-core, coppice-state, coppice-scheduler,
   coppice-consensus) by name and semantics. Check the Rust source before
   inventing a field. Units: instants are `Date` (ISO 8601 strings on the
   wire), durations are whole seconds (`...Seconds`), costs are µCU
   (`...Ucu`), cpu is millicores. Format with `src/lib/format.ts` helpers —
   never hand-roll formatting.
4. **Routes are thin.** Files in `src/routes/` only wire params to a page
   component from `src/features/<area>/`. Page logic, tables, panels live
   in the feature directory.
5. **Semantic colors only.** Use the token utilities (`bg-background`,
   `text-muted-foreground`, `border-border`, `bg-card`, chart colors
   `var(--chart-1..5)`), never raw palette classes like `bg-zinc-800`,
   so light and dark themes both work. Reusable primitives live in
   `src/components/ui/` (vendored shadcn/ui style); shared app widgets
   (StatTile, CapacityHistory, StatePill, IdLink, LogViewer, …) in
   `src/components/`. Look there before writing a new one.

## How to add a new page

1. Create `src/features/<area>/` with a `<Name>Page.tsx` component.
2. Add a route file in `src/routes/` (TanStack Router file conventions:
   `foo.index.tsx` → `/foo`, `foo.$fooId.tsx` → `/foo/:fooId`) that calls
   `createFileRoute(...)` and renders the page component.
   `src/routeTree.gen.ts` regenerates automatically on `npm run dev`or
   `build`; it is committed — never edit it by hand.
3. Add the nav entry in `src/routes/__root.tsx` if it is a top-level area.
4. Data: add/extend a `CoppiceApi` method + mock implementation + hook
   (see below) rather than deriving complex state in the component.

## How to replace a mock endpoint with a real one

This is the intended path for future sessions, one endpoint at a time.
The server-side contract (route table, consistency params, error codes,
JSON conventions) is fixed by ADR 0031
(`docs/decisions/0031-http-api-surface.md`, as amended: every request
and response body is a handwritten serde DTO — nothing on the wire is
proto3 JSON); the axum router in `crates/coppice-api/src/http/` already
routes every endpoint below, with unimplemented ones answering
`501 UNIMPLEMENTED`:

1. Define the endpoint's response DTOs in
   `crates/coppice-api/src/http/dto.rs` (shape mirrors this repo's
   `src/api/types.ts` by name and semantics, spelled snake_case on the
   wire: `cpu_millis` keys, `"memory_limit_exceeded"` enum strings, bare
   typed-string ids, instants as ISO 8601 strings and durations as
   `_seconds` numbers, other integers as JSON numbers, `null`
   optionals, `[]` empties), add the projection in
   `crates/coppice-api/src/http/project.rs`, and swap its stub handler
   in `crates/coppice-api/src/http/routes.rs` for a real one backed by
   the coordinator.
2. Add the method to `src/api/real-client.ts` (`createRealClient()`),
   which implements `CoppiceApi` with `fetch` against `/api/v1/...`. The
   real client owns the wire mapping: snake_case keys and enum strings ↔
   the camelCase/PascalCase `types.ts` shapes (both directions — write
   bodies use the same DTO conventions), `Date` ↔ ISO 8601 strings, and
   error translation (`{ code, message }` → `ApiError`).
3. Flip that one method in the delegation table in `src/api/index.ts`
   (`{ ...mock, listJobs: real.listJobs }`).
4. Do not delete the mock implementation — it backs tests and offline
   `npm run dev` — and do not change `types.ts` to match wire quirks;
   the real client adapts wire → domain types.

## Mock world

`src/api/mock/world.ts` is a single seeded, deterministic simulation of a
cluster (nodes, quota tree, jobs with attempts/allocations, coordinators)
that advances on a ~1s tick. Invariants (funded ≤ requested, allocations
reference existing jobs/nodes, per-node funded totals fit capacity,
timelines are ordered) are enforced by tests in `src/api/mock/*.test.ts`
— keep them passing when you extend it. Add new mock data by extending
the world, not by hardcoding values in components.

## Known gaps (deliberate)

- **Logs are invented, and now partially reconciled.** The real log API
  landed (ADR 0034, `GetJobLogsResponse`/`LogEntry` in dto.rs) with a
  richer per-attempt shape (`attempt`, `stream`, per-attempt `sources`
  availability) than the UI's `LogChunk`/`LogEntry` (`t`, `level`,
  `target`, `message`). `real-client.ts`'s `getJobLogs` maps losslessly
  enough to render (`stream` → `level`, `attempt` → `target`) but drops
  `sources`; a follow-up should widen `LogChunk` to carry the real fields
  instead of approximating them at the boundary.
- **Job usage now mirrors the wire contract exactly.** `getJobUsage`
  returns `GetJobUsageResponse` (cumulative `UsagePoint` counters plus
  per-attempt `sources`), both from the mock and the real client — there
  is no more separate "UI's own shape" for this endpoint. A CPU
  instantaneous rate is derived by differencing consecutive samples at
  the point of use (`job-usage-section.tsx`), not served pre-computed.
- **Auth is real, and thin.** `src/auth/oidc.ts` runs the
  authorization-code + PKCE login (ADR 0022) against the issuer named by
  `GET /api/v1/auth/config`, holding tokens in memory only; the real client
  attaches the bearer token and `getSession()` maps the server's ADR 0023
  authority summary onto `Session`. A 401 anywhere restarts the login from
  `src/api/query-client.ts`. What the UI does _not_ have yet: any use of
  per-binding subtree scope (`canConfigureEntities` is all-or-nothing), and
  any role-aware chrome beyond that one check. `src/auth/auth-chrome.tsx` is
  the one place that decides how much auth chrome a posture gets: in
  `mode: "open"` there is no login and no identity at all, only the
  insecure-deployment badge. Under `VITE_COPPICE_MOCK` the mock client still
  answers "Demo User" with no coordinator attached.
