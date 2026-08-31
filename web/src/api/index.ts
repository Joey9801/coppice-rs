import type { CoppiceApi } from './client'
import { createMockClient } from './mock/mock-client'
import { createRealClient } from './real-client'

/**
 * Client selection.
 *
 * The delegation table below flips one `CoppiceApi` method at a time from
 * the mock to the real HTTP client as its coordinator endpoint lands (see
 * "How to replace a mock endpoint with a real one" in CLAUDE.md) — most
 * reads are real now; `getNodeUtilization`, `getNodeHistory`, `getNodeLogs`,
 * `getCoordinatorLogs` stay on the mock because their routes still answer
 * `501 UNIMPLEMENTED` server-side. Keep the mock compiling —
 * it also backs tests and `npm run dev` without a running coordinator.
 *
 * `VITE_COPPICE_MOCK` (a build-time Vite env flag, set via `.env` or the
 * shell — `VITE_COPPICE_MOCK=1 npm run dev`) forces the mock client for
 * every method, real endpoints included; see README.md. The direct
 * `import.meta.env.VITE_COPPICE_MOCK` reference (not a wrapped helper) is
 * what lets Vite statically eliminate the branch that isn't taken from a
 * production build, so the unused client's code is not bundled.
 */
const mock = createMockClient()

export const api: CoppiceApi = import.meta.env.VITE_COPPICE_MOCK
  ? mock
  : (() => {
      const real = createRealClient()
      return {
        ...mock,
        getSession: real.getSession,

        getClusterOverview: real.getClusterOverview,
        getQueueStats: real.getQueueStats,

        listJobs: real.listJobs,
        getJob: real.getJob,
        getJobTimeline: real.getJobTimeline,
        getJobUsage: real.getJobUsage,
        getJobLogs: real.getJobLogs,

        listNodes: real.listNodes,
        getNode: real.getNode,

        getCoordinatorStatus: real.getCoordinatorStatus,

        listQuotaEntities: real.listQuotaEntities,
        getQuotaEntity: real.getQuotaEntity,
        configureQuotaEntity: real.configureQuotaEntity,
      } satisfies CoppiceApi
    })()

export { ApiError } from './client'
export type { CoppiceApi } from './client'
