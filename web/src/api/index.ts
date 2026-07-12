import type { CoppiceApi } from './client'
import { createMockClient } from './mock/mock-client'

/**
 * Client selection.
 *
 * Today everything is served by the mock. When real endpoints land, build
 * a `realClient` (fetch against `/api/v1/...`) and move methods over one
 * at a time, e.g.:
 *
 *   const real = createRealClient()
 *   export const api: CoppiceApi = { ...mock, listJobs: real.listJobs }
 *
 * until the mock has no callers left. Keep the mock compiling — it also
 * backs tests and `vite dev` without a running coordinator.
 */
const mock = createMockClient()

export const api: CoppiceApi = mock

export { ApiError } from './client'
export type { CoppiceApi } from './client'
