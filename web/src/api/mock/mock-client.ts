import { ApiError } from '../client'
import type { CoppiceApi } from '../client'
import type {
  AttemptId,
  ConfigureQuotaEntityInput,
  CoordinatorId,
  JobId,
  ListJobsFilter,
  NodeId,
  QuotaEntityId,
} from '../types'
import { isMockInvalid, isMockNotFound, MockWorld } from './world'

/**
 * The mock `CoppiceApi`, backed by a singleton `MockWorld`.
 *
 * Each call:
 *  1. advances the world lazily to the current wall clock (no timers, which
 *     keeps tests deterministic — nothing runs unless a method is called),
 *  2. awaits a small artificial latency, and
 *  3. returns freshly built view objects (never internal mutable state).
 *
 * Unknown ids surface as `ApiError('NotFound', …)`.
 */
export function createMockClient(): CoppiceApi {
  const world = new MockWorld(Date.now() * 1000)

  const settle = async <T>(build: () => T): Promise<T> => {
    world.advanceTo(Date.now() * 1000)
    await delay(20 + Math.random() * 60)
    try {
      return build()
    } catch (err) {
      if (isMockNotFound(err)) throw new ApiError('NotFound', (err as Error).message)
      if (isMockInvalid(err)) throw new ApiError('InvalidArgument', (err as Error).message)
      throw err
    }
  }

  return {
    getSession: async () => ({
      subject: 'demo',
      name: 'Demo User',
      email: null,
      roles: ['admin'],
    }),

    getClusterOverview: () => settle(() => world.buildClusterOverview()),
    getQueueStats: () => settle(() => world.buildQueueStats()),

    listJobs: (filter: ListJobsFilter) => settle(() => world.listJobs(filter)),
    getJob: (id: JobId) => settle(() => world.buildJobDetail(id)),
    getJobTimeline: (id: JobId) => settle(() => world.buildJobTimeline(id)),
    getJobUsage: (id: JobId, attempt?: AttemptId | null) =>
      settle(() => world.buildJobUsage(id, attempt ?? null)),
    getJobLogs: (id: JobId, cursor: string | null) => settle(() => world.buildJobLogs(id, cursor)),

    listNodes: () => settle(() => world.buildNodeSummaries()),
    getNode: (id: NodeId) => settle(() => world.buildNodeDetail(id)),
    getNodeUtilization: (id: NodeId) => settle(() => world.buildNodeUtilization(id)),
    getNodeHistory: (id: NodeId) => settle(() => world.buildNodeHistory(id)),
    getNodeLogs: (id: NodeId, cursor: string | null) =>
      settle(() => world.buildNodeLogs(id, cursor)),

    getCoordinatorStatus: () => settle(() => world.buildCoordinatorStatus()),
    getCoordinatorLogs: (id: CoordinatorId, cursor: string | null) =>
      settle(() => world.buildCoordinatorLogs(id, cursor)),

    // The demo session always holds `admin`, so the mock never rejects with
    // PermissionDenied — the real client will (ADR 0023 scoped bindings).
    listQuotaEntities: () => settle(() => world.listQuotaEntities()),
    getQuotaEntity: (id: QuotaEntityId) => settle(() => world.buildQuotaEntityDetail(id)),
    configureQuotaEntity: (input: ConfigureQuotaEntityInput) =>
      settle(() => world.configureQuotaEntity(input)),
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
