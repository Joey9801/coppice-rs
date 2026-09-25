import { ApiError } from '../client'
import type { CoppiceApi } from '../client'
import type {
  AttemptId,
  ConfigureQuotaEntityInput,
  CoordinatorId,
  JobId,
  JobMetadata,
  ListJobsRequest,
  LogRequest,
  NodeId,
  QuotaEntityRef,
} from '../types'
import { isMockInvalid, isMockNotFound, isMockRejected, MockWorld } from './world'

/**
 * The mock `CoppiceApi`, backed by a singleton `MockWorld`.
 *
 * Each call:
 *  1. advances the world lazily to the current wall clock (no timers, which
 *     keeps tests deterministic — nothing runs unless a method is called),
 *  2. awaits a small artificial latency, and
 *  3. returns freshly built view objects (never internal mutable state).
 *
 * Unknown ids surface as `ApiError('NotFound', …)`; malformed input as
 * `InvalidArgument`; a well-formed write the state refuses (a sibling name
 * clash, an unknown parent) as `Rejected`, like the server's 409.
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
      if (isMockRejected(err)) throw new ApiError('Rejected', (err as Error).message)
      throw err
    }
  }

  return {
    getSession: async () => ({
      subject: 'demo',
      name: 'Demo User',
      email: null,
      roles: ['admin'],
      implicitAdmin: false,
    }),

    getClusterOverview: () => settle(() => world.buildClusterOverview()),
    getQueueStats: () => settle(() => world.buildQueueStats()),

    listJobs: (request: ListJobsRequest) => settle(() => world.listJobs(request)),
    getJob: (id: JobId) => settle(() => world.buildJobDetail(id)),
    getJobTimeline: (id: JobId) => settle(() => world.buildJobTimeline(id)),
    getJobUsage: (id: JobId, attempt?: AttemptId | null) =>
      settle(() => world.buildJobUsage(id, attempt ?? null)),
    getJobLogs: (id: JobId, cursor: string | null, request: LogRequest) =>
      settle(() => world.buildJobLogs(id, cursor, request)),
    replaceJobMetadata: (id: JobId, metadata: JobMetadata) =>
      settle(() => world.replaceJobMetadata(id, metadata)),
    updateJobMetadata: (id: JobId, patch: { set?: JobMetadata; unset?: string[] }) =>
      settle(() => world.updateJobMetadata(id, patch)),

    listNodes: () => settle(() => world.buildNodeSummaries()),
    getNode: (id: NodeId) => settle(() => world.buildNodeDetail(id)),
    getNodeUtilization: (id: NodeId) => settle(() => world.buildNodeUtilization(id)),
    getNodeLogs: (id: NodeId, cursor: string | null, request: LogRequest) =>
      settle(() => world.buildNodeLogs(id, cursor, request)),

    getCoordinatorStatus: () => settle(() => world.buildCoordinatorStatus()),
    getCoordinatorLogs: (id: CoordinatorId, cursor: string | null, request: LogRequest) =>
      settle(() => world.buildCoordinatorLogs(id, cursor, request)),

    // The demo session always holds `admin`, so the mock never rejects with
    // PermissionDenied — the real client will (ADR 0023 scoped bindings).
    listQuotaEntities: () => settle(() => world.listQuotaEntities()),
    getQuotaEntity: (ref: QuotaEntityRef) => settle(() => world.buildQuotaEntityDetail(ref)),
    configureQuotaEntity: (input: ConfigureQuotaEntityInput) =>
      settle(() => world.configureQuotaEntity(input)),
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
