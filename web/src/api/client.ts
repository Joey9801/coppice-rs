import type {
  AttemptId,
  ClusterOverview,
  ConfigureQuotaEntityInput,
  CoordinatorId,
  CoordinatorStatus,
  JobDetail,
  JobId,
  JobList,
  JobUsage,
  ListJobsFilter,
  LogChunk,
  NodeDetail,
  NodeHistoryEntry,
  NodeId,
  NodeSummary,
  NodeUtilization,
  QueueStats,
  QuotaEntityDetail,
  QuotaEntityId,
  QuotaEntityNode,
  Session,
  TimelineEvent,
} from './types'

/**
 * The full client surface of the coordinator API, one method per (future)
 * endpoint. Today the only implementation is the mock (`./mock`); the real
 * HTTP/JSON client will implement this same interface against
 * `/api/v1/...` on the coordinator's client listener.
 *
 * Rules:
 * - UI code never calls a client directly — it uses the hooks in
 *   `./queries.ts`, which are the only consumer of this interface.
 * - To wire a real endpoint: implement the method in the real client and
 *   flip its entry in the delegation table in `./index.ts`. Nothing else
 *   (hooks, components) changes.
 * - The real client owns wire mapping (proto3 JSON: camelCase fields,
 *   64-bit ints as strings, typed-string ids) and auth headers; the
 *   methods here speak the plain TS types in `./types.ts`.
 */
export interface CoppiceApi {
  // Session / auth
  getSession(): Promise<Session>

  // Cluster overview
  //
  // `recentEvents` is a bounded most-recent-N window, NOT an event stream:
  // a large cluster can emit far more events than a browser can render, so
  // the server owns windowing/coalescing. If a live feed is ever wanted,
  // it must be a server-throttled subscription (ADR 0008 cursors), still
  // delivering bounded batches — never the raw firehose.
  getClusterOverview(): Promise<ClusterOverview>
  getQueueStats(): Promise<QueueStats>

  // Jobs
  listJobs(filter: ListJobsFilter): Promise<JobList>
  getJob(id: JobId): Promise<JobDetail>
  getJobTimeline(id: JobId): Promise<TimelineEvent[]>
  /** Usage samples for one attempt; null/omitted = current (else latest). */
  getJobUsage(id: JobId, attempt?: AttemptId | null): Promise<JobUsage>
  getJobLogs(id: JobId, cursor: string | null): Promise<LogChunk>

  // Nodes
  listNodes(): Promise<NodeSummary[]>
  getNode(id: NodeId): Promise<NodeDetail>
  getNodeUtilization(id: NodeId): Promise<NodeUtilization>
  getNodeHistory(id: NodeId): Promise<NodeHistoryEntry[]>
  getNodeLogs(id: NodeId, cursor: string | null): Promise<LogChunk>

  // Coordinators
  getCoordinatorStatus(): Promise<CoordinatorStatus>
  getCoordinatorLogs(id: CoordinatorId, cursor: string | null): Promise<LogChunk>

  // Quota entities
  listQuotaEntities(): Promise<QuotaEntityNode[]>
  getQuotaEntity(id: QuotaEntityId): Promise<QuotaEntityDetail>
  /**
   * Proposes `ConfigureQuotaEntity` (upsert; create when `input.entity` is
   * null). Requires an `admin` role binding covering the entity (ADR 0023) —
   * rejections surface as `PermissionDenied`.
   */
  configureQuotaEntity(input: ConfigureQuotaEntityInput): Promise<QuotaEntityNode>
}

/** Error shape all clients throw; mirrors `coppice_api::ApiError` loosely. */
export type ApiErrorCode =
  'NotFound' | 'InvalidArgument' | 'PermissionDenied' | 'Unavailable' | 'Internal'

export class ApiError extends Error {
  readonly code: ApiErrorCode

  constructor(code: ApiErrorCode, message: string) {
    super(message)
    this.name = 'ApiError'
    this.code = code
  }
}
