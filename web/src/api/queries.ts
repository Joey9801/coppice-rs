import {
  keepPreviousData,
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query'
import { api } from './index'
import type {
  AttemptId,
  ConfigureQuotaEntityInput,
  CoordinatorId,
  JobId,
  ListJobsRequest,
  NodeId,
  QuotaEntityId,
} from './types'

/**
 * TanStack Query hooks — the ONLY way UI code reads data. One hook per
 * `CoppiceApi` method; query keys are centralized here so invalidation
 * and future streaming/subscription upgrades happen in one place.
 *
 * `LIVE` marks views that should track the (mock) world as it ticks;
 * when real endpoints land these become event-subscription driven
 * (ADR 0008 cursors) instead of polls, again only in this file.
 */
const LIVE = { refetchInterval: 2_000 } as const

export const queryKeys = {
  session: ['session'] as const,
  overview: ['overview'] as const,
  queueStats: ['queue-stats'] as const,
  // Keyed on the request MINUS its cursor: paging within one filter shares a
  // cache entry (the pages accumulate), and a filter change starts fresh.
  jobs: (request: Omit<ListJobsRequest, 'cursor'>) => ['jobs', request] as const,
  job: (id: JobId) => ['job', id] as const,
  jobTimeline: (id: JobId) => ['job', id, 'timeline'] as const,
  jobUsage: (id: JobId, attempt: AttemptId | null) =>
    ['job', id, 'usage', attempt ?? 'current'] as const,
  jobLogs: (id: JobId) => ['job', id, 'logs'] as const,
  nodes: ['nodes'] as const,
  node: (id: NodeId) => ['node', id] as const,
  nodeUtilization: (id: NodeId) => ['node', id, 'utilization'] as const,
  nodeHistory: (id: NodeId) => ['node', id, 'history'] as const,
  nodeLogs: (id: NodeId) => ['node', id, 'logs'] as const,
  coordinators: ['coordinators'] as const,
  coordinatorLogs: (id: CoordinatorId) => ['coordinators', id, 'logs'] as const,
  quotaEntities: ['quota-entities'] as const,
  quotaEntity: (id: QuotaEntityId) => ['quota-entity', id] as const,
}

export function useSession() {
  return useQuery({
    queryKey: queryKeys.session,
    queryFn: () => api.getSession(),
    staleTime: Infinity,
  })
}

export function useClusterOverview() {
  return useQuery({
    queryKey: queryKeys.overview,
    queryFn: () => api.getClusterOverview(),
    ...LIVE,
  })
}

export function useQueueStats() {
  return useQuery({
    queryKey: queryKeys.queueStats,
    queryFn: () => api.getQueueStats(),
    ...LIVE,
  })
}

/**
 * Keyset-paginated jobs (ListJobs v1). `useInfiniteQuery` accumulates pages;
 * `nextCursor` threads through as the next page's `cursor`, and a null
 * `nextCursor` (never a merely short page) ends pagination. Stays LIVE: the
 * accumulated pages refetch on the poll cadence. The `cursor` is dropped from
 * the request when keying so paging shares one cache entry per filter.
 */
export function useJobs(request: ListJobsRequest) {
  const { cursor: _cursor, ...keyable } = request
  return useInfiniteQuery({
    queryKey: queryKeys.jobs(keyable),
    queryFn: ({ pageParam }) => api.listJobs({ ...keyable, cursor: pageParam }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.nextCursor ?? undefined,
    placeholderData: keepPreviousData,
    ...LIVE,
  })
}

export function useJob(id: JobId) {
  return useQuery({
    queryKey: queryKeys.job(id),
    queryFn: () => api.getJob(id),
    ...LIVE,
  })
}

export function useJobTimeline(id: JobId) {
  return useQuery({
    queryKey: queryKeys.jobTimeline(id),
    queryFn: () => api.getJobTimeline(id),
    ...LIVE,
  })
}

export function useJobUsage(id: JobId, attempt: AttemptId | null = null) {
  return useQuery({
    queryKey: queryKeys.jobUsage(id, attempt),
    queryFn: () => api.getJobUsage(id, attempt),
    placeholderData: keepPreviousData,
    ...LIVE,
  })
}

export function useJobLogs(id: JobId) {
  return useQuery({
    queryKey: queryKeys.jobLogs(id),
    queryFn: () => api.getJobLogs(id, null),
    ...LIVE,
  })
}

export function useNodes() {
  return useQuery({
    queryKey: queryKeys.nodes,
    queryFn: () => api.listNodes(),
    ...LIVE,
  })
}

export function useNode(id: NodeId) {
  return useQuery({
    queryKey: queryKeys.node(id),
    queryFn: () => api.getNode(id),
    ...LIVE,
  })
}

export function useNodeUtilization(id: NodeId) {
  return useQuery({
    queryKey: queryKeys.nodeUtilization(id),
    queryFn: () => api.getNodeUtilization(id),
    ...LIVE,
  })
}

export function useNodeHistory(id: NodeId) {
  return useQuery({
    queryKey: queryKeys.nodeHistory(id),
    queryFn: () => api.getNodeHistory(id),
    ...LIVE,
  })
}

export function useNodeLogs(id: NodeId) {
  return useQuery({
    queryKey: queryKeys.nodeLogs(id),
    queryFn: () => api.getNodeLogs(id, null),
    ...LIVE,
  })
}

export function useCoordinatorStatus() {
  return useQuery({
    queryKey: queryKeys.coordinators,
    queryFn: () => api.getCoordinatorStatus(),
    ...LIVE,
  })
}

export function useCoordinatorLogs(id: CoordinatorId) {
  return useQuery({
    queryKey: queryKeys.coordinatorLogs(id),
    queryFn: () => api.getCoordinatorLogs(id, null),
    ...LIVE,
  })
}

export function useQuotaEntities() {
  return useQuery({
    queryKey: queryKeys.quotaEntities,
    queryFn: () => api.listQuotaEntities(),
    placeholderData: keepPreviousData,
    ...LIVE,
  })
}

export function useQuotaEntity(id: QuotaEntityId) {
  return useQuery({
    queryKey: queryKeys.quotaEntity(id),
    queryFn: () => api.getQuotaEntity(id),
    ...LIVE,
  })
}

/**
 * Proposes `ConfigureQuotaEntity`. On success, everything derived from the
 * tree (the list, per-entity details, job rows carrying entity names) is
 * invalidated; the 2s LIVE polls pick the rest up.
 */
export function useConfigureQuotaEntity() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: ConfigureQuotaEntityInput) => api.configureQuotaEntity(input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.quotaEntities })
      void queryClient.invalidateQueries({ queryKey: ['quota-entity'] })
    },
  })
}
