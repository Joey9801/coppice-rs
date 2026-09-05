import { useCallback } from 'react'
import { useLogController, type LogFetcher } from './log-controller'
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
 * accumulated pages refetch on the poll cadence. Cursors are owned entirely
 * by the infinite query, so the hook does not accept one — a caller-supplied
 * cursor would be silently ignored, not resumed from.
 */
export function useJobs(request: Omit<ListJobsRequest, 'cursor'>) {
  return useInfiniteQuery({
    queryKey: queryKeys.jobs(request),
    queryFn: ({ pageParam }) => api.listJobs({ ...request, cursor: pageParam }),
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

function useLogPager(source: 'job' | 'node' | 'coordinator', id: string, fetchPage: LogFetcher) {
  const client = useQueryClient()
  const fetch = useCallback<LogFetcher>(
    (cursor, request) =>
      client.fetchQuery({
        queryKey: [...queryKeys[`${source}Logs`](id), cursor, request],
        queryFn: () => fetchPage(cursor, request),
        staleTime: 0,
        gcTime: 0,
        retry: false,
      }),
    [client, source, id, fetchPage],
  )
  return useLogController(fetch)
}

export function useJobLogs(id: JobId) {
  const fetchPage = useCallback<LogFetcher>(
    (cursor, request) => api.getJobLogs(id, cursor, request),
    [id],
  )
  return useLogPager('job', id, fetchPage)
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

export function useNodeLogs(id: NodeId) {
  const fetchPage = useCallback<LogFetcher>(
    (cursor, request) => api.getNodeLogs(id, cursor, request),
    [id],
  )
  return useLogPager('node', id, fetchPage)
}

export function useCoordinatorStatus() {
  return useQuery({
    queryKey: queryKeys.coordinators,
    queryFn: () => api.getCoordinatorStatus(),
    ...LIVE,
  })
}

export function useCoordinatorLogs(id: CoordinatorId) {
  const fetchPage = useCallback<LogFetcher>(
    (cursor, request) => api.getCoordinatorLogs(id, cursor, request),
    [id],
  )
  return useLogPager('coordinator', id, fetchPage)
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
