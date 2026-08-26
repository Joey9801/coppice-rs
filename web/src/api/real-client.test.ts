import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from './client'
import { createRealClient } from './real-client'

/**
 * Focused wire-mapping tests for the real client: fetch is stubbed so these
 * never touch a network, and each test asserts one boundary concern —
 * request shape (path/query), response mapping (snake_case → camelCase,
 * ISO strings → `Date`, lower_snake enum values → the PascalCase unions in
 * `types.ts`), or error translation. Full read-path coverage (every field
 * of every DTO) lives in the Rust `dto.rs`/`routes.rs` tests; these guard
 * the TS-side boundary only.
 */

function jsonResponse(body: unknown, init?: Partial<{ status: number; headers: HeadersInit }>) {
  return new Response(JSON.stringify(body), {
    status: init?.status ?? 200,
    headers: { 'Content-Type': 'application/json', ...init?.headers },
  })
}

let fetchMock: ReturnType<typeof vi.fn>

beforeEach(() => {
  fetchMock = vi.fn()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('getClusterOverview', () => {
  it('maps snake_case fields, phase keys, and ISO instants', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        cluster_id: 'cluster-00000000-0000-0000-0000-000000000001',
        queue: {
          depth: 3,
          drain_rate_per_minute: 1.5,
          arrival_rate_per_minute: null,
          oldest_queued_age_seconds: 42,
          by_state: { queued: 3, running: 1 },
          history: [
            {
              t: '2026-01-01T00:00:00.000000Z',
              depth: 3,
              drained_per_minute: 1,
              arrived_per_minute: 2,
            },
          ],
        },
        capacity: {
          nodes: { total: 2, schedulable: 2, lost: 0 },
          capacity: { cpu_millis: 1000, memory_bytes: 2, disk_bytes: 3 },
          allocated: { cpu_millis: 100, memory_bytes: 1, disk_bytes: 1 },
          used: { cpu_millis: 50, memory_bytes: 0, disk_bytes: 0 },
        },
        recent_events: { floor_index: 1, events: [] },
      }),
    )
    const client = createRealClient()
    const overview = await client.getClusterOverview()

    expect(fetchMock).toHaveBeenCalledWith('/api/v1/overview', expect.anything())
    expect(overview.clusterId).toBe('cluster-00000000-0000-0000-0000-000000000001')
    expect(overview.queue.depth).toBe(3)
    expect(overview.queue.drainRatePerMinute).toBe(1.5)
    expect(overview.queue.arrivalRatePerMinute).toBeNull()
    expect(overview.queue.byState.Queued).toBe(3)
    expect(overview.queue.byState.Running).toBe(1)
    expect(overview.queue.byState.Succeeded).toBe(0)
    expect(overview.queue.history[0]!.t).toBeInstanceOf(Date)
    expect(overview.queue.history[0]!.t.toISOString()).toBe('2026-01-01T00:00:00.000Z')
    expect(overview.capacity.capacity.cpuMillis).toBe(1000)
  })
})

describe('listJobs', () => {
  it('encodes the filter as URL-encoded JSON with wire casing', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({ jobs: [], next_cursor: null }))
    const client = createRealClient()
    await client.listJobs({
      filter: { phase: { in: ['Queued', 'Running'] } },
      limit: 50,
    })

    const url = fetchMock.mock.calls[0]![0] as string
    expect(url.startsWith('/api/v1/jobs?')).toBe(true)
    const params = new URLSearchParams(url.split('?')[1])
    expect(JSON.parse(params.get('filter')!)).toEqual({ phase: { in: ['queued', 'running'] } })
    expect(params.get('limit')).toBe('50')
  })

  it('reconstructs Attempting state from the flattened wire fields', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        jobs: [
          {
            id: 'job-00000000-0000-0000-0000-000000000001',
            state: 'attempting',
            attempt: 'attempt-00000000-0000-0000-0000-000000000001',
            image: 'busybox',
            quota_entity: 'quota-00000000-0000-0000-0000-000000000001',
            quota_entity_name: 'root',
            priority: 0,
            submitted_at: '2026-01-01T00:00:00.000000Z',
            terminal_at: null,
            node: 'node-00000000-0000-0000-0000-000000000001',
            attempt_state: 'running',
            funding_fraction: null,
            cost_ucu: 10,
            outcome: null,
          },
        ],
        next_cursor: null,
      }),
    )
    const client = createRealClient()
    const result = await client.listJobs({})
    expect(result.jobs[0]!.state).toEqual({
      kind: 'Attempting',
      attempt: 'attempt-00000000-0000-0000-0000-000000000001',
    })
    expect(result.jobs[0]!.attemptState).toBe('Running')
  })
})

describe('getJobUsage', () => {
  it('scopes the request to the attempt and passes cumulative counters through', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        samples: [
          {
            attempt: 'attempt-00000000-0000-0000-0000-000000000001',
            at: '2026-01-01T00:00:01.000000Z',
            cpu_usage_total_us: 500_000,
            cpu_throttled_total_us: 0,
            memory_used_bytes: 1024,
            memory_peak_bytes: 2048,
            disk_writable_bytes: 10,
            disk_image_bytes: 20,
            net_rx_bytes_total: 0,
            net_tx_bytes_total: 0,
            blkio_read_bytes_total: 0,
            blkio_write_bytes_total: 0,
          },
        ],
        sources: [
          {
            attempt: 'attempt-00000000-0000-0000-0000-000000000001',
            node: 'node-00000000-0000-0000-0000-000000000001',
            availability: 'available',
            truncated: false,
            earliest_available_at: null,
            reason: null,
          },
        ],
        next_cursor: null,
      }),
    )
    const client = createRealClient()
    const usage = await client.getJobUsage(
      'job-00000000-0000-0000-0000-000000000001',
      'attempt-00000000-0000-0000-0000-000000000001',
    )

    const url = fetchMock.mock.calls[0]![0] as string
    expect(url).toContain('/jobs/job-00000000-0000-0000-0000-000000000001/usage?')
    expect(url).toContain('attempt=attempt-00000000-0000-0000-0000-000000000001')
    expect(usage.samples[0]!.cpuUsageTotalUs).toBe(500_000)
    expect(usage.samples[0]!.at).toBeInstanceOf(Date)
    expect(usage.sources[0]!.availability).toBe('available')
  })
})

describe('error translation', () => {
  it('maps a wire error code to the matching ApiErrorCode', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({ code: 'NOT_FOUND', message: 'job not found' }, { status: 404 }),
    )
    const client = createRealClient()
    await expect(client.getJob('job-00000000-0000-0000-0000-000000000001')).rejects.toMatchObject({
      code: 'NotFound',
      message: 'job not found',
    })
  })

  it('carries the Coppice-Leader hint on a NotLeader response', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse(
        { code: 'NOT_LEADER', message: 'not the leader' },
        { status: 421, headers: { 'Coppice-Leader': 'coordinator-2:7070' } },
      ),
    )
    const client = createRealClient()
    try {
      await client.getQuotaEntity('quota-00000000-0000-0000-0000-000000000001')
      expect.unreachable('expected getQuotaEntity to reject')
    } catch (err) {
      expect(err).toBeInstanceOf(ApiError)
      expect((err as ApiError).code).toBe('NotLeader')
      expect((err as ApiError).leaderHint).toBe('coordinator-2:7070')
    }
  })

  it('translates a network failure to Unavailable', async () => {
    fetchMock.mockRejectedValueOnce(new TypeError('fetch failed'))
    const client = createRealClient()
    await expect(client.listNodes()).rejects.toMatchObject({ code: 'Unavailable' })
  })
})
