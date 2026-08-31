import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from './client'
import { setTokensForTests } from '@/auth/oidc'
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

describe('getQuotaEntity', () => {
  it('maps a null over_quota_ratio/penalty (infinite on the wire) to Infinity', async () => {
    const entityWithInfiniteFields = {
      id: 'quota-00000000-0000-0000-0000-000000000001',
      name: 'zero-quota-team',
      parent: null,
      quota_ucu: 0,
      usage_ucu: 10,
      over_quota_ratio: null,
      penalty: null,
      created_at: '2026-01-01T00:00:00.000000Z',
      updated_at: '2026-01-01T00:00:00.000000Z',
      queued_count: 0,
      running_count: 1,
    }
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        entity: entityWithInfiniteFields,
        chain: [
          {
            id: 'quota-00000000-0000-0000-0000-000000000001',
            name: 'zero-quota-team',
            parent: null,
            quota_ucu: 0,
            usage_ucu: 10,
            over_quota_ratio: null,
            penalty: null,
          },
        ],
        children: [],
        stats: {
          by_state: {},
          oldest_queued_age_seconds: null,
          burn_rate_ucu_per_second: 0,
          charged_ucu_24h: null,
          usage_history: [],
        },
      }),
    )
    const client = createRealClient()
    const detail = await client.getQuotaEntity('quota-00000000-0000-0000-0000-000000000001')

    expect(detail.entity.overQuotaRatio).toBe(Infinity)
    expect(detail.entity.penalty).toBe(Infinity)
    expect(detail.chain[0]!.overQuotaRatio).toBe(Infinity)
    expect(detail.chain[0]!.penalty).toBe(Infinity)
  })
})

describe('getJob queue explainer', () => {
  it('maps a null penalty_chain entry and penalty_product (infinite on the wire) to Infinity', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        id: 'job-00000000-0000-0000-0000-000000000001',
        state: 'queued',
        spec: {
          image: 'busybox',
          command: [],
          entrypoint: null,
          requests: { cpu_millis: 100, memory_bytes: 1, disk_bytes: 1 },
          priority: 0,
          max_runtime_seconds: null,
          quota_entity: 'quota-00000000-0000-0000-0000-000000000001',
          retry: { max_retries: 0, retry_user_errors: false },
        },
        submitted_at: '2026-01-01T00:00:00.000000Z',
        state_since: '2026-01-01T00:00:00.000000Z',
        terminal_at: null,
        retries_used: 0,
        abort_requested: null,
        entity_chain: [],
        attempts: [],
        queue: {
          multiplier: 1,
          penalty_chain: [
            {
              entity: 'quota-00000000-0000-0000-0000-000000000001',
              name: 'zero-quota-team',
              usage_ucu: 10,
              quota_ucu: 0,
              over_quota_ratio: null,
              penalty: null,
            },
          ],
          penalty_product: null,
          age_seconds: 5,
        },
        accrual: null,
        cost: {
          rate_ucu_per_second: 0,
          rate_breakdown: { cpu: 0, memory: 0, disk: 0 },
          priority_multiplier: 1,
          unbounded_multiplier: 1,
          effective_rate_ucu_per_second: 0,
          charge_window_seconds: 0,
          charge_window_is_default: true,
          estimated_ucu: 0,
          charged_ucu: 0,
          refund_fraction: 0,
          actual_ucu: null,
          true_up: null,
        },
      }),
    )
    const client = createRealClient()
    const job = await client.getJob('job-00000000-0000-0000-0000-000000000001')

    expect(job.queue).not.toBeNull()
    expect(job.queue!.penaltyChain[0]!.overQuotaRatio).toBe(Infinity)
    expect(job.queue!.penaltyChain[0]!.penalty).toBe(Infinity)
    expect(job.queue!.penaltyProduct).toBe(Infinity)
    // An infinite penalty product means the job's effective score is the
    // lowest possible (multiplier / Infinity), not the finite fallback used
    // for a not-yet-computed (zero) penalty product.
    expect(job.queue!.score).toBe(0)
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

  it('maps a bodyless 401 by status so the re-login still fires', async () => {
    // A 401 from something in front of the coordinator (a reverse proxy, an
    // auth gateway) carries no `{ code, message }` body at all; falling back
    // to `Internal` would silently disable the centralized re-login.
    fetchMock.mockResolvedValueOnce(new Response(null, { status: 401 }))
    await expect(createRealClient().listNodes()).rejects.toMatchObject({
      code: 'Unauthenticated',
      message: 'request failed with status 401',
    })
  })

  it('maps a non-JSON 403 by status', async () => {
    fetchMock.mockResolvedValueOnce(
      new Response('<html>Forbidden</html>', {
        status: 403,
        headers: { 'Content-Type': 'text/html' },
      }),
    )
    await expect(createRealClient().listNodes()).rejects.toMatchObject({
      code: 'PermissionDenied',
    })
  })

  it('translates a network failure to Unavailable', async () => {
    fetchMock.mockRejectedValueOnce(new TypeError('fetch failed'))
    const client = createRealClient()
    await expect(client.listNodes()).rejects.toMatchObject({ code: 'Unavailable' })
  })
})

describe('getSession', () => {
  it('summarizes the ADR 0023 bindings into flat roles', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        principal: 'auth0|42',
        groups: ['batch-users'],
        auth_method: 'bearer',
        name: 'Ada Lovelace',
        email: 'ada@example.test',
        bindings: [
          { role: 'admin', scope: 'quota-00000000-0000-0000-0000-000000000001' },
          { role: 'submitter', scope: null },
          { role: 'submitter', scope: 'quota-00000000-0000-0000-0000-000000000002' },
        ],
        implicit_admin: false,
      }),
    )
    const session = await createRealClient().getSession()
    expect(fetchMock.mock.calls[0]![0]).toBe('/api/v1/session')
    expect(session).toEqual({
      subject: 'auth0|42',
      name: 'Ada Lovelace',
      email: 'ada@example.test',
      roles: ['submitter', 'admin'],
      implicitAdmin: false,
    })
  })

  it('honors implicit_admin for a principal with no bindings', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        principal: 'cert:ops-laptop',
        groups: [],
        auth_method: 'operator_cert',
        name: null,
        email: null,
        bindings: [],
        implicit_admin: true,
      }),
    )
    const session = await createRealClient().getSession()
    expect(session.roles).toEqual([])
    expect(session.implicitAdmin).toBe(true)
    // No name or email claim: the opaque principal is what gets rendered.
    expect(session.name).toBe('cert:ops-laptop')
  })

  it('falls back to the email claim for a display name', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        principal: 'auth0|43',
        groups: [],
        auth_method: 'bearer',
        name: null,
        email: 'grace@example.test',
        bindings: [{ role: 'operator', scope: null }],
        implicit_admin: false,
      }),
    )
    const session = await createRealClient().getSession()
    expect(session.name).toBe('grace@example.test')
  })
})

describe('bearer credentials', () => {
  afterEach(() => {
    setTokensForTests(null)
  })

  it('attaches the held access token to every request', async () => {
    setTokensForTests({
      accessToken: 'at-live',
      refreshToken: null,
      expiresAt: Date.now() + 300_000,
    })
    fetchMock.mockResolvedValueOnce(jsonResponse({ nodes: [] }))
    await createRealClient().listNodes()
    const headers = (fetchMock.mock.calls[0]![1] as RequestInit).headers as Record<string, string>
    expect(headers.Authorization).toBe('Bearer at-live')
  })

  it('sends no Authorization header when no token is held (open mode)', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({ nodes: [] }))
    await createRealClient().listNodes()
    const headers = (fetchMock.mock.calls[0]![1] as RequestInit).headers as Record<string, string>
    expect(headers.Authorization).toBeUndefined()
  })
})
