import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import { authNavigation, bootstrapAuth, resetAuthForTests } from '@/auth/oidc'
import { ApiError } from './client'
import { createQueryClient } from './query-client'

/**
 * The centralized 401 handling: any `Unauthenticated` reaching the query
 * layer — from a read or a write — restarts the login with the route the
 * user was on, and nothing else does.
 */

const ISSUER = 'https://idp.example'

function json(body: unknown) {
  return new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } })
}

function stubFetch(mode: 'oidc' | 'open') {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const url = String(input)
      if (url.startsWith('/api/v1/auth/config')) {
        return Promise.resolve(
          json(
            mode === 'open'
              ? { mode: 'open' }
              : { mode: 'oidc', issuer: ISSUER, client_id: 'coppice-web', audience: 'coppice-api' },
          ),
        )
      }
      if (url.startsWith(`${ISSUER}/.well-known/openid-configuration`)) {
        return Promise.resolve(
          json({
            issuer: ISSUER,
            authorization_endpoint: `${ISSUER}/authorize`,
            token_endpoint: `${ISSUER}/token`,
            scopes_supported: ['openid'],
            code_challenge_methods_supported: ['S256'],
          }),
        )
      }
      return Promise.reject(new Error(`unexpected fetch: ${url}`))
    }),
  )
}

let assign: Mock<(url: string) => void>

beforeEach(() => {
  resetAuthForTests()
  sessionStorage.clear()
  window.history.replaceState({}, '', '/jobs?phase=Running')
  assign = vi.fn((_url: string) => {})
  authNavigation.assign = assign
})

afterEach(() => {
  vi.unstubAllGlobals()
  resetAuthForTests()
})

async function failWith(error: unknown) {
  const client = createQueryClient()
  await client
    .fetchQuery({ queryKey: ['probe'], queryFn: () => Promise.reject(error), retry: false })
    .catch(() => {})
  return client
}

describe('createQueryClient', () => {
  it('redirects to login on a 401, preserving the attempted route', async () => {
    stubFetch('oidc')
    await bootstrapAuth()

    await failWith(new ApiError('Unauthenticated', 'missing bearer token'))
    await vi.waitFor(() => expect(assign).toHaveBeenCalledTimes(1))

    expect(String(assign.mock.calls[0]![0])).toContain(`${ISSUER}/authorize?`)
    const pending = JSON.parse(sessionStorage.getItem('coppice-auth-pending')!) as {
      returnTo: string
    }
    expect(pending.returnTo).toBe('/jobs?phase=Running')
  })

  it('does not redirect in open mode', async () => {
    stubFetch('open')
    await bootstrapAuth()

    await failWith(new ApiError('Unauthenticated', 'should not happen in open mode'))
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(assign).not.toHaveBeenCalled()
  })

  it('leaves other errors alone', async () => {
    stubFetch('oidc')
    await bootstrapAuth()

    await failWith(new ApiError('NotFound', 'no such job'))
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(assign).not.toHaveBeenCalled()
  })
})
