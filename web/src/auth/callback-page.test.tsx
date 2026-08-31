import { QueryClientProvider, useQuery } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import { ApiError } from '@/api/client'
import { queryKeys } from '@/api/queries'
import { createQueryClient } from '@/api/query-client'
import { AuthCallbackPage } from './callback-page'
import {
  authNavigation,
  bootstrapAuth,
  getAccessToken,
  requireLogin,
  resetAuthForTests,
} from './oidc'

/**
 * The callback route, from the arriving redirect to the returned-to page.
 *
 * The regression this pins: the app shell mounts before the callback route
 * finishes, so its session query has already run *without* a token, failed
 * 401, and — `staleTime: Infinity`, 401s never retried — would sit on that
 * error forever. Completing the exchange has to invalidate what the
 * unauthenticated app fetched, or the user lands back on a route rendering a
 * signed-out shell with a perfectly good token in hand.
 */

const { replace } = vi.hoisted(() => ({ replace: vi.fn<(to: string) => void>() }))

vi.mock('@tanstack/react-router', () => ({
  useRouter: () => ({ history: { replace } }),
}))

const ISSUER = 'https://idp.example/realms/coppice'

function json(body: unknown) {
  return new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } })
}

/** Stands in for the shell's `useSession`: 401 without a token, name with one. */
let sessionFetches = 0
function SessionProbe() {
  const { data, isError } = useQuery({
    queryKey: queryKeys.session,
    queryFn: () => {
      sessionFetches += 1
      return getAccessToken() === null
        ? Promise.reject(new ApiError('Unauthenticated', 'no bearer token'))
        : Promise.resolve({ name: 'Ada Lovelace' })
    },
    staleTime: Infinity,
  })
  if (isError) return <span>signed out</span>
  return <span>{data ? data.name : 'loading'}</span>
}

let assign: Mock<(url: string) => void>

beforeEach(() => {
  resetAuthForTests()
  sessionStorage.clear()
  sessionFetches = 0
  replace.mockClear()
  window.history.replaceState({}, '', '/jobs')
  assign = vi.fn((_url: string) => {})
  authNavigation.assign = assign

  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const url = String(input)
      if (url.startsWith('/api/v1/auth/config')) {
        return Promise.resolve(
          json({ mode: 'oidc', issuer: ISSUER, client_id: 'coppice-web', audience: 'coppice-api' }),
        )
      }
      if (url.startsWith(`${ISSUER}/.well-known/openid-configuration`)) {
        return Promise.resolve(
          json({
            issuer: ISSUER,
            authorization_endpoint: `${ISSUER}/protocol/openid-connect/auth`,
            token_endpoint: `${ISSUER}/protocol/openid-connect/token`,
            scopes_supported: ['openid'],
            code_challenge_methods_supported: ['S256'],
          }),
        )
      }
      if (url.startsWith(`${ISSUER}/protocol/openid-connect/token`)) {
        return Promise.resolve(json({ access_token: 'at-1', expires_in: 300 }))
      }
      return Promise.reject(new Error(`unexpected fetch: ${url}`))
    }),
  )
})

afterEach(() => {
  vi.unstubAllGlobals()
  resetAuthForTests()
})

describe('AuthCallbackPage', () => {
  it('refetches the session the pre-login 401 poisoned, then returns to the route', async () => {
    await bootstrapAuth()
    requireLogin('/jobs')
    await vi.waitFor(() => expect(assign).toHaveBeenCalledTimes(1))
    const state = new URL(assign.mock.calls[0]![0]).searchParams.get('state')!

    // The shell renders first, unauthenticated: its session query 401s and,
    // left alone, would never run again.
    const client = createQueryClient()
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <SessionProbe />
      </QueryClientProvider>,
    )
    expect(await screen.findByText('signed out')).toBeInTheDocument()
    expect(sessionFetches).toBe(1)

    // …and only then does the browser come back from the IdP.
    window.history.replaceState({}, '', `/auth/callback?code=abc123&state=${state}`)
    rerender(
      <QueryClientProvider client={client}>
        <SessionProbe />
        <AuthCallbackPage />
      </QueryClientProvider>,
    )

    expect(await screen.findByText('Ada Lovelace')).toBeInTheDocument()
    expect(sessionFetches).toBe(2)
    expect(replace).toHaveBeenCalledWith('/jobs')
  })

  it('reports a failed exchange instead of navigating', async () => {
    await bootstrapAuth()
    window.history.replaceState({}, '', '/auth/callback?error=access_denied')

    render(
      <QueryClientProvider client={createQueryClient()}>
        <AuthCallbackPage />
      </QueryClientProvider>,
    )

    expect(await screen.findByText('Sign-in failed')).toBeInTheDocument()
    expect(replace).not.toHaveBeenCalled()
  })
})
