import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import {
  authNavigation,
  bootstrapAuth,
  completeLogin,
  getAccessToken,
  getAuthMode,
  requireLogin,
  resetAuthForTests,
} from './oidc'

/**
 * The PKCE flow with every network hop stubbed: the coordinator's
 * `/api/v1/auth/config`, the issuer's discovery document, and the token
 * endpoint. No IdP is involved, and the full-page redirect goes through the
 * `authNavigation` seam (jsdom does not implement navigation).
 */

const ISSUER = 'https://idp.example/realms/coppice'

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

/** Routes a stubbed `fetch` by URL prefix; unmatched URLs fail loudly. */
function routeFetch(routes: Record<string, () => Response>) {
  return vi.fn((input: RequestInfo | URL) => {
    const url = String(input)
    for (const [prefix, respond] of Object.entries(routes)) {
      if (url.startsWith(prefix)) return Promise.resolve(respond())
    }
    return Promise.reject(new Error(`unexpected fetch: ${url}`))
  })
}

const DISCOVERY = () =>
  json({
    issuer: ISSUER,
    authorization_endpoint: `${ISSUER}/protocol/openid-connect/auth`,
    token_endpoint: `${ISSUER}/protocol/openid-connect/token`,
    scopes_supported: ['openid', 'profile', 'email', 'offline_access'],
    code_challenge_methods_supported: ['S256'],
  })

const OIDC_CONFIG = () =>
  json({ mode: 'oidc', issuer: ISSUER, client_id: 'coppice-web', audience: 'coppice-api' })

let assign: Mock<(url: string) => void>

beforeEach(() => {
  resetAuthForTests()
  sessionStorage.clear()
  window.history.replaceState({}, '', '/')
  assign = vi.fn((_url: string) => {})
  authNavigation.assign = assign
})

afterEach(() => {
  vi.unstubAllGlobals()
  resetAuthForTests()
})

describe('bootstrapAuth', () => {
  it('reports open mode and never starts a login', async () => {
    vi.stubGlobal('fetch', routeFetch({ '/api/v1/auth/config': () => json({ mode: 'open' }) }))

    await bootstrapAuth()
    expect(getAuthMode()).toBe('open')

    requireLogin('/jobs')
    await Promise.resolve()
    expect(assign).not.toHaveBeenCalled()
    expect(getAccessToken()).toBeNull()
  })

  it('leaves the posture unknown when the coordinator cannot be reached', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('offline'))),
    )
    vi.spyOn(console, 'error').mockImplementation(() => {})

    await bootstrapAuth()
    expect(getAuthMode()).toBeNull()

    requireLogin('/jobs')
    await Promise.resolve()
    expect(assign).not.toHaveBeenCalled()
  })
})

describe('discovery', () => {
  it('refuses a discovery document issued for a different issuer', async () => {
    vi.stubGlobal(
      'fetch',
      routeFetch({
        '/api/v1/auth/config': OIDC_CONFIG,
        [`${ISSUER}/.well-known/openid-configuration`]: () =>
          json({
            // Same host, neighbouring realm: the endpoints below would mint a
            // token the cluster rejects, so the flow must stop here.
            issuer: 'https://idp.example/realms/other',
            authorization_endpoint: 'https://idp.example/realms/other/auth',
            token_endpoint: 'https://idp.example/realms/other/token',
            scopes_supported: ['openid'],
            code_challenge_methods_supported: ['S256'],
          }),
      }),
    )
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    consoleError.mockClear()
    await bootstrapAuth()

    requireLogin('/jobs')
    await vi.waitFor(() => expect(consoleError).toHaveBeenCalled())

    expect(assign).not.toHaveBeenCalled()
    expect(String(consoleError.mock.calls[0]![1])).toContain('realms/other')
    expect(sessionStorage.getItem('coppice-auth-pending')).toBeNull()
  })
})

describe('requireLogin', () => {
  it('redirects to the authorization endpoint with an S256 challenge and state', async () => {
    vi.stubGlobal(
      'fetch',
      routeFetch({
        '/api/v1/auth/config': OIDC_CONFIG,
        [`${ISSUER}/.well-known/openid-configuration`]: DISCOVERY,
      }),
    )
    await bootstrapAuth()

    requireLogin('/jobs?phase=Queued')
    await vi.waitFor(() => expect(assign).toHaveBeenCalledTimes(1))

    const url = new URL(assign.mock.calls[0]![0] as string)
    expect(url.origin + url.pathname).toBe(`${ISSUER}/protocol/openid-connect/auth`)
    expect(url.searchParams.get('response_type')).toBe('code')
    expect(url.searchParams.get('client_id')).toBe('coppice-web')
    expect(url.searchParams.get('code_challenge_method')).toBe('S256')
    expect(url.searchParams.get('code_challenge')).toMatch(/^[A-Za-z0-9_-]{43}$/)
    expect(url.searchParams.get('scope')).toBe('openid profile email offline_access')
    expect(url.searchParams.get('redirect_uri')).toBe(`${window.location.origin}/auth/callback`)

    // The attempted route and the PKCE secrets survive the redirect in
    // sessionStorage; the verifier is never the challenge itself.
    const pending = JSON.parse(sessionStorage.getItem('coppice-auth-pending')!) as {
      returnTo: string
      state: string
      verifier: string
    }
    expect(pending.returnTo).toBe('/jobs?phase=Queued')
    expect(pending.state).toBe(url.searchParams.get('state'))
    expect(pending.verifier).not.toBe(url.searchParams.get('code_challenge'))
  })

  it('defaults the return path to the current route', async () => {
    vi.stubGlobal(
      'fetch',
      routeFetch({
        '/api/v1/auth/config': OIDC_CONFIG,
        [`${ISSUER}/.well-known/openid-configuration`]: DISCOVERY,
      }),
    )
    await bootstrapAuth()
    window.history.replaceState({}, '', '/nodes/node-7')

    requireLogin()
    await vi.waitFor(() => expect(assign).toHaveBeenCalledTimes(1))

    const pending = JSON.parse(sessionStorage.getItem('coppice-auth-pending')!) as {
      returnTo: string
    }
    expect(pending.returnTo).toBe('/nodes/node-7')
  })

  it('does not start a second login while one is in flight', async () => {
    vi.stubGlobal(
      'fetch',
      routeFetch({
        '/api/v1/auth/config': OIDC_CONFIG,
        [`${ISSUER}/.well-known/openid-configuration`]: DISCOVERY,
      }),
    )
    await bootstrapAuth()

    requireLogin('/jobs')
    requireLogin('/nodes')
    await vi.waitFor(() => expect(assign).toHaveBeenCalledTimes(1))
    expect(assign).toHaveBeenCalledTimes(1)
  })
})

describe('completeLogin', () => {
  async function arriveAtCallback(tokenResponse: () => Response) {
    vi.stubGlobal(
      'fetch',
      routeFetch({
        '/api/v1/auth/config': OIDC_CONFIG,
        [`${ISSUER}/.well-known/openid-configuration`]: DISCOVERY,
        [`${ISSUER}/protocol/openid-connect/token`]: tokenResponse,
      }),
    )
    await bootstrapAuth()
    requireLogin('/entities/quota-root')
    await vi.waitFor(() => expect(assign).toHaveBeenCalledTimes(1))
    const state = new URL(assign.mock.calls[0]![0] as string).searchParams.get('state')!
    window.history.replaceState({}, '', `/auth/callback?code=abc123&state=${state}`)
    return state
  }

  it('exchanges the code and returns to the attempted route', async () => {
    await arriveAtCallback(() =>
      json({ access_token: 'at-1', refresh_token: 'rt-1', expires_in: 300, token_type: 'Bearer' }),
    )

    const returnTo = await completeLogin(window.location.search)

    expect(returnTo).toBe('/entities/quota-root')
    expect(getAccessToken()).toBe('at-1')
    // The exchange is a public-client POST carrying the verifier, no secret.
    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>
    const tokenCall = fetchMock.mock.calls.find((c) => String(c[0]).endsWith('/token'))!
    const init = tokenCall[1] as RequestInit
    const form = new URLSearchParams(init.body as string)
    expect(init.method).toBe('POST')
    expect(form.get('grant_type')).toBe('authorization_code')
    expect(form.get('code')).toBe('abc123')
    expect(form.get('client_id')).toBe('coppice-web')
    expect(form.get('code_verifier')).toBeTruthy()
    expect(form.get('client_secret')).toBeNull()
    // Tokens are memory-only: the transient PKCE entry is consumed, and
    // nothing at all is left in web storage afterwards.
    expect(sessionStorage.getItem('coppice-auth-pending')).toBeNull()
    expect(sessionStorage.length).toBe(0)
  })

  it('rejects a state that does not match this browser session', async () => {
    await arriveAtCallback(() => json({ access_token: 'at-1' }))
    window.history.replaceState({}, '', '/auth/callback?code=abc123&state=forged')

    await expect(completeLogin(window.location.search)).rejects.toThrow(/state mismatch/)
    expect(getAccessToken()).toBeNull()
  })

  it('surfaces an identity-provider error response', async () => {
    await arriveAtCallback(() => json({ access_token: 'at-1' }))
    window.history.replaceState(
      {},
      '',
      '/auth/callback?error=access_denied&error_description=Consent+refused',
    )

    await expect(completeLogin(window.location.search)).rejects.toThrow('Consent refused')
    expect(getAccessToken()).toBeNull()
  })

  it('surfaces a failed token exchange', async () => {
    await arriveAtCallback(() => json({ error: 'invalid_grant' }, 400))

    await expect(completeLogin(window.location.search)).rejects.toThrow('invalid_grant')
    expect(getAccessToken()).toBeNull()
  })
})
