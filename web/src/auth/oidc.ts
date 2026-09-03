/**
 * OIDC authorization-code + PKCE login (ADR 0022).
 *
 * This module is the whole browser side of authentication: it bootstraps the
 * deployment's auth posture, runs the redirect flow, holds the tokens, and
 * hands the access token to the real API client. It deliberately owns its
 * own `fetch` calls rather than going through `CoppiceApi`:
 * `/api/v1/auth/config` is the *pre-authentication* bootstrap (it decides
 * whether there is a session to fetch at all) and the discovery/token
 * endpoints live on the IdP, not the coordinator. Everything that is app
 * data — `/api/v1/session` included — stays a `CoppiceApi` method behind a
 * hook, per `web/CLAUDE.md`.
 *
 * Design choices, and why:
 *
 * - **Hand-rolled PKCE over a library.** The flow needs three things a
 *   browser already provides: a random verifier, its S256 challenge
 *   (`crypto.subtle.digest`), and a form-encoded POST to the token
 *   endpoint. That is ~60 lines here versus a dependency; nothing in
 *   `oauth4webapi` would be doing more.
 * - **Tokens live in memory only.** `tokens` below is a module variable and
 *   never touches `localStorage`: a reload re-runs the (silent, if the IdP
 *   session is live) redirect instead of leaving a bearer token readable by
 *   any script on the origin. `sessionStorage` holds only the transient
 *   PKCE verifier / state / return path, which must survive the redirect
 *   and are cleared the moment the callback consumes them.
 * - **Refresh is proactive.** When the IdP grants a refresh token the
 *   expiry schedules a `setTimeout` one minute early, so a long-lived tab
 *   never shows the user a 401. Without a refresh token, expiry simply
 *   surfaces as a 401 and the centralized handler re-runs the flow.
 * - **Open mode is not a degraded OIDC mode.** `mode: "open"` means the
 *   deployment has no authentication at all; there is nothing to log into,
 *   so every login path is a no-op and the UI shows a no-auth warning instead
 *   of auth chrome.
 */

/** Where the IdP redirects back to; must match the client's registered URI. */
export const CALLBACK_PATH = '/auth/callback'

/** `sessionStorage` key for the in-flight authorization request. */
const PENDING_KEY = 'coppice-auth-pending'

/** Refresh this long before the access token actually expires. */
const REFRESH_LEAD_MS = 60_000

export type AuthMode = 'open' | 'oidc'

export interface OidcSettings {
  issuer: string
  clientId: string
  /** The audience the cluster requires; already defaulted to `clientId`. */
  audience: string | null
}

export type AuthConfig = { mode: 'open' } | ({ mode: 'oidc' } & OidcSettings)

/** The subset of the discovery document this flow uses. */
interface DiscoveryMetadata {
  authorizationEndpoint: string
  tokenEndpoint: string
  scopesSupported: string[]
}

interface Tokens {
  accessToken: string
  refreshToken: string | null
  /** Epoch milliseconds. */
  expiresAt: number
}

interface PendingLogin {
  verifier: string
  state: string
  returnTo: string
}

/**
 * Navigation seam. Full-page navigation is not implemented in jsdom, so the
 * redirect goes through this indirection and tests replace `assign`.
 */
export const authNavigation = {
  assign(url: string) {
    window.location.assign(url)
  },
}

/** `null` until `bootstrapAuth()` resolves, or if that fetch failed. */
let config: AuthConfig | null = null
let metadata: DiscoveryMetadata | null = null
let tokens: Tokens | null = null
let refreshTimer: ReturnType<typeof setTimeout> | null = null
let loginInFlight = false

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

/**
 * Reads `GET /api/v1/auth/config` (public, pre-auth) once at startup.
 * Resolves to `null` if the coordinator could not be reached — an unknown
 * posture, in which the UI neither claims to be insecure nor tries to log
 * in; the failing API calls behind it report the real problem.
 *
 * Under `VITE_COPPICE_MOCK` there is no coordinator to ask: the mock client
 * answers `getSession()` on its own, so auth is skipped entirely.
 */
export async function bootstrapAuth(): Promise<AuthConfig | null> {
  if (import.meta.env.VITE_COPPICE_MOCK) {
    config = null
    return null
  }
  try {
    const response = await fetch('/api/v1/auth/config', {
      headers: { Accept: 'application/json' },
    })
    if (!response.ok) throw new Error(`auth config request failed: ${response.status}`)
    const body = (await response.json()) as {
      mode: string
      issuer?: string
      client_id?: string
      audience?: string
    }
    config =
      body.mode === 'oidc'
        ? {
            mode: 'oidc',
            issuer: body.issuer ?? '',
            clientId: body.client_id ?? '',
            audience: body.audience ?? null,
          }
        : { mode: 'open' }
  } catch (err) {
    console.error('could not read the deployment auth configuration', err)
    config = null
  }
  return config
}

/** The deployment's auth posture; `null` until bootstrap resolves. */
export function getAuthConfig(): AuthConfig | null {
  return config
}

export function getAuthMode(): AuthMode | null {
  return config?.mode ?? null
}

/** The bearer token for outbound API calls, or `null` when unauthenticated. */
export function getAccessToken(): string | null {
  return tokens?.accessToken ?? null
}

// ---------------------------------------------------------------------------
// Login
// ---------------------------------------------------------------------------

/**
 * Starts the redirect flow, returning to `returnTo` (default: the current
 * location) once the callback completes. A no-op when there is nothing to
 * log into (open or unknown posture), while a login is already under way, or
 * while the callback route is settling — so a 401 can never spin.
 */
export function requireLogin(returnTo?: string): void {
  if (loginInFlight || isCallbackLocation()) return
  beginLogin(returnTo)
}

/**
 * Starts the flow unconditionally — for an explicit user action ("try
 * again" on the callback route), which must work in exactly the places
 * `requireLogin` declines to act on its own. Still a no-op when there is no
 * OIDC configuration to log in against.
 */
export function beginLogin(returnTo?: string): void {
  if (config?.mode !== 'oidc') return
  loginInFlight = true
  void startLogin(returnTo ?? currentPath()).catch((err) => {
    loginInFlight = false
    console.error('could not start the login flow', err)
  })
}

/** True while the browser is sitting on the callback route. */
export function isCallbackLocation(): boolean {
  return window.location.pathname === CALLBACK_PATH
}

function currentPath(): string {
  const { pathname, search, hash } = window.location
  return pathname === CALLBACK_PATH ? '/' : `${pathname}${search}${hash}`
}

function redirectUri(): string {
  return `${window.location.origin}${CALLBACK_PATH}`
}

async function startLogin(returnTo: string): Promise<void> {
  const settings = config
  if (settings?.mode !== 'oidc') return
  const meta = await ensureMetadata(settings)

  const verifier = randomUrlSafe(64)
  const state = randomUrlSafe(32)
  const pending: PendingLogin = { verifier, state, returnTo }
  sessionStorage.setItem(PENDING_KEY, JSON.stringify(pending))

  const params = new URLSearchParams({
    response_type: 'code',
    client_id: settings.clientId,
    redirect_uri: redirectUri(),
    scope: requestedScopes(meta),
    state,
    code_challenge: await s256(verifier),
    code_challenge_method: 'S256',
  })
  authNavigation.assign(`${meta.authorizationEndpoint}?${params.toString()}`)
}

/**
 * `openid profile email` (the name/email claims the session surfaces), plus
 * `offline_access` when the IdP advertises it — that is what makes a refresh
 * token available. No audience/`resource` parameter is sent: the effective
 * audience is IdP-side client configuration, and every way of asking for one
 * from the browser is a vendor extension.
 */
function requestedScopes(meta: DiscoveryMetadata): string {
  const scopes = ['openid', 'profile', 'email']
  if (meta.scopesSupported.includes('offline_access')) scopes.push('offline_access')
  return scopes.join(' ')
}

/**
 * Completes the flow from the callback URL's query string: validates the
 * `state` against the value stashed before the redirect, exchanges the code
 * as a public client (no secret), and returns the path to navigate back to.
 */
export async function completeLogin(search: string): Promise<string> {
  const settings = config
  if (settings?.mode !== 'oidc') {
    throw new Error('no OIDC login is configured for this deployment')
  }
  const params = new URLSearchParams(search)
  const raw = sessionStorage.getItem(PENDING_KEY)
  sessionStorage.removeItem(PENDING_KEY)

  const error = params.get('error')
  if (error) {
    throw new Error(params.get('error_description') ?? `the identity provider returned ${error}`)
  }
  if (!raw) throw new Error('no login was in progress')
  const pending = JSON.parse(raw) as PendingLogin
  if (params.get('state') !== pending.state) {
    throw new Error('login state mismatch — the response did not match this browser session')
  }
  const code = params.get('code')
  if (!code) throw new Error('the identity provider returned no authorization code')

  const meta = await ensureMetadata(settings)
  await exchange(meta, {
    grant_type: 'authorization_code',
    code,
    redirect_uri: redirectUri(),
    client_id: settings.clientId,
    code_verifier: pending.verifier,
  })
  loginInFlight = false
  return pending.returnTo
}

interface WireTokenResponse {
  access_token: string
  refresh_token?: string | null
  expires_in?: number
  token_type?: string
}

async function exchange(meta: DiscoveryMetadata, form: Record<string, string>): Promise<void> {
  const response = await fetch(meta.tokenEndpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Accept: 'application/json',
    },
    body: new URLSearchParams(form).toString(),
  })
  if (!response.ok) {
    let detail = `status ${response.status}`
    try {
      const body = (await response.json()) as { error?: string; error_description?: string }
      detail = body.error_description ?? body.error ?? detail
    } catch {
      // Non-JSON error body — the status is all we can report.
    }
    throw new Error(`token exchange failed: ${detail}`)
  }
  const body = (await response.json()) as WireTokenResponse
  setTokens({
    accessToken: body.access_token,
    // A refresh response may legitimately omit a new refresh token; keep the
    // one already held rather than downgrading to no refresh at all.
    refreshToken: body.refresh_token ?? tokens?.refreshToken ?? null,
    expiresAt: Date.now() + (body.expires_in ?? 300) * 1000,
  })
}

function setTokens(next: Tokens): void {
  tokens = next
  if (refreshTimer !== null) clearTimeout(refreshTimer)
  refreshTimer = null
  if (!next.refreshToken) return
  const delay = Math.max(next.expiresAt - Date.now() - REFRESH_LEAD_MS, 1_000)
  refreshTimer = setTimeout(() => {
    void refreshTokens()
  }, delay)
}

/**
 * Proactive refresh. On failure the tokens are dropped and the login flow
 * restarts — the same path a 401 would take, just without the failed request.
 */
async function refreshTokens(): Promise<void> {
  const settings = config
  const refreshToken = tokens?.refreshToken
  if (settings?.mode !== 'oidc' || !refreshToken) return
  try {
    const meta = await ensureMetadata(settings)
    await exchange(meta, {
      grant_type: 'refresh_token',
      refresh_token: refreshToken,
      client_id: settings.clientId,
    })
  } catch (err) {
    console.error('token refresh failed; restarting login', err)
    tokens = null
    requireLogin()
  }
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

async function ensureMetadata(settings: OidcSettings): Promise<DiscoveryMetadata> {
  if (metadata) return metadata
  const issuer = settings.issuer.replace(/\/$/, '')
  const url = `${issuer}/.well-known/openid-configuration`
  const response = await fetch(url, { headers: { Accept: 'application/json' } })
  if (!response.ok) {
    throw new Error(`discovery request to ${url} failed: ${response.status}`)
  }
  const body = (await response.json()) as {
    issuer?: string
    authorization_endpoint?: string
    token_endpoint?: string
    scopes_supported?: string[]
    code_challenge_methods_supported?: string[]
  }
  // OIDC Discovery §4.3: the document's `issuer` MUST be identical to the
  // issuer used as the prefix of the well-known URL — which is `issuer`
  // above, the configured value with any trailing slash removed. A mismatch
  // means the document does not belong to the issuer the cluster validates
  // tokens against (a redirect to a neighbouring tenant, a copied config, a
  // hijacked discovery host), so the flow stops here rather than sending the
  // user to an authorization endpoint that cannot mint an accepted token.
  if (body.issuer !== issuer) {
    throw new Error(
      `discovery document at ${url} is for issuer ${String(body.issuer)}, not ${issuer}`,
    )
  }
  if (!body.authorization_endpoint || !body.token_endpoint) {
    throw new Error(`discovery document at ${url} is missing required endpoints`)
  }
  if (
    body.code_challenge_methods_supported &&
    !body.code_challenge_methods_supported.includes('S256')
  ) {
    throw new Error(`the identity provider at ${settings.issuer} does not support PKCE S256`)
  }
  metadata = {
    authorizationEndpoint: body.authorization_endpoint,
    tokenEndpoint: body.token_endpoint,
    scopesSupported: body.scopes_supported ?? [],
  }
  return metadata
}

// ---------------------------------------------------------------------------
// PKCE primitives (RFC 7636)
// ---------------------------------------------------------------------------

function base64Url(bytes: Uint8Array): string {
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

function randomUrlSafe(bytes: number): string {
  return base64Url(crypto.getRandomValues(new Uint8Array(bytes)))
}

async function s256(verifier: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier))
  return base64Url(new Uint8Array(digest))
}

// ---------------------------------------------------------------------------
// Test seam
// ---------------------------------------------------------------------------

/** Drops all module state so each test starts from a cold boot. */
export function resetAuthForTests(): void {
  if (refreshTimer !== null) clearTimeout(refreshTimer)
  config = null
  metadata = null
  tokens = null
  refreshTimer = null
  loginInFlight = false
  sessionStorage.removeItem(PENDING_KEY)
}

/** Installs a token directly, standing in for a completed login. */
export function setTokensForTests(next: Tokens | null): void {
  tokens = next
}
