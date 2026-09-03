import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { queryKeys } from '@/api/queries'
import type { Session } from '@/api/types'
import { AuthChrome } from './auth-chrome'
import { bootstrapAuth, resetAuthForTests } from './oidc'

/**
 * How much auth chrome each posture gets. The rule this guards: an open
 * deployment shows the no-auth badge and *nothing else* — no
 * identity, because there is no identity, only the anonymous implicit admin
 * every request is answered as.
 */

const ISSUER = 'https://idp.example'

const SESSION: Session = {
  subject: 'auth0|42',
  name: 'Ada Lovelace',
  email: 'ada@example.test',
  roles: ['submitter'],
  implicitAdmin: false,
}

function stubAuthConfig(mode: 'open' | 'oidc') {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      if (!String(input).startsWith('/api/v1/auth/config')) {
        return Promise.reject(new Error(`unexpected fetch: ${String(input)}`))
      }
      const body =
        mode === 'open'
          ? { mode: 'open' }
          : { mode: 'oidc', issuer: ISSUER, client_id: 'coppice-web', audience: 'coppice-api' }
      return Promise.resolve(
        new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } }),
      )
    }),
  )
}

/** Renders the chrome with the session already cached, so nothing fetches. */
function renderChrome() {
  const client = new QueryClient()
  client.setQueryData(queryKeys.session, SESSION)
  render(
    <QueryClientProvider client={client}>
      <AuthChrome />
    </QueryClientProvider>,
  )
}

beforeEach(() => {
  resetAuthForTests()
})

afterEach(() => {
  vi.unstubAllGlobals()
  resetAuthForTests()
})

describe('AuthChrome', () => {
  it('shows the no-auth badge and explains its access in open mode', async () => {
    stubAuthConfig('open')
    await bootstrapAuth()

    renderChrome()

    const badge = screen.getByText('No auth configured')
    expect(badge).toBeInTheDocument()
    expect(badge).toHaveAttribute('tabindex', '0')
    expect(badge).not.toHaveAttribute('title')
    fireEvent.focus(badge)
    await waitFor(() => {
      expect(screen.getByRole('tooltip')).toHaveTextContent(
        'Authentication is disabled; every request has administrative access.',
      )
    })
    expect(screen.queryByText(SESSION.name)).not.toBeInTheDocument()
  })

  it('shows the identity without an auth-warning badge in oidc mode', async () => {
    stubAuthConfig('oidc')
    await bootstrapAuth()

    renderChrome()

    expect(screen.getByText(SESSION.name)).toBeInTheDocument()
    expect(screen.queryByText('No auth configured')).not.toBeInTheDocument()
  })
})
