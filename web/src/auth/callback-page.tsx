import { useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useRouter } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'
import { beginLogin, completeLogin } from './oidc'

/**
 * The OIDC redirect target (`/auth/callback`). It exchanges the authorization
 * code for tokens and then hands the user back to the route they were on
 * when the login started — by client-side navigation, deliberately: the
 * access token lives in memory, so a full page load here would throw away
 * the credential the exchange just obtained and start the flow over.
 *
 * The exchange is one-shot (the PKCE verifier is consumed from
 * `sessionStorage`), so the effect guards against React's double-invoke in
 * StrictMode rather than relying on the effect running exactly once.
 *
 * On success the *whole* query cache is reset, not just the session query.
 * Everything fetched before the token existed was fetched unauthenticated:
 * the app shell's `useSession` (`staleTime: Infinity`, and 401s are
 * deliberately not retried) is the one that would otherwise stay poisoned
 * forever, but any page-level query that raced the redirect is in the same
 * state. Resetting is both simpler than enumerating them and strictly more
 * correct — there is no cached value from before the login that is worth
 * keeping, and `resetQueries` clears the errors as well as the data so the
 * returned-to route renders a fresh load rather than a stale failure.
 */
export function AuthCallbackPage() {
  const router = useRouter()
  const queryClient = useQueryClient()
  const started = useRef(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (started.current) return
    started.current = true
    completeLogin(window.location.search).then(
      (returnTo) => {
        // Refetches every mounted observer (the shell's session badge is one
        // of them) and drops the rest; not awaited, so the user is handed
        // back to their route without waiting on the refetches.
        void queryClient.resetQueries()
        router.history.replace(returnTo)
      },
      (err: unknown) => {
        setError(err instanceof Error ? err.message : 'the login could not be completed')
      },
    )
  }, [router, queryClient])

  return (
    <div className="flex min-h-[50vh] items-center justify-center">
      {error === null ? (
        <p className="text-sm text-muted-foreground">Signing in…</p>
      ) : (
        <div className="flex max-w-md flex-col items-center gap-3 text-center">
          <p className="text-sm font-medium">Sign-in failed</p>
          <p className="text-sm text-muted-foreground">{error}</p>
          <Button
            variant="outline"
            onClick={() => {
              beginLogin('/')
            }}
          >
            Try again
          </Button>
        </div>
      )}
    </div>
  )
}
