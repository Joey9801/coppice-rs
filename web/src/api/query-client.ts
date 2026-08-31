import { MutationCache, QueryCache, QueryClient } from '@tanstack/react-query'
import { requireLogin } from '@/auth/oidc'
import { ApiError } from './client'

/**
 * The app's `QueryClient`, and the one place a 401 is handled.
 *
 * Every read and write in the app goes through the hooks in `queries.ts`,
 * so a cache-level `onError` sees every `Unauthenticated` the coordinator
 * can produce. It restarts the login flow with the route the user was
 * actually on, and `requireLogin` returns them to it after the callback.
 * Components therefore never think about auth: they see a failed query
 * while the redirect is being arranged, and a fresh page afterwards.
 *
 * `requireLogin` is itself a no-op in open mode, when the posture is
 * unknown, and while a login is already in flight — a 401 can neither loop
 * nor bounce the user at a login route that does not exist.
 *
 * Retrying a 401 is pointless (the credential will not improve on its own),
 * so `retry` declines them and keeps React Query's default backoff for
 * everything else.
 */
export function createQueryClient(): QueryClient {
  return new QueryClient({
    queryCache: new QueryCache({ onError: onQueryError }),
    mutationCache: new MutationCache({ onError: onQueryError }),
    defaultOptions: {
      queries: {
        // Most views poll (the mock world ticks ~1s); keep staleTime short so
        // navigation between pages reuses fresh-enough data without a spinner.
        staleTime: 2_000,
        refetchOnWindowFocus: false,
        retry: (failureCount, error) => !isUnauthenticated(error) && failureCount < 3,
      },
      mutations: {
        retry: false,
      },
    },
  })
}

function onQueryError(error: unknown): void {
  if (isUnauthenticated(error)) requireLogin()
}

function isUnauthenticated(error: unknown): boolean {
  return error instanceof ApiError && error.code === 'Unauthenticated'
}
