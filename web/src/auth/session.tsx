/**
 * The components' view of authentication.
 *
 * The real flow ships: `./oidc.ts` bootstraps `GET /api/v1/auth/config` at
 * startup and, in `mode: "oidc"`, runs an authorization-code + PKCE login
 * against the issuer's discovery metadata, holding the access token in
 * memory. `real-client.ts` attaches it as a bearer token to every API call
 * and implements `getSession()` against `GET /api/v1/session`, mapping the
 * server's ADR 0023 authority summary (`bindings` + `implicit_admin`) onto
 * the `Session` shape in `api/types.ts`. A 401 from anywhere in the query
 * layer restarts the login and returns to the attempted route — centrally,
 * in `api/query-client.ts`, never in a component.
 *
 * In `mode: "open"` (an intentionally unauthenticated deployment, including
 * `coppice dev`) there is nothing to log into: the server answers every
 * request as the anonymous implicit admin, and the only auth chrome is the
 * no-auth badge — no identity badge, since there is no identity
 * (`./auth-chrome.tsx` owns that decision). Under `VITE_COPPICE_MOCK` the
 * mock client still answers "Demo User" with no coordinator at all.
 *
 * Components only ever consume the `Session` shape and the helpers here.
 */
import type { Session } from '@/api/types'
import { getAuthMode, type AuthMode } from './oidc'

export { useSession } from '@/api/queries'

/**
 * This deployment's auth posture, or `null` when it is not known (the
 * bootstrap request failed, or the mock client is serving). Constant for the
 * lifetime of the page — resolved before the app first renders — so this is
 * a plain read rather than a subscription.
 */
export function useAuthMode(): AuthMode | null {
  return getAuthMode()
}

/**
 * Whether the session may propose `ConfigureQuotaEntity` (edit the entity
 * tree). ADR 0023 grants that to `admin` bindings, at any scope; `roles` here
 * is the flattened set of matching binding roles and cannot express subtree
 * scoping, so this stays all-or-nothing (an admin binding scoped to one
 * subtree unlocks the form everywhere — the server still rejects the writes
 * it does not authorize) until scoped bindings ride in the session payload.
 * `implicitAdmin` covers the unscoped admins that are not representable as
 * bindings at all: operator certificates and the open posture.
 */
export function canConfigureEntities(session: Session | undefined): boolean {
  if (!session) return false
  return session.roles.includes('admin') || session.implicitAdmin
}
