/**
 * Auth stub. `useSession` re-exports the session query hook; the mock
 * client answers with a hardcoded "Demo User".
 *
 * The SSO seam (ADRs 0022/0023, not yet written as code): when OIDC
 * lands, the real client's `getSession()` returns the authenticated
 * principal, a 401 from any endpoint triggers the login redirect (handled
 * centrally in the query layer / real client, not in components), and
 * `roles` comes from the replicated role bindings. Components only ever
 * consume the `Session` shape from api/types.ts, which does not change.
 */
export { useSession } from '@/api/queries'
