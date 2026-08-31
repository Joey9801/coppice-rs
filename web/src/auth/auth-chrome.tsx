/**
 * All of the app shell's auth chrome, and the one place that decides how
 * much of it a deployment gets.
 *
 * The postures are mutually exclusive, deliberately:
 *
 * - `open` — the deployment authenticates nobody (`[auth] insecure_open`,
 *   and `coppice dev`'s default). There is no identity to show: the server
 *   answers every request as the same anonymous implicit admin, so an
 *   identity badge would be pure noise dressed up as an account. The single
 *   piece of chrome is the standing insecure-deployment warning.
 * - `oidc` — the real login; the identity badge names whoever the token
 *   belongs to, and there is nothing insecure to warn about.
 * - unknown (`null`) — the bootstrap failed, or `VITE_COPPICE_MOCK` is
 *   serving with no coordinator at all. Treated as the authenticated case:
 *   the mock answers "Demo User", and a real failed bootstrap simply renders
 *   nothing (the session query has no data either).
 */
import { ShieldAlert } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { useAuthMode, useSession } from './session'

export function AuthChrome() {
  // Not a subscription: the posture is fixed for the lifetime of the page.
  if (useAuthMode() === 'open') return <OpenModeIndicator />
  return <UserBadge />
}

/**
 * A quiet standing reminder that this cluster authenticates nobody and
 * authorizes everything.
 */
function OpenModeIndicator() {
  return (
    <Badge
      variant="destructive"
      title="This deployment has authentication disabled: every request is an unauthenticated admin."
    >
      <ShieldAlert className="size-3.5" />
      Insecure deployment
    </Badge>
  )
}

function UserBadge() {
  const { data: session } = useSession()
  if (!session) return null
  return (
    <div className="flex items-center gap-2">
      <span className="flex size-7 items-center justify-center rounded-full bg-primary text-xs font-semibold text-primary-foreground">
        {session.name
          .split(' ')
          .map((w) => w[0])
          .join('')
          .slice(0, 2)}
      </span>
      <span className="text-sm text-muted-foreground">{session.name}</span>
    </div>
  )
}
