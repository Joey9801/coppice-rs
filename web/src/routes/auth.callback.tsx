import { createFileRoute } from '@tanstack/react-router'
import { AuthCallbackPage } from '@/auth/callback-page'

/** The OIDC redirect URI — must stay in sync with `CALLBACK_PATH`. */
export const Route = createFileRoute('/auth/callback')({
  component: AuthCallbackPage,
})
