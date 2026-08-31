import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider, createRouter } from '@tanstack/react-router'
import { createQueryClient } from '@/api/query-client'
import { bootstrapAuth } from '@/auth/oidc'
import { ThemeProvider } from '@/lib/theme'
import { routeTree } from './routeTree.gen'
import './styles.css'

const queryClient = createQueryClient()

const router = createRouter({
  routeTree,
  defaultPreload: 'intent',
  context: { queryClient },
})

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router
  }
}

// The deployment's auth posture decides whether there is a login at all, and
// the callback route cannot complete an exchange without it — so nothing
// renders (and no query fires a premature 401) until it is known. The
// bootstrap resolves either way; a failed one leaves the posture unknown
// rather than blocking the app.
void bootstrapAuth().then(() => {
  createRoot(document.getElementById('root')!).render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <ThemeProvider>
          <RouterProvider router={router} />
        </ThemeProvider>
      </QueryClientProvider>
    </StrictMode>,
  )
})
