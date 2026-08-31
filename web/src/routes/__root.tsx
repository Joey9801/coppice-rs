import type { QueryClient } from '@tanstack/react-query'
import { Link, Outlet, createRootRouteWithContext } from '@tanstack/react-router'
import { Boxes, Gauge, ListTodo, Moon, Network, ServerCog, Sun } from 'lucide-react'
import { AuthChrome } from '@/auth/auth-chrome'
import { useTheme } from '@/lib/theme'

interface RouterContext {
  queryClient: QueryClient
}

export const Route = createRootRouteWithContext<RouterContext>()({
  component: AppShell,
})

const NAV = [
  { to: '/', label: 'Overview', icon: Gauge },
  { to: '/jobs', label: 'Jobs', icon: ListTodo },
  { to: '/entities', label: 'Entities', icon: Network },
  { to: '/nodes', label: 'Nodes', icon: Boxes },
  { to: '/coordinators', label: 'Coordinators', icon: ServerCog },
] as const

function AppShell() {
  return (
    <div className="flex min-h-screen">
      <aside className="flex w-52 shrink-0 flex-col border-r bg-sidebar text-sidebar-foreground">
        <div className="flex h-14 items-center gap-2 border-b px-4">
          <img src="/coppice.svg" alt="" className="size-6" />
          <span className="text-base font-semibold tracking-tight">Coppice</span>
        </div>
        <nav className="flex flex-col gap-1 p-2">
          {NAV.map(({ to, label, icon: Icon }) => (
            <Link
              key={to}
              to={to}
              activeOptions={{ exact: to === '/' }}
              className="flex items-center gap-2.5 rounded-md px-3 py-2 text-sm font-medium text-muted-foreground hover:bg-sidebar-accent hover:text-foreground"
              activeProps={{ className: 'bg-sidebar-accent text-foreground' }}
            >
              <Icon className="size-4" />
              {label}
            </Link>
          ))}
        </nav>
        <div className="mt-auto p-3 text-[11px] text-muted-foreground">
          Serving mock data — no coordinator attached
        </div>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-14 items-center justify-end gap-3 border-b px-6">
          <ThemeToggle />
          <AuthChrome />
        </header>
        <main className="min-w-0 flex-1 p-6">
          <Outlet />
        </main>
      </div>
    </div>
  )
}

function ThemeToggle() {
  const { theme, toggle } = useTheme()
  return (
    <button
      type="button"
      onClick={toggle}
      aria-label="Toggle theme"
      className="rounded-md p-2 text-muted-foreground hover:bg-accent hover:text-foreground"
    >
      {theme === 'dark' ? <Sun className="size-4" /> : <Moon className="size-4" />}
    </button>
  )
}
