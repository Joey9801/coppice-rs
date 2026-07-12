import type { ComponentProps } from 'react'
import { ChevronDown } from 'lucide-react'
import { cn } from '@/lib/utils'

/**
 * Styled native `<select>` — deliberately not a Radix select. Keeps forms
 * simple (native keyboard/mobile behavior) and SSR-free. Wrap options as
 * plain `<option>` children.
 */
function Select({ className, children, ...props }: ComponentProps<'select'>) {
  return (
    <div className="relative w-fit">
      <select
        data-slot="select"
        className={cn(
          'flex h-9 w-full appearance-none rounded-md border border-input bg-transparent py-1 pl-3 pr-8 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50',
          className,
        )}
        {...props}
      >
        {children}
      </select>
      <ChevronDown className="pointer-events-none absolute right-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
    </div>
  )
}

export { Select }
