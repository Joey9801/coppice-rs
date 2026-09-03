import type { CoordinatorId } from '@/api/types'
import { CopyButton } from '@/components/copy-button'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'

export interface CoordinatorLabelProps {
  id: CoordinatorId
  /** Short form from `shortCoordinatorLabels` — shared so tags match across the page. */
  shortLabel: string
  /** Dial address, shown in the tooltip alongside the canonical id. */
  addr?: string
  /** Copy affordance; off inside interactive parents (e.g. tab triggers). */
  copyable?: boolean
  className?: string
}

/**
 * Compact, consistent display of a coordinator identity: a short scannable
 * tag, with the full id (and address) in an accessible tooltip. The tag never
 * wraps or overflows; the tooltip carries the canonical identity.
 */
export function CoordinatorLabel({
  id,
  shortLabel,
  addr,
  copyable = true,
  className,
}: CoordinatorLabelProps) {
  return (
    <span
      className={cn(
        'inline-flex min-w-0 max-w-full items-center gap-1 whitespace-nowrap',
        className,
      )}
    >
      <TooltipProvider delayDuration={300}>
        <Tooltip>
          <TooltipTrigger asChild>
            <span
              tabIndex={0}
              className="min-w-0 cursor-default truncate font-mono text-sm tabular-nums text-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              c-{shortLabel}
            </span>
          </TooltipTrigger>
          <TooltipContent>
            <div className="text-xs">
              <div className="font-mono">coordinator {id.toString()}</div>
              {addr ? <div className="font-mono text-muted-foreground">{addr}</div> : null}
            </div>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
      {copyable ? <CopyButton value={id.toString()} ariaLabel="Copy coordinator id" /> : null}
    </span>
  )
}
