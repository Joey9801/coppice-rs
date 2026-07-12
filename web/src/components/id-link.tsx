import { useState } from 'react'
import { Link } from '@tanstack/react-router'
import { Check, Copy } from 'lucide-react'
import { shortId } from '@/lib/format'
import { cn } from '@/lib/utils'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

export interface IdLinkProps {
  id: string
  full?: boolean
  className?: string
}

/**
 * Renders a typed id (`job-…`, `node-…`, …) in mono, linking to its detail
 * route when the prefix is routable, with a copy-to-clipboard affordance.
 */
export function IdLink({ id, full = false, className }: IdLinkProps) {
  const label = full ? id : shortId(id)

  let inner
  if (id.startsWith('job-')) {
    inner = (
      <Link
        to="/jobs/$jobId"
        params={{ jobId: id }}
        className="font-mono text-sm text-primary hover:underline"
      >
        {label}
      </Link>
    )
  } else if (id.startsWith('node-')) {
    inner = (
      <Link
        to="/nodes/$nodeId"
        params={{ nodeId: id }}
        className="font-mono text-sm text-primary hover:underline"
      >
        {label}
      </Link>
    )
  } else {
    inner = <span className="font-mono text-sm text-foreground">{label}</span>
  }

  return (
    <span className={cn('inline-flex items-center gap-1 whitespace-nowrap', className)}>
      {inner}
      <CopyButton value={id} />
    </span>
  )
}

function CopyButton({ value }: { value: string }) {
  const [copied, setCopied] = useState(false)

  const copy = () => {
    void navigator.clipboard?.writeText(value).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 1200)
    })
  }

  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            onClick={copy}
            aria-label="Copy id"
            className="inline-flex size-5 items-center justify-center rounded text-muted-foreground transition-colors hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            {copied ? (
              <Check className="size-3 text-emerald-600 dark:text-emerald-400" />
            ) : (
              <Copy className="size-3" />
            )}
          </button>
        </TooltipTrigger>
        <TooltipContent>
          <span className="font-mono">{value}</span>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}
