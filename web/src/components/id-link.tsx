import { Link } from '@tanstack/react-router'
import { shortId } from '@/lib/format'
import { cn } from '@/lib/utils'
import { CopyButton } from './copy-button'

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
