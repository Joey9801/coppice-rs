import { formatRelativeTime, formatTimestamp } from '@/lib/format'
import { useNow } from '@/lib/use-now'
import { Badge } from '@/components/ui/badge'

/**
 * The provider is reclaiming this node's host (ADR 0044 §5). The deadline is
 * the agent's wall clock, so display only.
 */
export function InterruptionBadge({
  deadline,
  now: nowProp,
  className,
}: {
  deadline: Date
  now?: Date
  className?: string
}) {
  const ticking = useNow()
  const now = nowProp ?? ticking
  return (
    <Badge
      variant="destructive"
      className={className}
      title={`Provider interruption — host reclaimed at ${formatTimestamp(deadline)} (agent clock)`}
    >
      interruption {formatRelativeTime(deadline, now)}
    </Badge>
  )
}
