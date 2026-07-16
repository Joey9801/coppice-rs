import { useEffect, useState } from 'react'
import { formatTimeAgo, formatTimestamp } from '@/lib/format'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

export interface TimeAgoProps {
  t: Date
  className?: string
}

/** Relative time that self-refreshes every 30s, with an absolute tooltip. */
export function TimeAgo({ t, className }: TimeAgoProps) {
  const [, setTick] = useState(0)

  useEffect(() => {
    const id = setInterval(() => setTick((n) => n + 1), 30_000)
    return () => clearInterval(id)
  }, [])

  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className={className}>{formatTimeAgo(t)}</span>
        </TooltipTrigger>
        <TooltipContent>{formatTimestamp(t)}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}
