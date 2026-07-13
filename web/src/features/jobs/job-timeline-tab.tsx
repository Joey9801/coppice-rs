import type { ReactNode } from 'react'
import {
  Activity,
  Ban,
  CircleCheck,
  CircleDot,
  type LucideIcon,
  LogOut,
  RefreshCw,
  Send,
  Shuffle,
} from 'lucide-react'
import { jobStateLabel, type JobId, type TimelineEvent } from '@/api/types'
import { useJobTimeline } from '@/api/queries'
import { formatTimeAgo, formatTimestampUs } from '@/lib/format'
import { EmptyState, IdLink } from '@/components'
import { Skeleton } from '@/components/ui/skeleton'

export function JobTimelineTab({ jobId }: { jobId: JobId }) {
  const timeline = useJobTimeline(jobId)

  if (timeline.isLoading) {
    return (
      <div className="space-y-4 py-2">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-8" />
        ))}
      </div>
    )
  }

  if (!timeline.data || timeline.data.length === 0) {
    return <EmptyState icon={Activity} title="No events yet" />
  }

  return (
    <ol className="relative py-2">
      {timeline.data.map((event, i) => {
        const Icon = eventIcon(event)
        const last = i === timeline.data.length - 1
        return (
          <li key={i} className="relative flex gap-3 pb-5 last:pb-0">
            {!last ? (
              <span className="absolute left-3 top-6 -ml-px h-full w-px bg-border" aria-hidden />
            ) : null}
            <span className="relative z-10 mt-0.5 flex size-6 shrink-0 items-center justify-center rounded-full border bg-card text-muted-foreground">
              <Icon className="size-3.5" />
            </span>
            <div className="min-w-0 flex-1">
              <div className="text-sm text-foreground">{eventSentence(event)}</div>
              <div className="mt-0.5 text-xs text-muted-foreground">
                <span title={formatTimestampUs(event.atUs)}>{formatTimestampUs(event.atUs)}</span>
                <span className="mx-1.5">·</span>
                <span>{formatTimeAgo(event.atUs)}</span>
              </div>
            </div>
          </li>
        )
      })}
    </ol>
  )
}

function eventIcon(event: TimelineEvent): LucideIcon {
  switch (event.kind) {
    case 'JobSubmitted':
      return Send
    case 'JobStateChanged':
      return Shuffle
    case 'AttemptStateChanged':
      return Activity
    case 'AllocationFunded':
      return CircleCheck
    case 'StopRequested':
      return Ban
    case 'NodeEpochBumped':
      return RefreshCw
    case 'JobEvicted':
      return LogOut
    default:
      return CircleDot
  }
}

function eventSentence(event: TimelineEvent): ReactNode {
  switch (event.kind) {
    case 'JobSubmitted':
      return 'Job submitted'
    case 'JobStateChanged':
      return (
        <>
          State changed <Mono>{jobStateLabel(event.from)}</Mono> →{' '}
          <Mono>{jobStateLabel(event.to)}</Mono>
        </>
      )
    case 'AttemptStateChanged':
      return (
        <>
          Attempt <IdLink id={event.attempt} /> became <Mono>{event.state}</Mono> on{' '}
          <IdLink id={event.node} />
        </>
      )
    case 'AllocationFunded':
      return (
        <>
          Allocation <span className="font-mono text-xs">{event.allocation}</span> fully funded on{' '}
          <IdLink id={event.node} />
        </>
      )
    case 'StopRequested':
      return event.reason ? `Stop requested — ${event.reason}` : 'Stop requested'
    case 'NodeEpochBumped':
      return (
        <>
          Node <IdLink id={event.node} /> epoch bumped to <Mono>{event.epoch}</Mono>
        </>
      )
    case 'JobEvicted':
      return (
        <>
          Evicted from <IdLink id={event.node} />
        </>
      )
    default:
      return 'Unknown event'
  }
}

function Mono({ children }: { children: ReactNode }) {
  return <span className="font-mono text-xs">{children}</span>
}
