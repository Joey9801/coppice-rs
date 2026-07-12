import type { ReactNode } from 'react'
import type { LucideIcon } from 'lucide-react'
import { ArrowRight, Ban, Coins, LogOut, Play, Plus, RefreshCw } from 'lucide-react'
import type { TimelineEvent } from '@/api/types'
import { IdLink, TimeAgo } from '@/components'

interface RenderedEvent {
  icon: LucideIcon
  body: ReactNode
}

/** Map a timeline event to an icon + human sentence. Exhaustive over kinds. */
function renderEvent(event: TimelineEvent): RenderedEvent {
  switch (event.kind) {
    case 'JobSubmitted':
      return {
        icon: Plus,
        body: (
          <>
            Job <IdLink id={event.job} /> submitted
          </>
        ),
      }
    case 'JobStateChanged':
      return {
        icon: ArrowRight,
        body: (
          <>
            Job <IdLink id={event.job} /> {event.from} <ArrowGlyph /> {event.to}
          </>
        ),
      }
    case 'AttemptStateChanged':
      return {
        icon: Play,
        body: (
          <>
            Attempt of Job <IdLink id={event.job} /> <ArrowGlyph /> {event.state} on{' '}
            <IdLink id={event.node} />
          </>
        ),
      }
    case 'AllocationFunded':
      return {
        icon: Coins,
        body: (
          <>
            Allocation funded for Job <IdLink id={event.job} /> on <IdLink id={event.node} />
          </>
        ),
      }
    case 'StopRequested':
      return {
        icon: Ban,
        body: (
          <>
            Stop requested for Job <IdLink id={event.job} />
            {event.reason ? <span className="text-muted-foreground"> — {event.reason}</span> : null}
          </>
        ),
      }
    case 'NodeEpochBumped':
      return {
        icon: RefreshCw,
        body: (
          <>
            Node <IdLink id={event.node} /> epoch bumped to {event.epoch}
          </>
        ),
      }
    case 'JobEvicted':
      return {
        icon: LogOut,
        body: (
          <>
            Job <IdLink id={event.job} /> evicted from <IdLink id={event.node} />
          </>
        ),
      }
    default: {
      const _exhaustive: never = event
      return _exhaustive
    }
  }
}

function ArrowGlyph() {
  return <span className="text-muted-foreground">&rarr;</span>
}

export interface EventsFeedProps {
  events: TimelineEvent[]
}

/**
 * Stable identity for a rendered event row so polling refreshes reuse DOM
 * nodes instead of remounting the whole list (index keys would remount
 * every row whenever a new event shifts the window).
 */
function eventKey(event: TimelineEvent): string {
  const subject =
    'attempt' in event
      ? event.attempt
      : 'allocation' in event
        ? event.allocation
        : 'job' in event
          ? event.job
          : event.node
  return `${event.atUs}:${event.kind}:${subject}`
}

/**
 * Newest-first list of recent cluster timeline events.
 *
 * Throughput note: this renders whatever bounded window the API hands it
 * (`ClusterOverview.recentEvents`, most-recent-N). The browser never
 * consumes the cluster's raw event stream — server-side windowing and
 * coalescing keep this cheap on large clusters, not the client.
 */
export function EventsFeed({ events }: EventsFeedProps) {
  return (
    <ul className="divide-y divide-border">
      {events.map((event) => {
        const { icon: Icon, body } = renderEvent(event)
        return (
          <li key={eventKey(event)} className="flex items-start gap-3 py-2.5 text-sm">
            <span className="mt-0.5 flex size-6 shrink-0 items-center justify-center rounded-full bg-muted text-muted-foreground">
              <Icon className="size-3.5" />
            </span>
            <span className="min-w-0 flex-1 leading-relaxed text-foreground">{body}</span>
            <TimeAgo tUs={event.atUs} className="shrink-0 text-xs text-muted-foreground" />
          </li>
        )
      })}
    </ul>
  )
}
