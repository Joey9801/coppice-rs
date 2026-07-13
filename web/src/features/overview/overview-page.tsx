import { Link } from '@tanstack/react-router'
import { Activity } from 'lucide-react'
import { JOB_PHASES, type JobPhase } from '@/api/types'
import { useClusterOverview } from '@/api/queries'
import { formatDurationUs } from '@/lib/format'
import {
  EmptyState,
  PageHeader,
  ResourceTriple,
  SparkLine,
  StatePill,
  StatTile,
} from '@/components'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'
import { EventsFeed } from './events-feed'
import { QueueChart } from './queue-chart'

function formatRatePerMinute(n: number): string {
  return `${n.toFixed(1)}/min`
}

export function OverviewPage() {
  const { data, isLoading, isError } = useClusterOverview()

  if (isLoading) return <OverviewSkeleton />

  if (isError || !data) {
    return (
      <div>
        <PageHeader title="Cluster overview" />
        <Card>
          <EmptyState
            icon={Activity}
            title="Couldn't load the cluster overview"
            description="The overview data is unavailable right now. It will refresh automatically."
          />
        </Card>
      </div>
    )
  }

  const { clusterId, queue, capacity, recentEvents } = data
  const depthSeries = queue.history.map((h) => ({ t: h.tUs, v: h.depth }))
  const drainSeries = queue.history.map((h) => ({ t: h.tUs, v: h.drainedPerMinute }))
  const nonzeroStates = JOB_PHASES.filter((s) => queue.byState[s] > 0)

  return (
    <div>
      <PageHeader
        title="Cluster overview"
        description={<span className="font-mono text-muted-foreground">{clusterId}</span>}
      />

      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 lg:grid-cols-5">
        <StatTile
          label="Queue depth"
          value={queue.depth}
          hint={
            <>
              &uarr; {formatRatePerMinute(queue.arrivalRatePerMinute)} in &middot; &darr;{' '}
              {formatRatePerMinute(queue.drainRatePerMinute)} out
            </>
          }
        >
          <SparkLine data={depthSeries} color="var(--chart-1)" />
        </StatTile>

        <StatTile label="Drain rate" value={formatRatePerMinute(queue.drainRatePerMinute)}>
          <SparkLine data={drainSeries} color="var(--chart-2)" />
        </StatTile>

        <StatTile
          label="Oldest queued"
          value={queue.oldestQueuedAgeUs != null ? formatDurationUs(queue.oldestQueuedAgeUs) : '—'}
        />

        <StatTile
          label="Running jobs"
          value={queue.byState.Running}
          hint={`${queue.byState.Preparing} preparing / ${queue.byState.Finalizing} finalizing`}
        />

        <StatTile
          label="Nodes"
          value={capacity.nodes.total}
          hint={
            <>
              {capacity.nodes.schedulable} schedulable &middot;{' '}
              <span className={cn(capacity.nodes.lost > 0 && 'text-red-600 dark:text-red-400')}>
                {capacity.nodes.lost} lost
              </span>
            </>
          }
        />
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Queue depth</CardTitle>
          </CardHeader>
          <CardContent>
            {queue.history.length > 0 ? (
              <QueueChart history={queue.history} />
            ) : (
              <EmptyState title="No queue history yet" />
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Jobs by state</CardTitle>
          </CardHeader>
          <CardContent>
            {nonzeroStates.length > 0 ? (
              <ul className="space-y-1">
                {nonzeroStates.map((state) => (
                  <li key={state}>
                    <Link
                      to="/jobs"
                      search={{ state } as { state: JobPhase }}
                      className="flex items-center justify-between rounded-md px-2 py-1.5 hover:bg-accent"
                    >
                      <StatePill state={state} />
                      <span className="text-sm font-medium tabular-nums text-foreground">
                        {queue.byState[state]}
                      </span>
                    </Link>
                  </li>
                ))}
              </ul>
            ) : (
              <EmptyState title="No jobs" />
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Capacity</CardTitle>
          </CardHeader>
          <CardContent>
            <ResourceTriple
              capacity={capacity.capacity}
              allocated={capacity.allocated}
              used={capacity.used}
            />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Recent events</CardTitle>
          </CardHeader>
          <CardContent>
            {recentEvents.length > 0 ? (
              <EventsFeed events={recentEvents} />
            ) : (
              <EmptyState title="No recent events" />
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function OverviewSkeleton() {
  return (
    <div>
      <PageHeader title="Cluster overview" />
      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 lg:grid-cols-5">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-28" />
        ))}
      </div>
      <div className="mt-4 grid gap-4 lg:grid-cols-3">
        <Skeleton className="h-72 lg:col-span-2" />
        <Skeleton className="h-72" />
      </div>
      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Skeleton className="h-56" />
        <Skeleton className="h-56" />
      </div>
    </div>
  )
}
