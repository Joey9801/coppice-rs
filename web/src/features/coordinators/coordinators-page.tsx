import { Network } from 'lucide-react'
import type { ReactNode } from 'react'
import { useCoordinatorStatus } from '@/api/queries'
import type { CoordinatorSnapshot } from '@/api/types'
import { formatBytes } from '@/lib/format'
import { EmptyState, KeyValueGrid, PageHeader, StatTile, TimeAgo } from '@/components'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'
import { MembershipCard } from './membership-card'
import { CoordinatorLogsCard } from './coordinator-logs-card'
import { CoordinatorLabel } from './coordinator-label'
import { shortCoordinatorLabels } from './coordinator-labels'
import { SnapshotCard } from './snapshot-card'

export function CoordinatorsPage() {
  const { data, isLoading, isError } = useCoordinatorStatus()

  if (isLoading) return <CoordinatorsSkeleton />

  if (isError || !data) {
    return (
      <div>
        <PageHeader title="Coordinators" />
        <Card>
          <EmptyState
            icon={Network}
            title="Couldn't load coordinator status"
            description="The consensus status is unavailable right now. It will refresh automatically."
          />
        </Card>
      </div>
    )
  }

  const {
    clusterId,
    leader,
    term,
    knownCommitted,
    lastApplied,
    stateVersion,
    snapshot,
    stateCounts,
    members,
  } = data

  const applyLagging = lastApplied < knownCommitted
  const labels = shortCoordinatorLabels(members.map((m) => m.id))
  const labelOf = (id: number) => labels.get(id) ?? id.toString()

  return (
    <div>
      <PageHeader
        title="Coordinators"
        description={
          <span>
            <span className="font-mono text-muted-foreground">{clusterId}</span>
            <span className="mx-1.5">·</span>
            election term {term.toLocaleString()}
          </span>
        }
      />

      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatTile
          label="Leader"
          value={
            leader != null ? (
              <CoordinatorLabel id={leader} shortLabel={labelOf(leader)} />
            ) : (
              <span className="text-red-600 dark:text-red-400">no leader</span>
            )
          }
          hint={`election term ${term.toLocaleString()}`}
        />
        <StatTile
          label="Committed position"
          value={knownCommitted.toLocaleString()}
          hint={
            <span className={cn(applyLagging && 'text-amber-600 dark:text-amber-400')}>
              applied {lastApplied.toLocaleString()}
              {applyLagging ? ` (${(knownCommitted - lastApplied).toLocaleString()} behind)` : ''}
            </span>
          }
        />
        <StatTile label="Applied updates" value={stateVersion.toLocaleString()} />
        <StatTile
          label="Since snapshot"
          value={
            snapshot ? (
              `${snapshot.entriesSinceSnapshot.toLocaleString()} entries`
            ) : (
              <span className="text-muted-foreground">no snapshot yet</span>
            )
          }
          hint={
            snapshot ? (
              <SnapshotTileHint snapshot={snapshot} />
            ) : (
              'The first snapshot has not been taken.'
            )
          }
        />
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <MembershipCard
          members={members}
          leader={leader}
          labels={labels}
          className="lg:col-span-2"
        />

        <Card>
          <CardHeader>
            <CardTitle>Cluster data</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <KeyValueGrid
              items={[
                { label: 'Jobs', value: stateCounts.jobs.toLocaleString() },
                { label: 'Attempts', value: stateCounts.attempts.toLocaleString() },
                { label: 'Allocations', value: stateCounts.allocations.toLocaleString() },
                { label: 'Nodes', value: stateCounts.nodes.toLocaleString() },
                { label: 'Quota entities', value: stateCounts.quotaEntities.toLocaleString() },
                { label: 'Last applied', value: lastApplied.toLocaleString() },
                { label: 'Committed position', value: knownCommitted.toLocaleString() },
                { label: 'Applied updates', value: stateVersion.toLocaleString() },
              ]}
            />
            <p className="text-xs text-muted-foreground">
              Every coordinator keeps the same cluster data. Applied updates are counted separately
              from the committed log position shown above.
            </p>
          </CardContent>
        </Card>

        <SnapshotCard snapshot={snapshot} lastApplied={lastApplied} />

        <CoordinatorLogsCard members={members} labels={labels} className="lg:col-span-2" />
      </div>
    </div>
  )
}

/** Size and age line under the "Since snapshot" tile; omits unreported metadata. */
function SnapshotTileHint({ snapshot }: { snapshot: CoordinatorSnapshot }) {
  const parts: ReactNode[] = []
  if (snapshot.sizeBytes != null) parts.push(formatBytes(snapshot.sizeBytes))
  if (snapshot.takenAt)
    parts.push(
      <span key="taken">
        taken <TimeAgo t={snapshot.takenAt} />
      </span>,
    )
  if (parts.length === 0) return 'snapshot metadata not reported'
  return (
    <>
      {parts.map((p, i) => (
        <span key={i}>
          {i > 0 ? ' · ' : ''}
          {p}
        </span>
      ))}
    </>
  )
}

function CoordinatorsSkeleton() {
  return (
    <div>
      <PageHeader title="Coordinators" />
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-28" />
        ))}
      </div>
      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Skeleton className="h-64 lg:col-span-2" />
        <Skeleton className="h-72" />
        <Skeleton className="h-72" />
        <Skeleton className="h-80 lg:col-span-2" />
      </div>
    </div>
  )
}
