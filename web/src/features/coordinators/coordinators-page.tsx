import { Network } from 'lucide-react'
import { useCoordinatorStatus } from '@/api/queries'
import { formatBytes, formatTimestampUs } from '@/lib/format'
import { EmptyState, KeyValueGrid, PageHeader, StatTile, TimeAgo } from '@/components'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'
import { MembershipCard } from './membership-card'
import { CoordinatorLogsCard } from './coordinator-logs-card'

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

  return (
    <div>
      <PageHeader
        title="Coordinators"
        description={
          <span>
            <span className="font-mono text-muted-foreground">{clusterId}</span>
            <span className="mx-1.5">·</span>
            term {term.toLocaleString()}
          </span>
        }
      />

      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatTile
          label="Leader"
          value={
            leader != null ? (
              `coordinator ${leader}`
            ) : (
              <span className="text-red-600 dark:text-red-400">no leader</span>
            )
          }
          hint={`term ${term.toLocaleString()}`}
        />
        <StatTile
          label="Committed index"
          value={knownCommitted.toLocaleString()}
          hint={
            <span className={cn(applyLagging && 'text-amber-600 dark:text-amber-400')}>
              applied {lastApplied.toLocaleString()}
            </span>
          }
        />
        <StatTile
          label="State version"
          value={stateVersion.toLocaleString()}
          hint="commands applied"
        />
        <StatTile
          label="Since snapshot"
          value={`${snapshot.entriesSinceSnapshot.toLocaleString()} entries`}
          hint={
            <>
              {formatBytes(snapshot.sizeBytes)} · taken <TimeAgo tUs={snapshot.takenAtUs} />
            </>
          }
        />
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <MembershipCard members={members} leader={leader} className="lg:col-span-2" />

        <Card>
          <CardHeader>
            <CardTitle>Replicated state</CardTitle>
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
                { label: 'Known committed', value: knownCommitted.toLocaleString() },
                { label: 'State version', value: stateVersion.toLocaleString() },
              ]}
            />
            <p className="text-xs text-muted-foreground">
              The state machine is deterministic — every replica applies the same command log;
              version counts applied commands, distinct from the Raft log index.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Snapshot</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <KeyValueGrid
              items={[
                { label: 'Size', value: formatBytes(snapshot.sizeBytes) },
                {
                  label: 'Last included index',
                  value: snapshot.lastIncludedIndex.toLocaleString(),
                },
                {
                  label: 'Taken',
                  value: (
                    <span>
                      {formatTimestampUs(snapshot.takenAtUs)} (
                      <TimeAgo tUs={snapshot.takenAtUs} />)
                    </span>
                  ),
                },
                {
                  label: 'Entries since snapshot',
                  value: snapshot.entriesSinceSnapshot.toLocaleString(),
                },
              ]}
            />
            <p className="text-xs text-muted-foreground">
              Followers that fall behind the retained log receive this snapshot instead of replay.
            </p>
          </CardContent>
        </Card>

        <CoordinatorLogsCard members={members} className="lg:col-span-2" />
      </div>
    </div>
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
