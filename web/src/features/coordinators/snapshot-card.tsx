import type { CoordinatorSnapshot } from '@/api/types'
import { formatBytes, formatTimestamp } from '@/lib/format'
import { KeyValueGrid, TimeAgo } from '@/components'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

const NOT_REPORTED = <span className="text-muted-foreground">not reported</span>

export interface SnapshotCardProps {
  /** The serving replica's last snapshot, or null while none has been taken. */
  snapshot: CoordinatorSnapshot | null
  /** Highest applied log index on the serving replica. */
  lastApplied: number
  className?: string
}

/**
 * The Snapshot panel. With no snapshot yet it says so plainly — no epoch
 * timestamps, no `0 B`, no invented last-included index — and reports the
 * honest equivalent, entries since the log's start.
 */
export function SnapshotCard({ snapshot, lastApplied, className }: SnapshotCardProps) {
  return (
    <Card className={className}>
      <CardHeader>
        <CardTitle>Snapshot</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {snapshot ? (
          <>
            <KeyValueGrid
              items={[
                {
                  label: 'Size',
                  value:
                    snapshot.sizeBytes != null ? formatBytes(snapshot.sizeBytes) : NOT_REPORTED,
                },
                {
                  label: 'Last included index',
                  value: snapshot.lastIncludedIndex.toLocaleString(),
                },
                {
                  label: 'Taken',
                  value: snapshot.takenAt ? (
                    <span>
                      {formatTimestamp(snapshot.takenAt)} (<TimeAgo t={snapshot.takenAt} />)
                    </span>
                  ) : (
                    NOT_REPORTED
                  ),
                },
                {
                  label: 'Entries since snapshot',
                  value: snapshot.entriesSinceSnapshot.toLocaleString(),
                },
              ]}
            />
            <p className="text-xs text-muted-foreground">
              A coordinator that falls behind can use this snapshot to catch up faster than
              replaying every stored entry.
            </p>
          </>
        ) : (
          <>
            <p className="text-sm text-muted-foreground">
              No snapshot has been taken yet. The first one is written once enough log entries have
              accrued.
            </p>
            <KeyValueGrid
              items={[
                {
                  label: 'Entries since log start',
                  value: lastApplied.toLocaleString(),
                },
              ]}
            />
            <p className="text-xs text-muted-foreground">
              Until then, a coordinator that falls behind replays the log from the start.
            </p>
          </>
        )}
      </CardContent>
    </Card>
  )
}
