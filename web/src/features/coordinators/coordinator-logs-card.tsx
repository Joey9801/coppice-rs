import type { CoordinatorId, CoordinatorMember } from '@/api/types'
import { useCoordinatorLogs } from '@/api/queries'
import { LogViewer } from '@/components'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { CoordinatorLabel } from './coordinator-label'

/** One tab body — calls the log hook for exactly the mounted (active) member. */
function CoordinatorLogTab({ id }: { id: CoordinatorId }) {
  const { data, isLoading, isError } = useCoordinatorLogs(id)
  return (
    <LogViewer
      entries={data?.entries ?? []}
      loading={isLoading}
      emptyText={isError ? "Couldn't load coordinator logs." : 'No log entries.'}
    />
  )
}

export interface CoordinatorLogsCardProps {
  members: CoordinatorMember[]
  /** Short labels from `shortCoordinatorLabels`, shared across the page. */
  labels: Map<CoordinatorId, string>
  className?: string
}

export function CoordinatorLogsCard({ members, labels, className }: CoordinatorLogsCardProps) {
  const first = members[0]
  return (
    <Card className={className}>
      <CardHeader>
        <CardTitle>Coordinator logs</CardTitle>
      </CardHeader>
      <CardContent>
        {first ? (
          <Tabs defaultValue={String(first.id)}>
            <TabsList className="max-w-full flex-wrap">
              {members.map((m) => (
                <TabsTrigger key={m.id} value={String(m.id)}>
                  <CoordinatorLabel
                    id={m.id}
                    shortLabel={labels.get(m.id) ?? m.id.toString()}
                    copyable={false}
                  />
                </TabsTrigger>
              ))}
            </TabsList>
            {members.map((m) => (
              <TabsContent key={m.id} value={String(m.id)}>
                <CoordinatorLogTab id={m.id} />
              </TabsContent>
            ))}
          </Tabs>
        ) : null}
        <p className="mt-3 text-xs text-muted-foreground">Log collection is not available yet.</p>
      </CardContent>
    </Card>
  )
}
