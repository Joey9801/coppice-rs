import type { CoordinatorId, CoordinatorMember } from '@/api/types'
import { useCoordinatorLogs } from '@/api/queries'
import { LogViewer } from '@/components'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { CoordinatorLabel } from './coordinator-label'

/** One tab body — calls the log hook for exactly the mounted (active) member. */
function CoordinatorLogTab({ id }: { id: CoordinatorId }) {
  const logs = useCoordinatorLogs(id)
  return <LogViewer controller={logs} structured />
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
          <Tabs defaultValue={first.id}>
            <TabsList className="max-w-full flex-wrap">
              {members.map((m) => (
                <TabsTrigger key={m.id} value={m.id}>
                  <CoordinatorLabel
                    id={m.id}
                    shortLabel={labels.get(m.id) ?? m.id}
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
      </CardContent>
    </Card>
  )
}
