import type { JobId } from '@/api/types'
import { useJobLogs } from '@/api/queries'
import { LogViewer } from '@/components'

export function JobLogsTab({ jobId }: { jobId: JobId }) {
  const logs = useJobLogs(jobId)

  return (
    <div className="space-y-2">
      <LogViewer entries={logs.data?.entries ?? []} loading={logs.isLoading} />
      <p className="text-xs text-muted-foreground">
        Mock data — log storage is not designed in the backend yet.
      </p>
    </div>
  )
}
