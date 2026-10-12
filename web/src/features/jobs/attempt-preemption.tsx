import type { AttemptPreemption } from '@/api/types'
import { formatDuration, formatRelativeTime, formatTimestamp } from '@/lib/format'
import { IdLink, preemptReasonLabel } from '@/components'

/** An attempt's termination record (ADR 0044 §3), one line under its attempts-table row. */
export function AttemptPreemptionDetail({
  preemption,
  now = new Date(),
}: {
  preemption: AttemptPreemption
  now?: Date
}) {
  const { reason, forJob, requestedAt, notifiedAt, deadline, noticeShortfallSeconds } = preemption
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
      <span>
        <span className="font-medium text-foreground">{preemptReasonLabel(reason)}</span> preemption
        {forJob != null ? (
          <>
            {' '}
            · made way for <IdLink id={forJob} className="text-xs" />
          </>
        ) : null}
      </span>
      <Instant label="requested" t={requestedAt} now={now} />
      {notifiedAt == null ? (
        <span>notice not yet delivered</span>
      ) : (
        <Instant label="notified" t={notifiedAt} now={now} />
      )}
      {deadline != null ? <Instant label="deadline" t={deadline} now={now} /> : null}
      {noticeShortfallSeconds != null && noticeShortfallSeconds > 0 ? (
        <span
          className="font-medium text-destructive"
          title="Declared notice minus the notice the attempt actually got"
        >
          notice short by {formatDuration(noticeShortfallSeconds)}
        </span>
      ) : null}
    </div>
  )
}

function Instant({ label, t, now }: { label: string; t: Date; now: Date }) {
  return (
    <span title={formatTimestamp(t)}>
      {label} <span className="tabular-nums text-foreground">{formatRelativeTime(t, now)}</span>
    </span>
  )
}
