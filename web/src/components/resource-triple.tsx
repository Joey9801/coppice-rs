import type { Resources } from '@/api/types'
import { formatBytes, formatCpu } from '@/lib/format'
import { ResourceBar } from '@/components/resource-bar'

export interface ResourceTripleProps {
  capacity: Resources
  allocated?: Resources
  /** `null`/absent renders as "not reported", never a fabricated zero. */
  used?: Resources | null
  className?: string
}

/**
 * `null` (explicit "not measured") is preserved through to `ResourceBar`
 * distinct from `undefined` (caller does not track `used` at all) — plain
 * optional chaining would collapse both to `undefined` and lose that
 * distinction.
 */
function dimension(used: Resources | null | undefined, pick: (r: Resources) => number) {
  return used === null ? null : used === undefined ? undefined : pick(used)
}

/** CPU / memory / disk capacity bars stacked, formatted per dimension. */
export function ResourceTriple({ capacity, allocated, used, className }: ResourceTripleProps) {
  return (
    <div className={className}>
      <div className="space-y-3">
        <ResourceBar
          label="CPU"
          capacity={capacity.cpuMillis}
          allocated={allocated?.cpuMillis}
          used={dimension(used, (r) => r.cpuMillis)}
          format={formatCpu}
        />
        <ResourceBar
          label="Memory"
          capacity={capacity.memoryBytes}
          allocated={allocated?.memoryBytes}
          used={dimension(used, (r) => r.memoryBytes)}
          format={formatBytes}
        />
        <ResourceBar
          label="Disk"
          capacity={capacity.diskBytes}
          allocated={allocated?.diskBytes}
          used={dimension(used, (r) => r.diskBytes)}
          format={formatBytes}
        />
      </div>
    </div>
  )
}
