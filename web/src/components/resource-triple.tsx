import type { Resources } from '@/api/types'
import { formatBytes, formatCpu } from '@/lib/format'
import { ResourceBar } from '@/components/resource-bar'

export interface ResourceTripleProps {
  capacity: Resources
  allocated?: Resources
  used?: Resources
  className?: string
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
          used={used?.cpuMillis}
          format={formatCpu}
        />
        <ResourceBar
          label="Memory"
          capacity={capacity.memoryBytes}
          allocated={allocated?.memoryBytes}
          used={used?.memoryBytes}
          format={formatBytes}
        />
        <ResourceBar
          label="Disk"
          capacity={capacity.diskBytes}
          allocated={allocated?.diskBytes}
          used={used?.diskBytes}
          format={formatBytes}
        />
      </div>
    </div>
  )
}
