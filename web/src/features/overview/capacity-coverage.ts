import { formatCpu } from '@/lib/format'

/**
 * Coverage note for the Capacity card's `used` figure: `used` is a sum over
 * `reportingNodes` of `totalNodes` (see `ClusterCapacity.used`), so it is a
 * PARTIAL figure whenever they differ — this must never be presented as a
 * complete total with no annotation. Returns `null` when coverage is full
 * (nothing to say).
 */
export function capacityCoverageNote(reportingNodes: number, totalNodes: number): string | null {
  if (reportingNodes >= totalNodes) return null
  if (reportingNodes === 0) return 'no nodes reporting usage'
  return `used: ${reportingNodes}/${totalNodes} nodes reporting`
}

/**
 * Tooltip text for the capacity chart's "Used" series value. `used` is a sum
 * over `reportingNodes` of `totalNodes` (see `ClusterCapacity.used`): when
 * coverage is partial the annotation must say so; a `null` value is a
 * coverage gap (no node reporting at that instant), never "0".
 */
export function formatUsedTooltip(
  value: number | null,
  reportingNodes: number,
  totalNodes: number,
): string {
  if (value == null) return 'not reported'
  const cpu = formatCpu(value)
  return reportingNodes < totalNodes
    ? `${cpu} (${reportingNodes}/${totalNodes} nodes reporting)`
    : cpu
}
