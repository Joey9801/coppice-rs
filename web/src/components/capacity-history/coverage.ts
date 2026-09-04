/**
 * Coverage language for a measured `used` figure. A cluster-wide `used` is
 * a sum over `reportingNodes` of `totalNodes` (see `ClusterCapacity.used`),
 * so it is a PARTIAL figure whenever they differ and must never be
 * presented as a complete total with no annotation.
 */

/** Panel-level note, or `null` when coverage is full (nothing to say). */
export function capacityCoverageNote(reportingNodes: number, totalNodes: number): string | null {
  if (reportingNodes >= totalNodes) return null
  if (reportingNodes === 0) return 'no nodes reporting usage'
  return `used: ${reportingNodes}/${totalNodes} nodes reporting`
}

/**
 * Parenthesised coverage suffix for one reading — `null` when coverage is
 * full, or when the sample carries no node counts at all (a single node's
 * history: `used: null` already says whether it reported).
 */
export function coverageSuffix(
  reportingNodes: number | null,
  totalNodes: number | null,
): string | null {
  if (reportingNodes === null || totalNodes === null) return null
  if (reportingNodes >= totalNodes) return null
  return `${reportingNodes}/${totalNodes} nodes reporting`
}
