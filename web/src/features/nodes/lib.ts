import type { NodeUtilization } from '@/api/types'
import type { CapacityHistorySample } from '@/components'

/** True when a query error is an ApiError with code `NotFound`. */
export function isNotFound(error: unknown): boolean {
  return (
    typeof error === 'object' && error !== null && (error as { code?: unknown }).code === 'NotFound'
  )
}

/** Sorted `key=value` label badges input, stable by key. */
export function sortedLabels(labels: Record<string, string>): Array<[string, string]> {
  return Object.entries(labels).sort(([a], [b]) => a.localeCompare(b))
}

/**
 * Project a node's utilization history onto the shared capacity-history
 * shape. The endpoint carries the node's advertised `capacity` once (it is
 * a property of the node, not of the sample) while `allocated`/`used` vary
 * per sample; `used: null` (not measured at that instant) is carried
 * through untouched.
 */
export function nodeCapacityHistory(utilization: NodeUtilization): CapacityHistorySample[] {
  return utilization.samples.map((s) => ({
    t: s.t,
    capacity: utilization.capacity,
    allocated: s.allocated,
    used: s.used,
  }))
}
