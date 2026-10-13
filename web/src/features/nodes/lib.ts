import type { NodeDrain, NodeUtilization } from '@/api/types'
import { formatTimeUntil } from '@/lib/format'
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

/**
 * Compact status of an operator drain (ADR 0044 §5, §7): `complete` (the node
 * holds no live work), `empty in 2h` style bounded emptying time (a regular
 * drain's horizon, or a deadline drain's effective time), or `unbounded`
 * (some live work has no enforced bound, nothing new is admitted). A deadline
 * drain is prefixed `deadline` so it reads differently from a regular one.
 */
export function drainStatus(drain: NodeDrain, now: Date = new Date()): string {
  if (drain.complete) return 'complete'
  const prefix = drain.deadline === null ? '' : 'deadline: '
  if (drain.emptyBy === null) return `${prefix}unbounded`
  return `${prefix}empty ${formatTimeUntil(drain.emptyBy, now)}`
}
