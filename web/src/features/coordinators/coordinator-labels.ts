import type { CoordinatorId } from '@/api/types'

/**
 * Raft ids are random 64-bit integers (ADR 0025), so a full "coordinator
 * 7234980239847293847" label overflows tiles and is impossible to scan.
 * Members are instead shown as a short mono tag — `c-` plus a decimal prefix
 * of the id — with the full canonical identity available through the tooltip
 * and the copy affordance.
 *
 * Prefixes are computed for the whole member set at once and stretched until
 * every member's tag is unique, so two members can never collide on the same
 * short label. Ids shorter than the minimum prefix are shown in full.
 */
export function shortCoordinatorLabels(ids: CoordinatorId[]): Map<CoordinatorId, string> {
  const items = ids.map((id) => ({ id, digits: id.toString() }))
  const MIN_PREFIX = 4
  const labels = new Map<CoordinatorId, string>()
  for (const { id, digits } of items) {
    let len = Math.min(digits.length, MIN_PREFIX)
    while (
      len < digits.length &&
      items.some((o) => o.id !== id && o.digits.startsWith(digits.slice(0, len)))
    ) {
      len += 1
    }
    labels.set(id, len >= digits.length ? digits : `${digits.slice(0, len)}…`)
  }
  return labels
}
