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
