/**
 * Shape predicates for metadata string values (ADR 0042) — the pure half of
 * `metadata-value.tsx`, kept in its own module so the rendering file exports
 * components only.
 */

/** A typed Coppice id (ADR 0024): `<prefix>-<uuid>`. */
const TYPED_ID_PATTERN =
  /^(job|node|quota|alloc|attempt|group|cluster|machine|token)-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

/**
 * True when `s` parses as a URL whose scheme is `http:` or `https:`. Only
 * those two become links — `javascript:`, `data:`, `file:` and friends are
 * shown as plain text.
 */
export function isExternalUrl(s: string): boolean {
  try {
    const url = new URL(s)
    return url.protocol === 'http:' || url.protocol === 'https:'
  } catch {
    return false
  }
}

/** True when `s` matches the typed-id shape (ADR 0024) for a routable prefix. */
export function isTypedCoppiceId(s: string): boolean {
  return TYPED_ID_PATTERN.test(s)
}
