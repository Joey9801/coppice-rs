/**
 * Quota-entity naming rules (ADR 0045), shared by the entity form's
 * client-side validation and the mock world's apply-time checks so both
 * reject exactly what the server rejects.
 *
 * A name is one path **segment**; a path is the ancestors' segments root
 * first, joined by `/`, never stored — always derived from the parent chain.
 */

/** Longest permitted segment, in characters. */
export const ENTITY_SEGMENT_MAX_CHARS = 63

const SEGMENT_CHARS = /^[A-Za-z0-9._-]+$/
const ALNUM = /^[A-Za-z0-9]/

/**
 * Whether `s` parses as a `QuotaEntityId` the way `coppice_core::id` does:
 * the literal `quota-` prefix, then anything `Uuid::try_parse` accepts. Of
 * its forms only the hyphenated and the 32-hex "simple" one can be spelled
 * with segment characters, so those are the two that matter here.
 */
export function isQuotaEntityId(s: string): boolean {
  if (!s.startsWith('quota-')) return false
  const rest = s.slice('quota-'.length)
  return (
    /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(rest) ||
    /^[0-9a-fA-F]{32}$/.test(rest)
  )
}

/**
 * Why `name` is not a valid segment, as a sentence fit to show a person, or
 * `null` when it is valid. The rules: 1–63 characters from `[A-Za-z0-9._-]`,
 * the first alphanumeric, and not itself a `quota-<uuid>` id (so a reference
 * string is never ambiguous between an id and a one-segment path).
 */
export function entitySegmentError(name: string): string | null {
  if (name.length === 0) return 'Name is required.'
  if (name.includes('/')) {
    return 'A name is a single segment — no "/". Pick the parent to nest it.'
  }
  if (name.length > ENTITY_SEGMENT_MAX_CHARS) {
    return `Name must be at most ${ENTITY_SEGMENT_MAX_CHARS} characters.`
  }
  if (!SEGMENT_CHARS.test(name)) {
    return 'Use only letters, digits, ".", "_" and "-".'
  }
  if (!ALNUM.test(name)) return 'Name must start with a letter or digit.'
  if (isQuotaEntityId(name)) return 'Name must not look like an entity id (quota-<uuid>).'
  return null
}

export function isValidEntitySegment(name: string): boolean {
  return entitySegmentError(name) === null
}

/** `parentPath/segment`, or just `segment` for a root. */
export function joinEntityPath(parentPath: string | null, segment: string): string {
  return parentPath ? `${parentPath}/${segment}` : segment
}
