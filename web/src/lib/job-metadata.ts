/**
 * Job metadata limits (ADR 0042).
 *
 * The server enforces every limit here twice (at the API edge for
 * admission, again at apply); this module is the client-side copy so the
 * inline editor can reject an oversized key or value before proposing a
 * write that would only come back `INVALID_ARGUMENT`. The numbers must
 * stay in step with the ADR's table.
 */
import type { JobMetadata } from '@/api/types'

/** Key: 1–64 bytes of ASCII letters, digits, `.`, `_`, `-`, `/`, `:`. */
export const JOB_METADATA_KEY_PATTERN = /^[A-Za-z0-9._\-/:]{1,64}$/

/** One value, in UTF-8 bytes; empty is allowed. */
export const JOB_METADATA_MAX_VALUE_BYTES = 1024

/** Keys per job. */
export const JOB_METADATA_MAX_KEYS = 64

/** UTF-8 byte length of a string (limits are in bytes, not code units). */
export function utf8ByteLength(text: string): number {
  return new TextEncoder().encode(text).length
}

/** Human-readable reason the key is invalid, or null when it is fine. */
export function validateMetadataKey(key: string): string | null {
  if (key.length === 0) return 'Key must not be empty.'
  if (utf8ByteLength(key) > 64) return 'Key must be at most 64 bytes.'
  if (!JOB_METADATA_KEY_PATTERN.test(key)) {
    return 'Key may use only letters, digits and . _ - / : characters.'
  }
  return null
}

/** Human-readable reason the value is invalid, or null when it is fine. */
export function validateMetadataValue(value: string): string | null {
  const bytes = utf8ByteLength(value)
  if (bytes > JOB_METADATA_MAX_VALUE_BYTES) {
    return `Value is ${bytes} bytes; the limit is ${JOB_METADATA_MAX_VALUE_BYTES}.`
  }
  return null
}

/**
 * Validate a whole map (every key, every value and the key count). Returns
 * the first reason it is invalid, or null.
 */
export function validateMetadataMap(metadata: JobMetadata): string | null {
  const keys = Object.keys(metadata)
  if (keys.length > JOB_METADATA_MAX_KEYS) {
    return `A job may carry at most ${JOB_METADATA_MAX_KEYS} metadata keys.`
  }
  for (const key of keys) {
    const keyError = validateMetadataKey(key)
    if (keyError) return keyError
    const valueError = validateMetadataValue(metadata[key] as string)
    if (valueError) return `${key}: ${valueError}`
  }
  return null
}
