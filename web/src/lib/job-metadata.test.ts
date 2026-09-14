import { describe, expect, it } from 'vitest'
import {
  JOB_METADATA_MAX_KEYS,
  JOB_METADATA_MAX_VALUE_BYTES,
  utf8ByteLength,
  validateMetadataKey,
  validateMetadataMap,
  validateMetadataValue,
} from './job-metadata'

describe('validateMetadataKey', () => {
  it('accepts the ADR charset', () => {
    expect(validateMetadataKey('pipeline.run/stage:2-a_b')).toBeNull()
  })

  it('rejects an empty key, a disallowed character and an over-long key', () => {
    expect(validateMetadataKey('')).not.toBeNull()
    expect(validateMetadataKey('has space')).not.toBeNull()
    expect(validateMetadataKey('k'.repeat(65))).not.toBeNull()
  })
})

describe('validateMetadataValue', () => {
  it('accepts an empty value and one exactly at the byte limit', () => {
    expect(validateMetadataValue('')).toBeNull()
    expect(validateMetadataValue('x'.repeat(JOB_METADATA_MAX_VALUE_BYTES))).toBeNull()
  })

  it('measures UTF-8 bytes, not code units', () => {
    // 'é' is two bytes, so 513 of them overflow a 1024-byte limit.
    expect(utf8ByteLength('é')).toBe(2)
    expect(validateMetadataValue('é'.repeat(512))).toBeNull()
    expect(validateMetadataValue('é'.repeat(513))).not.toBeNull()
  })

  it('rejects a value over 1024 bytes', () => {
    expect(validateMetadataValue('x'.repeat(JOB_METADATA_MAX_VALUE_BYTES + 1))).not.toBeNull()
  })
})

describe('validateMetadataMap', () => {
  it('accepts an empty map', () => {
    expect(validateMetadataMap({})).toBeNull()
  })

  it('rejects more than 64 keys', () => {
    const many = Object.fromEntries(
      Array.from({ length: JOB_METADATA_MAX_KEYS + 1 }, (_, i) => [`k${i}`, String(i)]),
    )
    expect(validateMetadataMap(many)).not.toBeNull()
  })

  it('accepts 64 keys at the value limit — there is no whole-map cap', () => {
    const many = Object.fromEntries(
      Array.from({ length: JOB_METADATA_MAX_KEYS }, (_, i) => [
        `k${i}`,
        'x'.repeat(JOB_METADATA_MAX_VALUE_BYTES),
      ]),
    )
    expect(validateMetadataMap(many)).toBeNull()
  })

  it('names the offending key in a value error', () => {
    expect(validateMetadataMap({ ticket: 'x'.repeat(2000) })).toMatch(/^ticket: /)
  })
})
