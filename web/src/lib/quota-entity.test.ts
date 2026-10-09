import { describe, expect, it } from 'vitest'
import {
  entitySegmentError,
  isQuotaEntityId,
  isValidEntitySegment,
  joinEntityPath,
} from './quota-entity'

const UUID = '3f9c2e10-1a2b-4c3d-8e5f-6a7b8c9d0e1f'

describe('entity segment grammar (ADR 0045)', () => {
  it.each(['acme', 'a', '9', 'team-1.x_y', 'Platform', 'quota-foo', 'a'.repeat(63)])(
    'accepts %s',
    (name) => {
      expect(entitySegmentError(name)).toBeNull()
      expect(isValidEntitySegment(name)).toBe(true)
    },
  )

  it.each([
    '',
    'a'.repeat(64),
    '-x',
    '.x',
    '_x',
    'a/b',
    'a b',
    'a@b',
    'café',
    `quota-${UUID}`,
    `quota-${UUID.replaceAll('-', '')}`,
    `quota-${UUID.toUpperCase()}`,
  ])('rejects %j', (name) => {
    expect(entitySegmentError(name)).not.toBeNull()
    expect(isValidEntitySegment(name)).toBe(false)
  })

  it('explains a slash as "one segment"', () => {
    expect(entitySegmentError('a/b')).toMatch(/single segment/)
  })
})

describe('isQuotaEntityId', () => {
  it('parses the hyphenated and simple uuid forms after the quota- prefix', () => {
    expect(isQuotaEntityId(`quota-${UUID}`)).toBe(true)
    expect(isQuotaEntityId(`quota-${UUID.replaceAll('-', '')}`)).toBe(true)
  })

  it('rejects other prefixes and non-uuid tails', () => {
    expect(isQuotaEntityId(`job-${UUID}`)).toBe(false)
    expect(isQuotaEntityId('quota-foo')).toBe(false)
    expect(isQuotaEntityId('acme/eng')).toBe(false)
  })
})

describe('joinEntityPath', () => {
  it('joins under a parent and leaves a root bare', () => {
    expect(joinEntityPath('acme/eng', 'platform')).toBe('acme/eng/platform')
    expect(joinEntityPath(null, 'acme')).toBe('acme')
  })
})
