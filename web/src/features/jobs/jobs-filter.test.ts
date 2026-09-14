import { describe, expect, it } from 'vitest'
import { buildFilter } from './jobs-filter'

describe('buildFilter metadata leaf', () => {
  it('treats a bare key as a presence test', () => {
    expect(buildFilter({ mkey: 'ticket' })).toEqual({ metadata: { key: 'ticket' } })
  })

  it('degrades to presence when the value is empty', () => {
    expect(buildFilter({ mkey: 'ticket', mval: '' })).toEqual({ metadata: { key: 'ticket' } })
  })

  it('emits exact string equality for a non-empty value', () => {
    expect(buildFilter({ mkey: 'ticket', mval: 'INC-1234' })).toEqual({
      metadata: { key: 'ticket', equals: 'INC-1234' },
    })
  })

  it('never interprets the value — a numeric-looking operand stays a string', () => {
    expect(buildFilter({ mkey: 'attempt_budget', mval: '3' })).toEqual({
      metadata: { key: 'attempt_budget', equals: '3' },
    })
  })

  it('ignores a value with no key', () => {
    expect(buildFilter({ mval: 'INC-1234' })).toBeUndefined()
  })

  it('ANDs the metadata leaf with the other filters', () => {
    expect(buildFilter({ state: 'Running', mkey: 'name' })).toEqual({
      all: [{ phase: { in: ['Running'] } }, { metadata: { key: 'name' } }],
    })
  })

  it('is absent when no param is set', () => {
    expect(buildFilter({})).toBeUndefined()
  })
})
