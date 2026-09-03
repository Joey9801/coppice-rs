import { describe, expect, it } from 'vitest'
import type { HostFacts } from '@/api/types'
import { formatHostKernel, formatHostOs, formatNodeCapacity } from './host-facts'

function host(overrides: Partial<HostFacts> = {}): HostFacts {
  return {
    os: 'macos',
    osVersion: '15.5',
    kernelVersion: '24.5.0',
    arch: 'aarch64',
    cpuModel: 'Apple M2 Pro',
    physicalCores: 8,
    logicalCores: 8,
    totalMemoryBytes: 16 * 1024 ** 3,
    totalDiskBytes: 512 * 1024 ** 3,
    agentVersion: '0.0.1',
    ...overrides,
  }
}

describe('host facts display', () => {
  it('labels bare macOS values from older agents', () => {
    const facts = host()
    expect(formatHostOs(facts)).toBe('macOS 15.5')
    expect(formatHostKernel(facts)).toBe('Darwin 24.5.0')
  })

  it('does not duplicate labels and preserves Linux PRETTY_NAME', () => {
    expect(formatHostOs(host({ osVersion: 'macOS 15.5', kernelVersion: 'Darwin 24.5.0' }))).toBe(
      'macOS 15.5',
    )
    expect(
      formatHostKernel(host({ osVersion: 'macOS 15.5', kernelVersion: 'Darwin 24.5.0' })),
    ).toBe('Darwin 24.5.0')
    expect(
      formatHostOs(
        host({ os: 'linux', osVersion: 'Debian GNU/Linux 12 (bookworm)', kernelVersion: '6.1.0' }),
      ),
    ).toBe('Debian GNU/Linux 12 (bookworm)')
    expect(formatHostKernel(host({ os: 'linux', kernelVersion: '6.1.0' }))).toBe('Linux 6.1.0')
  })

  it('labels other Unix families and keeps missing values honest', () => {
    expect(formatHostOs(host({ os: 'freebsd', osVersion: '14.1-RELEASE' }))).toBe(
      'FreeBSD 14.1-RELEASE',
    )
    expect(formatHostKernel(host({ os: 'solaris', kernelVersion: '5.11' }))).toBe('SunOS 5.11')
    expect(formatHostOs(host({ os: 'macos', osVersion: '' }))).toBe('macOS')
    expect(formatHostKernel(host({ os: 'macos', kernelVersion: '' }))).toBeNull()
  })

  it('includes disk capacity in the node-list display value', () => {
    expect(
      formatNodeCapacity({
        cpuMillis: 16_000,
        memoryBytes: 64 * 1024 ** 3,
        diskBytes: 2 * 1024 ** 4,
      }),
    ).toBe('16 cores · 64 GiB · 2 TiB')
  })
})
