import type { ReactNode } from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { JobDetail, JobSpec } from '@/api/types'
import { JobSpecCard } from './job-spec-card'

// TanStack Router `Link` needs a router context we don't set up in unit
// tests; render it as a plain anchor encoding `to`/`params` in the href.
vi.mock('@tanstack/react-router', () => ({
  Link: ({
    to,
    params,
    children,
    ...rest
  }: {
    to: string
    params?: Record<string, string>
    children?: ReactNode
  }) => (
    <a href={`${to}?${new URLSearchParams(params).toString()}`} {...rest}>
      {children}
    </a>
  ),
}))

function spec(overrides: Partial<JobSpec> = {}): JobSpec {
  return {
    image: 'busybox',
    command: [],
    entrypoint: null,
    env: {},
    requests: { cpuMillis: 100, memoryBytes: 1, diskBytes: 1 },
    priority: 0,
    maxRuntimeSeconds: null,
    quotaEntity: 'quota-00000000-0000-0000-0000-000000000001',
    quotaEntityPath: 'acme/team-a',
    retry: { maxRetries: 0, retryUserErrors: false },
    ...overrides,
  }
}

function job(overrides: Partial<JobSpec> = {}): JobDetail {
  return { spec: spec(overrides), entityChain: [] } as unknown as JobDetail
}

describe('JobSpecCard env', () => {
  it('shows "none" when the job declared no env', () => {
    render(<JobSpecCard job={job()} />)
    expect(screen.getByText('none')).toBeInTheDocument()
  })

  it('lists env var names sorted ascending, behind a disclosure toggle', () => {
    render(<JobSpecCard job={job({ env: { ZETA: '1', ALPHA: '2', MID: '3' } })} />)
    fireEvent.click(screen.getByText('3 variables'))
    const names = screen.getAllByText(/^(ZETA|ALPHA|MID)$/).map((el) => el.textContent)
    expect(names).toEqual(['ALPHA', 'MID', 'ZETA'])
  })

  it('preserves whitespace inside a value', () => {
    render(<JobSpecCard job={job({ env: { MSG: 'a  b\nc' } })} />)
    fireEvent.click(screen.getByText('1 variable'))
    const value = screen.getByText((_, el) => el?.tagName === 'DD' && el.textContent === 'a  b\nc')
    expect(value).toHaveClass('whitespace-pre-wrap')
  })
})
