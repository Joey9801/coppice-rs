import type { ReactNode } from 'react'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { AttemptPreemption } from '@/api/types'
import { outcomePill } from '@/components'
import { AttemptPreemptionDetail } from './attempt-preemption'

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

const NOW = new Date('2026-01-01T00:10:00Z')
const OTHER_JOB = 'job-00000000-0000-0000-0000-000000000002'

function record(overrides: Partial<AttemptPreemption> = {}): AttemptPreemption {
  return {
    reason: 'Priority',
    forJob: OTHER_JOB,
    requestedAt: new Date('2026-01-01T00:09:00Z'),
    notifiedAt: new Date('2026-01-01T00:09:30Z'),
    deadline: new Date('2026-01-01T00:12:00Z'),
    noticeShortfallSeconds: 0,
    ...overrides,
  }
}

describe('AttemptPreemptionDetail', () => {
  it('shows the reason, the job it made way for, notified at and the deadline', () => {
    render(<AttemptPreemptionDetail preemption={record()} now={NOW} />)
    expect(screen.getByText('priority')).toBeInTheDocument()
    expect(screen.getByText(/made way for/)).toBeInTheDocument()
    expect(screen.getByRole('link').getAttribute('href')).toContain(OTHER_JOB)
    expect(screen.getByText('30s ago')).toBeInTheDocument()
    expect(screen.getByText('in 2m')).toBeInTheDocument()
  })

  it('hides a zero shortfall and shows a non-zero one', () => {
    const { rerender } = render(<AttemptPreemptionDetail preemption={record()} now={NOW} />)
    expect(screen.queryByText(/notice short by/)).toBeNull()
    rerender(
      <AttemptPreemptionDetail preemption={record({ noticeShortfallSeconds: 90 })} now={NOW} />,
    )
    expect(screen.getByText('notice short by 1m 30s')).toBeInTheDocument()
  })

  it('omits "made way for" for node-driven reasons', () => {
    render(
      <AttemptPreemptionDetail
        preemption={record({ reason: 'Interruption', forJob: null })}
        now={NOW}
      />,
    )
    expect(screen.getByText('interruption')).toBeInTheDocument()
    expect(screen.queryByText(/made way for/)).toBeNull()
  })

  it('says the notice is pending until the agent acknowledges it', () => {
    render(
      <AttemptPreemptionDetail
        preemption={record({ notifiedAt: null, deadline: null, noticeShortfallSeconds: null })}
        now={NOW}
      />,
    )
    expect(screen.getByText('notice not yet delivered')).toBeInTheDocument()
    expect(screen.queryByText(/deadline/)).toBeNull()
  })
})

describe('outcomePill for a preempted attempt', () => {
  it('labels the outcome with its reason', () => {
    render(
      <>
        {outcomePill({
          kind: 'Preempted',
          class: 'Platform',
          reason: 'Relocation',
          forJob: OTHER_JOB,
        })}
      </>,
    )
    expect(screen.getByText('Preempted (relocation)')).toBeInTheDocument()
  })
})
