import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { AccrualView, AllocationView } from '@/api/types'
import { JobAccrualPanel } from './job-accrual-panel'

// The panel's id link uses the router; these tests render it bare.
vi.mock('@tanstack/react-router', () => ({
  Link: () => null,
}))

function allocation(overrides: Partial<AllocationView> = {}): AllocationView {
  return {
    id: 'alloc-00000000-0000-0000-0000-000000000001',
    job: 'job-00000000-0000-0000-0000-000000000001',
    attempt: 'attempt-00000000-0000-0000-0000-000000000001',
    node: 'node-00000000-0000-0000-0000-000000000001',
    requested: { cpuMillis: 8_000, memoryBytes: 4 * 1024 ** 3, diskBytes: 0 },
    funded: { cpuMillis: 2_000, memoryBytes: 1024 ** 3, diskBytes: 0 },
    state: 'Accruing',
    seq: 3,
    ...overrides,
  }
}

function accrual(projectedStart: Date | null): AccrualView {
  return {
    allocation: allocation(),
    fundedFraction: { cpu: 0.25, memory: 0.25, disk: 1 },
    projectedStart,
  }
}

describe('JobAccrualPanel projected start', () => {
  it('shows a bounded projection as a concrete time', () => {
    // Half an hour out: the exact wording depends on render timing, so
    // assert the "in <duration>" shape rather than a frozen string.
    const projected = new Date(Date.now() + 30 * 60 * 1000)
    render(<JobAccrualPanel accrual={accrual(projected)} />)

    expect(screen.getByText(/Projected start/)).toBeInTheDocument()
    expect(screen.getByText(/^in \d/)).toBeInTheDocument()
    // The bounded case never carries the unbounded warning.
    expect(screen.queryByText(/unbounded/)).not.toBeInTheDocument()
  })

  it('says so honestly when the projection is unbounded', () => {
    render(<JobAccrualPanel accrual={accrual(null)} />)

    expect(
      screen.getByText(/unbounded — no guaranteed release covers this yet/),
    ).toBeInTheDocument()
  })
})
