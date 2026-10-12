import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { InterruptionBadge } from './interruption-badge'

describe('InterruptionBadge', () => {
  const now = new Date('2026-01-01T00:00:00Z')

  it('shows how long until the provider reclaims the host', () => {
    render(<InterruptionBadge deadline={new Date('2026-01-01T00:02:00Z')} now={now} />)
    expect(screen.getByText('interruption in 2m')).toBeInTheDocument()
  })

  it('reads as past once the deadline has gone by', () => {
    render(<InterruptionBadge deadline={new Date('2025-12-31T23:59:00Z')} now={now} />)
    expect(screen.getByText('interruption 1m ago')).toBeInTheDocument()
  })
})
