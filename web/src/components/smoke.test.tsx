import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import type { LogEntry } from '@/api/types'
import { KeyValueGrid } from './key-value-grid'
import { LogViewer } from './log-viewer'
import { StatePill } from './state-pill'

describe('shared components smoke test', () => {
  it('renders StatePill, KeyValueGrid and LogViewer with visible text', () => {
    const logs: LogEntry[] = [
      {
        t: new Date(1_720_000_000_000),
        level: 'error',
        target: 'scheduler',
        message: 'placement failed',
      },
    ]

    render(
      <div>
        <StatePill state="Running" />
        <KeyValueGrid items={[{ label: 'Image', value: 'busybox:latest' }]} />
        <LogViewer entries={logs} />
      </div>,
    )

    expect(screen.getByText('Running')).toBeInTheDocument()
    expect(screen.getByText('Image')).toBeInTheDocument()
    expect(screen.getByText('busybox:latest')).toBeInTheDocument()
    expect(screen.getByText('placement failed')).toBeInTheDocument()
  })
})
