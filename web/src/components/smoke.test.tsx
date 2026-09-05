import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { logController, logEntry, logPage } from '@/test/log-fixtures'
import { KeyValueGrid } from './key-value-grid'
import { LogViewer } from './log-viewer'
import { StatePill } from './state-pill'

describe('shared components smoke test', () => {
  it('renders StatePill, KeyValueGrid and LogViewer with visible text', () => {
    const logs = logController({
      data: logPage([
        logEntry({ level: 'error', target: 'scheduler', message: 'placement failed' }),
      ]),
    })

    render(
      <div>
        <StatePill state="Running" />
        <KeyValueGrid items={[{ label: 'Image', value: 'busybox:latest' }]} />
        <LogViewer controller={logs} />
      </div>,
    )

    expect(screen.getByText('Running')).toBeInTheDocument()
    expect(screen.getByText('Image')).toBeInTheDocument()
    expect(screen.getByText('busybox:latest')).toBeInTheDocument()
    expect(screen.getByText('placement failed')).toBeInTheDocument()
  })
})
