import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { LogController } from '@/api/log-controller'
import { LogViewer } from './log-viewer'
import type { LogEntry } from '@/api/types'

const entries: LogEntry[] = ['debug', 'info', 'warn', 'error'].map((level, i) => ({
  id: level,
  t: new Date('2026-01-01T12:34:56Z'),
  level: level as LogEntry['level'],
  target: 'agent',
  message: `message ${i}`,
}))
const controller = (extra: Partial<LogController> = {}): LogController => ({
  data: { entries, nextCursor: null, live: true },
  loading: false,
  error: null,
  mode: 'tail',
  setMode: vi.fn(),
  limit: 200,
  setLimit: vi.fn(),
  playing: true,
  togglePlaying: vi.fn(),
  hasOlder: true,
  hasNewer: true,
  loadOlder: vi.fn(async () => {}),
  loadNewer: vi.fn(async () => {}),
  retry: vi.fn(async () => {}),
  ...extra,
})
beforeEach(() => {
  HTMLDialogElement.prototype.showModal = function () {
    this.open = true
  }
})

describe('log viewer', () => {
  it('toggles UI timestamps and applies inclusive structured thresholds, never job severity', () => {
    const { rerender } = render(<LogViewer entries={entries} structured />)
    expect(screen.queryByText('message 0')).not.toBeInTheDocument()
    expect(screen.getByText('message 1')).toBeVisible()
    fireEvent.change(screen.getByLabelText('Verbosity'), { target: { value: 'warn' } })
    expect(screen.queryByText('message 1')).not.toBeInTheDocument()
    expect(screen.getByText('message 2')).toBeVisible()
    expect(screen.getByText('message 3')).toBeVisible()
    fireEvent.change(screen.getByLabelText('Verbosity'), { target: { value: 'debug' } })
    expect(screen.getByText('message 0')).toBeVisible()
    fireEvent.click(screen.getByLabelText('Timestamps'))
    expect(screen.queryByText(/12:34/)).not.toBeInTheDocument()
    rerender(<LogViewer entries={entries} />)
    expect(screen.queryByLabelText('Verbosity')).not.toBeInTheDocument()
    expect(screen.queryByText('ERROR')).not.toBeInTheDocument()
  })

  it('wires both boundaries and source controls', () => {
    const logs = controller()
    render(<LogViewer controller={logs} />)
    expect(screen.getByRole('button', { name: 'Tail' })).toHaveAttribute('aria-pressed', 'true')
    fireEvent.click(screen.getByText('Head'))
    expect(logs.setMode).toHaveBeenCalledWith('head')
    fireEvent.click(screen.getByLabelText('Pause log updates'))
    expect(logs.togglePlaying).toHaveBeenCalledOnce()
    fireEvent.click(screen.getByText('Load older lines'))
    fireEvent.click(screen.getByText('Load newer lines'))
    expect(logs.loadOlder).toHaveBeenCalledOnce()
    expect(logs.loadNewer).toHaveBeenCalledOnce()
  })

  it('uses a modal dialog, Escape restores focus, and unsupported controls are disabled', async () => {
    const { rerender } = render(<LogViewer controller={controller()} />)
    fireEvent.click(screen.getByLabelText('Maximize log viewer'))
    const modal = screen.getByRole('dialog')
    expect(modal).toHaveAttribute('open')
    fireEvent(modal, new Event('cancel', { bubbles: true, cancelable: true }))
    await waitFor(() => expect(screen.getByLabelText('Maximize log viewer')).toHaveFocus())
    rerender(
      <LogViewer
        controller={controller({ data: { entries: [], nextCursor: null, unsupported: true } })}
        structured
      />,
    )
    expect(screen.getByText('Log collection is not available for this source.')).toBeVisible()
    expect(screen.getByText('Tail')).toBeDisabled()
    expect(screen.getByLabelText('Verbosity')).toBeDisabled()
  })

  it('follows only at the bottom while playing and preserves prepended scroll position', () => {
    const logs = controller()
    const { rerender } = render(<LogViewer controller={logs} />)
    const output = screen.getByLabelText('Log output')
    let height = 1000
    Object.defineProperties(output, {
      scrollHeight: { get: () => height },
      clientHeight: { value: 200 },
    })
    output.scrollTop = 800
    fireEvent.scroll(output)
    rerender(<LogViewer controller={{ ...logs, data: { ...logs.data, entries: [...entries] } }} />)
    expect(output.scrollTop).toBe(1000)
    output.scrollTop = 200
    fireEvent.scroll(output)
    height = 1200
    rerender(<LogViewer controller={{ ...logs, data: { ...logs.data, entries: [...entries] } }} />)
    expect(output.scrollTop).toBe(200)
    fireEvent.click(screen.getByText('Load older lines'))
    height = 1500
    rerender(
      <LogViewer
        controller={{ ...logs, playing: false, data: { ...logs.data, entries: [...entries] } }}
      />,
    )
    expect(output.scrollTop).toBe(500)
    output.scrollTop = 1300
    fireEvent.scroll(output)
    height = 1700
    rerender(
      <LogViewer
        controller={{ ...logs, playing: false, data: { ...logs.data, entries: [...entries] } }}
      />,
    )
    expect(output.scrollTop).toBe(1300)
  })
})
