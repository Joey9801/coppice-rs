import { vi } from 'vitest'
import type { LogController } from '@/api/log-controller'
import type { LogEntry, LogPage } from '@/api/types'

export function logEntry(extra: Partial<LogEntry> = {}): LogEntry {
  return {
    id: 'entry',
    at: '2026-01-01T12:34:56.000000Z',
    attempt: null,
    stream: null,
    truncated: false,
    target: 'agent',
    message: 'output',
    ...extra,
  }
}
export function logPage(entries: LogEntry[] = [], extra: Partial<LogPage> = {}): LogPage {
  return { entries, nextCursor: null, resumeCursor: null, sources: [], live: true, ...extra }
}
type SupportedController = Omit<LogController, 'data'> & { data: LogPage }
export function logController(extra: Partial<SupportedController> = {}): SupportedController {
  return {
    data: logPage(),
    loading: false,
    polling: false,
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
  }
}
