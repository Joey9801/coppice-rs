import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { describe, expect, it, vi } from 'vitest'
import { useJobLogs, useNodeLogs, useCoordinatorLogs } from './queries'
import { api } from './index'
import { logPage } from '@/test/log-fixtures'
vi.mock('./index', () => ({
  api: { getJobLogs: vi.fn(), getNodeLogs: vi.fn(), getCoordinatorLogs: vi.fn() },
}))

describe('shared log query wiring', () => {
  it.each([
    [useJobLogs, 'getJobLogs'],
    [useNodeLogs, 'getNodeLogs'],
    [useCoordinatorLogs, 'getCoordinatorLogs'],
  ] as const)('forwards window options through a source-scoped query', async (hook, method) => {
    vi.mocked(api[method]).mockResolvedValue(logPage([], { live: false }))
    const client = new QueryClient()
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    )
    const { result, rerender, unmount } = renderHook(({ id }) => hook(id), {
      wrapper,
      initialProps: { id: 'source-a' },
    })
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(api[method]).toHaveBeenLastCalledWith('source-a', null, { order: 'desc', limit: 200 })
    act(() => result.current.setMode('head'))
    await waitFor(() =>
      expect(api[method]).toHaveBeenLastCalledWith('source-a', null, { order: 'asc', limit: 200 }),
    )
    rerender({ id: 'source-b' })
    await waitFor(() =>
      expect(api[method]).toHaveBeenLastCalledWith('source-b', null, { order: 'asc', limit: 200 }),
    )
    unmount()
    client.clear()
  })
})
