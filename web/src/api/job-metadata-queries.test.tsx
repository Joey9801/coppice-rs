import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { describe, expect, it, vi } from 'vitest'
import { useReplaceJobMetadata, useUpdateJobMetadata } from './queries'
import { api } from './index'

vi.mock('./index', () => ({
  api: { replaceJobMetadata: vi.fn(), updateJobMetadata: vi.fn() },
}))

const JOB = 'job-00000000-0000-0000-0000-000000000001'

function harness() {
  const client = new QueryClient()
  const invalidate = vi.spyOn(client, 'invalidateQueries')
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  )
  return { client, invalidate, wrapper }
}

describe('job metadata mutations', () => {
  it('sends a patch and invalidates the job and every jobs listing', async () => {
    vi.mocked(api.updateJobMetadata).mockResolvedValue({} as never)
    const { client, invalidate, wrapper } = harness()
    const { result } = renderHook(() => useUpdateJobMetadata(), { wrapper })

    result.current.mutate({ id: JOB, set: { name: 'nightly-train-42' }, unset: ['stale'] })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(api.updateJobMetadata).toHaveBeenCalledWith(JOB, {
      set: { name: 'nightly-train-42' },
      unset: ['stale'],
    })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['job', JOB] })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['jobs'] })
    client.clear()
  })

  it('sends a full replacement map and invalidates the same keys', async () => {
    vi.mocked(api.replaceJobMetadata).mockResolvedValue({} as never)
    const { client, invalidate, wrapper } = harness()
    const { result } = renderHook(() => useReplaceJobMetadata(), { wrapper })

    result.current.mutate({ id: JOB, metadata: { oncall: 'yes' } })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(api.replaceJobMetadata).toHaveBeenCalledWith(JOB, { oncall: 'yes' })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['job', JOB] })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['jobs'] })
    client.clear()
  })
})
