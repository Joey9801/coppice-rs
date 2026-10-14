import { useEffect, useState } from 'react'

/**
 * The current time, refreshed every `intervalMs` so relative times and
 * runtimes keep advancing without waiting for an unrelated re-render.
 */
export function useNow(intervalMs = 1000): Date {
  const [now, setNow] = useState(() => new Date())

  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])

  return now
}
