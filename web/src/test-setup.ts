import { cleanup } from '@testing-library/react'
import { afterEach } from 'vitest'
import '@testing-library/jest-dom/vitest'

// Vitest `globals` are off (every test imports its helpers explicitly), so
// testing-library cannot register its own auto-cleanup: without this, a
// render leaks into the next test's `screen` queries.
afterEach(() => {
  cleanup()
})
