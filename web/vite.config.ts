/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { tanstackRouter } from '@tanstack/router-plugin/vite'
import path from 'node:path'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    // Must run before the react plugin: generates src/routeTree.gen.ts from src/routes/.
    tanstackRouter({ target: 'react', autoCodeSplitting: true }),
    react(),
    tailwindcss(),
  ],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src'),
    },
  },
  server: {
    // Honor a harness/CI-assigned port (e.g. Claude Code preview autoPort);
    // vite only reads ports from the CLI/config, never the environment.
    port: process.env.PORT ? Number(process.env.PORT) : undefined,
    proxy: {
      // Forward the JSON API to a live coordinator's client listener
      // (ADR 0031, default `:7070`) so `npm run dev` can drive a real
      // `coppice dev` cluster instead of the in-browser mock — set
      // COPPICE_API_ADDR to point at a different replica/host, e.g.
      // `COPPICE_API_ADDR=http://127.0.0.1:7071 npm run dev`. In production
      // (and if `VITE_COPPICE_MOCK` forces the mock client) nothing calls
      // `/api/v1/...` from this dev-only proxy — it does not apply to
      // `vite build` output, which is instead embedded and served
      // same-origin by the coordinator binary itself (see README.md).
      '/api/v1': {
        target: process.env.COPPICE_API_ADDR ?? 'http://127.0.0.1:7070',
        changeOrigin: true,
      },
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['src/test-setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
  },
})
