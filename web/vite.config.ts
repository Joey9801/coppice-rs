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
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['src/test-setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
  },
})
