/// <reference types="vite/client" />

interface ImportMetaEnv {
  /**
   * Build-time mock switch (see README.md "Mock vs. real data"). Truthy
   * ("1" / "true") forces the mock `CoppiceApi` client for every method,
   * real endpoints included — set it in `.env`/`.env.local` or the shell,
   * e.g. `VITE_COPPICE_MOCK=1 npm run dev`. Read only as the direct
   * `import.meta.env.VITE_COPPICE_MOCK` expression in `src/api/index.ts` so
   * Vite can statically eliminate whichever client branch is unused from a
   * production build.
   */
  readonly VITE_COPPICE_MOCK?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
