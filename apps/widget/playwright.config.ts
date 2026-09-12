import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './tests', testMatch: '*.spec.ts', workers: 1, retries: 0,
  use: { baseURL: 'http://127.0.0.1:4173', headless: true },
  webServer: { command: 'node scripts/harness.mjs', url: 'http://127.0.0.1:4173', reuseExistingServer: false },
});
