import 'dotenv/config';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    setupFiles: ['./packages/core/test/setup.ts'],
    globals: false,
    testTimeout: 10000,
    fileParallelism: false,
  },
});