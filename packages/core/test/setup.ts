import 'dotenv/config';
import { Pool } from 'pg';
import { beforeAll, afterAll, beforeEach } from 'vitest';

const connectionString = process.env.TEST_DATABASE_URL;

if (!connectionString) {
  throw new Error('TEST_DATABASE_URL environment variable is missing.');
}

export const testPool = new Pool({ connectionString });

beforeAll(async () => {
  await testPool.query('SELECT 1');
});

beforeEach(async () => {
  await testPool.query('TRUNCATE jobs, job_events RESTART IDENTITY CASCADE');
});

afterAll(async () => {
  await testPool.end();
});