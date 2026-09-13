import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { JobQueue } from '../src/queue.js';
import { testPool } from './setup.js';

describe('JobQueue.enqueueBatch', () => {
  let queue: JobQueue;

  beforeAll(() => {
    const connectionString = process.env.TEST_DATABASE_URL!;
    queue = new JobQueue({ connectionString });
  });

  afterAll(async () => {
    await queue.close();
  });

  it('does nothing for an empty array', async () => {
    await expect(queue.enqueueBatch([])).resolves.toBeUndefined();
    const { rows } = await testPool.query(`SELECT COUNT(*)::int AS count FROM jobs`);
    expect(rows[0].count).toBe(0);
  });

  it('inserts multiple jobs and a created event for each, in one transaction', async () => {
    await queue.enqueueBatch([
      { queueName: 'batch_test', payload: { i: 1 } },
      { queueName: 'batch_test', payload: { i: 2 } },
      { queueName: 'batch_test', payload: { i: 3 } },
    ]);

    const { rows: jobs } = await testPool.query(
      `SELECT payload, status FROM jobs WHERE queue_name = 'batch_test' ORDER BY (payload->>'i')::int ASC`
    );
    expect(jobs).toHaveLength(3);
    expect(jobs.every((j) => j.status === 'pending')).toBe(true);

    const { rows: events } = await testPool.query(
      `SELECT event_type, COUNT(*)::int AS count FROM job_events GROUP BY event_type`
    );
    expect(events).toEqual([{ event_type: 'created', count: 3 }]);
  });

  it('respects per-job priority and maxAttempts within a batch', async () => {
    await queue.enqueueBatch([
      { queueName: 'batch_test_2', payload: {}, priority: 5, maxAttempts: 7 },
    ]);

    const { rows } = await testPool.query(
      `SELECT priority, max_attempts FROM jobs WHERE queue_name = 'batch_test_2'`
    );
    expect(rows[0]).toEqual({ priority: 5, max_attempts: 7 });
  });
});