import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { JobQueue } from '../src/queue.js';
import { testPool } from './setup.js';

describe('JobQueue.enqueue', () => {
  let queue: JobQueue;

  beforeAll(() => {
    const connectionString = process.env.TEST_DATABASE_URL!;
    queue = new JobQueue({ connectionString });
  });

  afterAll(async () => {
    await queue.close();
  });

  console.log('queue DB URL:', process.env.TEST_DATABASE_URL);

  it('creates a job with correct defaults and a created event', async () => {
    const job = await queue.enqueue({
      queueName: 'test_queue',
      payload: { hello: 'world' },
    });

    expect(job.status).toBe('pending');
    expect(job.attempts).toBe(0);
    expect(job.max_attempts).toBe(3);
    expect(job.priority).toBe(0);
    expect(job.payload).toEqual({ hello: 'world' });

    const { rows } = await testPool.query(
      'SELECT event_type FROM job_events WHERE job_id = $1',
      [job.id]
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].event_type).toBe('created');
  });

  it('respects custom priority and maxAttempts', async () => {
    const job = await queue.enqueue({
      queueName: 'test_queue',
      payload: {},
      priority: 5,
      maxAttempts: 10,
    });

    expect(job.priority).toBe(5);
    expect(job.max_attempts).toBe(10);
  });
});