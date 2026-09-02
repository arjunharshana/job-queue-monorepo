import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { JobQueue } from '../src/queue.js';
import { testPool } from './setup.js';

describe('JobQueue.claim', () => {
  let queue: JobQueue;

  beforeAll(() => {
    const connectionString = process.env.TEST_DATABASE_URL!;
    queue = new JobQueue({ connectionString, max: 20 });
  });

  afterAll(async () => {
    await queue.close();
  });

  it('returns null when the queue is empty', async () => {
    const result = await queue.claim('empty_queue', 'worker-1', 30);
    expect(result).toBeNull();
  });

  it('claims a pending job and marks it active with lock fields set', async () => {
    const enqueued = await queue.enqueue({
      queueName: 'claim_test',
      payload: { n: 1 },
    });

    const claimed = await queue.claim('claim_test', 'worker-1', 30);

    expect(claimed).not.toBeNull();
    expect(claimed!.id).toBe(enqueued.id);
    expect(claimed!.status).toBe('active');
    expect(claimed!.attempts).toBe(1);
    expect(claimed!.locked_by).toBe('worker-1');
    expect(claimed!.lease_expires_at).not.toBeNull();

    const { rows } = await testPool.query(
      `SELECT event_type FROM job_events WHERE job_id = $1 ORDER BY created_at ASC`,
      [enqueued.id]
    );
    expect(rows.map((r) => r.event_type)).toEqual(['created', 'claimed']);
  });

  it('does not claim a job scheduled for the future', async () => {
    const future = new Date(Date.now() + 60_000);
    await queue.enqueue({
      queueName: 'claim_test',
      payload: {},
      runAt: future,
    });

    const claimed = await queue.claim('claim_test', 'worker-1', 30);
    expect(claimed).toBeNull();
  });

  it('never allows two concurrent claimers to receive the same job', async () => {
    const NUM_JOBS = 5;
    const NUM_WORKERS = 15;

    for (let i = 0; i < NUM_JOBS; i++) {
      await queue.enqueue({ queueName: 'concurrency_test', payload: { i } });
    }

    const results = await Promise.all(
      Array.from({ length: NUM_WORKERS }, (_, i) =>
        queue.claim('concurrency_test', `worker-${i + 1}`, 30)
      )
    );

    const claimedIds = results.filter((r) => r !== null).map((r) => r!.id);

    expect(claimedIds).toHaveLength(NUM_JOBS);
    expect(new Set(claimedIds).size).toBe(NUM_JOBS); // zero duplicates

    const { rows } = await testPool.query(
      `SELECT status, COUNT(*)::int AS count FROM jobs WHERE queue_name = 'concurrency_test' GROUP BY status`
    );
    expect(rows).toEqual([{ status: 'active', count: NUM_JOBS }]);
  });
});