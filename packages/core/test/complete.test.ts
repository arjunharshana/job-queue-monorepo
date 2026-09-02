import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { JobQueue } from '../src/queue.js';
import { testPool } from './setup.js';

describe('JobQueue.complete', () => {
  let queue: JobQueue;

  beforeAll(() => {
    const connectionString = process.env.TEST_DATABASE_URL!;
    queue = new JobQueue({ connectionString });
  });

  afterAll(async () => {
    await queue.close();
  });

  it('marks an active job completed and clears lock fields', async () => {
    const enqueued = await queue.enqueue({ queueName: 'complete_test', payload: {} });
    const claimed = await queue.claim('complete_test', 'worker-1', 30);

    const success = await queue.complete(claimed!.id, 'worker-1');
    expect(success).toBe(true);

    const job = await queue.getJob(enqueued.id);
    expect(job!.status).toBe('completed');
    expect(job!.locked_by).toBeNull();
    expect(job!.locked_at).toBeNull();
    expect(job!.lease_expires_at).toBeNull();

    const { rows } = await testPool.query(
      `SELECT event_type FROM job_events WHERE job_id = $1 ORDER BY created_at ASC`,
      [enqueued.id]
    );
    expect(rows.map((r) => r.event_type)).toEqual(['created', 'claimed', 'completed']);
  });

  it('returns false and makes no changes when called by a worker that does not own the lock', async () => {
    const enqueued = await queue.enqueue({ queueName: 'complete_test', payload: {} });
    const claimed = await queue.claim('complete_test', 'worker-1', 30);

    // A different worker (e.g. one that lost its lease) tries to complete it.
    const success = await queue.complete(claimed!.id, 'worker-2');
    expect(success).toBe(false);

    const job = await queue.getJob(enqueued.id);
    expect(job!.status).toBe('active'); // unchanged
    expect(job!.locked_by).toBe('worker-1'); // still owned by the real claimant

    const { rows } = await testPool.query(
      `SELECT event_type FROM job_events WHERE job_id = $1 ORDER BY created_at ASC`,
      [enqueued.id]
    );
    expect(rows.map((r) => r.event_type)).toEqual(['created', 'claimed']); // no 'completed' event
  });

  it('returns false when the job is not currently active', async () => {
    const enqueued = await queue.enqueue({ queueName: 'complete_test', payload: {} });
    // Never claimed — still pending.

    const success = await queue.complete(enqueued.id, 'worker-1');
    expect(success).toBe(false);
  });
});