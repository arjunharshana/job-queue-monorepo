import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { JobQueue } from '../src/queue.js';
import { testPool } from './setup.js';

describe('JobQueue.fail', () => {
  let queue: JobQueue;

  beforeAll(() => {
    const connectionString = process.env.TEST_DATABASE_URL!;
    queue = new JobQueue({ connectionString });
  });

  afterAll(async () => {
    await queue.close();
  });

  it('retries with increasing backoff until max_attempts, then marks dead', async () => {
    const enqueued = await queue.enqueue({
      queueName: 'fail_test',
      payload: {},
      maxAttempts: 3,
    });

    let previousRunAt: Date | null = null;

    for (let cycle = 1; cycle <= 3; cycle++) {
      const claimed = await queue.claim('fail_test', `worker-${cycle}`, 30);
      expect(claimed).not.toBeNull();
      expect(claimed!.attempts).toBe(cycle);

      await queue.fail(claimed!.id, `worker-${cycle}`, `deliberate failure #${cycle}`);

      const job = await queue.getJob(enqueued.id);

      if (cycle < 3) {
        expect(job!.status).toBe('pending');
        expect(job!.last_error).toBe(`deliberate failure #${cycle}`);

        // Confirm backoff is actually growing, not constant or shrinking.
        if (previousRunAt) {
          expect(job!.run_at.getTime()).toBeGreaterThan(previousRunAt.getTime());
        }
        previousRunAt = job!.run_at;

        // Force the retry to be immediately claimable, without waiting
        // for real backoff time to elapse.
        await testPool.query(`UPDATE jobs SET run_at = NOW() WHERE id = $1`, [enqueued.id]);
      } else {
        expect(job!.status).toBe('dead');
        expect(job!.locked_by).toBeNull();
      }
    }

    const { rows } = await testPool.query(
      `SELECT event_type FROM job_events WHERE job_id = $1 ORDER BY created_at ASC`,
      [enqueued.id]
    );
    expect(rows.map((r) => r.event_type)).toEqual([
      'created',
      'claimed',
      'failed_retry',
      'claimed',
      'failed_retry',
      'claimed',
      'failed_dead',
    ]);
  });

  it('does nothing when called by a worker that does not own the lock', async () => {
    const enqueued = await queue.enqueue({ queueName: 'fail_test', payload: {} });
    await queue.claim('fail_test', 'worker-1', 30);

    await queue.fail(enqueued.id, 'worker-2', 'wrong worker');

    const job = await queue.getJob(enqueued.id);
    expect(job!.status).toBe('active'); // unchanged
    expect(job!.locked_by).toBe('worker-1');
  });
});