import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { JobQueue } from '../src/queue.js';
import { testPool } from './setup.js';

async function expireLease(jobId: string): Promise<void> {
  await testPool.query(
    `UPDATE jobs SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
    [jobId]
  );
}

describe('JobQueue.reapStaleJobs', () => {
  let queue: JobQueue;

  beforeAll(() => {
    const connectionString = process.env.TEST_DATABASE_URL!;
    queue = new JobQueue({ connectionString });
  });

  afterAll(async () => {
    await queue.close();
  });

  it('does nothing when there are no expired leases', async () => {
    const enqueued = await queue.enqueue({ queueName: 'reaper_test', payload: {} });
    await queue.claim('reaper_test', 'worker-1', 30);

    const reapedCount = await queue.reapStaleJobs();
    expect(reapedCount).toBe(0);

    const job = await queue.getJob(enqueued.id);
    expect(job!.status).toBe('active');
  });

  it('retries an expired lease with backoff, then marks dead after max_attempts', async () => {
    const enqueued = await queue.enqueue({
      queueName: 'reaper_test',
      payload: {},
      maxAttempts: 3,
    });

    let previousRunAt: Date | null = null;

    for (let cycle = 1; cycle <= 3; cycle++) {
      const claimed = await queue.claim('reaper_test', `worker-${cycle}`, 30);
      expect(claimed).not.toBeNull();
      expect(claimed!.attempts).toBe(cycle);

      await expireLease(claimed!.id);

      const reapedCount = await queue.reapStaleJobs();
      expect(reapedCount).toBeGreaterThanOrEqual(1);

      const job = await queue.getJob(enqueued.id);

      if (cycle < 3) {
        expect(job!.status).toBe('pending');

        if (previousRunAt) {
          expect(job!.run_at.getTime()).toBeGreaterThan(previousRunAt.getTime());
        }
        previousRunAt = job!.run_at;

        await testPool.query(`UPDATE jobs SET run_at = NOW() WHERE id = $1`, [enqueued.id]);
      } else {
        expect(job!.status).toBe('dead');
      }
    }

    const { rows } = await testPool.query(
      `SELECT event_type FROM job_events WHERE job_id = $1 ORDER BY created_at ASC`,
      [enqueued.id]
    );
    expect(rows.map((r) => r.event_type)).toEqual([
      'created',
      'claimed',
      'reaped_retry',
      'claimed',
      'reaped_retry',
      'claimed',
      'reaped_dead',
    ]);
  });

  it('reaps multiple expired jobs across different queues in one pass', async () => {
    const jobA = await queue.enqueue({ queueName: 'reaper_test_a', payload: {} });
    const jobB = await queue.enqueue({ queueName: 'reaper_test_b', payload: {} });

    const claimedA = await queue.claim('reaper_test_a', 'worker-a', 30);
    const claimedB = await queue.claim('reaper_test_b', 'worker-b', 30);

    await expireLease(claimedA!.id);
    await expireLease(claimedB!.id);

    const reapedCount = await queue.reapStaleJobs();
    expect(reapedCount).toBe(2);

    expect((await queue.getJob(jobA.id))!.status).toBe('pending');
    expect((await queue.getJob(jobB.id))!.status).toBe('pending');
  });
});