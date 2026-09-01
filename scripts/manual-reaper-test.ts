import 'dotenv/config';
import { JobQueue } from '@jobqueue/core';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function runReaperTest() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) throw new Error('DATABASE_URL is missing');

  const queue = new JobQueue({ connectionString });

  try {
    const job = await queue.enqueue({
      queueName: 'reaper_test',
      payload: { action: 'crash_simulation' },
    });
    console.log(`Enqueued job ${job.id}, max_attempts=${job.max_attempts}`);

    let cycle = 0;
    while (true) {
      cycle++;
      const claimed = await queue.claim('reaper_test', `crashing-worker-${cycle}`, 2);

      if (!claimed) {
        console.log(`Cycle ${cycle}: nothing claimable, waiting...`);
        await sleep(1000);
        continue;
      }

      console.log(
        `Cycle ${cycle}: claimed by crashing-worker-${cycle}, attempts=${claimed.attempts}, lease_expires_at=${claimed.lease_expires_at?.toISOString()}`
      );
      console.log('  -> simulating hard crash (never calling complete/fail)...');
      await sleep(3000);

      const reapedCount = await queue.reapStaleJobs();
      console.log(`  -> reaper ran, reaped ${reapedCount} job(s) globally`);

      const check = await queue['pool'].query(
        'SELECT status, attempts, run_at FROM jobs WHERE id = $1',
        [job.id]
      );
      const row = check.rows[0];
      console.log(`  -> job status=${row.status}, attempts=${row.attempts}`);

      if (row.status === 'dead') {
        console.log('Job reached DEAD status via reaping. Test complete.');
        break;
      }

    }

    console.log('\n--- Final Job Events Audit Trail ---');
    const events = await queue['pool'].query(
      'SELECT event_type, created_at FROM job_events WHERE job_id = $1 ORDER BY created_at ASC',
      [job.id]
    );
    events.rows.forEach((e: { event_type: string; created_at: Date }) => {
      console.log(`[${e.created_at.toISOString()}] ${e.event_type}`);
    });
  } finally {
    await queue.close();
  }
}

runReaperTest();