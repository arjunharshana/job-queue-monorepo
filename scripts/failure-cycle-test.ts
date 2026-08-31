import 'dotenv/config';
import { JobQueue } from '@jobqueue/core';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function runFailureLifecycleTest() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) throw new Error('DATABASE_URL is missing');
  
  const queue = new JobQueue({ connectionString });
  const workerId = 'test-worker-failure-cycle';

  try {
    const job = await queue.enqueue({
      queueName: 'failure_test',
      payload: { note: 'this job will be deliberately failed to death' },
    });
    console.log(`Enqueued job ${job.id}, max_attempts=${job.max_attempts}`);

    let previousRunAt: Date | null = null;
    let cycle = 0;

    while (true) {
      cycle++;
      const claimed = await queue.claim('failure_test', workerId, 30);
      
      if (!claimed) {
        process.stdout.write('.'); // Print a dot while waiting
        await sleep(1000);
        continue;
      }
      
      console.log(`\nCycle ${cycle}: claimed job, attempts=${claimed.attempts}`);
      await queue.fail(claimed.id, workerId, `deliberate failure #${cycle}`);

      // Inspect the raw row directly to verify the backoff
      const check = await queue['pool'].query(
        'SELECT status, attempts, run_at FROM jobs WHERE id = $1',
        [job.id]
      );
      const row = check.rows[0];
      
      console.log(`  -> status=${row.status}, attempts=${row.attempts}, run_at=${row.run_at.toISOString()}`);

      if (previousRunAt && row.status === 'pending') {
        const grew = row.run_at.getTime() > previousRunAt.getTime();
        console.log(`  -> run_at increased vs previous cycle: ${grew}`);
      }
      previousRunAt = row.run_at;

      if (row.status === 'dead') {
        console.log('Job reached DEAD status. Test complete.');
        break;
      }

      // Calculate how long to wait until the backoff expires
      const waitMs = Math.max(0, row.run_at.getTime() - Date.now()) + 200;
      console.log(`  -> waiting ${Math.round(waitMs / 1000)}s for backoff to elapse...`);
      await sleep(waitMs);
    }

    // Fetch and print the audit trail
    console.log('\n--- Final Job Events Audit Trail ---');
    const events = await queue['pool'].query(
      'SELECT event_type, attempt_number, created_at FROM job_events WHERE job_id = $1 ORDER BY created_at ASC',
      [job.id]
    );
    
    events.rows.forEach((e: { event_type: string, attempt_number: number, created_at: Date }) => {
      console.log(`[${e.created_at.toISOString()}] ${e.event_type}`);
    });
  } finally {
    await queue.close();
  }
}

runFailureLifecycleTest();