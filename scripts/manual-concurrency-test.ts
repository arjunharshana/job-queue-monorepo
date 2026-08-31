import 'dotenv/config';
import { JobQueue } from '@jobqueue/core';

async function runConcurrencyTest() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) throw new Error('DATABASE_URL is missing');
  
  const queue = new JobQueue({ connectionString, max: 20 }); 

  try {
    console.log('Enqueueing 5 test jobs...');
    for (let i = 0; i < 5; i++) {
      await queue.enqueue({
        queueName: 'load_test',
        payload: { taskNumber: i }
      });
    }

    console.log('Firing off 15 concurrent claim requests in the same millisecond...');
    
    // Create 15 simultaneous promises
    const workerPromises = Array.from({ length: 15 }).map(async (_, index) => {
      const workerId = `worker-${index + 1}`;
      const job = await queue.claim('load_test', workerId, 30);
      return { workerId, claimedJobId: job?.id || null };
    });

    const results = await Promise.all(workerPromises);
    
    const claimed = results.filter(r => r.claimedJobId !== null);
    const missed = results.filter(r => r.claimedJobId === null);

    console.log(`\n--- RESULTS ---`);
    console.log(`Jobs claimed: ${claimed.length} (Expected: 5)`);
    console.log(`Empty hands: ${missed.length} (Expected: 10)`);

    const uniqueJobs = new Set(claimed.map(r => r.claimedJobId));
    if (uniqueJobs.size === claimed.length) {
      console.log('SKIP LOCKED VERIFIED: 0 duplicate claims across all workers!');
    } else {
      console.error('DANGER: Duplicate claims detected. SKIP LOCKED failed.');
    }

  } catch (err) {
    console.error('Test failed:', err);
  } finally {
    await queue.close();
  }
}

runConcurrencyTest();