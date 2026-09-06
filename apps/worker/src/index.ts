import { config } from 'dotenv';
import { JobQueue, Job } from '@jobqueue/core';
import { randomUUID } from 'crypto';

config({ path: '../../.env' });

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error('DATABASE_URL environment variable is missing');
}

const queue = new JobQueue({ connectionString });
const workerId = `worker-${randomUUID().split('-')[0]}`;
const QUEUE_NAME = 'email_notifications';
const POLL_INTERVAL_MS = 2000;
const REAPER_INTERVAL_MS = 30000; 

let isShuttingDown = false;
let reaperTimer: NodeJS.Timeout;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function startReaper() {
  reaperTimer = setInterval(async () => {
    try {
      const reapedCount = await queue.reapStaleJobs();
      if (reapedCount > 0) {
        console.log(`[${workerId}] Reaped ${reapedCount} zombie jobs and marked them DEAD.`);
      }
    } catch (error) {
      console.error(`[${workerId}] Reaper failed:`, error);
    }
  }, REAPER_INTERVAL_MS);
}

async function startWorker() {
  console.log(`[${workerId}] Worker started. Polling queue: '${QUEUE_NAME}'`);
  
  startReaper();

  while (!isShuttingDown) {
    try {
      const job = await queue.claim(QUEUE_NAME, workerId, 60);

      if (!job) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      try {
        console.log(`[${workerId}] Processing job ${job.id}...`);
        await sleep(1500);
        await queue.complete(job.id, workerId);
        console.log(`[${workerId}] Job ${job.id} completed successfully.`);
      } catch (error: any) {
        await queue.fail(job.id, workerId, error.message);
        console.error(`[${workerId}] Job ${job.id} failed: ${error.message}`);
      }
    } catch (error) {
      console.error(`[${workerId}] Critical queue error:`, error);
      await sleep(POLL_INTERVAL_MS); 
    }
  }

  console.log(`[${workerId}] Worker closed gracefully.`);
  clearInterval(reaperTimer);
  await queue.close();
  process.exit(0);
}

function handleShutdown(signal: string) {
  console.log(`\n[${workerId}] Received ${signal}. Finishing active job before exiting...`);
  isShuttingDown = true;
}

process.on('SIGINT', () => handleShutdown('SIGINT'));
process.on('SIGTERM', () => handleShutdown('SIGTERM'));

startWorker();