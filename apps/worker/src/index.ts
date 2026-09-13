import { config } from 'dotenv';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { Redis } from 'ioredis';
import { JobQueue } from '@jobqueue/core';
import { workerConfig } from './config.js';
import { WorkerLoop } from './worker-loop.js';
import './handlers.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
config({ path: path.resolve(__dirname, '../../../.env') });

const queue = new JobQueue({ connectionString: workerConfig.databaseUrl });
const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');
const loop = new WorkerLoop(queue, redis);

let reaperTimer: NodeJS.Timeout;

function startReaper(): void {
  reaperTimer = setInterval(async () => {
    try {
      const reapedJobs = await queue.reapStaleJobs();
      if (reapedJobs.length > 0) {
        console.log(`[${workerConfig.workerId}] Reaped ${reapedJobs.length} stale job(s)`);
        for (const job of reapedJobs) {
          await redis.publish(
            'job_events:broadcast',
            JSON.stringify({ jobId: job.id, event: job.status === 'dead' ? 'reaped_dead' : 'reaped_retry' })
          );
        }
      }
    } catch (error) {
      console.error(`[${workerConfig.workerId}] Reaper error:`, error);
    }
  }, workerConfig.reapIntervalMs);
}

async function main(): Promise<void> {
  console.log(
    `[${workerConfig.workerId}] Starting. queue=${workerConfig.queueName} concurrency=${workerConfig.concurrency}`
  );
  startReaper();
  await loop.run();
  clearInterval(reaperTimer);
  await queue.close();
  redis.disconnect();
  console.log(`[${workerConfig.workerId}] Shut down cleanly`);
  process.exit(0);
}

function handleShutdown(signal: string): void {
  console.log(`[${workerConfig.workerId}] Received ${signal}, draining in-flight jobs...`);
  loop.stop();
}

process.on('SIGINT', () => handleShutdown('SIGINT'));
process.on('SIGTERM', () => handleShutdown('SIGTERM'));

main().catch((error) => {
  console.error('Fatal worker error:', error);
  process.exit(1);
});