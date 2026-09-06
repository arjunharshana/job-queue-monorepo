import { config } from 'dotenv';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { JobQueue } from '@jobqueue/core';
import { workerConfig } from './config.js';
import { WorkerLoop } from './worker-loop.js';
import './handlers.js'; // registers handlers as a side effect of import

const __dirname = path.dirname(fileURLToPath(import.meta.url));
config({ path: path.resolve(__dirname, '../../../.env') });

const queue = new JobQueue({ connectionString: workerConfig.databaseUrl });
const loop = new WorkerLoop(queue);

let reaperTimer: NodeJS.Timeout;

function startReaper(): void {
  reaperTimer = setInterval(async () => {
    try {
      const reapedCount = await queue.reapStaleJobs();
      if (reapedCount > 0) {
        console.log(`[${workerConfig.workerId}] Reaped ${reapedCount} stale job(s)`);
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