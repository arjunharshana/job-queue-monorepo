import { config } from 'dotenv';
import { Redis } from 'ioredis';
import { JobQueue, EnqueueOptions, JsonValue } from '@jobqueue/core';
import { fileURLToPath } from 'url';
import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
config({ path: path.resolve(__dirname, '../../../.env') });

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error('DATABASE_URL environment variable is missing');
}

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
const redis = new Redis(redisUrl);
const queue = new JobQueue({ connectionString });

async function startSyncer() {
  console.log('[Syncer] Started, watching Redis job_buffer:pending...');
  
  while (true) {
    try {
      // Block for up to 5 seconds waiting for the first job
      const popped = await redis.blpop('job_buffer:pending', 5);
      if (!popped) continue;
      
      const jobs: EnqueueOptions<JsonValue>[] = [JSON.parse(popped[1])];

      const rest = await redis.lpop('job_buffer:pending', 99);
      if (rest) {
        jobs.push(...rest.map((j: string) => JSON.parse(j)));
      }
      
      await queue.enqueueBatch(jobs);
      console.log(`[Syncer] Flushed batch of ${jobs.length} jobs to Postgres.`);
    } catch (err) {
      console.error('[Syncer] Error flushing batch:', err);
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

startSyncer();