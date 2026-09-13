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

const DEAD_LETTER_KEY = 'job_buffer:malformed';

function safeParse(raw: string): EnqueueOptions<JsonValue> | null {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function startSyncer() {
  console.log('[Syncer] Started, watching Redis job_buffer:pending...');

  while (true) {
    try {
      const popped = await redis.blpop('job_buffer:pending', 5);
      if (!popped) continue;

      const rawItems = [popped[1]];
      const rest = await redis.lpop('job_buffer:pending', 99);
      if (rest) rawItems.push(...rest);

      const jobs: EnqueueOptions<JsonValue>[] = [];
      for (const raw of rawItems) {
        const parsed = safeParse(raw);
        if (parsed) {
          jobs.push(parsed);
        } else {
          // Don't silently drop malformed data — quarantine it for inspection.
          console.error('[Syncer] Malformed job in buffer, quarantining:', raw);
          await redis.rpush(DEAD_LETTER_KEY, raw);
        }
      }

      if (jobs.length > 0) {
        await queue.enqueueBatch(jobs);
        console.log(`[Syncer] Flushed batch of ${jobs.length} jobs to Postgres.`);
      }
    } catch (err) {
      console.error('[Syncer] Error flushing batch:', err);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

startSyncer();