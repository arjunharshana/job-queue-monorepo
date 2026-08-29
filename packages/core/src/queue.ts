import { Pool, PoolConfig } from 'pg';
import { Job, EnqueueOptions, JsonValue } from './types.js';

export class JobQueue {
  private pool: Pool;

  constructor(config: PoolConfig) {
    this.pool = new Pool(config);
  }

  public async close(): Promise<void> {
    await this.pool.end();
  }

  public async enqueue<T extends JsonValue>(options: EnqueueOptions<T>): Promise<Job<T>> {
    const client = await this.pool.connect();
    
    try {
      await client.query('BEGIN');

      const jobInsertQuery = `
        INSERT INTO jobs (
          queue_name, payload, priority, max_attempts, run_at
        )
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *;
      `;
      
      const jobValues = [
        options.queueName,
        JSON.stringify(options.payload),
        options.priority ?? 0,
        options.maxAttempts ?? 3,
        options.runAt ?? new Date()
      ];
      
      const { rows: jobRows } = await client.query(jobInsertQuery, jobValues);
      const job = jobRows[0] as Job<T>;

      const eventInsertQuery = `
        INSERT INTO job_events (job_id, event_type)
        VALUES ($1, $2);
      `;
      await client.query(eventInsertQuery, [job.id, 'created']);

      await client.query('COMMIT');
      return job;

    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}