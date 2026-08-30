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

  public async claim<T extends JsonValue>(
    queueName: string,
    workerId: string,
    leaseSeconds: number = 30
  ): Promise<Job<T> | null> {
    const client = await this.pool.connect();
    
    try {
      await client.query('BEGIN');

      const claimQuery = `
        WITH next_job AS (
            SELECT id FROM jobs
            WHERE queue_name = $1 
              AND status = 'pending' 
              AND run_at <= NOW()
            ORDER BY priority ASC, run_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs
        SET 
            status = 'active',
            locked_at = NOW(),
            locked_by = $2,
            lease_expires_at = NOW() + ($3 * INTERVAL '1 second'),
            updated_at = NOW(),
            attempts = attempts + 1
        WHERE id = (SELECT id FROM next_job)
        RETURNING *;
      `;
      
      const { rows } = await client.query(claimQuery, [queueName, workerId, leaseSeconds]);
      
      if (rows.length === 0) {
        await client.query('COMMIT');
        return null; // Queue is empty
      }

      const job = rows[0] as Job<T>;

      const eventInsertQuery = `
        INSERT INTO job_events (job_id, event_type, worker_id)
        VALUES ($1, $2, $3);
      `;
      await client.query(eventInsertQuery, [job.id, 'claimed', workerId]);

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