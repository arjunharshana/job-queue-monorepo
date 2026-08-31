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

public async complete(jobId: string, workerId: string): Promise<boolean> {
  const client = await this.pool.connect();
  try {
    await client.query('BEGIN');

    const updateQuery = `
      UPDATE jobs
      SET status = 'completed',
          locked_at = NULL,
          locked_by = NULL,
          lease_expires_at = NULL,
          updated_at = NOW()
      WHERE id = $1 AND locked_by = $2 AND status = 'active'
      RETURNING id;
    `;
    const { rows } = await client.query(updateQuery, [jobId, workerId]);

    if (rows.length === 0) {
      await client.query('ROLLBACK');
      return false; // lease was already reassigned elsewhere
    }

    await client.query(
      `INSERT INTO job_events (job_id, event_type, worker_id) VALUES ($1, 'completed', $2)`,
      [jobId, workerId]
    );

    await client.query('COMMIT');
    return true;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

public async fail(jobId: string, workerId: string, errorMessage: string): Promise<void> {
  const client = await this.pool.connect();
  try {
    await client.query('BEGIN');

    const selectQuery = `
      SELECT attempts, max_attempts FROM jobs
      WHERE id = $1 AND locked_by = $2 AND status = 'active'
      FOR UPDATE;
    `;
    const { rows } = await client.query(selectQuery, [jobId, workerId]);

    if (rows.length === 0) {
      await client.query('ROLLBACK');
      return;
    }

    const { attempts, max_attempts } = rows[0];
    const isDead = attempts >= max_attempts;

    if (isDead) {
      await client.query(
        `UPDATE jobs
         SET status = 'dead', last_error = $1, locked_at = NULL, locked_by = NULL,
             lease_expires_at = NULL, updated_at = NOW()
         WHERE id = $2`,
        [errorMessage, jobId]
      );
      await client.query(
        `INSERT INTO job_events (job_id, event_type, worker_id, error_message)
         VALUES ($1, 'failed_dead', $2, $3)`,
        [jobId, workerId, errorMessage]
      );
    } else {
      // Exponential backoff with jitter: base 2s, doubling per attempt, capped at 5 min.
      const baseMs = 2000;
      const capMs = 5 * 60 * 1000;
      const backoffMs = Math.min(capMs, baseMs * 2 ** attempts);
      const jitterMs = Math.random() * backoffMs * 0.2; // +/-20% jitter
      const delayMs = backoffMs + jitterMs; 

      await client.query(
        `UPDATE jobs
         SET status = 'pending', last_error = $1, locked_at = NULL, locked_by = NULL,
             lease_expires_at = NULL, run_at = NOW() + ($2 * INTERVAL '1 millisecond'),
             updated_at = NOW()
         WHERE id = $3`,
        [errorMessage, delayMs, jobId]
      );
      await client.query(
        `INSERT INTO job_events (job_id, event_type, worker_id, error_message)
         VALUES ($1, 'failed_retry', $2, $3)`,
        [jobId, workerId, errorMessage]
      );
    }

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
  
}