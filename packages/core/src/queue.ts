import { Pool, PoolConfig } from 'pg';
import { Job, EnqueueOptions, JsonValue, JsonObject } from './types.js';
import {
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_LEASE_SECONDS,
  JOB_STATUS,
  JobEventType,
  REAPER_ERROR_MESSAGE,
} from './constants.js';
import { computeBackoffMs, CLEAR_LOCK_FIELDS_SQL } from './utils.js';

export class JobQueue {
  private pool: Pool;

  constructor(config: PoolConfig) {
    this.pool = new Pool(config);
  }

  public async close(): Promise<void> {
    await this.pool.end();
  }

  public async getJob<T extends JsonValue = JsonObject>(
    jobId: string
  ): Promise<Job<T> | null> {
    const { rows } = await this.pool.query('SELECT * FROM jobs WHERE id = $1', [jobId]);
    return rows[0] ?? null;
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
        options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
        options.runAt ?? new Date()
      ];

      const { rows: jobRows } = await client.query(jobInsertQuery, jobValues);
      const job = jobRows[0] as Job<T>;

      const eventInsertQuery = `
        INSERT INTO job_events (job_id, event_type)
        VALUES ($1, $2);
      `;
      await client.query(eventInsertQuery, [job.id, JobEventType.CREATED]);

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
    leaseSeconds: number = DEFAULT_LEASE_SECONDS
  ): Promise<Job<T> | null> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const claimQuery = `
        WITH next_job AS (
            SELECT id FROM jobs
            WHERE queue_name = $1 
              AND status = '${JOB_STATUS.PENDING}' 
              AND run_at <= NOW()
            ORDER BY priority ASC, run_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs
        SET 
            status = '${JOB_STATUS.ACTIVE}',
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
        return null;
      }

      const job = rows[0] as Job<T>;

      const eventInsertQuery = `
        INSERT INTO job_events (job_id, event_type, worker_id)
        VALUES ($1, $2, $3);
      `;
      await client.query(eventInsertQuery, [job.id, JobEventType.CLAIMED, workerId]);

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
        SET status = '${JOB_STATUS.COMPLETED}',
            ${CLEAR_LOCK_FIELDS_SQL},
            updated_at = NOW()
        WHERE id = $1 AND locked_by = $2 AND status = '${JOB_STATUS.ACTIVE}'
        RETURNING id;
      `;
      const { rows } = await client.query(updateQuery, [jobId, workerId]);

      if (rows.length === 0) {
        await client.query('ROLLBACK');
        return false;
      }

      await client.query(
        `INSERT INTO job_events (job_id, event_type, worker_id) VALUES ($1, $2, $3)`,
        [jobId, JobEventType.COMPLETED, workerId]
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
        WHERE id = $1 AND locked_by = $2 AND status = '${JOB_STATUS.ACTIVE}'
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
           SET status = '${JOB_STATUS.DEAD}', last_error = $1, 
               ${CLEAR_LOCK_FIELDS_SQL}, updated_at = NOW()
           WHERE id = $2`,
          [errorMessage, jobId]
        );
        await client.query(
          `INSERT INTO job_events (job_id, event_type, worker_id, error_message)
           VALUES ($1, $2, $3, $4)`,
          [jobId, JobEventType.FAILED_DEAD, workerId, errorMessage]
        );
      } else {
        const delayMs = computeBackoffMs(attempts);

        await client.query(
          `UPDATE jobs
           SET status = '${JOB_STATUS.PENDING}', last_error = $1, 
               ${CLEAR_LOCK_FIELDS_SQL}, run_at = NOW() + ($2 * INTERVAL '1 millisecond'),
               updated_at = NOW()
           WHERE id = $3`,
          [errorMessage, delayMs, jobId]
        );
        await client.query(
          `INSERT INTO job_events (job_id, event_type, worker_id, error_message)
           VALUES ($1, $2, $3, $4)`,
          [jobId, JobEventType.FAILED_RETRY, workerId, errorMessage]
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

  public async reapStaleJobs(): Promise<number> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const { rows: expired } = await client.query(
        `SELECT id, attempts, max_attempts
         FROM jobs
         WHERE status = '${JOB_STATUS.ACTIVE}' AND lease_expires_at < NOW()
         FOR UPDATE SKIP LOCKED`
      );

      for (const job of expired) {
        const isDead = job.attempts >= job.max_attempts;

        if (isDead) {
          await client.query(
            `UPDATE jobs
             SET status = '${JOB_STATUS.DEAD}', last_error = $1, 
                 ${CLEAR_LOCK_FIELDS_SQL}, updated_at = NOW()
             WHERE id = $2`,
            [REAPER_ERROR_MESSAGE, job.id]
          );
          await client.query(
            `INSERT INTO job_events (job_id, event_type, error_message)
             VALUES ($1, $2, $3)`,
            [job.id, JobEventType.REAPED_DEAD, REAPER_ERROR_MESSAGE]
          );
        } else {
          const delayMs = computeBackoffMs(job.attempts);

          await client.query(
            `UPDATE jobs
             SET status = '${JOB_STATUS.PENDING}', last_error = $1, 
                 ${CLEAR_LOCK_FIELDS_SQL}, run_at = NOW() + ($2 * INTERVAL '1 millisecond'),
                 updated_at = NOW()
             WHERE id = $3`,
            [REAPER_ERROR_MESSAGE, delayMs, job.id]
          );
          await client.query(
            `INSERT INTO job_events (job_id, event_type, error_message)
             VALUES ($1, $2, $3)`,
            [job.id, JobEventType.REAPED_RETRY, REAPER_ERROR_MESSAGE]
          );
        }
      }

      await client.query('COMMIT');
      return expired.length;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

}