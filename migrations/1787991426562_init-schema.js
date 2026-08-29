exports.up = (pgm) => {
  pgm.sql(`
    CREATE TYPE job_status AS ENUM ('pending', 'active', 'completed', 'dead');

    CREATE TABLE jobs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        queue_name VARCHAR(255) NOT NULL,
        payload JSONB NOT NULL DEFAULT '{}',

        status job_status NOT NULL DEFAULT 'pending',
        attempts INT NOT NULL DEFAULT 0,
        max_attempts INT NOT NULL DEFAULT 3,

        priority INT NOT NULL DEFAULT 0,
        run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

        locked_at TIMESTAMPTZ,
        locked_by VARCHAR(255),
        lease_expires_at TIMESTAMPTZ,

        last_error TEXT,

        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE job_events (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        job_id UUID NOT NULL REFERENCES jobs(id),
        event_type VARCHAR(50) NOT NULL,
        attempt_number INT NOT NULL DEFAULT 0,
        worker_id VARCHAR(255),
        error_message TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ language 'plpgsql';

    CREATE TRIGGER update_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

    CREATE INDEX idx_jobs_claim ON jobs (queue_name, priority ASC, run_at ASC) WHERE status = 'pending';
    CREATE INDEX idx_jobs_reaper ON jobs (lease_expires_at ASC) WHERE status = 'active';
    CREATE INDEX idx_job_events_job_id ON job_events (job_id);
  `);
};

exports.down = (pgm) => {
  pgm.sql(`
    DROP TABLE IF EXISTS job_events;
    DROP TABLE IF EXISTS jobs;
    DROP TYPE IF EXISTS job_status;
    DROP FUNCTION IF EXISTS update_updated_at_column;
  `);
};