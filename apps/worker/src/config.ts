function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} environment variable is missing`);
  }
  return value;
}

function optionalEnvInt(name: string, defaultValue: number): number {
  const raw = process.env[name];
  if (!raw) return defaultValue;
  const parsed = Number.parseInt(raw, 10);
  if (Number.isNaN(parsed)) {
    throw new Error(`${name} must be a valid integer, got: ${raw}`);
  }
  return parsed;
}

export const workerConfig = {
  databaseUrl: requireEnv('DATABASE_URL'),
  queueName: requireEnv('WORKER_QUEUE_NAME'),
  workerId: process.env.WORKER_ID ?? `worker-${process.pid}-${Date.now()}`,
  concurrency: optionalEnvInt('WORKER_CONCURRENCY', 5),
  leaseSeconds: optionalEnvInt('WORKER_LEASE_SECONDS', 30),
  pollIntervalMs: optionalEnvInt('WORKER_POLL_INTERVAL_MS', 1000),
  reapIntervalMs: optionalEnvInt('WORKER_REAP_INTERVAL_MS', 15000),
  shutdownTimeoutMs: optionalEnvInt('WORKER_SHUTDOWN_TIMEOUT_MS', 30000),
};