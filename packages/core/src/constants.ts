export const DEFAULT_MAX_ATTEMPTS = 3;
export const DEFAULT_LEASE_SECONDS = 30;

export const BACKOFF_BASE_MS = 2000;
export const BACKOFF_CAP_MS = 5 * 60 * 1000; // 5 minutes
export const BACKOFF_JITTER_FRACTION = 0.2; // +/- 20%

export const REAPER_ERROR_MESSAGE = 'Lease expired — worker presumed dead';

export const JOB_STATUS = {
  PENDING: 'pending',
  ACTIVE: 'active',
  COMPLETED: 'completed',
  DEAD: 'dead',
} as const;

export const JobEventType = {
  CREATED: 'created',
  CLAIMED: 'claimed',
  COMPLETED: 'completed',
  FAILED_RETRY: 'failed_retry',
  FAILED_DEAD: 'failed_dead',
  REAPED_RETRY: 'reaped_retry',
  REAPED_DEAD: 'reaped_dead',
} as const;

export type JobEventType = (typeof JobEventType)[keyof typeof JobEventType];