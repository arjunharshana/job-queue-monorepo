import { BACKOFF_BASE_MS, BACKOFF_CAP_MS, BACKOFF_JITTER_FRACTION } from './constants.js';

export function computeBackoffMs(attempts: number): number {
  const backoffMs = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** attempts);
  const jitterMs = Math.random() * backoffMs * BACKOFF_JITTER_FRACTION;
  return backoffMs + jitterMs;
}

export const CLEAR_LOCK_FIELDS_SQL = `
  locked_at = NULL,
  locked_by = NULL,
  lease_expires_at = NULL
`;