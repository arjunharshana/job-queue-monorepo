import { describe, it, expect } from 'vitest';
import { computeBackoffMs } from '../src/utils.js';
import { BACKOFF_BASE_MS, BACKOFF_CAP_MS } from '../src/constants.js';

describe('computeBackoffMs', () => {
  it('calculates base delay with up to 20% jitter for 0 attempts', () => {
    const ms = computeBackoffMs(0);
    expect(ms).toBeGreaterThanOrEqual(BACKOFF_BASE_MS);
    expect(ms).toBeLessThanOrEqual(BACKOFF_BASE_MS * 1.2);
  });

  it('doubles exponentially with jitter', () => {
    const ms = computeBackoffMs(3); // 2000 * 2^3 = 16000
    expect(ms).toBeGreaterThanOrEqual(16000);
    expect(ms).toBeLessThanOrEqual(16000 * 1.2);
  });

  it('respects the hard cap for high attempt counts', () => {
    const ms = computeBackoffMs(20); // 20 attempts should hit the 5 min cap
    expect(ms).toBeGreaterThanOrEqual(BACKOFF_CAP_MS);
    expect(ms).toBeLessThanOrEqual(BACKOFF_CAP_MS * 1.2);
  });
});