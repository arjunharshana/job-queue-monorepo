import { JobQueue, JsonObject, Job } from '@jobqueue/core';
import { getHandler } from './handlers.js';
import { workerConfig } from './config.js';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export class WorkerLoop {
  private queue: JobQueue;
  private inFlight = 0;
  private stopping = false;
  private stopped = false;

  constructor(queue: JobQueue) {
    this.queue = queue;
  }

  async run(): Promise<void> {
    while (!this.stopping) {
      if (this.inFlight >= workerConfig.concurrency) {
        await sleep(50);
        continue;
      }

      const job = await this.queue.claim<JsonObject>(
        workerConfig.queueName,
        workerConfig.workerId,
        workerConfig.leaseSeconds
      );

      if (!job) {
        await sleep(workerConfig.pollIntervalMs);
        continue;
      }

      this.inFlight++;
      this.processJob(job).finally(() => {
        this.inFlight--;
      });
    }

    const deadline = Date.now() + workerConfig.shutdownTimeoutMs;
    while (this.inFlight > 0 && Date.now() < deadline) {
      await sleep(100);
    }
    if (this.inFlight > 0) {
      console.warn(
        `[${workerConfig.workerId}] Shutdown timeout reached with ${this.inFlight} job(s) still in flight.`
      );
    }
    this.stopped = true;
  }

  private async processJob(job: Job<JsonObject>): Promise<void> {
    try {
      const handler = getHandler(workerConfig.queueName);
      console.log(`[${workerConfig.workerId}] Processing job ${job.id} (attempt ${job.attempts})`);
      await handler(job);
      await this.queue.complete(job.id, workerConfig.workerId);
      console.log(`[${workerConfig.workerId}] Job ${job.id} completed`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await this.queue.fail(job.id, workerConfig.workerId, message);
      console.error(`[${workerConfig.workerId}] Job ${job.id} failed: ${message}`);
    }
  }

  stop(): void {
    this.stopping = true;
  }

  isStopped(): boolean {
    return this.stopped;
  }
}