import { Job, JsonObject } from '@jobqueue/core';

export type JobHandler = (job: Job<JsonObject>) => Promise<void>;

const registry = new Map<string, JobHandler>();

export function registerHandler(queueName: string, handler: JobHandler): void {
  if (registry.has(queueName)) {
    throw new Error(`A handler is already registered for queue "${queueName}"`);
  }
  registry.set(queueName, handler);
}

export function getHandler(queueName: string): JobHandler {
  const handler = registry.get(queueName);
  if (!handler) {
    throw new Error(`No handler registered for queue "${queueName}"`);
  }
  return handler;
}

registerHandler('email_notifications', async (job) => {
  const { userId, template } = job.payload as { userId?: string; template?: string };
  if (!userId || !template) {
    throw new Error('email_notifications job missing required fields: userId, template');
  }
  console.log(`[handler] Sending "${template}" email to user ${userId}`);
  await new Promise((resolve) => setTimeout(resolve, 500));
});

registerHandler('webhook_delivery', async (job) => {
  const { url } = job.payload as { url?: string };
  if (!url) {
    throw new Error('webhook_delivery job missing required field: url');
  }
  console.log(`[handler] Delivering webhook to ${url}`);
  await new Promise((resolve) => setTimeout(resolve, 300));
});

registerHandler('load_test', async (job) => {
  console.log(`[handler] Executing load_test for job ${job.id}`);
  
  const workMs = process.env.WORK_MS ? parseInt(process.env.WORK_MS, 10) : 50;
  const failPercent = process.env.FAIL_PERCENT ? parseInt(process.env.FAIL_PERCENT, 10) : 0;
  
  await new Promise((resolve) => setTimeout(resolve, workMs));

  if (failPercent > 0 && Math.random() * 100 < failPercent) {
    throw new Error(`Simulated random failure (${failPercent}% chance)`);
  }
});