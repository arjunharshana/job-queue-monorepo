import { Router, Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import { JobQueue } from '@jobqueue/core';
import { Redis } from 'ioredis';
import { randomUUID } from 'node:crypto';

const enqueueSchema = z.object({
  queueName: z.string().min(1),
  payload: z.record(z.string(), z.any()),
  priority: z.number().int().optional(),
  maxAttempts: z.number().int().positive().optional(),
  runAt: z.coerce.date().optional(),
});

export function createRoutes(queue: JobQueue, redis: Redis): Router {
  const router = Router();

  router.post('/jobs', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const parsed = enqueueSchema.safeParse(req.body);
      if (!parsed.success) {
        return res.status(400).json({ error: { message: parsed.error.message } });
      }

      const jobId = randomUUID();
      const bufferedJob = { id: jobId, ...parsed.data, bufferedAt: new Date().toISOString() };

      await redis.rpush('job_buffer:pending', JSON.stringify(bufferedJob));

      res.status(202).json({ id: jobId, status: 'buffered', queueName: parsed.data.queueName });
    } catch (error) {
      next(error);
    }
  });

  router.get('/jobs/:id', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { id } = req.params;
      if (typeof id !== 'string') {
        return res.status(400).json({ error: { message: 'Invalid job id' } });
      }

      const job = await queue.getJob(id);
      if (!job) {
        return res.status(404).json({ error: { message: 'Job not found' } });
      }
      res.json(job);
    } catch (error) {
      next(error);
    }
  });

  return router;
}