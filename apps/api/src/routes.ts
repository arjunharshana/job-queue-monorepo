import { Router, Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import { JobQueue } from '@jobqueue/core';
import { Redis } from 'ioredis';
import { randomUUID, timingSafeEqual } from 'node:crypto';

const enqueueSchema = z.object({
  queueName: z.string().min(1),
  payload: z.record(z.string(), z.any()),
  priority: z.number().int().optional(),
  maxAttempts: z.number().int().positive().optional(),
  runAt: z.coerce.date().optional(),
});

function requireApiKey(req: Request, res: Response, next: NextFunction) {
  const providedKey = req.header('x-api-key') || '';
  const expectedKey = process.env.API_KEY || '';

  if (!expectedKey) {
    return res.status(500).json({ error: { message: 'Server API_KEY not configured' } });
  }

  const providedBuffer = Buffer.from(providedKey);
  const expectedBuffer = Buffer.from(expectedKey);

  if (providedBuffer.length !== expectedBuffer.length || !timingSafeEqual(providedBuffer, expectedBuffer)) {
    return res.status(401).json({ error: { message: 'Unauthorized' } });
  }

  next();
}

export function createRoutes(queue: JobQueue, redis: Redis): Router {
  const router = Router();

  router.get('/health', (req, res) => {
    res.status(200).json({ status: 'ok' });
  });

  // To drop in your custom WAF, run `npm install arjun-express-waf -w apps/api` 
  // and add `router.use(expressWaf());` right above this line.
  router.use(requireApiKey);

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