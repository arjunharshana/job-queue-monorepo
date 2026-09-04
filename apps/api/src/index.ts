import { config } from 'dotenv';
import express, { Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import { JobQueue, JsonValue } from '@jobqueue/core';

config({ path: '../../.env' });

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error('DATABASE_URL environment variable is missing');
}

const queue = new JobQueue({ connectionString });
const app = express();
app.use(express.json());

const enqueueSchema = z.object({
  queueName: z.string().min(1),
  payload: z.record(z.string(), z.any()), // refined below if needed
  priority: z.number().int().optional(),
  maxAttempts: z.number().int().positive().optional(),
  runAt: z.coerce.date().optional(),
});

app.post('/jobs', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const parsed = enqueueSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: { message: parsed.error.message } });
    }

    const job = await queue.enqueue(parsed.data as { queueName: string; payload: JsonValue });
    res.status(201).json(job);
  } catch (error) {
    next(error);
  }
});

app.get('/jobs/:id', async (req: Request, res: Response, next: NextFunction) => {
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

app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  console.error(error);
  res.status(500).json({ error: { message: 'Internal server error' } });
});

const PORT = process.env.PORT ?? 3000;
app.listen(PORT, () => {
  console.log(`API listening on port ${PORT}`);
});