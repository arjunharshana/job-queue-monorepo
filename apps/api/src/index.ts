import express from 'express';
import { createServer } from 'node:http';
import { Redis } from 'ioredis';
import { JobQueue } from '@jobqueue/core';
import { apiConfig } from './config.js';
import { createRoutes } from './routes.js';
import { attachWebSocketBridge } from './websocket.js';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// 1. Setup __dirname for ES modules
const __dirname = path.dirname(fileURLToPath(import.meta.url));

const queue = new JobQueue({ connectionString: apiConfig.databaseUrl });
const redis = new Redis(apiConfig.redisUrl);

const app = express();
app.use(express.json());

app.use(express.static(path.join(__dirname, '../public')));

app.use(createRoutes(queue, redis));

app.use((error: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  console.error(error);
  res.status(500).json({ error: { message: 'Internal server error' } });
});

const httpServer = createServer(app);
attachWebSocketBridge(httpServer, apiConfig.redisUrl);

httpServer.listen(apiConfig.port, () => {
  console.log(`API listening on port ${apiConfig.port} (HTTP + WebSocket)`);
});