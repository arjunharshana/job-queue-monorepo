import { WebSocketServer, WebSocket } from 'ws';
import { Server } from 'node:http';
import { Redis } from 'ioredis';
import { timingSafeEqual } from 'node:crypto';

export function attachWebSocketBridge(httpServer: Server, redisUrl: string): void {
  const wss = new WebSocketServer({ server: httpServer });
  const subscriber = new Redis(redisUrl);

  wss.on('connection', (ws, req) => {
    const url = new URL(req.url || '', `http://${req.headers.host}`);
    const providedKey = url.searchParams.get('key') || '';
    const expectedKey = process.env.API_KEY || '';

    if (!expectedKey) {
      ws.close(1011, 'Server API_KEY not configured');
      return;
    }

    const providedBuffer = Buffer.from(providedKey);
    const expectedBuffer = Buffer.from(expectedKey);

    if (providedBuffer.length !== expectedBuffer.length || !timingSafeEqual(providedBuffer, expectedBuffer)) {
      ws.close(1008, 'Unauthorized');
      return;
    }

    console.log('[WS] Client connected');
    ws.on('close', () => console.log('[WS] Client disconnected'));
  });

  subscriber.subscribe('job_events:broadcast', (err) => {
    if (err) {
      console.error('[WS] Failed to subscribe to job_events:broadcast:', err);
    } else {
      console.log('[WS] Subscribed to job_events:broadcast');
    }
  });

  subscriber.on('message', (_channel, message) => {
    for (const client of wss.clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    }
  });
}