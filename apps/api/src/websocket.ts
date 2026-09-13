import { WebSocketServer, WebSocket } from 'ws';
import { Server } from 'node:http';
import { Redis } from 'ioredis';

export function attachWebSocketBridge(httpServer: Server, redisUrl: string): void {
  const wss = new WebSocketServer({ server: httpServer });
  const subscriber = new Redis(redisUrl);

  wss.on('connection', (ws) => {
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