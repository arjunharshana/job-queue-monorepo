import WebSocket from 'ws';

const ws = new WebSocket('ws://localhost:3000');

ws.on('open', () => {
  console.log('[Listener] Connected. Waiting for job events...');
  console.log('[Listener] Now go enqueue a job and let a worker process it.');
});

ws.on('message', (data) => {
  const event = JSON.parse(data.toString());
  console.log(`[Listener] Received:`, event);
});

ws.on('close', () => console.log('[Listener] Disconnected'));
ws.on('error', (err) => console.error('[Listener] Error:', err));