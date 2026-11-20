import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';
import {
  CUSTOMERS,
  GATEWAY_PORT,
  MAX_INSTANCES,
  PARTITIONS,
  PRODUCTS,
  SERVICE_NAMES,
  SERVICE_PORT,
  SERVICES,
  TOPICS,
  type InstanceStatus,
  type Order,
  type ServiceEvent,
  type ServiceName,
} from '../shared/config.ts';
import { createKafka, retry } from '../shared/kafka.ts';
import { fetchLag, type GroupLag } from './lag.ts';
import { createObserver, type ObservedRecord } from './observer.ts';
import { createSimulator } from './simulator.ts';
import { ensureTopics, recreateDataTopics } from './topics.ts';

const PUBLIC_DIR = fileURLToPath(new URL('../../public/', import.meta.url));
const OFFLINE_AFTER_MS = 3000;
const RECENT_LIMIT = 300;

interface ServiceState {
  online: boolean;
  lastSeen: number;
  instances: InstanceStatus[];
  processed: number;
  stats: Record<string, string | number>;
}

const kafka = createKafka('gateway');
const admin = kafka.admin();
const producer = kafka.producer();

const services = Object.fromEntries(
  SERVICE_NAMES.map((s): [ServiceName, ServiceState] => [s, { online: false, lastSeen: 0, instances: [], processed: 0, stats: {} }]),
) as Record<ServiceName, ServiceState>;
let lag = {} as Record<ServiceName, GroupLag>;
let recent: unknown[] = [];
let nextOrderNumber = 1001;
let resetting = false;

// ---------- WebSocket ----------

const clients = new Set<WebSocket>();

function broadcast(event: unknown) {
  const data = JSON.stringify(event);
  for (const ws of clients) if (ws.readyState === WebSocket.OPEN) ws.send(data);
}

function remember(event: unknown) {
  recent.push(event);
  if (recent.length > RECENT_LIMIT) recent = recent.slice(-RECENT_LIMIT);
  broadcast(event);
}

const statusEvent = (service: ServiceName) => ({ type: 'status', service, ...services[service] });

function snapshot() {
  return {
    type: 'snapshot',
    config: {
      customers: CUSTOMERS,
      products: PRODUCTS,
      services: SERVICE_NAMES.map((s) => SERVICES[s]),
      topics: TOPICS,
      partitions: PARTITIONS,
      maxInstances: MAX_INSTANCES,
    },
    services: SERVICE_NAMES.map(statusEvent),
    lag,
    simulator: simulator.state(),
    resetting,
    recent,
  };
}

// ---------- Kafka → Dashboard ----------

const observer = createObserver((record: ObservedRecord) => {
  if (record.topic === TOPICS.serviceEvents) return onServiceEvent(record.value as ServiceEvent);
  remember({ type: 'message', ...record, ts: Date.now() });
});

function onServiceEvent(event: ServiceEvent) {
  if (event.type === 'processed') return remember(event);
  const state = services[event.service];
  if (!state) return;
  // Heartbeats can arrive late after a restart; never let an old one override a newer one.
  if (event.ts < state.lastSeen) return;
  Object.assign(state, {
    online: Date.now() - event.ts < OFFLINE_AFTER_MS,
    lastSeen: event.ts,
    instances: event.instances,
    processed: event.processed,
    stats: event.stats,
  });
  broadcast(statusEvent(event.service));
}

setInterval(() => {
  for (const service of SERVICE_NAMES) {
    const state = services[service];
    if (state.online && Date.now() - state.lastSeen > OFFLINE_AFTER_MS) {
      Object.assign(state, { online: false, instances: [] });
      broadcast(statusEvent(service));
    }
  }
}, 1000);

async function pollLag() {
  if (!resetting) {
    try {
      lag = await fetchLag(admin);
      broadcast({ type: 'lag', lag });
    } catch (err) {
      console.warn(`[gateway] Lag nicht ermittelbar: ${(err as Error).message}`);
    }
  }
  setTimeout(pollLag, 1000);
}

// ---------- Orders ----------

function buildOrder(input: { customerId?: string; productId?: string }): Order {
  const customer = CUSTOMERS.find((c) => c.id === input.customerId) ?? CUSTOMERS[Math.floor(Math.random() * CUSTOMERS.length)];
  const product = PRODUCTS.find((p) => p.id === input.productId) ?? PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)];
  return {
    orderId: `B-${nextOrderNumber++}`,
    customerId: customer.id,
    productId: product.id,
    quantity: 1,
    amount: product.price,
    createdAt: new Date().toISOString(),
  };
}

async function placeOrders(inputs: { customerId?: string; productId?: string }[]) {
  if (resetting) throw new Error('Reset läuft gerade');
  const orders = inputs.map(buildOrder);
  // The customer id is the message key: the same customer always lands on the same partition.
  await producer.send({
    topic: TOPICS.orders,
    messages: orders.map((o) => ({ key: o.customerId, value: JSON.stringify(o) })),
  });
  return orders;
}

const simulator = createSimulator(
  () => placeOrders([{}]),
  () => broadcast({ type: 'simulator', ...simulator.state() }),
);

// ---------- Service control ----------

async function control(service: ServiceName, action: string) {
  const res = await fetch(`http://${service}:${SERVICE_PORT}/control`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ action }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? res.statusText);
  return res.json();
}

async function reset() {
  resetting = true;
  broadcast({ type: 'resetting' });
  simulator.update({ enabled: false });
  try {
    await Promise.allSettled(SERVICE_NAMES.map((s) => control(s, 'pause-all')));
    await observer.stopData();
    await recreateDataTopics(admin);
    nextOrderNumber = 1001;
    recent = [];
    lag = {} as Record<ServiceName, GroupLag>;
    await observer.start();
    await Promise.allSettled(SERVICE_NAMES.map((s) => control(s, 'resume')));
    await observer.waitUntilAssigned();
  } finally {
    resetting = false;
    broadcast(snapshot());
  }
}

// ---------- HTTP ----------

const MIME: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.svg': 'image/svg+xml',
};

async function readJson(req: IncomingMessage): Promise<any> {
  let body = '';
  for await (const chunk of req) body += chunk;
  return body ? JSON.parse(body) : {};
}

function sendJson(res: ServerResponse, code: number, body: unknown) {
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body));
}

async function serveStatic(url: string, res: ServerResponse) {
  const path = normalize(join(PUBLIC_DIR, url === '/' ? 'index.html' : url));
  if (!path.startsWith(PUBLIC_DIR)) return sendJson(res, 403, { error: 'forbidden' });
  try {
    const content = await readFile(path);
    res.writeHead(200, { 'content-type': MIME[extname(path)] ?? 'application/octet-stream', 'cache-control': 'no-cache' });
    res.end(content);
  } catch {
    sendJson(res, 404, { error: 'not found' });
  }
}

async function handleApi(req: IncomingMessage, res: ServerResponse, url: string) {
  if (req.method === 'GET' && url === '/api/state') return sendJson(res, 200, snapshot());
  if (req.method !== 'POST') return sendJson(res, 405, { error: 'method not allowed' });

  if (url === '/api/orders') {
    const [order] = await placeOrders([await readJson(req)]);
    return sendJson(res, 201, order);
  }
  if (url === '/api/orders/burst') {
    const { count = 20 } = await readJson(req);
    const orders = await placeOrders(Array.from({ length: Math.min(200, Math.max(1, Number(count))) }, () => ({})));
    return sendJson(res, 201, { count: orders.length });
  }
  if (url === '/api/simulator') {
    simulator.update(await readJson(req));
    return sendJson(res, 200, simulator.state());
  }
  if (url === '/api/reset') {
    await reset();
    return sendJson(res, 200, { ok: true });
  }
  const match = url.match(/^\/api\/services\/([a-z]+)\/([a-z-]+)$/);
  if (match && match[1] in SERVICES) {
    const service = match[1] as ServiceName;
    try {
      return sendJson(res, 200, await control(service, match[2]));
    } catch (err) {
      const offline = (err as Error).name === 'TypeError' || /fetch failed/.test((err as Error).message);
      return sendJson(res, 503, {
        error: offline ? `Container „${service}“ ist gestoppt – starte ihn mit: docker compose start ${service}` : (err as Error).message,
      });
    }
  }
  sendJson(res, 404, { error: 'not found' });
}

const server = createServer(async (req, res) => {
  const url = (req.url ?? '/').split('?')[0];
  try {
    if (url.startsWith('/api/')) await handleApi(req, res, url);
    else await serveStatic(url, res);
  } catch (err) {
    console.error(`[gateway] ${req.method} ${url}: ${(err as Error).message}`);
    if (!res.headersSent) sendJson(res, 500, { error: (err as Error).message });
  }
});

const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify(snapshot()));
  ws.on('close', () => clients.delete(ws));
});

// ---------- Startup ----------

async function shutdown() {
  simulator.update({ enabled: false });
  await Promise.allSettled([observer.stop(), producer.disconnect(), admin.disconnect()]);
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

await retry('kafka', () => admin.connect());
await retry('topics', () => ensureTopics(admin));
await producer.connect();
await observer.start();
// librdkafka segfaults on fetchOffsets while the group coordinator of a fresh cluster is still loading,
// so lag polling waits until our own consumer has been assigned partitions by that coordinator.
await observer.waitUntilAssigned();
setTimeout(pollLag, 2000);
server.listen(GATEWAY_PORT, () => console.log(`[gateway] Dashboard auf http://localhost:${GATEWAY_PORT}`));
