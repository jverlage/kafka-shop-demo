import { createServer } from 'node:http';
import {
  MAX_INSTANCES,
  SERVICE_PORT,
  SERVICES,
  TOPICS,
  type InstanceStatus,
  type ServiceEvent,
  type ServiceName,
} from '../shared/config.ts';
import { createConsumer, createKafka, retry, sleep, waitForTopics, type Consumer } from '../shared/kafka.ts';
import { HANDLERS } from './handlers.ts';

const name = process.env.SERVICE_NAME as ServiceName;
const def = SERVICES[name];
if (!def) throw new Error(`Unbekannter SERVICE_NAME: ${process.env.SERVICE_NAME}`);

const handler = HANDLERS[name]();
const admin = createKafka(`${name}-admin`).admin();
const producer = createKafka(`${name}-producer`).producer();
const requiredTopics = [def.consumes, TOPICS.serviceEvents, ...(def.produces ? [def.produces] : [])];

let processed = 0;

// Each instance is its own Kafka client, so it joins the consumer group as a separate member.
class Instance {
  readonly id: string;
  readonly slot: number;
  private readonly consumer: Consumer;

  constructor(slot: number) {
    this.slot = slot;
    this.id = `${name}-${slot}`;
    this.consumer = createConsumer(createKafka(this.id), def.name, true);
  }

  async start() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [def.consumes] });
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        await sleep(def.delayMs);
        let payload: any;
        try {
          payload = JSON.parse(message.value?.toString() ?? '');
        } catch {
          console.warn(`[${this.id}] ungültige Nachricht bei ${topic}/${partition}@${message.offset} übersprungen`);
          return;
        }
        const result = handler.handle(payload);
        processed += 1;
        try {
          if (def.produces && result.output) {
            await producer.send({
              topic: def.produces,
              messages: [{ key: result.output.key, value: JSON.stringify(result.output.value) }],
            });
          }
          await emit({
            type: 'processed',
            service: name,
            instance: this.id,
            topic,
            partition,
            offset: message.offset,
            orderId: payload.orderId,
            customerId: payload.customerId,
            info: result.info,
            ts: Date.now(),
          });
        } catch (err) {
          console.error(`[${this.id}] Senden fehlgeschlagen: ${(err as Error).message}`);
        }
        console.log(`[${this.id}] P${partition}@${message.offset} ${payload.orderId}: ${result.info}`);
      },
    });
  }

  partitions(): number[] {
    try {
      return this.consumer
        .assignment()
        .filter((tp) => tp.topic === def.consumes)
        .map((tp) => tp.partition)
        .sort((a, b) => a - b);
    } catch {
      return [];
    }
  }

  async stop() {
    await this.consumer.disconnect();
  }
}

let instances: Instance[] = [];

async function addInstance() {
  if (instances.length >= MAX_INSTANCES) return;
  const used = new Set(instances.map((i) => i.slot));
  let slot = 1;
  while (used.has(slot)) slot++;
  const instance = new Instance(slot);
  instances = [...instances, instance].sort((a, b) => a.slot - b.slot);
  console.log(`[${name}] starte Instanz ${instance.id}`);
  try {
    await instance.start();
  } catch (err) {
    instances = instances.filter((i) => i !== instance);
    throw err;
  }
}

async function removeInstance() {
  const instance = instances.at(-1);
  if (!instance) return;
  instances = instances.slice(0, -1);
  console.log(`[${name}] stoppe Instanz ${instance.id}`);
  await instance.stop();
}

async function removeAll() {
  while (instances.length) await removeInstance();
}

const ACTIONS: Record<string, () => Promise<void>> = {
  start: async () => {
    if (instances.length === 0) await addInstance();
  },
  stop: removeAll,
  'scale-up': addInstance,
  'scale-down': removeInstance,
  'pause-all': removeAll,
  resume: async () => {
    handler.reset();
    processed = 0;
    await waitForTopics(admin, requiredTopics);
    await addInstance();
  },
};

// Control actions run one after another so that rapid clicks cannot interleave connects and disconnects.
let queue: Promise<void> = Promise.resolve();
function enqueue(action: () => Promise<void>): Promise<void> {
  const run = queue.then(action);
  queue = run.catch(() => {});
  return run;
}

function status() {
  const list: InstanceStatus[] = instances.map((i) => ({ id: i.id, partitions: i.partitions() }));
  return { service: name, instances: list, processed, stats: handler.stats() };
}

async function emit(event: ServiceEvent) {
  await producer.send({ topic: TOPICS.serviceEvents, messages: [{ key: name, value: JSON.stringify(event) }] });
}

async function heartbeat() {
  try {
    await emit({ type: 'heartbeat', ...status(), ts: Date.now() });
  } catch (err) {
    console.error(`[${name}] Heartbeat fehlgeschlagen: ${(err as Error).message}`);
  }
}

const server = createServer(async (req, res) => {
  const send = (code: number, body: unknown) => {
    res.writeHead(code, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body));
  };
  if (req.method === 'GET' && req.url === '/status') return send(200, status());
  if (req.method !== 'POST' || req.url !== '/control') return send(404, { error: 'not found' });

  let body = '';
  for await (const chunk of req) body += chunk;
  const action = ACTIONS[(JSON.parse(body || '{}') as { action?: string }).action ?? ''];
  if (!action) return send(400, { error: 'unbekannte Aktion' });
  try {
    await enqueue(action);
    void heartbeat();
    send(200, status());
  } catch (err) {
    send(500, { error: (err as Error).message });
  }
});

async function shutdown() {
  console.log(`[${name}] fahre herunter …`);
  await Promise.allSettled(instances.map((i) => i.stop()));
  await Promise.allSettled([producer.disconnect(), admin.disconnect()]);
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

await retry('kafka', () => admin.connect());
await waitForTopics(admin, requiredTopics);
await producer.connect();
await enqueue(addInstance);
server.listen(SERVICE_PORT, () => console.log(`[${name}] bereit – konsumiert ${def.consumes}, Steuerung auf :${SERVICE_PORT}`));
setInterval(heartbeat, 1000);
