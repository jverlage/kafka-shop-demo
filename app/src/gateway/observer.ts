import { DASHBOARD_GROUP, DATA_TOPICS, TOPICS } from '../shared/config.ts';
import { createConsumer, createKafka, sleep, type Consumer } from '../shared/kafka.ts';

export interface ObservedRecord {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: any;
}

// Reads the shop topics as its own consumer group, so the dashboard sees the same messages as the services.
// Telemetry gets a separate group that starts at "latest": replaying old heartbeats would only show stale state.
export function createObserver(onRecord: (record: ObservedRecord) => void) {
  let data: Consumer | null = null;
  let events: Consumer | null = null;

  async function subscribe(groupId: string, topics: string[], fromBeginning: boolean) {
    const consumer = createConsumer(createKafka(groupId), groupId, fromBeginning);
    await consumer.connect();
    await consumer.subscribe({ topics });
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        let value: any;
        try {
          value = JSON.parse(message.value?.toString() ?? 'null');
        } catch {
          return;
        }
        onRecord({ topic, partition, offset: message.offset, key: message.key?.toString() ?? null, value });
      },
    });
    return consumer;
  }

  return {
    async start() {
      data = await subscribe(DASHBOARD_GROUP, DATA_TOPICS, true);
      events ??= await subscribe(`${DASHBOARD_GROUP}-events`, [TOPICS.serviceEvents], false);
    },
    async waitUntilAssigned() {
      while (!data?.assignment().length) await sleep(250);
    },
    // Only the data consumer stops: the topics it reads get deleted on reset, the telemetry topic does not.
    async stopData() {
      await data?.disconnect();
      data = null;
    },
    async stop() {
      await Promise.allSettled([data?.disconnect(), events?.disconnect()]);
      data = events = null;
    },
  };
}
