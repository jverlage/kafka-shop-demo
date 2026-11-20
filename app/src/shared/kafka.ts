import confluent from '@confluentinc/kafka-javascript';
import { BROKERS } from './config.ts';

const { Kafka, logLevel } = confluent.KafkaJS;

export type KafkaClient = InstanceType<typeof Kafka>;
export type Admin = ReturnType<KafkaClient['admin']>;
export type Producer = ReturnType<KafkaClient['producer']>;
export type Consumer = ReturnType<KafkaClient['consumer']>;

export function createKafka(clientId: string): KafkaClient {
  return new Kafka({ kafkaJS: { brokers: BROKERS, clientId, logLevel: logLevel.WARN } });
}

export function createConsumer(kafka: KafkaClient, groupId: string, fromBeginning: boolean): Consumer {
  return kafka.consumer({
    kafkaJS: {
      groupId,
      fromBeginning,
      autoCommit: true,
      autoCommitInterval: 500,
      sessionTimeout: 6000,
      heartbeatInterval: 1000,
    },
  });
}

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export async function retry<T>(label: string, fn: () => Promise<T>, attempts = 60, delayMs = 2000): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt >= attempts) throw err;
      console.log(`[${label}] noch nicht bereit (${(err as Error).message}), neuer Versuch …`);
      await sleep(delayMs);
    }
  }
}

export async function waitForTopics(admin: Admin, topics: string[]): Promise<void> {
  await retry('topics', async () => {
    const existing = await admin.listTopics();
    const missing = topics.filter((t) => !existing.includes(t));
    if (missing.length) throw new Error(`fehlend: ${missing.join(', ')}`);
  });
}
