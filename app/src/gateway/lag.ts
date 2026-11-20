import { PARTITIONS, SERVICES, SERVICE_NAMES, type ServiceName } from '../shared/config.ts';
import type { Admin } from '../shared/kafka.ts';

export interface GroupLag {
  total: number;
  partitions: number[];
}

// Lag = newest offset in the partition minus the offset the group has committed.
export async function fetchLag(admin: Admin): Promise<Record<ServiceName, GroupLag>> {
  const topics = [...new Set(SERVICE_NAMES.map((s) => SERVICES[s].consumes))];
  const ends = new Map<string, { high: number; low: number }[]>();
  for (const topic of topics) {
    const offsets = await admin.fetchTopicOffsets(topic);
    const byPartition = Array.from({ length: PARTITIONS }, () => ({ high: 0, low: 0 }));
    for (const o of offsets) byPartition[o.partition] = { high: Number(o.high), low: Number(o.low) };
    ends.set(topic, byPartition);
  }

  const result = {} as Record<ServiceName, GroupLag>;
  for (const service of SERVICE_NAMES) {
    const topic = SERVICES[service].consumes;
    const committed = new Map<number, number>();
    const [entry] = await admin.fetchOffsets({ groupId: service, topics: [topic] });
    for (const p of entry?.partitions ?? []) committed.set(p.partition, Number(p.offset));
    const partitions = ends.get(topic)!.map(({ high, low }, partition) => {
      const offset = committed.get(partition) ?? -1;
      return Math.max(0, high - (offset >= 0 ? offset : low));
    });
    result[service] = { total: partitions.reduce((a, b) => a + b, 0), partitions };
  }
  return result;
}
