import { DASHBOARD_GROUP, DATA_TOPICS, PARTITIONS, SERVICE_NAMES, TOPICS } from '../shared/config.ts';
import { retry, sleep, type Admin } from '../shared/kafka.ts';

const ALL_TOPICS = [...DATA_TOPICS, TOPICS.serviceEvents];

export async function ensureTopics(admin: Admin) {
  const existing = await admin.listTopics();
  const missing = ALL_TOPICS.filter((t) => !existing.includes(t));
  if (missing.length) {
    await admin.createTopics({
      topics: missing.map((topic) => ({
        topic,
        numPartitions: PARTITIONS,
        // Heartbeats are only interesting live, so they do not need to pile up.
        configEntries: topic === TOPICS.serviceEvents ? [{ name: 'retention.ms', value: '600000' }] : [],
      })),
    });
    console.log(`[gateway] Topics angelegt: ${missing.join(', ')}`);
  }
}

// Deletes the data topics and the consumer groups' offsets so the demo starts again at offset 0.
export async function recreateDataTopics(admin: Admin) {
  await admin.deleteTopics({ topics: DATA_TOPICS });
  await retry('topic-delete', async () => {
    const existing = await admin.listTopics();
    if (DATA_TOPICS.some((t) => existing.includes(t))) throw new Error('Löschen läuft noch');
  }, 30, 500);
  try {
    await admin.deleteGroups([...SERVICE_NAMES, DASHBOARD_GROUP]);
  } catch (err) {
    console.warn(`[gateway] Consumer Groups nicht gelöscht: ${(err as Error).message}`);
  }
  await retry('topic-create', async () => {
    await ensureTopics(admin);
    await sleep(300);
  }, 30, 1000);
}
