import { randomUUID } from 'node:crypto';
import { setTimeout } from 'node:timers/promises';
import { Kafka, logLevel } from 'kafkajs';
import { encodeProto, eventTopic, loadProto, schemaId } from './schemas.js';

export interface SearchEvent {
  event_id: string;
  run_id: string;
  event_type: 'search_impression' | 'result_click';
  event_time_ms: number;
  search_id: string;
  session_id: string;
  user_id: string;
  query_text: string;
  result_id: string;
  result_position: number;
  is_bot: number;
}

export function createEvents(count: number, runId: string): SearchEvent[] {
  const queries = ['  ClickHouse  ', 'Kafka Streams', ' Flink SQL '];
  return Array.from({ length: count }, (_, i) => {
    const base = {
      run_id: runId,
      event_time_ms: Date.now(),
      search_id: randomUUID(),
      session_id: randomUUID(),
      user_id: `user-${i % 8}`,
      query_text: queries[i % queries.length],
      is_bot: i % 5 === 0 ? 1 : 0,
    };
    return [
      { ...base, event_id: randomUUID(), event_type: 'search_impression' as const,
        result_id: '', result_position: 0 },
      { ...base, event_id: randomUUID(), event_type: 'result_click' as const,
        event_time_ms: base.event_time_ms + 250,
        result_id: `product-${i % 12}`, result_position: (i % 10) + 1 },
    ];
  }).flat();
}

export async function publish(events: SearchEvent[], intervalMs: number): Promise<void> {
  const type = await loadProto('search_event', 'SearchEvent');
  const id = await schemaId(eventTopic);
  const kafka = new Kafka({
    clientId: 'search-demo-producer',
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    logLevel: logLevel.WARN,
  });
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  await producer.connect();
  try {
    for (const event of events) {
      await producer.send({
        topic: eventTopic,
        messages: [{ key: event.search_id, value: encodeProto(type, id, event) }],
      });
      if (intervalMs > 0) await setTimeout(intervalMs);
    }
  } finally {
    await producer.disconnect();
  }
}
