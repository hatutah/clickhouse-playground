import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createEvents, publish } from './events.js';
import { connectorRunning, eventually, flinkUrl, request } from './http.js';
import { eventTopic, clickTopic, registryUrl } from './schemas.js';
import { readFile } from 'node:fs/promises';

async function query(sql: string): Promise<string> {
  return (await request(process.env.CLICKHOUSE_URL ?? 'http://localhost:8123', {
    method: 'POST',
    headers: { Authorization: `Basic ${Buffer.from('demo:demo').toString('base64')}` },
    body: sql,
  })).text();
}

await eventually('sink connector', connectorRunning);
for (const topic of [eventTopic, clickTopic]) {
  const schema = await (await request(`${registryUrl}/subjects/${topic}-value/versions/latest`)).json() as { schemaType: string };
  assert.equal(schema.schemaType, 'PROTOBUF');
}
const eventSchema = await readFile('schemas/search_event.proto', 'utf8');
const compatibility = await (await request(`${registryUrl}/compatibility/subjects/${eventTopic}-value/versions/latest`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json' },
  body: JSON.stringify({ schemaType: 'PROTOBUF', schema: eventSchema.replace('string event_id = 1;', 'int64 event_id = 1;') }),
})).json() as { is_compatible: boolean };
assert.equal(compatibility.is_compatible, false);
await eventually('Flink transform', async () => {
  const { jobs } = await (await request(`${flinkUrl}/jobs/overview`)).json() as {
    jobs: { name: string; state: string }[];
  };
  return jobs.filter(job => job.name === 'search-click-transform' && job.state === 'RUNNING').length === 1;
});

const runId = randomUUID();
const events = createEvents(10, runId);
const clicks = events.filter(e => e.event_type === 'result_click' && !e.is_bot);
await publish(events, 0);
await eventually('both ClickHouse ingestion paths', async () => {
  const raw = Number(await query(`SELECT count() FROM search_demo.raw_events WHERE run_id = '${runId}'`));
  const transformed = Number(await query(`SELECT count() FROM search_demo.search_clicks WHERE run_id = '${runId}'`));
  return raw >= events.length && transformed >= clicks.length;
});

const raw = JSON.parse(await query(`SELECT * FROM search_demo.raw_events WHERE run_id = '${runId}' FORMAT JSON`)).data as Record<string, unknown>[];
const actual = JSON.parse(await query(`SELECT * FROM search_demo.search_clicks WHERE run_id = '${runId}' FORMAT JSON`)).data as Record<string, unknown>[];
assert.equal(raw.length, events.length);
assert.deepEqual(new Set(raw.map(row => row.event_id)), new Set(events.map(event => event.event_id)));
for (const row of raw) {
  const event = events.find(event => event.event_id === row.event_id)!;
  assert.deepEqual({ ...row, event_time_ms: Number(row.event_time_ms) }, event);
}
assert.equal(actual.length, clicks.length);
assert.deepEqual(new Set(actual.map(row => row.event_id)), new Set(clicks.map(event => event.event_id)));
for (const row of actual) {
  const event = clicks.find(event => event.event_id === row.event_id)!;
  assert.equal(row.query_normalized, event.query_text.trim().toLowerCase());
  assert.equal(row.position_bucket, event.result_position <= 3 ? 'top_3' : 'below_3');
  assert.equal(row.result_position, event.result_position);
}
console.log(`PASS run_id=${runId}: ${raw.length} raw events via Kafka engine; ${actual.length} human clicks via Flink + Connect.`);
