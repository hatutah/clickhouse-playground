import { readFile } from 'node:fs/promises';
import { connectUrl, connectorRunning, eventually, request } from './http.js';

await eventually('Kafka Connect REST API', async () => {
  await request(`${connectUrl}/connector-plugins`);
  return true;
});
const config = await readFile('connect/clickhouse-sink.json', 'utf8');
await request(`${connectUrl}/connectors/search-clicks-sink/config`, {
  method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: config,
});
await eventually('ClickHouse sink task', connectorRunning);
console.log('ClickHouse sink connector is RUNNING.');
