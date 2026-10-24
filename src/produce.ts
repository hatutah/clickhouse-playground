import { randomUUID } from 'node:crypto';
import { createEvents, publish } from './events.js';

const count = Number(process.env.COUNT ?? 20);
const intervalMs = Number(process.env.INTERVAL_MS ?? 200);
if (!Number.isSafeInteger(count) || count < 1 || !Number.isFinite(intervalMs) || intervalMs < 0) {
  throw new Error('COUNT must be a positive integer; INTERVAL_MS must be nonnegative.');
}
const runId = randomUUID();
const events = createEvents(count, runId);
await publish(events, intervalMs);
console.log(JSON.stringify({ run_id: runId, raw_events: events.length,
  expected_clicks: events.filter(e => e.event_type === 'result_click' && !e.is_bot).length }));
