import { readFile } from 'node:fs/promises';
import { eventTopic, clickTopic, registryUrl } from './schemas.js';
import { request } from './http.js';

for (const [topic, file] of [[eventTopic, 'search_event'], [clickTopic, 'search_click']]) {
  const subject = `${topic}-value`;
  const headers = { 'Content-Type': 'application/vnd.schemaregistry.v1+json' };
  await request(`${registryUrl}/config/${subject}`, {
    method: 'PUT', headers, body: JSON.stringify({ compatibility: 'BACKWARD_TRANSITIVE' }),
  });
  const schema = await readFile(`schemas/${file}.proto`, 'utf8');
  const result = await request(`${registryUrl}/subjects/${subject}/versions`, {
    method: 'POST', headers, body: JSON.stringify({ schemaType: 'PROTOBUF', schema }),
  });
  console.log(`${subject}: ${await result.text()}`);
}
