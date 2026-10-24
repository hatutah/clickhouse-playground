import { readFile } from 'node:fs/promises';
import protobuf from 'protobufjs';
import { request } from './http.js';

export const registryUrl = process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8082';
export const eventTopic = 'search_events';
export const clickTopic = 'search_clicks';

export async function loadProto(file: string, message: string): Promise<protobuf.Type> {
  const schema = await readFile(`schemas/${file}.proto`, 'utf8');
  return protobuf.parse(schema, { keepCase: true }).root.lookupType(`demo.events.${message}`);
}

export async function schemaId(topic: string): Promise<number> {
  const latest = await (await request(`${registryUrl}/subjects/${topic}-value/versions/latest`)).json() as { id: number };
  return latest.id;
}

export function encodeProto(type: protobuf.Type, id: number, value: object): Buffer {
  const error = type.verify(value);
  if (error) throw new Error(error);
  // Confluent envelope: magic byte, schema ID, first top-level message index.
  const header = Buffer.alloc(6);
  header.writeUInt32BE(id, 1);
  return Buffer.concat([header, type.encode(type.create(value)).finish()]);
}
