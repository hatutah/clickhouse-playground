import { setTimeout } from 'node:timers/promises';

export async function request(url: string, init?: RequestInit): Promise<Response> {
  const response = await fetch(url, { ...init, signal: AbortSignal.timeout(5000) });
  if (!response.ok) throw new Error(`${url}: ${response.status} ${await response.text()}`);
  return response;
}

export async function eventually(label: string, check: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 120_000;
  let detail = 'condition not met';
  while (Date.now() < deadline) {
    try {
      if (await check()) return;
    } catch (error) {
      detail = String(error);
    }
    await setTimeout(2000);
  }
  throw new Error(`Timed out waiting for ${label}: ${detail}`);
}

export const connectUrl = process.env.CONNECT_URL ?? 'http://localhost:8083';
export const flinkUrl = process.env.FLINK_URL ?? 'http://localhost:8081';

export async function connectorRunning(): Promise<boolean> {
  const status = await (await request(`${connectUrl}/connectors/search-clicks-sink/status`)).json() as {
    connector: { state: string }; tasks: { state: string; trace?: string }[];
  };
  const failed = status.tasks.find(task => task.state === 'FAILED');
  if (failed) throw new Error(failed.trace ?? 'Connector task failed');
  return status.connector.state === 'RUNNING' && status.tasks.length === 1 &&
    status.tasks.every(task => task.state === 'RUNNING');
}
