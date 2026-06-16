/**
 * 1.23.0 — syncDevtools `<path>/replay` JSON endpoint.
 *
 * Wires `engine.replayTo()` into the devtools dashboard via a
 * `GET <path>/replay?at=<ms>&tables=<csv>` JSON endpoint and exposes a
 * Replay surface in the dashboard HTML.
 */
import { describe, expect, test } from 'bun:test';
import { Elysia } from 'elysia';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';
import { syncDevtools } from '../src/devtools';

type Task = { id: number; title: string; done?: boolean };

const wireEngine = () => {
	const store = new Map<number, Task>();
	const engine = createSyncEngine({ instanceId: 'engine-replay' });
	engine.registerReader('tasks', { all: () => [...store.values()] });
	engine.registerWriter<Task>('tasks', {
		delete: (row) => {
			store.delete(row.id);
		},
		insert: (data) => {
			store.set(data.id, data);
			return data;
		},
		update: (data) => {
			store.set(data.id, data);
			return data;
		}
	});
	engine.register(
		defineCollection<Task>({
			hydrate: () => [...store.values()],
			key: (task) => task.id,
			match: () => true,
			name: 'tasks'
		})
	);
	return { engine, store };
};

describe('syncDevtools — /replay endpoint', () => {
	test('serves dashboard HTML at the configured path', async () => {
		const { engine } = wireEngine();
		const app = new Elysia().use(syncDevtools({ engine }));
		const response = await app.handle(
			new Request('http://localhost/sync/devtools')
		);
		expect(response.status).toBe(200);
		expect(response.headers.get('content-type')).toContain('text/html');
		const html = await response.text();
		expect(html).toContain('Point-in-time replay');
		expect(html).toContain('/sync/devtools/replay');
		expect(html).toContain('/sync/devtools/stream');
	});

	test('replays state at a target timestamp via JSON', async () => {
		const { engine } = wireEngine();
		const app = new Elysia().use(syncDevtools({ engine }));
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'first' }
		});
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'second' }
		});
		const at = Date.now();
		const response = await app.handle(
			new Request(`http://localhost/sync/devtools/replay?at=${at}`)
		);
		expect(response.status).toBe(200);
		expect(response.headers.get('content-type')).toContain(
			'application/json'
		);
		const body = (await response.json()) as {
			asOfVersion: number;
			asOfAt: number;
			rows: Record<string, Task[] | undefined>;
			truncated: boolean;
		};
		expect(body.truncated).toBe(false);
		expect(body.asOfVersion).toBeGreaterThan(0);
		const rows = body.rows.tasks;
		expect(rows).toBeDefined();
		expect(rows?.map((task) => task.id).sort()).toEqual([1, 2]);
	});

	test('respects the tables csv filter', async () => {
		const { engine } = wireEngine();
		const app = new Elysia().use(syncDevtools({ engine }));
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'a' }
		});
		const at = Date.now();
		const matched = await app.handle(
			new Request(
				`http://localhost/sync/devtools/replay?at=${at}&tables=tasks`
			)
		);
		const matchedBody = (await matched.json()) as {
			rows: Record<string, unknown[]>;
		};
		expect(matchedBody.rows.tasks).toHaveLength(1);

		const filtered = await app.handle(
			new Request(
				`http://localhost/sync/devtools/replay?at=${at}&tables=other`
			)
		);
		const filteredBody = (await filtered.json()) as {
			rows: Record<string, unknown[]>;
		};
		expect(filteredBody.rows).toEqual({});
	});

	test('returns 400 when `at` is missing or malformed', async () => {
		const { engine } = wireEngine();
		const app = new Elysia().use(syncDevtools({ engine }));
		const missing = await app.handle(
			new Request('http://localhost/sync/devtools/replay')
		);
		expect(missing.status).toBe(400);
		const malformed = await app.handle(
			new Request('http://localhost/sync/devtools/replay?at=not-a-number')
		);
		expect(malformed.status).toBe(400);
	});

	test('honors a custom mount path', async () => {
		const { engine } = wireEngine();
		const app = new Elysia().use(
			syncDevtools({ engine, path: '/_admin/sync' })
		);
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 7, title: 'x' }
		});
		const response = await app.handle(
			new Request(`http://localhost/_admin/sync/replay?at=${Date.now()}`)
		);
		expect(response.status).toBe(200);
		const body = (await response.json()) as {
			rows: Record<string, Task[] | undefined>;
		};
		expect(body.rows.tasks?.[0]).toMatchObject({ id: 7, title: 'x' });
	});
});
