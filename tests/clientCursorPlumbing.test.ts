import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';
import { createSyncConnection } from '../src/engine/connection';
import type { ClientFrame, ServerFrame } from '../src/engine/connection';

type Task = { id: number; title: string };

const makeEngine = (instanceId: string) => {
	const store = new Map<number, Task>();
	const engine = createSyncEngine({ instanceId });
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

describe('connection forwards cursor on ServerFrame (1.18.0)', () => {
	test('snapshot frame carries cursor', async () => {
		const { engine, store } = makeEngine('engine-A');
		store.set(1, { id: 1, title: 'one' });
		const sent: ServerFrame[] = [];
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: (frame) => {
				sent.push(frame);
			}
		});
		await connection.handle({
			collection: 'tasks',
			id: 's1',
			type: 'subscribe'
		});

		const snapshot = sent.find((frame) => frame.type === 'snapshot');
		expect(snapshot).toBeDefined();
		expect((snapshot as { cursor?: string }).cursor).toBeDefined();
		expect(typeof (snapshot as { cursor?: string }).cursor).toBe('string');
		// Cursor decodes to the engine's per-origin version vector.
		const decoded = JSON.parse((snapshot as { cursor: string }).cursor);
		expect(decoded['engine-A']).toBeGreaterThanOrEqual(0);
		connection.close();
	});

	test('diff frame carries cursor', async () => {
		const { engine } = makeEngine('engine-A');
		const sent: ServerFrame[] = [];
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: (frame) => {
				sent.push(frame);
			}
		});
		await connection.handle({
			collection: 'tasks',
			id: 's1',
			type: 'subscribe'
		});
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'new' }
		});
		// Microtask flush queues the diff; wait for it.
		await new Promise((resolve) =>
			queueMicrotask(() => resolve(undefined))
		);

		const diff = sent.find((frame) => frame.type === 'diff');
		expect(diff).toBeDefined();
		expect((diff as { cursor?: string }).cursor).toBeDefined();
		connection.close();
	});

	test('catch-up resume via cursor wire round-trip', async () => {
		const { engine, store } = makeEngine('engine-A');
		store.set(1, { id: 1, title: 'pre-existing' });
		const sent: ServerFrame[] = [];
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: (frame) => {
				sent.push(frame);
			}
		});

		// First subscribe — get a cursor from the snapshot.
		await connection.handle({
			collection: 'tasks',
			id: 's1',
			type: 'subscribe'
		});
		const snapshot = sent.find((frame) => frame.type === 'snapshot')!;
		const cursor = (snapshot as { cursor: string }).cursor;
		await connection.handle({ id: 's1', type: 'unsubscribe' });

		// Server changes while client is offline.
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'added-offline' }
		});

		// Reconnect with cursor as `since` — engine returns a catch-up diff.
		sent.length = 0;
		await connection.handle({
			collection: 'tasks',
			id: 's2',
			since: cursor,
			type: 'subscribe'
		});

		const catchup = sent.find((frame) => frame.type === 'diff');
		expect(catchup).toBeDefined();
		expect((catchup as { changed: Task[] }).changed).toContainEqual({
			id: 2,
			title: 'added-offline'
		});
		connection.close();
	});

	test('ClientFrame.subscribe.since accepts both number and string', async () => {
		const { engine, store } = makeEngine('engine-A');
		store.set(1, { id: 1, title: 'x' });
		const sent: ServerFrame[] = [];
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: (frame) => {
				sent.push(frame);
			}
		});

		// Legacy form: number.
		await connection.handle({
			collection: 'tasks',
			id: 's1',
			since: 0,
			type: 'subscribe'
		} satisfies ClientFrame);
		// New form: cursor string.
		await connection.handle({
			collection: 'tasks',
			id: 's2',
			since: JSON.stringify({ 'engine-A': 0 }),
			type: 'subscribe'
		} satisfies ClientFrame);

		// Both paths succeed (snapshot or diff in response — no error frames).
		expect(sent.some((frame) => frame.type === 'error')).toBe(false);
		connection.close();
	});
});

describe('OnDiff fires with cursor argument', () => {
	test('callback receives (diff, version, cursor)', async () => {
		const { engine } = makeEngine('engine-A');
		const calls: Array<{ version: number; cursor?: string }> = [];
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: (_diff, version, cursor) => {
				calls.push({ cursor, version });
			},
			params: undefined
		});
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'x' }
		});
		expect(calls).toHaveLength(1);
		expect(calls[0]!.version).toBeGreaterThan(0);
		expect(typeof calls[0]!.cursor).toBe('string');
		sub.unsubscribe();
	});

	test('callback that ignores cursor still works (backwards compat)', async () => {
		const { engine } = makeEngine('engine-A');
		const versions: number[] = [];
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			// Legacy 2-arg callback shape — no cursor parameter.
			onDiff: (_diff, version) => {
				versions.push(version);
			},
			params: undefined
		});
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'x' }
		});
		expect(versions).toHaveLength(1);
		sub.unsubscribe();
	});
});
