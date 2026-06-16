import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';
import { createInMemoryClusterBus } from '../src/engine/cluster';

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

describe('cursor + resume (single instance) — 1.17.0', () => {
	test('Subscription includes a cursor string', async () => {
		const { engine } = makeEngine('engine-A');
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		expect(typeof sub.cursor).toBe('string');
		expect(JSON.parse(sub.cursor)).toEqual({ 'engine-A': 0 });
		sub.unsubscribe();
	});

	test('cursor advances as the engine applies changes', async () => {
		const { engine } = makeEngine('engine-A');
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		const before = JSON.parse(sub.cursor);
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'x' }
		});
		const sub2 = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		const after = JSON.parse(sub2.cursor);
		expect(after['engine-A']).toBeGreaterThan(before['engine-A']);
		sub.unsubscribe();
		sub2.unsubscribe();
	});

	test('resume via cursor returns a catch-up diff for missed changes', async () => {
		const { engine, store } = makeEngine('engine-A');
		store.set(1, { id: 1, title: 'before' });
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		const cursor = sub.cursor;
		sub.unsubscribe();

		// Server changes while client is offline.
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'while-offline' }
		});

		// Resume with cursor — engine returns a catch-up diff.
		const resumed = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined,
			since: cursor
		});
		expect(resumed.catchup).toBeDefined();
		expect(resumed.initial).toEqual([]);
		expect(resumed.catchup!.changed).toContainEqual({
			id: 2,
			title: 'while-offline'
		});
		resumed.unsubscribe();
	});

	test('legacy `since: number` form still works (pre-1.17 compatibility)', async () => {
		const { engine } = makeEngine('engine-A');
		const first = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		const legacySince = first.version;
		first.unsubscribe();

		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'x' }
		});

		const resumed = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined,
			since: legacySince // a bare number
		});
		expect(resumed.catchup).toBeDefined();
		expect(resumed.catchup!.changed).toContainEqual({ id: 1, title: 'x' });
		resumed.unsubscribe();
	});
});

describe('cross-instance resume (1.17.0) — the headline feature', () => {
	test('cursor from instance A serves catch-up on instance B', async () => {
		const bus = createInMemoryClusterBus();
		const { engine: engineA } = makeEngine('engine-A');
		const { engine: engineB } = makeEngine('engine-B');

		const offA = await engineA.connectCluster(bus);
		const offB = await engineB.connectCluster(bus);

		// Subscribe on A, get the cursor.
		const subA = await engineA.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		const cursorFromA = subA.cursor;
		subA.unsubscribe();

		// Now the user moves: a change happens via A while the client is moving.
		await engineA.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'via-A' }
		});
		// Give the bus a moment to propagate.
		await new Promise((resolve) => setTimeout(resolve, 5));

		// Client reconnects to B with the cursor from A.
		const resumedOnB = await engineB.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined,
			since: cursorFromA
		});
		// B serves the catch-up from its own log of peer-A changes.
		expect(resumedOnB.catchup).toBeDefined();
		expect(resumedOnB.catchup!.changed).toContainEqual({
			id: 1,
			title: 'via-A'
		});

		resumedOnB.unsubscribe();
		await offA();
		await offB();
	});

	test('cursor from unknown instance falls back to fresh snapshot', async () => {
		const { engine } = makeEngine('engine-A');
		const fakeCursor = JSON.stringify({ 'unknown-instance': 100 });
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined,
			since: fakeCursor
		});
		// Cursor references an instance we've never heard of with version 100 — fall back to snapshot.
		expect(sub.catchup).toBeUndefined();
		expect(sub.initial).toBeDefined();
		sub.unsubscribe();
	});

	test('malformed cursor falls back to snapshot', async () => {
		const { engine } = makeEngine('engine-A');
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined,
			since: 'not-a-valid-json-cursor'
		});
		expect(sub.catchup).toBeUndefined();
		expect(sub.initial).toBeDefined();
		sub.unsubscribe();
	});

	test('LoggedChange.origin + originVersion are surfaced on streamChanges', async () => {
		const bus = createInMemoryClusterBus();
		const { engine: engineA } = makeEngine('engine-A');
		const { engine: engineB } = makeEngine('engine-B');
		const offA = await engineA.connectCluster(bus);
		const offB = await engineB.connectCluster(bus);

		await engineA.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'from-A' }
		});
		await new Promise((resolve) => setTimeout(resolve, 5));

		// Drain engine B's stream — should see a peer-A change with origin='engine-A'.
		const controller = new AbortController();
		const stream = engineB.streamChanges({ signal: controller.signal });
		const iter = stream[Symbol.asyncIterator]();
		const first = await iter.next();
		controller.abort();
		expect(first.done).toBe(false);
		expect(first.value!.origin).toBe('engine-A');
		expect(first.value!.originVersion).toBeGreaterThan(0);

		await offA();
		await offB();
	});
});
