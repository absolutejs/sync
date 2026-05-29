import { afterEach, describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';

type Task = { id: number; title: string };

// Helper: a minimal "tasks" engine with a single in-memory collection.
const makeEngine = (options: Parameters<typeof createSyncEngine>[0] = {}) => {
	const store = new Map<number, Task>();
	const engine = createSyncEngine(options);
	engine.registerReader('tasks', { all: () => [...store.values()] });
	engine.registerWriter<Task>('tasks', {
		delete: (row) => { store.delete(row.id); },
		insert: (data) => { store.set(data.id, data); return data; },
		update: (data) => { store.set(data.id, data); return data; }
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

describe('engine.metrics()', () => {
	test('returns a structured snapshot of engine state', () => {
		const { engine } = makeEngine();
		const snap = engine.metrics();
		expect(snap.at).toBeGreaterThan(0);
		expect(snap.uptimeMs).toBeGreaterThanOrEqual(0);
		expect(snap.version).toBe(0);
		expect(snap.changeLog.entries).toBe(0);
		expect(snap.changeLog.capacity).toBeGreaterThan(0);
		expect(snap.changeLog.oldestVersion).toBeNull();
		expect(snap.changeLog.oldestAgeMs).toBeNull();
		expect(snap.changeLog.retainMs).toBeNull();
		expect(snap.subscriptions.total).toBe(0);
		expect(snap.mutations.completed).toBe(0);
		expect(snap.mutations.failed).toBe(0);
		expect(snap.mutations.inFlight).toBe(0);
	});

	test('tracks active subscriptions per collection', async () => {
		const { engine } = makeEngine();
		const subA = await engine.subscribe<Task>({
			collection: 'tasks',
			onDiff: () => {}
		});
		const subB = await engine.subscribe<Task>({
			collection: 'tasks',
			onDiff: () => {}
		});
		const after = engine.metrics();
		expect(after.subscriptions.total).toBe(2);
		expect(after.subscriptions.byCollection.tasks).toBe(2);

		subA.unsubscribe();
		const post = engine.metrics();
		expect(post.subscriptions.total).toBe(1);
		expect(post.subscriptions.byCollection.tasks).toBe(1);
		subB.unsubscribe();
	});

	test('tracks change-log state and version', async () => {
		const { engine } = makeEngine();
		await engine.applyChange<Task>('tasks', {
			op: 'upsert',
			row: { id: 1, title: 'one' }
		});
		await engine.applyChange<Task>('tasks', {
			op: 'upsert',
			row: { id: 2, title: 'two' }
		});
		const snap = engine.metrics();
		expect(snap.version).toBe(2);
		expect(snap.changeLog.entries).toBe(2);
		expect(snap.changeLog.oldestVersion).toBe(1);
		expect(snap.changeLog.oldestAgeMs).toBeGreaterThanOrEqual(0);
	});

	test('counts completed mutations', async () => {
		const { engine } = makeEngine();
		engine.registerMutation(
			defineMutation<{ id: number; title: string }>({
				handler: (args, _ctx, actions) => {
					actions.insert('tasks', args);
					return args;
				},
				name: 'addTask'
			})
		);
		await engine.runMutation('addTask', { id: 1, title: 'a' }, {});
		await engine.runMutation('addTask', { id: 2, title: 'b' }, {});
		const snap = engine.metrics();
		expect(snap.mutations.completed).toBe(2);
		expect(snap.mutations.failed).toBe(0);
		expect(snap.mutations.inFlight).toBe(0);
	});

	test('counts failed mutations', async () => {
		const { engine } = makeEngine();
		engine.registerMutation(
			defineMutation({
				handler: () => {
					throw new Error('nope');
				},
				name: 'fail'
			})
		);
		await expect(
			engine.runMutation('fail', undefined, {})
		).rejects.toThrow('nope');
		const snap = engine.metrics();
		expect(snap.mutations.completed).toBe(0);
		expect(snap.mutations.failed).toBe(1);
		expect(snap.mutations.inFlight).toBe(0);
	});

	test('reactiveCache + schedules surface their capacity', () => {
		const { engine } = makeEngine({ reactiveCache: { max: 64, ttlMs: 5000 } });
		const snap = engine.metrics();
		expect(snap.reactiveCache.capacity).toBe(64);
		expect(snap.reactiveCache.entries).toBe(0);
		expect(snap.schedules.registered).toBe(0);
	});
});

describe('changeLogRetainMs (time-based change-log retention)', () => {
	test('retains entries within the window, drops older', async () => {
		const { engine } = makeEngine({ changeLogRetainMs: 50 });
		await engine.applyChange<Task>('tasks', {
			op: 'upsert',
			row: { id: 1, title: 'first' }
		});
		expect(engine.metrics().changeLog.entries).toBe(1);

		// Wait past the retention window, then add another change. The
		// per-entry sweep runs on logChange; the old entry should fall off.
		await new Promise((resolve) => setTimeout(resolve, 80));
		await engine.applyChange<Task>('tasks', {
			op: 'upsert',
			row: { id: 2, title: 'second' }
		});
		const snap = engine.metrics();
		expect(snap.changeLog.entries).toBe(1);
		expect(snap.changeLog.oldestVersion).toBe(2);
		expect(snap.changeLog.retainMs).toBe(50);
	});

	test('null (default) means no time-based eviction — only count cap applies', async () => {
		const { engine } = makeEngine();
		for (let i = 1; i <= 5; i++) {
			await engine.applyChange<Task>('tasks', {
				op: 'upsert',
				row: { id: i, title: `t-${i}` }
			});
		}
		await new Promise((resolve) => setTimeout(resolve, 50));
		const snap = engine.metrics();
		expect(snap.changeLog.entries).toBe(5);
		expect(snap.changeLog.retainMs).toBeNull();
	});

	test('count cap still applies even when retainMs is set', async () => {
		const { engine } = makeEngine({
			changeLogRetainMs: 60_000,
			changeLogSize: 3
		});
		for (let i = 1; i <= 5; i++) {
			await engine.applyChange<Task>('tasks', {
				op: 'upsert',
				row: { id: i, title: `t-${i}` }
			});
		}
		const snap = engine.metrics();
		expect(snap.changeLog.entries).toBe(3);
		expect(snap.changeLog.oldestVersion).toBe(3);
	});
});

describe('LoggedChange.at (1.13.0)', () => {
	test('exposed on streamChanges entries', async () => {
		const { engine } = makeEngine();
		await engine.applyChange<Task>('tasks', {
			op: 'upsert',
			row: { id: 1, title: 'x' }
		});

		const controller = new AbortController();
		const stream = engine.streamChanges({ signal: controller.signal });
		const iter = stream[Symbol.asyncIterator]();
		const first = await iter.next();
		controller.abort();
		expect(first.done).toBe(false);
		const entry = first.value!;
		expect(typeof entry.at).toBe('number');
		expect(entry.at).toBeGreaterThan(0);
	});
});

// Restore process state between tests.
afterEach(() => {});
