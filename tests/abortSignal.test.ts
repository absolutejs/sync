import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine, AbortError } from '../src/engine/syncEngine';

type Task = { id: number; title: string };

const makeEngine = () => {
	const store = new Map<number, Task>();
	const engine = createSyncEngine();
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

describe('AbortSignal — subscribe (1.15.0)', () => {
	test('already-aborted signal throws AbortError without subscribing', async () => {
		const { engine } = makeEngine();
		const controller = new AbortController();
		controller.abort('client gone');
		await expect(
			engine.subscribe<Task>({
				collection: 'tasks',
				ctx: {},
				onDiff: () => {},
				params: undefined,
				signal: controller.signal
			})
		).rejects.toBeInstanceOf(AbortError);
	});

	test('signal aborting AFTER subscribe completes auto-unsubscribes', async () => {
		const { engine, store } = makeEngine();
		const controller = new AbortController();
		const seenDiffs: number[] = [];
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: (diff) => { seenDiffs.push(diff.added.length); },
			params: undefined,
			signal: controller.signal
		});
		expect(sub.initial).toEqual([]);
		store.set(1, { id: 1, title: 'before' });
		await engine.applyChange<Task>('tasks', { op: 'insert', row: { id: 1, title: 'before' } });
		expect(seenDiffs.length).toBe(1);

		controller.abort();
		// After abort, further changes should NOT reach onDiff.
		await engine.applyChange<Task>('tasks', { op: 'insert', row: { id: 2, title: 'after' } });
		expect(seenDiffs.length).toBe(1);
	});

	test('omitting signal preserves pre-1.15.0 behavior', async () => {
		const { engine } = makeEngine();
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		expect(sub.initial).toEqual([]);
		sub.unsubscribe();
	});

	test('signal does NOT impact the subscription handle returned to the caller', async () => {
		const { engine } = makeEngine();
		const controller = new AbortController();
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined,
			signal: controller.signal
		});
		// The Subscription handle remains callable; abort just triggers
		// unsubscribe internally — the caller can still call it themselves.
		expect(typeof sub.unsubscribe).toBe('function');
		sub.unsubscribe();
		controller.abort(); // no error
	});

	test('aborting mid-hydrate (between authorize + finalize) aborts cleanly', async () => {
		// We can't easily race the synchronous default-view subscribe, but a
		// slow `authorize` is a clean place to insert an abort. The engine
		// re-checks the signal AFTER each major await (authorize, hydrate).
		const engine = createSyncEngine();
		engine.registerReader('tasks', { all: () => [] });
		engine.register(
			defineCollection<Task>({
				authorize: async () => {
					await new Promise((resolve) => setTimeout(resolve, 20));
					return true;
				},
				hydrate: () => [],
				key: (task) => task.id,
				match: () => true,
				name: 'tasks'
			})
		);
		const controller = new AbortController();
		const pending = engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined,
			signal: controller.signal
		});
		// Abort while authorize is sleeping.
		setTimeout(() => controller.abort(), 5);
		await expect(pending).rejects.toBeInstanceOf(AbortError);
	});
});

describe('AbortSignal — hydrate (1.15.0)', () => {
	test('already-aborted signal throws AbortError', async () => {
		const { engine } = makeEngine();
		const controller = new AbortController();
		controller.abort();
		await expect(
			engine.hydrate('tasks', undefined, {}, { signal: controller.signal })
		).rejects.toBeInstanceOf(AbortError);
	});

	test('mid-hydrate abort surfaces an AbortError', async () => {
		const engine = createSyncEngine();
		engine.registerReader('tasks', { all: () => [] });
		engine.register(
			defineCollection<Task>({
				authorize: async () => {
					await new Promise((resolve) => setTimeout(resolve, 20));
					return true;
				},
				hydrate: () => [],
				key: (task) => task.id,
				match: () => true,
				name: 'tasks'
			})
		);
		const controller = new AbortController();
		const pending = engine.hydrate('tasks', undefined, {}, {
			signal: controller.signal
		});
		setTimeout(() => controller.abort(), 5);
		await expect(pending).rejects.toBeInstanceOf(AbortError);
	});

	test('no-signal path still works (backwards-compatible)', async () => {
		const { engine, store } = makeEngine();
		store.set(1, { id: 1, title: 'x' });
		const rows = await engine.hydrate('tasks', undefined, {});
		expect(rows).toHaveLength(1);
	});
});
