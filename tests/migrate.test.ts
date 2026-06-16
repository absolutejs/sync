/**
 * 1.24.0 — G7 tenant migration primitives.
 *
 * `engine.fence()` pauses mutations on the source so its captured
 * state stops drifting. `engine.exportSnapshot()` walks every
 * registered reader and returns a portable snapshot.
 * `engine.importSnapshot()` bulk-loads the snapshot via the target's
 * registered writers.
 */
import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';
import { EngineFencedError, type EngineSnapshot } from '../src/engine/migrate';

type Task = { id: number; title: string; done?: boolean };

const wireEngine = (instanceId: string) => {
	const store = new Map<number, Task>();
	const engine = createSyncEngine({ instanceId });
	engine.registerReader('tasks', { all: () => [...store.values()] });
	engine.registerWriter<Task>('tasks', {
		delete: (row: Task) => {
			store.delete(row.id);
		},
		insert: (data: Task) => {
			store.set(data.id, data);
			return data;
		},
		update: (data: Task) => {
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
	engine.registerMutation(
		defineMutation({
			handler: (args: Task, _ctx, actions) =>
				actions.insert('tasks', args),
			name: 'addTask'
		})
	);
	return { engine, store };
};

describe('engine.fence — write pause', () => {
	test('runMutation throws EngineFencedError while fenced', async () => {
		const { engine } = wireEngine('source-A');
		const fence = engine.fence({ reason: 'snapshotting' });
		await expect(
			engine.runMutation('addTask', { id: 1, title: 'x' }, {})
		).rejects.toBeInstanceOf(EngineFencedError);
		await expect(
			engine.runMutation('addTask', { id: 1, title: 'x' }, {})
		).rejects.toThrow('snapshotting');
		fence.lift();
		await engine.runMutation('addTask', { id: 1, title: 'x' }, {});
	});

	test('subscribe + hydrate continue while fenced', async () => {
		const { engine, store } = wireEngine('source-A');
		store.set(1, { id: 1, title: 'a' });
		const fence = engine.fence({ reason: 'snapshotting' });
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		expect(sub.initial).toHaveLength(1);
		sub.unsubscribe();
		fence.lift();
	});

	test('multiple fences compose — engine unfences only after every lift', async () => {
		const { engine } = wireEngine('source-A');
		const fenceA = engine.fence({ reason: 'A' });
		const fenceB = engine.fence({ reason: 'B' });
		await expect(
			engine.runMutation('addTask', { id: 1, title: 'x' }, {})
		).rejects.toBeInstanceOf(EngineFencedError);
		fenceA.lift();
		await expect(
			engine.runMutation('addTask', { id: 1, title: 'x' }, {})
		).rejects.toBeInstanceOf(EngineFencedError);
		fenceB.lift();
		await engine.runMutation('addTask', { id: 1, title: 'x' }, {});
	});

	test('lift() is idempotent', async () => {
		const { engine } = wireEngine('source-A');
		const fence = engine.fence({ reason: 'once' });
		fence.lift();
		fence.lift(); // no-op, no throw
		await engine.runMutation('addTask', { id: 1, title: 'x' }, {});
	});
});

describe('engine.exportSnapshot — capture state', () => {
	test('snapshots every reader by default', async () => {
		const { engine, store } = wireEngine('source-A');
		store.set(1, { id: 1, title: 'a' });
		store.set(2, { id: 2, title: 'b' });
		const snapshot = await engine.exportSnapshot();
		expect(snapshot.sourceInstanceId).toBe('source-A');
		const tasks = snapshot.tables.tasks as Task[];
		expect(tasks).toHaveLength(2);
		expect(tasks.map((task) => task.id).sort()).toEqual([1, 2]);
		expect(typeof snapshot.exportedAt).toBe('number');
	});

	test('narrows to the requested tables', async () => {
		const { engine, store } = wireEngine('source-A');
		engine.registerReader('untouched', { all: () => [{ id: 'x' }] });
		store.set(1, { id: 1, title: 'a' });
		const snapshot = await engine.exportSnapshot({ tables: ['tasks'] });
		expect(Object.keys(snapshot.tables)).toEqual(['tasks']);
	});

	test('reports the source engine version', async () => {
		const { engine } = wireEngine('source-A');
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'a' }
		});
		const snapshot = await engine.exportSnapshot();
		expect(snapshot.version).toBeGreaterThan(0);
	});
});

describe('engine.importSnapshot — bulk load on target', () => {
	test('rehydrates the target engine via its writers', async () => {
		const { engine: source, store: sourceStore } = wireEngine('source-A');
		sourceStore.set(1, { id: 1, title: 'first' });
		sourceStore.set(2, { id: 2, title: 'second' });
		const snapshot = await source.exportSnapshot();
		const { engine: target, store: targetStore } = wireEngine('target-B');
		const result = await target.importSnapshot(snapshot);
		expect(result.tablesImported).toBe(1);
		expect(result.rowsImported).toBe(2);
		expect(result.perTable.tasks).toBe(2);
		expect(result.skipped).toEqual([]);
		expect(targetStore.size).toBe(2);
		expect(targetStore.get(1)?.title).toBe('first');
	});

	test('reports tables with no writer in `skipped`', async () => {
		const { engine: source, store } = wireEngine('source-A');
		store.set(1, { id: 1, title: 'a' });
		const snapshot: EngineSnapshot = {
			exportedAt: Date.now(),
			sourceInstanceId: 'source-A',
			tables: {
				orphan: [{ id: 1 }],
				tasks: [...store.values()]
			},
			version: 1
		};
		const { engine: target } = wireEngine('target-B');
		const result = await target.importSnapshot(snapshot);
		expect(result.skipped).toContain('orphan');
		expect(result.perTable.tasks).toBe(1);
	});

	test('honors the tables filter on import', async () => {
		const { engine: source, store } = wireEngine('source-A');
		store.set(1, { id: 1, title: 'a' });
		const snapshot = await source.exportSnapshot();
		const { engine: target, store: targetStore } = wireEngine('target-B');
		const result = await target.importSnapshot(snapshot, {
			tables: ['unrelated']
		});
		expect(result.rowsImported).toBe(0);
		expect(targetStore.size).toBe(0);
	});

	test('fires onProgress for each inserted row', async () => {
		const { engine: source, store } = wireEngine('source-A');
		for (let id = 1; id <= 5; id += 1) {
			store.set(id, { id, title: `row ${id}` });
		}
		const snapshot = await source.exportSnapshot();
		const { engine: target } = wireEngine('target-B');
		const progress: Array<{
			table: string;
			done: number;
			total: number;
		}> = [];
		await target.importSnapshot(snapshot, {
			onProgress: (table, done, total) =>
				progress.push({ done, table, total })
		});
		expect(progress).toHaveLength(5);
		expect(progress[0]).toEqual({ done: 1, table: 'tasks', total: 5 });
		expect(progress[4]).toEqual({ done: 5, table: 'tasks', total: 5 });
	});
});

describe('fence + export + import — end-to-end migration', () => {
	test('cross-region tenant move preserves data', async () => {
		const { engine: source, store: sourceStore } = wireEngine('us-west-1');
		await source.runMutation('addTask', { id: 1, title: 'a' }, {});
		await source.runMutation('addTask', { id: 2, title: 'b' }, {});
		await source.runMutation('addTask', { id: 3, title: 'c' }, {});

		const fence = source.fence({ reason: 'tenant-7 → us-east-2' });
		try {
			const snapshot = await source.exportSnapshot();
			expect(sourceStore.size).toBe(3);

			// Concurrent write attempt should be denied.
			await expect(
				source.runMutation('addTask', { id: 4, title: 'd' }, {})
			).rejects.toBeInstanceOf(EngineFencedError);

			const { engine: target, store: targetStore } =
				wireEngine('us-east-2');
			const result = await target.importSnapshot(snapshot);
			expect(result.rowsImported).toBe(3);
			expect(targetStore.size).toBe(3);
			expect([...targetStore.keys()].sort()).toEqual([1, 2, 3]);
		} finally {
			fence.lift();
		}

		// After lift, writes resume on the source.
		await source.runMutation('addTask', { id: 99, title: 'late' }, {});
		expect(sourceStore.has(99)).toBe(true);
	});
});
