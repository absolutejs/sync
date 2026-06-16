/**
 * 1.22.0 — engine.replayTo(at) tenant point-in-time replay.
 *
 * Walks the bounded change log forward to a target timestamp, folds
 * each op into a per-table keyed view, returns the resulting rows.
 * Truncated when the log has been trimmed past the window.
 */
import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';

type Task = { id: number; title: string; done?: boolean };

const wireEngine = (options: { changeLogSize?: number } = {}) => {
	const store = new Map<number, Task>();
	const engine = createSyncEngine({
		changeLogSize: options.changeLogSize,
		instanceId: 'engine-A'
	});
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

const waitMs = (ms: number) =>
	new Promise((resolve) => setTimeout(resolve, ms));

describe('engine.replayTo — basic walk', () => {
	test('returns empty rows when no changes have been logged', async () => {
		const { engine } = wireEngine();
		const result = await engine.replayTo({ at: Date.now() });
		expect(result.asOfVersion).toBe(0);
		expect(result.asOfAt).toBe(0);
		expect(result.rows).toEqual({});
		expect(result.truncated).toBe(false);
	});

	test('reconstructs state after a sequence of inserts', async () => {
		const { engine } = wireEngine();
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'first' }
		});
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'second' }
		});
		const now = Date.now();
		const result = await engine.replayTo({ at: now });
		expect(result.truncated).toBe(false);
		const rows = result.rows.tasks as Task[];
		expect(rows).toHaveLength(2);
		expect(rows.map((task) => task.id).sort()).toEqual([1, 2]);
	});

	test('replays an update — later state wins', async () => {
		const { engine } = wireEngine();
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'original' }
		});
		await engine.applyChange<Task>('tasks', {
			op: 'update',
			row: { id: 1, title: 'edited' }
		});
		const result = await engine.replayTo({ at: Date.now() });
		const rows = result.rows.tasks as Task[];
		expect(rows).toHaveLength(1);
		expect(rows[0]!.title).toBe('edited');
	});

	test('replays a delete — removed row disappears', async () => {
		const { engine } = wireEngine();
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'a' }
		});
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'b' }
		});
		await engine.applyChange<Task>('tasks', {
			op: 'delete',
			row: { id: 1 } as Task
		});
		const result = await engine.replayTo({ at: Date.now() });
		const rows = result.rows.tasks as Task[];
		expect(rows).toHaveLength(1);
		expect(rows[0]!.id).toBe(2);
	});

	test('cuts off at targetAt — later entries are NOT folded in', async () => {
		const { engine } = wireEngine();
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'before' }
		});
		// Wait to push the next change into a later millisecond.
		await waitMs(15);
		const cutoff = Date.now();
		await waitMs(15);
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'after' }
		});
		const result = await engine.replayTo({ at: cutoff });
		const rows = result.rows.tasks as Task[];
		expect(rows).toHaveLength(1);
		expect(rows[0]!.title).toBe('before');
		expect(result.asOfVersion).toBe(1);
	});

	test('reports asOfAt + asOfVersion of the last folded entry', async () => {
		const { engine } = wireEngine();
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'a' }
		});
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'b' }
		});
		const result = await engine.replayTo({ at: Date.now() });
		expect(result.asOfVersion).toBe(2);
		expect(result.asOfAt).toBeGreaterThan(0);
	});
});

describe('engine.replayTo — table filtering', () => {
	test('only specified tables are folded into the result', async () => {
		const { engine } = wireEngine();
		const otherStore = new Map<number, { id: number; v: string }>();
		engine.registerReader('other', { all: () => [...otherStore.values()] });
		engine.registerWriter<{ id: number; v: string }>('other', {
			delete: (row) => {
				otherStore.delete(row.id);
			},
			insert: (data) => {
				otherStore.set(data.id, data);
				return data;
			},
			update: (data) => {
				otherStore.set(data.id, data);
				return data;
			}
		});
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 't' }
		});
		await engine.applyChange('other', {
			op: 'insert',
			row: { id: 1, v: 'x' }
		});
		const result = await engine.replayTo({
			at: Date.now(),
			tables: ['tasks']
		});
		expect(result.rows.tasks).toBeDefined();
		expect(result.rows.other).toBeUndefined();
	});
});

describe('engine.replayTo — truncated', () => {
	test('truncated=true when log has been trimmed past target', async () => {
		// Tiny log: only 2 entries retained.
		const { engine } = wireEngine({ changeLogSize: 2 });
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'one' }
		});
		await waitMs(5);
		const oldCutoff = Date.now();
		await waitMs(5);
		// Two more pushes evict the first entry — log[0] is now the
		// 'two' insert (version 2) and 'one' is gone.
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'two' }
		});
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 3, title: 'three' }
		});
		// Asking for state at oldCutoff (before any retained entry but
		// after some non-retained entry — version > 1) should report
		// truncated.
		const result = await engine.replayTo({ at: oldCutoff });
		expect(result.truncated).toBe(true);
	});

	test('truncated=false when log starts at version 1', async () => {
		const { engine } = wireEngine();
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'first' }
		});
		// Replay to a time BEFORE the change happened — that's
		// the empty pre-history. NOT truncated (the log starts at
		// version 1, so we have complete history).
		const result = await engine.replayTo({ at: 0 });
		expect(result.truncated).toBe(false);
		expect(result.rows).toEqual({});
	});

	test('truncated=false when targetAt is in the future', async () => {
		const { engine } = wireEngine();
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'a' }
		});
		const future = Date.now() + 1_000_000;
		const result = await engine.replayTo({ at: future });
		expect(result.truncated).toBe(false);
		const rows = result.rows.tasks as Task[];
		expect(rows).toHaveLength(1);
	});
});

describe('engine.replayTo — interaction with snapshot/restore', () => {
	test('replayTo on a restored engine reconstructs to a target before the import', async () => {
		const { engine: a } = wireEngine();
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'one' }
		});
		await waitMs(5);
		const mid = Date.now();
		await waitMs(5);
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'two' }
		});
		const snapshot = a.exportChangeLog();
		const { engine: b } = wireEngine();
		b.importChangeLog(snapshot);
		const result = await b.replayTo({ at: mid });
		const rows = result.rows.tasks as Task[];
		expect(rows).toHaveLength(1);
		expect(rows[0]!.title).toBe('one');
	});
});
