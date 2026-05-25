import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import type { TableWriter, TransactionRunner } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Task = { id: number; title: string };

/**
 * A toy transactional "database": a transaction stages writes against a copy and
 * only applies them to the committed store if the function resolves. A throw
 * rolls the staged writes back — exactly the all-or-nothing we want to verify.
 */
const makeDb = () => {
	const committed = new Map<number, Task>();
	let nextId = 1;
	let committedCount = 0;

	const transaction: TransactionRunner = async (run) => {
		const staging = new Map(committed);
		let txSawHandle = false;
		const tx = {
			insert: (data: { title: string }) => {
				const row: Task = { id: nextId++, title: data.title };
				staging.set(row.id, row);
				return row;
			},
			update: (data: Task) => {
				staging.set(data.id, data);
				return data;
			},
			delete: (row: { id: number }) => {
				staging.delete(row.id);
			},
			markSeen: () => {
				txSawHandle = true;
			}
		};
		const result = await run(tx);
		// Commit: only reached if `run` resolved (no rollback).
		committed.clear();
		for (const [key, value] of staging) {
			committed.set(key, value);
		}
		committedCount += 1;
		void txSawHandle;

		return result;
	};

	return {
		committed,
		transaction,
		commits: () => committedCount
	};
};

type Tx = {
	insert: (data: { title: string }) => Task;
	update: (data: Task) => Task;
	delete: (row: { id: number }) => void;
	markSeen: () => void;
};

const tasksWriter: TableWriter<Task, unknown, Tx> = {
	insert: (data, _ctx, tx) => tx.insert(data),
	update: (data, _ctx, tx) => tx.update(data),
	delete: (row, _ctx, tx) => tx.delete(row)
};

const collect = () => {
	const events: ViewDiff<Task>[] = [];
	return {
		events,
		onDiff: (diff: ViewDiff<Task>) => {
			events.push(diff);
		}
	};
};

const buildEngine = () => {
	const db = makeDb();
	const engine = createSyncEngine({ transaction: db.transaction });
	engine.register(
		defineCollection<Task>({
			name: 'tasks',
			key: (task) => task.id,
			hydrate: () => [...db.committed.values()],
			match: () => true
		})
	);
	engine.registerWriter('tasks', tasksWriter);

	return { db, engine };
};

describe('transactional mutations', () => {
	test('a committed mutation persists all writes and emits one diff after commit', async () => {
		const { db, engine } = buildEngine();
		engine.registerMutation(
			defineMutation({
				name: 'addTwo',
				handler: async (_args, _ctx, actions) => {
					await actions.insert<Task>('tasks', { title: 'a' });
					await actions.insert<Task>('tasks', { title: 'b' });
				}
			})
		);

		const { events, onDiff } = collect();
		await engine.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff
		});
		await engine.runMutation('addTwo', {}, {});

		expect(db.commits()).toBe(1);
		expect(
			[...db.committed.values()].map((task) => task.title).sort()
		).toEqual(['a', 'b']);
		// One atomic diff, emitted after the commit.
		expect(events).toHaveLength(1);
		expect(events[0]!.added.map((task) => task.title).sort()).toEqual([
			'a',
			'b'
		]);
	});

	test('a handler that throws rolls back every write and emits nothing', async () => {
		const { db, engine } = buildEngine();
		engine.registerMutation(
			defineMutation({
				name: 'addThenFail',
				handler: async (_args, _ctx, actions) => {
					await actions.insert<Task>('tasks', { title: 'a' });
					await actions.insert<Task>('tasks', { title: 'b' });
					throw new Error('boom');
				}
			})
		);

		const { events, onDiff } = collect();
		await engine.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff
		});

		await expect(engine.runMutation('addThenFail', {}, {})).rejects.toThrow(
			'boom'
		);

		// Nothing committed (rolled back) and nothing emitted.
		expect(db.commits()).toBe(0);
		expect(db.committed.size).toBe(0);
		expect(events).toHaveLength(0);
	});

	test('the writer receives the transaction handle', async () => {
		const { engine } = buildEngine();
		let seenTx: Tx | undefined;
		engine.registerWriter('tasks', {
			insert: (data, _ctx, tx) => {
				seenTx = tx as Tx;
				return (tx as Tx).insert(data as { title: string });
			},
			update: (data, _ctx, tx) => (tx as Tx).update(data as Task),
			delete: (row, _ctx, tx) => (tx as Tx).delete(row as { id: number })
		});
		engine.registerMutation(
			defineMutation({
				name: 'add',
				handler: (_args, _ctx, actions) =>
					actions.insert<Task>('tasks', { title: 'x' })
			})
		);

		await engine.runMutation('add', {}, {});
		expect(typeof seenTx?.insert).toBe('function');
	});

	test('without a transaction runner, writers still work (tx is undefined)', async () => {
		const rows = new Map<number, Task>();
		let nextId = 1;
		const engine = createSyncEngine(); // no transaction option
		engine.register(
			defineCollection<Task>({
				name: 'tasks',
				key: (task) => task.id,
				hydrate: () => [...rows.values()],
				match: () => true
			})
		);
		engine.registerWriter('tasks', {
			insert: (data: { title: string }) => {
				const row: Task = { id: nextId++, title: data.title };
				rows.set(row.id, row);
				return row;
			},
			update: (data: Task) => {
				rows.set(data.id, data);
				return data;
			},
			delete: (row: { id: number }) => {
				rows.delete(row.id);
			}
		});
		engine.registerMutation(
			defineMutation({
				name: 'add',
				handler: (_args, _ctx, actions) =>
					actions.insert<Task>('tasks', { title: 'solo' })
			})
		);

		const { events, onDiff } = collect();
		await engine.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff
		});
		await engine.runMutation('add', {}, {});

		expect(rows.size).toBe(1);
		expect(events[0]!.added).toEqual([{ id: 1, title: 'solo' }]);
	});
});
