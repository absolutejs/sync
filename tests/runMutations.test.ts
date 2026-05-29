/**
 * Tests for `engine.runMutations(specs, ctx)` — the v0.2 batch
 * primitive that holds one transaction open across N handlers, fans
 * out as ONE live diff on success, and rolls every accumulated write
 * back on any failure.
 *
 * We reuse the same toy in-memory transactional store the
 * transactionalMutation tests use, so the assertions about "the DB
 * actually saw one commit, not N" are real.
 */

import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import type { TableWriter, TransactionRunner } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Task = { id: number; title: string };
type Tx = {
	insert: (data: { title: string }) => Task;
	update: (data: Task) => Task;
	delete: (row: { id: number }) => void;
};

const makeDb = () => {
	const committed = new Map<number, Task>();
	let nextId = 1;
	let commits = 0;
	const transaction: TransactionRunner = async (run) => {
		const staging = new Map(committed);
		const tx: Tx = {
			insert: (data) => {
				const row: Task = { id: nextId++, title: data.title };
				staging.set(row.id, row);
				return row;
			},
			update: (data) => {
				staging.set(data.id, data);
				return data;
			},
			delete: (row) => {
				staging.delete(row.id);
			}
		};
		const result = await run(tx);
		committed.clear();
		for (const [k, v] of staging) committed.set(k, v);
		commits += 1;
		return result;
	};
	return { commits: () => commits, committed, transaction };
};

const tasksWriter: TableWriter<Task, unknown, Tx> = {
	insert: (data, _ctx, tx) => tx.insert(data),
	update: (data, _ctx, tx) => tx.update(data),
	delete: (row, _ctx, tx) => tx.delete(row)
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

const collect = () => {
	const events: ViewDiff<Task>[] = [];
	return {
		events,
		onDiff: (diff: ViewDiff<Task>) => {
			events.push(diff);
		}
	};
};

describe('engine.runMutations', () => {
	test('runs N mutations in one tx, returns results in order, fans out one diff', async () => {
		const { db, engine } = buildEngine();
		engine.registerMutation(
			defineMutation<{ title: string }, unknown, Task>({
				name: 'addTask',
				handler: async (args, _ctx, actions) =>
					actions.insert<Task>('tasks', { title: args.title })
			})
		);
		engine.registerMutation(
			defineMutation<unknown, unknown, number>({
				name: 'countTasks',
				handler: () => 99 // not a write — just a pure value
			})
		);
		const { events, onDiff } = collect();
		await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff,
			params: undefined
		});

		const results = await engine.runMutations(
			[
				{ args: { title: 'a' }, name: 'addTask' },
				{ args: { title: 'b' }, name: 'addTask' },
				{ args: {}, name: 'countTasks' }
			],
			{}
		);

		expect(results).toHaveLength(3);
		expect((results[0] as Task).title).toBe('a');
		expect((results[1] as Task).title).toBe('b');
		expect(results[2]).toBe(99);
		// One DB commit for the whole batch (vs 2 for separate runMutation calls).
		expect(db.commits()).toBe(1);
		// One ViewDiff fans out — both rows in a single batch.
		expect(events).toHaveLength(1);
		expect(events[0]!.added.map((row) => row.title).sort()).toEqual([
			'a',
			'b'
		]);
	});

	test('a throw in mutation #3 rolls the entire batch back — no partial commits, no diff', async () => {
		const { db, engine } = buildEngine();
		engine.registerMutation(
			defineMutation<{ title: string }, unknown, Task>({
				name: 'addTask',
				handler: async (args, _ctx, actions) =>
					actions.insert<Task>('tasks', { title: args.title })
			})
		);
		engine.registerMutation(
			defineMutation<unknown, unknown, never>({
				name: 'fail',
				handler: () => {
					throw new Error('rollback please');
				}
			})
		);
		const { events, onDiff } = collect();
		await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff,
			params: undefined
		});

		let caught: unknown;
		try {
			await engine.runMutations(
				[
					{ args: { title: 'a' }, name: 'addTask' },
					{ args: { title: 'b' }, name: 'addTask' },
					{ args: {}, name: 'fail' }
				],
				{}
			);
		} catch (err) {
			caught = err;
		}

		expect((caught as Error).message).toBe('rollback please');
		// No commit reached the DB.
		expect(db.committed.size).toBe(0);
		expect(db.commits()).toBe(0);
		// No live diff went out (the buffered changes were rolled back).
		expect(events).toHaveLength(0);
	});

	test('authorize: a denied mutation in the middle aborts the whole batch', async () => {
		const { db, engine } = buildEngine();
		type ActorCtx = { actor: string };
		engine.registerMutation(
			defineMutation<{ title: string }, ActorCtx, Task>({
				authorize: (_args, ctx) => ctx.actor === 'allowed',
				handler: (args, _ctx, actions) =>
					actions.insert<Task>('tasks', { title: args.title }),
				name: 'addNamed'
			})
		);
		engine.registerMutation(
			defineMutation<unknown, ActorCtx, Task>({
				authorize: (_args, ctx) => ctx.actor === 'allowed',
				handler: (_args, _ctx, actions) =>
					actions.insert<Task>('tasks', { title: 'auto' }),
				name: 'addAuto'
			})
		);

		let caught: unknown;
		try {
			await engine.runMutations(
				[
					{ args: { title: 'a' }, name: 'addNamed' },
					{ args: {}, name: 'addAuto' }
				],
				{ actor: 'denied' }
			);
		} catch (err) {
			caught = err;
		}
		expect(caught).toBeDefined();
		expect(db.commits()).toBe(0);
		expect(db.committed.size).toBe(0);
	});

	test('unknown mutation name throws BEFORE opening a tx (cheap typo guard)', async () => {
		const { db, engine } = buildEngine();
		engine.registerMutation(
			defineMutation<{ title: string }, unknown, Task>({
				name: 'addTask',
				handler: async (args, _ctx, actions) =>
					actions.insert<Task>('tasks', { title: args.title })
			})
		);
		let caught: unknown;
		try {
			await engine.runMutations(
				[
					{ args: { title: 'a' }, name: 'addTask' },
					{ args: {}, name: 'never:registered' }
				],
				{}
			);
		} catch (err) {
			caught = err;
		}
		expect((caught as Error).message).toMatch(/never:registered/);
		// Crucially: the tx never opened — no commit, no row visible.
		expect(db.commits()).toBe(0);
	});

	test('emits a single mutationBatch activity event with the ordered name list', async () => {
		const { engine } = buildEngine();
		engine.registerMutation(
			defineMutation<{ title: string }, unknown, Task>({
				name: 'addTask',
				handler: async (args, _ctx, actions) =>
					actions.insert<Task>('tasks', { title: args.title })
			})
		);
		const seen: unknown[] = [];
		const off = engine.onActivity((event) => {
			if (event.type === 'mutationBatch') seen.push(event);
		});
		await engine.runMutations(
			[
				{ args: { title: 'a' }, name: 'addTask' },
				{ args: { title: 'b' }, name: 'addTask' }
			],
			{}
		);
		off();
		expect(seen).toEqual([
			expect.objectContaining({
				names: ['addTask', 'addTask'],
				status: 'ok',
				type: 'mutationBatch'
			})
		]);
	});

	test('empty spec list returns an empty array without opening a tx', async () => {
		const { db, engine } = buildEngine();
		const results = await engine.runMutations([], {});
		expect(results).toEqual([]);
		expect(db.commits()).toBe(0);
	});

	test('works without a configured transaction (still buffers into one fan-out)', async () => {
		const engine = createSyncEngine(); // no `transaction` option
		const rows: Task[] = [];
		engine.register(
			defineCollection<Task>({
				name: 'tasks',
				key: (task) => task.id,
				hydrate: () => rows,
				match: () => true
			})
		);
		engine.registerWriter<Task>('tasks', {
			delete: (row) => {
				const i = rows.findIndex((r) => r.id === row.id);
				if (i >= 0) rows.splice(i, 1);
			},
			insert: (data) => {
				rows.push(data);
				return data;
			},
			update: (data) => {
				const i = rows.findIndex((r) => r.id === data.id);
				if (i >= 0) rows[i] = data;
				return data;
			}
		});
		let nextId = 1;
		engine.registerMutation(
			defineMutation<{ title: string }, unknown, Task>({
				name: 'add',
				handler: async (args, _ctx, actions) =>
					actions.insert<Task>('tasks', {
						id: nextId++,
						title: args.title
					})
			})
		);

		const { events, onDiff } = collect();
		await engine.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff,
			params: undefined
		});
		await engine.runMutations(
			[
				{ args: { title: 'x' }, name: 'add' },
				{ args: { title: 'y' }, name: 'add' }
			],
			{}
		);
		expect(events).toHaveLength(1); // still one batched fan-out
		expect(events[0]!.added.map((row) => row.title).sort()).toEqual([
			'x',
			'y'
		]);
	});
});
