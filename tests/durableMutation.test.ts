import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncConnection } from '../src/engine/connection';
import type { ServerFrame } from '../src/engine/connection';
import type {
	DurableMutationOperation,
	DurableMutationRunner,
	DurableMutationRunResult,
	TableWriter
} from '../src/engine/mutation';
import { defineMutation } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Task = { id: number; title: string };
type Receipt = { name: string; result: unknown };
type Tx = {
	rows: Map<number, Task>;
	receipts: Map<string, Receipt>;
};

const createDatabase = () => {
	let rows = new Map<number, Task>();
	let receipts = new Map<string, Receipt>();
	let commits = 0;

	const run = async <R>(
		operation: DurableMutationOperation,
		execute: (tx: unknown) => Promise<R>
	): Promise<DurableMutationRunResult<R>> => {
		const receiptKey = `${operation.scope}\0${operation.operationId}`;
		const existing = receipts.get(receiptKey);
		if (existing !== undefined) {
			if (existing.name !== operation.name) {
				throw new Error(
					'Operation id was already used by another mutation'
				);
			}
			return { replayed: true, result: existing.result as R };
		}

		const tx: Tx = {
			rows: new Map(rows),
			receipts: new Map(receipts)
		};
		const result = await execute(tx);
		tx.receipts.set(receiptKey, { name: operation.name, result });
		rows = tx.rows;
		receipts = tx.receipts;
		commits += 1;
		return { replayed: false, result };
	};
	const durableRunner: DurableMutationRunner = run;

	return {
		commits: () => commits,
		receipts: () => receipts,
		rows: () => rows,
		run: durableRunner
	};
};

const writer: TableWriter<Task, unknown, Tx> = {
	insert: (data, _ctx, tx) => {
		const row = data as Task;
		tx.rows.set(row.id, row);
		return row;
	},
	update: (data, _ctx, tx) => {
		const row = data as Task;
		tx.rows.set(row.id, row);
		return row;
	},
	delete: (row, _ctx, tx) => {
		tx.rows.delete((row as Task).id);
	}
};

const setup = () => {
	const database = createDatabase();
	let handlerCalls = 0;
	const engine = createSyncEngine({
		durableMutations: {
			scope: (ctx) => (ctx as { accountId: string }).accountId,
			run: database.run
		}
	});
	engine.register(
		defineCollection<Task>({
			name: 'tasks',
			key: (row) => row.id,
			hydrate: () => [...database.rows().values()],
			match: () => true
		})
	);
	engine.registerWriter('tasks', writer);
	engine.registerMutation(
		defineMutation<{ id: number; title: string }, unknown, Task>({
			name: 'createTask',
			handler: async (args, _ctx, actions) => {
				handlerCalls += 1;
				return actions.insert<Task>('tasks', args);
			}
		})
	);
	return { database, engine, handlerCalls: () => handlerCalls };
};

describe('durable mutations', () => {
	test('a replay returns the committed result without repeating effects or diffs', async () => {
		const { database, engine, handlerCalls } = setup();
		const diffs: ViewDiff<Task>[] = [];
		await engine.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: { accountId: 'account-a' },
			onDiff: (diff) => diffs.push(diff)
		});

		const first = await engine.runMutation(
			'createTask',
			{ id: 1, title: 'offline task' },
			{ accountId: 'account-a' },
			{ operationId: 'installation-a:operation-1' }
		);
		const replay = await engine.runMutation(
			'createTask',
			{ id: 1, title: 'offline task' },
			{ accountId: 'account-a' },
			{ operationId: 'installation-a:operation-1' }
		);

		expect(replay).toEqual(first);
		expect(handlerCalls()).toBe(1);
		expect(database.commits()).toBe(1);
		expect(database.rows().size).toBe(1);
		expect(database.receipts().size).toBe(1);
		expect(diffs).toHaveLength(1);
	});

	test('the authenticated scope isolates identical client operation ids', async () => {
		const { database, engine, handlerCalls } = setup();
		await engine.runMutation(
			'createTask',
			{ id: 1, title: 'a' },
			{ accountId: 'account-a' },
			{ operationId: 'installation-a:operation-1' }
		);
		await engine.runMutation(
			'createTask',
			{ id: 2, title: 'b' },
			{ accountId: 'account-b' },
			{ operationId: 'installation-a:operation-1' }
		);

		expect(handlerCalls()).toBe(2);
		expect(database.rows().size).toBe(2);
		expect(database.receipts().size).toBe(2);
	});

	test('a failed mutation commits neither business writes nor its receipt', async () => {
		const database = createDatabase();
		const engine = createSyncEngine({
			durableMutations: { scope: () => 'account-a', run: database.run }
		});
		engine.registerWriter('tasks', writer);
		engine.registerMutation(
			defineMutation({
				name: 'fail',
				handler: async (_args, _ctx, actions) => {
					await actions.insert('tasks', { id: 1, title: 'nope' });
					throw new Error('crash before commit');
				}
			})
		);

		await expect(
			engine.runMutation('fail', {}, {}, { operationId: 'op-1' })
		).rejects.toThrow('crash before commit');
		expect(database.rows().size).toBe(0);
		expect(database.receipts().size).toBe(0);
		expect(database.commits()).toBe(0);
	});

	test('rejects durable delivery when no receipt runner is configured', async () => {
		const engine = createSyncEngine();
		engine.registerMutation(
			defineMutation({ name: 'noop', handler: () => 'ok' })
		);
		await expect(
			engine.runMutation('noop', {}, {}, { operationId: 'op-1' })
		).rejects.toThrow('durableMutations');
	});

	test('the wire protocol forwards and echoes the stable operation id', async () => {
		const { engine, handlerCalls } = setup();
		const frames: ServerFrame[] = [];
		const connection = createSyncConnection({
			engine,
			ctx: { accountId: 'account-a' },
			send: (frame) => frames.push(frame)
		});
		const mutate = {
			type: 'mutate' as const,
			mutationId: 41,
			operationId: 'installation-a:operation-41',
			name: 'createTask',
			args: { id: 41, title: 'from wire' }
		};

		await connection.handle(mutate);
		await connection.handle(mutate);

		expect(handlerCalls()).toBe(1);
		expect(frames).toHaveLength(2);
		for (const frame of frames) {
			expect(frame.type).toBe('ack');
			if (frame.type === 'ack') {
				expect(frame.mutationId).toBe(41);
				expect(frame.operationId).toBe('installation-a:operation-41');
			}
		}
	});
});
