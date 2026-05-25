import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import type { TableWriter } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Task = { id: number; title: string; done: boolean };

/** An in-memory "database" table + a writer over it (stands in for any ORM). */
const makeTasksTable = () => {
	const rows = new Map<number, Task>();
	let nextId = 1;
	const writer: TableWriter<Task> = {
		insert: (data: { title: string }) => {
			const row: Task = { id: nextId++, title: data.title, done: false };
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
	};
	return { rows, writer };
};

const setup = () => {
	const { rows, writer } = makeTasksTable();
	const engine = createSyncEngine();
	engine.register(
		defineCollection<Task>({
			name: 'tasks',
			key: (task) => task.id,
			hydrate: () => [...rows.values()],
			match: () => true
		})
	);
	engine.registerWriter('tasks', writer);
	return { engine, rows };
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

describe('auto-emitting writes (actions.insert/update/delete)', () => {
	test('insert persists AND goes live without any manual change() call', async () => {
		const { engine, rows } = setup();
		engine.registerMutation(
			defineMutation({
				name: 'addTask',
				// Note: no actions.change — the write IS the emit.
				handler: (args: { title: string }, _ctx, actions) =>
					actions.insert<Task>('tasks', { title: args.title })
			})
		);

		const { events, onDiff } = collect();
		await engine.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff
		});

		const created = (await engine.runMutation(
			'addTask',
			{ title: 'milk' },
			{}
		)) as Task;

		// Persisted (with a DB-assigned id)…
		expect(created.id).toBe(1);
		expect(rows.get(1)).toEqual({ id: 1, title: 'milk', done: false });
		// …and emitted live, carrying the stored row.
		expect(events).toHaveLength(1);
		expect(events[0]!.added).toEqual([
			{ id: 1, title: 'milk', done: false }
		]);
	});

	test('update and delete persist and emit too', async () => {
		const { engine, rows } = setup();
		engine.registerMutation(
			defineMutation({
				name: 'add',
				handler: (args: { title: string }, _c, actions) =>
					actions.insert<Task>('tasks', { title: args.title })
			})
		);
		engine.registerMutation(
			defineMutation({
				name: 'toggle',
				handler: async (args: { id: number }, _c, actions) => {
					const current = rows.get(args.id)!;
					return actions.update<Task>('tasks', {
						...current,
						done: !current.done
					});
				}
			})
		);
		engine.registerMutation(
			defineMutation({
				name: 'remove',
				handler: (args: { id: number }, _c, actions) =>
					actions.delete('tasks', { id: args.id })
			})
		);

		const { events, onDiff } = collect();
		await engine.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff
		});

		await engine.runMutation('add', { title: 'a' }, {});
		await engine.runMutation('toggle', { id: 1 }, {});
		await engine.runMutation('remove', { id: 1 }, {});

		expect(events).toHaveLength(3);
		expect(events[0]!.added).toEqual([{ id: 1, title: 'a', done: false }]);
		expect(events[1]!.changed).toEqual([{ id: 1, title: 'a', done: true }]);
		expect(events[2]!.removed.map((row) => row.id)).toEqual([1]);
		expect(rows.size).toBe(0);
	});

	test('writes through one mutation stay atomic (one diff, one version)', async () => {
		const { engine } = setup();
		engine.registerMutation(
			defineMutation({
				name: 'addTwo',
				handler: async (_a, _c, actions) => {
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

		expect(events).toHaveLength(1);
		expect(events[0]!.added.map((row) => row.title).sort()).toEqual([
			'a',
			'b'
		]);
	});

	test('a write to a table with no registered writer throws (and emits nothing)', async () => {
		const { engine } = setup();
		engine.registerMutation(
			defineMutation({
				name: 'bad',
				handler: (_a, _c, actions) =>
					actions.insert('unregistered', { title: 'x' })
			})
		);
		const { events, onDiff } = collect();
		await engine.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff
		});
		await expect(engine.runMutation('bad', {}, {})).rejects.toThrow(
			'No writer registered'
		);
		expect(events).toHaveLength(0);
	});
});
