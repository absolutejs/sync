import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { definePermissions } from '../src/engine/permissions';
import { defineReactiveQuery } from '../src/engine/reactive';
import { createSyncEngine, UnauthorizedError } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Task = { id: number; userId: number; title: string; done?: boolean };
type Ctx = { userId: number };

const collect = <T>() => {
	const diffs: ViewDiff<T>[] = [];
	return {
		diffs,
		onDiff: (diff: ViewDiff<T>) => {
			diffs.push(diff);
		}
	};
};

// A read rule that scopes rows to their owner.
const ownerOnly = definePermissions<Ctx>({
	tasks: { read: (ctx, row: Task) => row.userId === ctx.userId }
});

describe('declarative read permissions', () => {
	test('filter the initial snapshot of a view collection (over a loose hydrate)', async () => {
		const tasks = new Map<number, Task>([
			[1, { id: 1, userId: 1, title: 'mine' }],
			[2, { id: 2, userId: 2, title: 'theirs' }]
		]);
		const engine = createSyncEngine({ permissions: ownerOnly });
		engine.register(
			defineCollection<Task, void, Ctx>({
				name: 'tasks',
				// Deliberately too loose: returns everyone's rows + matches all.
				hydrate: () => [...tasks.values()],
				match: () => true,
				key: (task) => task.id
			})
		);

		const sub = await engine.subscribe<Task, void, Ctx>({
			collection: 'tasks',
			params: undefined,
			ctx: { userId: 1 },
			onDiff: () => {}
		});
		// The read rule, not the loose hydrate/match, decides visibility.
		expect(sub.initial).toEqual([{ id: 1, userId: 1, title: 'mine' }]);
	});

	test('filter incremental diffs of a view collection', async () => {
		const engine = createSyncEngine({ permissions: ownerOnly });
		engine.register(
			defineCollection<Task, void, Ctx>({
				name: 'tasks',
				hydrate: () => [],
				match: () => true,
				key: (task) => task.id
			})
		);
		const { diffs, onDiff } = collect<Task>();
		await engine.subscribe<Task, void, Ctx>({
			collection: 'tasks',
			params: undefined,
			ctx: { userId: 1 },
			onDiff
		});

		// A change to another user's row never reaches user 1.
		await engine.applyChange('tasks', {
			op: 'insert',
			row: { id: 2, userId: 2, title: 'theirs' }
		});
		expect(diffs).toHaveLength(0);

		// A change to user 1's own row does.
		await engine.applyChange('tasks', {
			op: 'insert',
			row: { id: 1, userId: 1, title: 'mine' }
		});
		expect(diffs.at(-1)?.added).toEqual([
			{ id: 1, userId: 1, title: 'mine' }
		]);
	});

	test('filter a reactive query reading through ctx.db.all', async () => {
		const tasks = new Map<number, Task>([
			[1, { id: 1, userId: 1, title: 'mine' }],
			[2, { id: 2, userId: 2, title: 'theirs' }]
		]);
		const engine = createSyncEngine({ permissions: ownerOnly });
		engine.registerReader('tasks', { all: () => [...tasks.values()] });
		engine.registerReactive(
			defineReactiveQuery<Task, void, Ctx>({
				name: 'myTasks',
				key: (task) => task.id,
				run: ({ db }) => db.all<Task>('tasks')
			})
		);

		const { diffs, onDiff } = collect<Task>();
		const sub = await engine.subscribe<Task, void, Ctx>({
			collection: 'myTasks',
			params: undefined,
			ctx: { userId: 1 },
			onDiff
		});
		expect(sub.initial).toEqual([{ id: 1, userId: 1, title: 'mine' }]);

		// Inserting another user's task re-runs the query but yields no diff.
		tasks.set(3, { id: 3, userId: 2, title: 'theirs too' });
		await engine.applyChange('tasks', {
			op: 'insert',
			row: { id: 3, userId: 2, title: 'theirs too' }
		});
		expect(diffs).toHaveLength(0);
	});

	test('filter the one-shot hydrate (SSR/HTTP path)', async () => {
		const tasks: Task[] = [
			{ id: 1, userId: 1, title: 'mine' },
			{ id: 2, userId: 2, title: 'theirs' }
		];
		const engine = createSyncEngine({ permissions: ownerOnly });
		engine.register(
			defineCollection<Task, void, Ctx>({
				name: 'tasks',
				hydrate: () => tasks,
				key: (task) => task.id
			})
		);
		const rows = await engine.hydrate('tasks', undefined, { userId: 1 });
		expect(rows).toEqual([{ id: 1, userId: 1, title: 'mine' }]);
	});
});

describe('declarative write permissions', () => {
	// A store + writer + reader shared by the write tests.
	const makeWritableEngine = (
		permissions: Parameters<typeof createSyncEngine>[0]
	) => {
		const tasks = new Map<number, Task>();
		const engine = createSyncEngine(permissions);
		engine.registerWriter<Task, Ctx>('tasks', {
			insert: (data: Task) => {
				tasks.set(data.id, data);
				return data;
			},
			update: (data: Partial<Task> & { id: number }) => {
				const next = { ...tasks.get(data.id)!, ...data };
				tasks.set(data.id, next);
				return next;
			},
			delete: (row: { id: number }) => {
				tasks.delete(row.id);
			}
		});
		engine.registerReader<Ctx>('tasks', {
			all: () => [...tasks.values()],
			get: (id) => tasks.get(id as number),
			key: (row) => (row as Task).id
		});
		return { engine, tasks };
	};

	test('deny an insert whose new row violates the rule', async () => {
		const { engine, tasks } = makeWritableEngine({
			permissions: {
				tasks: {
					insert: (ctx: Ctx, row: Task) => row.userId === ctx.userId
				}
			}
		});
		engine.registerMutation(
			defineMutation<{ id: number; userId: number }, Ctx>({
				name: 'addTask',
				handler: (args, _ctx, actions) =>
					actions.insert('tasks', {
						id: args.id,
						userId: args.userId,
						title: 't'
					})
			})
		);

		// Creating a row for someone else is rejected and nothing is written.
		await expect(
			engine.runMutation('addTask', { id: 1, userId: 2 }, { userId: 1 })
		).rejects.toBeInstanceOf(UnauthorizedError);
		expect(tasks.size).toBe(0);

		// Creating your own row succeeds.
		await engine.runMutation(
			'addTask',
			{ id: 2, userId: 1 },
			{ userId: 1 }
		);
		expect(tasks.get(2)).toEqual({ id: 2, userId: 1, title: 't' });
	});

	test('check update/delete against the existing row, not the payload', async () => {
		const { engine, tasks } = makeWritableEngine({
			permissions: {
				tasks: {
					write: (ctx: Ctx, row: Task) => row.userId === ctx.userId
				}
			}
		});
		tasks.set(1, { id: 1, userId: 1, title: 'owned by 1' });
		engine.registerMutation(
			defineMutation<{ id: number; done: boolean }, Ctx>({
				name: 'toggle',
				// Minimal payload — no userId — so the rule must load the row.
				handler: (args, _ctx, actions) =>
					actions.update('tasks', { id: args.id, done: args.done })
			})
		);
		engine.registerMutation(
			defineMutation<{ id: number }, Ctx>({
				name: 'remove',
				handler: (args, _ctx, actions) =>
					actions.delete('tasks', { id: args.id })
			})
		);

		// User 2 can't touch user 1's row, even though the payload omits userId.
		await expect(
			engine.runMutation('toggle', { id: 1, done: true }, { userId: 2 })
		).rejects.toBeInstanceOf(UnauthorizedError);
		await expect(
			engine.runMutation('remove', { id: 1 }, { userId: 2 })
		).rejects.toBeInstanceOf(UnauthorizedError);
		expect(tasks.get(1)?.done).toBeUndefined();

		// The owner can.
		await engine.runMutation(
			'toggle',
			{ id: 1, done: true },
			{ userId: 1 }
		);
		expect(tasks.get(1)?.done).toBe(true);
		await engine.runMutation('remove', { id: 1 }, { userId: 1 });
		expect(tasks.has(1)).toBe(false);
	});

	test('a specific op rule overrides the write shorthand', async () => {
		const { engine, tasks } = makeWritableEngine({
			permissions: {
				tasks: {
					// Deletes are allowed for anyone; other writes are owner-only.
					write: (ctx: Ctx, row: Task) => row.userId === ctx.userId,
					delete: () => true
				}
			}
		});
		tasks.set(1, { id: 1, userId: 1, title: 'owned by 1' });
		engine.registerMutation(
			defineMutation<{ id: number }, Ctx>({
				name: 'remove',
				handler: (args, _ctx, actions) =>
					actions.delete('tasks', { id: args.id })
			})
		);
		// A non-owner can delete (specific delete rule wins over write).
		await engine.runMutation('remove', { id: 1 }, { userId: 2 });
		expect(tasks.has(1)).toBe(false);
	});
});

describe('permissions registration', () => {
	test('registerPermissions is equivalent to the constructor option', async () => {
		const tasks: Task[] = [
			{ id: 1, userId: 1, title: 'mine' },
			{ id: 2, userId: 2, title: 'theirs' }
		];
		const engine = createSyncEngine();
		engine.register(
			defineCollection<Task, void, Ctx>({
				name: 'tasks',
				hydrate: () => tasks,
				key: (task) => task.id
			})
		);
		// Without rules, everything is visible.
		expect(
			await engine.hydrate('tasks', undefined, { userId: 1 })
		).toHaveLength(2);

		engine.registerPermissions<Task, Ctx>('tasks', {
			read: (ctx, row) => row.userId === ctx.userId
		});
		expect(await engine.hydrate('tasks', undefined, { userId: 1 })).toEqual(
			[{ id: 1, userId: 1, title: 'mine' }]
		);
	});
});
