import { describe, expect, test } from 'bun:test';
import { defineMutation } from '../src/engine/mutation';
import { defineReactiveQuery } from '../src/engine/reactive';
import { defineSchema, field } from '../src/engine/schema';
import { createSyncEngine, SchemaError } from '../src/engine/syncEngine';

type Task = { id: number; title: string; done: boolean };

const makeEngine = (options: Parameters<typeof createSyncEngine>[0] = {}) => {
	const tasks = new Map<number, Task>();
	const engine = createSyncEngine(options);
	engine.registerReader('tasks', { all: () => [...tasks.values()] });
	engine.registerWriter<Task>('tasks', {
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
	engine.registerMutation(
		defineMutation({
			name: 'add',
			handler: (args: Task, _ctx, actions) =>
				actions.insert('tasks', args)
		})
	);
	engine.registerMutation(
		defineMutation({
			name: 'edit',
			handler: (args: Partial<Task> & { id: number }, _ctx, actions) =>
				actions.update('tasks', args)
		})
	);
	return { engine, tasks };
};

const taskSchema = defineSchema({
	tasks: {
		fields: { id: field.number, title: field.string, done: field.boolean }
	}
});

describe('schema validation (writes)', () => {
	test('accepts a valid write, rejects wrong types and missing fields', async () => {
		const { engine, tasks } = makeEngine({ schemas: taskSchema });
		await engine.runMutation('add', { id: 1, title: 'a', done: false }, {});
		expect(tasks.get(1)).toEqual({ id: 1, title: 'a', done: false });

		await expect(
			engine.runMutation('add', { id: 2, title: 5, done: false }, {})
		).rejects.toBeInstanceOf(SchemaError);
		await expect(
			engine.runMutation('add', { id: 3, title: 'b' }, {})
		).rejects.toBeInstanceOf(SchemaError);
		expect(tasks.has(2)).toBe(false);
		expect(tasks.has(3)).toBe(false);
	});

	test('update validates only the supplied fields', async () => {
		const { engine, tasks } = makeEngine({ schemas: taskSchema });
		await engine.runMutation('add', { id: 1, title: 'a', done: false }, {});

		await engine.runMutation('edit', { id: 1, done: true }, {});
		expect(tasks.get(1)?.done).toBe(true);

		await expect(
			engine.runMutation('edit', { id: 1, done: 'yes' }, {})
		).rejects.toBeInstanceOf(SchemaError);
	});

	test('field kit: optional, enum, array', async () => {
		const schemas = defineSchema({
			tasks: {
				fields: {
					id: field.number,
					title: field.string,
					done: field.boolean,
					note: field.optional(field.string),
					priority: field.enum('low', 'high'),
					tags: field.array(field.string)
				}
			}
		});
		const { engine, tasks } = makeEngine({ schemas });
		// `note` omitted (optional) — accepted.
		await engine.runMutation(
			'add',
			{ id: 1, title: 'a', done: false, priority: 'low', tags: ['x'] },
			{}
		);
		expect(tasks.get(1)).toBeDefined();

		await expect(
			engine.runMutation(
				'add',
				{ id: 2, title: 'a', done: false, priority: 'mid', tags: [] },
				{}
			)
		).rejects.toBeInstanceOf(SchemaError);
		await expect(
			engine.runMutation(
				'add',
				{ id: 3, title: 'a', done: false, priority: 'low', tags: [1] },
				{}
			)
		).rejects.toBeInstanceOf(SchemaError);
	});

	test('no schema registered → no validation', async () => {
		const { engine, tasks } = makeEngine();
		await engine.runMutation('add', { id: 1, title: 7, done: 'x' }, {});
		expect(tasks.has(1)).toBe(true);
	});
});

describe('schema migration (lazy upcast on read)', () => {
	test('migrates stored rows to the current shape on read', async () => {
		const tasks = new Map<number, { id: number; title: string }>();
		const engine = createSyncEngine({
			schemas: defineSchema({
				tasks: {
					version: 2,
					fields: {
						id: field.number,
						title: field.string,
						done: field.boolean
					},
					migrate: (row: { done?: boolean }) =>
						row.done === undefined ? { ...row, done: false } : row
				}
			})
		});
		engine.registerReader('tasks', { all: () => [...tasks.values()] });
		engine.registerReactive(
			defineReactiveQuery<Task>({
				name: 'tasks',
				key: (task) => task.id,
				run: ({ db }) => db.all<Task>('tasks')
			})
		);

		// A legacy row missing `done` is upcast when read through the engine.
		tasks.set(1, { id: 1, title: 'legacy' });
		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff: () => {}
		});
		expect(sub.initial).toEqual([{ id: 1, title: 'legacy', done: false }]);
		sub.unsubscribe();

		// engine.migrate exposes the same upcast for reads the engine doesn't own.
		expect(engine.migrate('tasks', { id: 2, title: 'x' })).toEqual({
			id: 2,
			title: 'x',
			done: false
		});
	});
});
