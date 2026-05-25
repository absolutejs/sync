import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { defineSchedule } from '../src/engine/schedule';
import { defineSearchCollection } from '../src/engine/search';
import { createTextIndex } from '../src/engine/textIndex';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { EngineActivity } from '../src/engine/devtools';

type Task = { id: number; title: string };

describe('engine devtools', () => {
	test('inspect() snapshots collections, ops, version, recent changes', async () => {
		const tasks = new Map<number, Task>();
		const engine = createSyncEngine();
		engine.registerReader('tasks', { all: () => [...tasks.values()] });
		engine.registerWriter<Task>('tasks', {
			insert: (data: Task) => data,
			update: (data: Task) => data,
			delete: () => {}
		});
		engine.register(
			defineCollection<Task>({
				name: 'tasks',
				hydrate: () => [...tasks.values()],
				match: () => true,
				key: (task) => task.id
			})
		);
		engine.registerSearch(
			defineSearchCollection<Task>({
				name: 'taskSearch',
				table: 'tasks',
				index: () =>
					createTextIndex<Task>({
						fields: ['title'],
						key: (task) => task.id
					}),
				source: () => [...tasks.values()],
				key: (task) => task.id
			})
		);
		engine.registerSchedule(
			defineSchedule({
				name: 'tick',
				pattern: '* * * * *',
				run: () => {}
			})
		);
		engine.registerMutation(
			defineMutation({
				name: 'addTask',
				handler: (args: Task, _ctx, actions) =>
					actions.insert('tasks', args)
			})
		);

		const sub = await engine.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff: () => {}
		});

		const snap = engine.inspect();
		const view = snap.collections.find((col) => col.name === 'tasks');
		expect(view?.kind).toBe('view');
		expect(view?.tables).toEqual(['tasks']);
		expect(view?.subscriptions).toBe(1);
		expect(
			snap.collections.find((col) => col.name === 'taskSearch')?.kind
		).toBe('search');
		expect(snap.mutations).toContain('addTask');
		expect(snap.schedules.map((sched) => sched.name)).toContain('tick');
		expect(snap.writers).toContain('tasks');
		expect(snap.readers).toContain('tasks');

		tasks.set(1, { id: 1, title: 'a' });
		await engine.applyChange('tasks', {
			op: 'insert',
			row: { id: 1, title: 'a' }
		});
		const next = engine.inspect();
		expect(next.version).toBeGreaterThan(snap.version);
		expect(next.recentChanges.at(-1)).toMatchObject({
			table: 'tasks',
			op: 'insert'
		});

		sub.unsubscribe();
		expect(
			engine.inspect().collections.find((col) => col.name === 'tasks')
				?.subscriptions
		).toBe(0);
	});

	test('onActivity() streams changes and mutation outcomes', async () => {
		const tasks = new Map<number, Task>();
		const engine = createSyncEngine();
		engine.registerWriter<Task>('tasks', {
			insert: (data: Task) => {
				tasks.set(data.id, data);
				return data;
			},
			update: (data: Task) => data,
			delete: () => {}
		});
		engine.registerMutation(
			defineMutation({
				name: 'addTask',
				handler: (args: Task, _ctx, actions) =>
					actions.insert('tasks', args)
			})
		);
		engine.registerMutation(
			defineMutation({
				name: 'boom',
				handler: () => {
					throw new Error('nope');
				}
			})
		);

		const events: EngineActivity[] = [];
		const off = engine.onActivity((event) => events.push(event));

		await engine.runMutation('addTask', { id: 1, title: 'a' }, {});
		expect(
			events.some(
				(event) =>
					event.type === 'change' &&
					event.table === 'tasks' &&
					event.op === 'insert'
			)
		).toBe(true);
		expect(
			events.some(
				(event) =>
					event.type === 'mutation' &&
					event.name === 'addTask' &&
					event.status === 'ok'
			)
		).toBe(true);

		await expect(engine.runMutation('boom', {}, {})).rejects.toThrow();
		expect(
			events.some(
				(event) =>
					event.type === 'mutation' &&
					event.name === 'boom' &&
					event.status === 'error'
			)
		).toBe(true);

		off();
		const before = events.length;
		await engine.runMutation('addTask', { id: 2, title: 'b' }, {});
		expect(events.length).toBe(before);
	});
});
