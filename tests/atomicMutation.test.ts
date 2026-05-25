import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Item = { id: number; n: number };

const itemsCollection = (name: string) =>
	defineCollection<Item>({
		name,
		key: (row) => row.id,
		hydrate: () => [],
		match: () => true
	});

const collect = () => {
	const events: { diff: ViewDiff<Item>; version: number }[] = [];
	return {
		events,
		onDiff: (diff: ViewDiff<Item>, version: number) => {
			events.push({ diff, version });
		}
	};
};

describe('atomic mutations', () => {
	test('many changes in one mutation emit a single merged diff at one version', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'addTwo',
				handler: async (_args, _ctx, actions) => {
					await actions.change('items', {
						op: 'insert',
						row: { id: 1, n: 1 }
					});
					await actions.change('items', {
						op: 'insert',
						row: { id: 2, n: 2 }
					});
				}
			})
		);

		const { events, onDiff } = collect();
		await engine.subscribe<Item>({
			collection: 'items',
			params: undefined,
			ctx: {},
			onDiff
		});
		await engine.runMutation('addTwo', {}, {});

		expect(events).toHaveLength(1);
		expect(events[0]!.diff.added.map((row) => row.id).sort()).toEqual([
			1, 2
		]);
		expect(events[0]!.diff.removed).toEqual([]);
	});

	test('add-then-remove of the same key within a mutation nets to nothing', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'addThenRemove',
				handler: async (_args, _ctx, actions) => {
					await actions.change('items', {
						op: 'insert',
						row: { id: 7, n: 1 }
					});
					await actions.change('items', {
						op: 'delete',
						row: { id: 7, n: 1 }
					});
				}
			})
		);

		const { events, onDiff } = collect();
		await engine.subscribe<Item>({
			collection: 'items',
			params: undefined,
			ctx: {},
			onDiff
		});
		await engine.runMutation('addThenRemove', {}, {});

		// No torn frame: the transient insert never reaches the client.
		expect(events).toHaveLength(0);
	});

	test('insert-then-update of the same key collapses to a single added row', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'addThenBump',
				handler: async (_args, _ctx, actions) => {
					await actions.change('items', {
						op: 'insert',
						row: { id: 3, n: 1 }
					});
					await actions.change('items', {
						op: 'update',
						row: { id: 3, n: 99 }
					});
				}
			})
		);

		const { events, onDiff } = collect();
		await engine.subscribe<Item>({
			collection: 'items',
			params: undefined,
			ctx: {},
			onDiff
		});
		await engine.runMutation('addThenBump', {}, {});

		expect(events).toHaveLength(1);
		expect(events[0]!.diff.added).toEqual([{ id: 3, n: 99 }]);
		expect(events[0]!.diff.changed).toEqual([]);
	});

	test('changes across collections commit at the same version', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.register(itemsCollection('others'));
		engine.registerMutation(
			defineMutation({
				name: 'addBoth',
				handler: async (_args, _ctx, actions) => {
					await actions.change('items', {
						op: 'insert',
						row: { id: 1, n: 1 }
					});
					await actions.change('others', {
						op: 'insert',
						row: { id: 1, n: 9 }
					});
				}
			})
		);

		const items = collect();
		const others = collect();
		await engine.subscribe<Item>({
			collection: 'items',
			params: undefined,
			ctx: {},
			onDiff: items.onDiff
		});
		await engine.subscribe<Item>({
			collection: 'others',
			params: undefined,
			ctx: {},
			onDiff: others.onDiff
		});
		await engine.runMutation('addBoth', {}, {});

		expect(items.events).toHaveLength(1);
		expect(others.events).toHaveLength(1);
		expect(items.events[0]!.version).toBe(others.events[0]!.version);
	});

	test('each mutation advances the version exactly once', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'addTwo',
				handler: async (_args, _ctx, actions) => {
					await actions.change('items', {
						op: 'insert',
						row: { id: 1, n: 1 }
					});
					await actions.change('items', {
						op: 'insert',
						row: { id: 2, n: 2 }
					});
				}
			})
		);
		engine.registerMutation(
			defineMutation({
				name: 'addOne',
				handler: async (_args, _ctx, actions) => {
					await actions.change('items', {
						op: 'insert',
						row: { id: 3, n: 3 }
					});
				}
			})
		);

		const { events, onDiff } = collect();
		await engine.subscribe<Item>({
			collection: 'items',
			params: undefined,
			ctx: {},
			onDiff
		});
		await engine.runMutation('addTwo', {}, {});
		await engine.runMutation('addOne', {}, {});

		expect(events).toHaveLength(2);
		expect(events[1]!.version).toBe(events[0]!.version + 1);
	});

	test('a throwing mutation commits nothing', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'addThenThrow',
				handler: async (_args, _ctx, actions) => {
					await actions.change('items', {
						op: 'insert',
						row: { id: 1, n: 1 }
					});
					throw new Error('boom');
				}
			})
		);

		const { events, onDiff } = collect();
		await engine.subscribe<Item>({
			collection: 'items',
			params: undefined,
			ctx: {},
			onDiff
		});
		await expect(
			engine.runMutation('addThenThrow', {}, {})
		).rejects.toThrow('boom');
		expect(events).toHaveLength(0);
	});
});
