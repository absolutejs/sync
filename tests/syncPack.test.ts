/**
 * `engine.registerPack` — the sync-packs dispatcher. Tests the contract
 * laid out in `src/engine/syncPacks.design.md`:
 *
 * - dispatches each pack field to the matching `engine.register*` method
 * - rejects table-ownership conflicts between packs (same table, two packs)
 * - lets the host re-register on top of a pack-owned table (host wins-last)
 * - surfaces a missing required dependency
 * - exposes the pack via `engine.inspect().packs`
 */

import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { defineReactiveQuery } from '../src/engine/reactive';
import { defineSchedule } from '../src/engine/schedule';
import { defineSchema, field } from '../src/engine/schema';
import {
	defineSyncPack,
	PackMissingDependencyError,
	PackTableConflictError
} from '../src/engine/pack';
import { createSyncEngine } from '../src/engine/syncEngine';

type Row = { id: string; value: number };

const rejection = async (fn: () => unknown): Promise<unknown> => {
	try {
		await fn();
	} catch (error) {
		return error;
	}
	throw new Error('expected throw');
};

describe('engine.registerPack', () => {
	test('dispatches every pack field to the matching register* method', async () => {
		const engine = createSyncEngine();
		const rows = new Map<string, Row>();
		const reader = { all: () => [...rows.values()] };
		const writer = {
			insert: (row: Row) => {
				rows.set(row.id, row);
				return row;
			},
			update: (row: Row) => {
				rows.set(row.id, row);
				return row;
			},
			delete: (row: { id: string }) => {
				rows.delete(row.id);
			}
		};

		let mutationCalled = false;
		let scheduleCalled = false;

		const pack = defineSyncPack({
			name: '@absolutejs/sync-pack-test',
			version: '0.1.0',
			ownsTables: ['testRows'],
			schemas: defineSchema({
				testRows: {
					fields: { id: field.string, value: field.number }
				}
			}),
			readers: { testRows: reader },
			writers: { testRows: writer },
			collections: [
				defineCollection<Row>({
					name: 'testRows',
					key: (row) => row.id,
					hydrate: () => [...rows.values()],
					match: () => true
				})
			],
			reactiveQueries: [
				defineReactiveQuery<Row>({
					name: 'allTestRows',
					key: (row) => row.id,
					run: ({ db }) => db.all<Row>('testRows')
				})
			],
			mutations: [
				defineMutation({
					name: 'testRows:add',
					handler: async (
						args: { id: string; value: number },
						_ctx,
						actions
					) => {
						mutationCalled = true;
						await actions.insert('testRows', {
							id: args.id,
							value: args.value
						});
					}
				})
			],
			schedules: [
				defineSchedule({
					name: 'testRows:tick',
					pattern: '* * * * *',
					run: () => {
						scheduleCalled = true;
					}
				})
			]
		});

		engine.registerPack(pack);

		// Mutation reachable through runMutation.
		await engine.runMutation('testRows:add', { id: 'a', value: 1 }, {});
		expect(mutationCalled).toBe(true);
		expect(rows.get('a')).toEqual({ id: 'a', value: 1 });

		// Schedule reachable through runSchedule.
		await engine.runSchedule('testRows:tick');
		expect(scheduleCalled).toBe(true);

		// Collection + reactive query reachable through subscribe.
		const subscription = await engine.subscribe<Row>({
			collection: 'testRows',
			params: undefined,
			ctx: {},
			onDiff: () => {}
		});
		expect(subscription.initial.find((r) => r.id === 'a')).toEqual({
			id: 'a',
			value: 1
		});

		// Inspection surfaces the pack.
		const inspection = engine.inspect();
		expect(inspection.packs).toEqual([
			{
				name: '@absolutejs/sync-pack-test',
				version: '0.1.0',
				ownsTables: ['testRows'],
				readsTables: []
			}
		]);
		expect(inspection.readers).toContain('testRows');
		expect(inspection.writers).toContain('testRows');
		expect(inspection.mutations).toContain('testRows:add');
		expect(inspection.schedules.map((s) => s.name)).toContain(
			'testRows:tick'
		);
		expect(inspection.collections.map((c) => c.name)).toContain('testRows');
		expect(inspection.collections.map((c) => c.name)).toContain(
			'allTestRows'
		);
	});

	test('rejects a second pack that claims a table the first pack owns', () => {
		const engine = createSyncEngine();
		engine.registerPack(
			defineSyncPack({
				name: 'pack-a',
				version: '0.1.0',
				ownsTables: ['shared']
			})
		);
		const error = rejection(() =>
			engine.registerPack(
				defineSyncPack({
					name: 'pack-b',
					version: '0.1.0',
					ownsTables: ['shared']
				})
			)
		);
		return error.then((thrown) => {
			expect(thrown).toBeInstanceOf(PackTableConflictError);
			const conflict = thrown as PackTableConflictError;
			expect(conflict.table).toBe('shared');
			expect(conflict.existingPack).toBe('pack-a');
			expect(conflict.newPack).toBe('pack-b');
		});
	});

	test('host-registered table is not counted as pack ownership (host wins-last)', () => {
		const engine = createSyncEngine();
		engine.registerReader('users', { all: () => [] });
		// Pack later claims `users` — allowed; the host's reader was not
		// pack-owned. Subsequent host registrations on top of the pack
		// (e.g. registerPermissions) still win.
		engine.registerPack(
			defineSyncPack({
				name: 'pack-a',
				version: '0.1.0',
				ownsTables: ['users']
			})
		);
		expect(engine.inspect().packs[0]?.ownsTables).toEqual(['users']);
	});

	test('requireDependencies surfaces a missing reader at register time', () => {
		const engine = createSyncEngine();
		const error = rejection(() =>
			engine.registerPack(
				defineSyncPack({
					name: 'comments',
					version: '0.1.0',
					ownsTables: ['comments'],
					readsTables: ['users'],
					requireDependencies: true
				})
			)
		);
		return error.then((thrown) => {
			expect(thrown).toBeInstanceOf(PackMissingDependencyError);
			const missing = thrown as PackMissingDependencyError;
			expect(missing.pack).toBe('comments');
			expect(missing.missingTable).toBe('users');
		});
	});

	test('requireDependencies passes when the reader is wired up first', () => {
		const engine = createSyncEngine();
		engine.registerReader('users', { all: () => [] });
		engine.registerPack(
			defineSyncPack({
				name: 'comments',
				version: '0.1.0',
				ownsTables: ['comments'],
				readsTables: ['users'],
				requireDependencies: true
			})
		);
		expect(engine.inspect().packs[0]?.readsTables).toEqual(['users']);
	});

	test('two packs with non-overlapping ownership coexist', () => {
		const engine = createSyncEngine();
		engine.registerPack(
			defineSyncPack({
				name: 'pack-a',
				version: '0.1.0',
				ownsTables: ['a']
			})
		);
		engine.registerPack(
			defineSyncPack({
				name: 'pack-b',
				version: '0.1.0',
				ownsTables: ['b']
			})
		);
		expect(engine.inspect().packs.map((p) => p.name)).toEqual([
			'pack-a',
			'pack-b'
		]);
	});
});
