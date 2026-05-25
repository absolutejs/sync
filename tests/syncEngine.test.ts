import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine, UnauthorizedError } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Order = { id: number; userId: number; status: 'open' | 'closed' };
type Params = { userId: number };
type Ctx = { userId: number };

const open = (id: number, userId: number): Order => ({
	id,
	userId,
	status: 'open'
});

/** A collection backed by an in-memory table, with incremental matching. */
const ordersCollection = (table: Order[]) =>
	defineCollection<Order, Params, Ctx>({
		name: 'orders',
		hydrate: (params) =>
			table.filter(
				(row) => row.userId === params.userId && row.status === 'open'
			),
		match: (row, params) =>
			row.userId === params.userId && row.status === 'open',
		authorize: (params, ctx) => params.userId === ctx.userId
	});

const collectDiffs = () => {
	const diffs: ViewDiff<Order>[] = [];
	return {
		diffs,
		onDiff: (diff: ViewDiff<Order>) => diffs.push(diff)
	};
};

describe('createSyncEngine', () => {
	test('hydrates the initial snapshot scoped to the caller', async () => {
		const table = [open(1, 5), open(2, 5), open(3, 9)];
		const engine = createSyncEngine();
		engine.register(ordersCollection(table));

		const sub = await engine.subscribe<Order, Params, Ctx>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff: () => {}
		});

		expect(sub.initial.map((row) => row.id)).toEqual([1, 2]);
		expect(engine.subscriptionCount('orders')).toBe(1);
	});

	test('authorize denies a cross-user subscription', async () => {
		const engine = createSyncEngine();
		engine.register(ordersCollection([]));

		await expect(
			engine.subscribe<Order, Params, Ctx>({
				collection: 'orders',
				params: { userId: 9 }, // asking for someone else's rows
				ctx: { userId: 5 },
				onDiff: () => {}
			})
		).rejects.toBeInstanceOf(UnauthorizedError);
		expect(engine.subscriptionCount()).toBe(0);
	});

	test('an unknown collection throws', async () => {
		const engine = createSyncEngine();
		await expect(
			engine.subscribe({
				collection: 'nope',
				params: undefined,
				ctx: {},
				onDiff: () => {}
			})
		).rejects.toThrow('Unknown collection');
	});

	test('applyChange pushes an incremental diff for an entering row', async () => {
		const table = [open(1, 5)];
		const engine = createSyncEngine();
		engine.register(ordersCollection(table));
		const { diffs, onDiff } = collectDiffs();

		await engine.subscribe<Order, Params, Ctx>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff
		});
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(2, 5)
		});

		expect(diffs).toHaveLength(1);
		expect(diffs[0]?.added.map((row) => row.id)).toEqual([2]);
	});

	test('a change to another user’s row never reaches the subscriber', async () => {
		const engine = createSyncEngine();
		engine.register(ordersCollection([open(1, 5)]));
		const { diffs, onDiff } = collectDiffs();

		await engine.subscribe<Order, Params, Ctx>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff
		});
		// user 9's order changes — must not leak into user 5's stream.
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(2, 9)
		});

		expect(diffs).toHaveLength(0);
	});

	test('an empty diff is not pushed', async () => {
		const engine = createSyncEngine();
		engine.register(ordersCollection([open(1, 5)]));
		const { diffs, onDiff } = collectDiffs();

		await engine.subscribe<Order, Params, Ctx>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff
		});
		// deleting a row not in the set yields an empty diff.
		await engine.applyChange<Order>('orders', {
			op: 'delete',
			row: { id: 999 } as Order
		});

		expect(diffs).toHaveLength(0);
	});

	test('fans the same change out to multiple subscribers', async () => {
		const engine = createSyncEngine();
		engine.register(ordersCollection([]));
		const a = collectDiffs();
		const b = collectDiffs();

		await engine.subscribe<Order, Params, Ctx>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff: a.onDiff
		});
		await engine.subscribe<Order, Params, Ctx>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff: b.onDiff
		});
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(7, 5)
		});

		expect(a.diffs[0]?.added.map((row) => row.id)).toEqual([7]);
		expect(b.diffs[0]?.added.map((row) => row.id)).toEqual([7]);
	});

	test('unsubscribe stops further diffs', async () => {
		const engine = createSyncEngine();
		engine.register(ordersCollection([]));
		const { diffs, onDiff } = collectDiffs();

		const sub = await engine.subscribe<Order, Params, Ctx>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff
		});
		sub.unsubscribe();
		expect(engine.subscriptionCount('orders')).toBe(0);
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(1, 5)
		});

		expect(diffs).toHaveLength(0);
	});

	test('refetch fallback re-hydrates when a collection has no predicate', async () => {
		const table = [open(1, 5)];
		// No `match` -> the engine re-runs hydrate on each change and diffs it.
		const collection = defineCollection<Order, Params, Ctx>({
			name: 'orders',
			hydrate: (params) =>
				table.filter(
					(row) =>
						row.userId === params.userId && row.status === 'open'
				),
			authorize: (params, ctx) => params.userId === ctx.userId
		});
		const engine = createSyncEngine();
		engine.register(collection);
		const { diffs, onDiff } = collectDiffs();

		const sub = await engine.subscribe<Order, Params, Ctx>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff
		});
		expect(sub.initial.map((row) => row.id)).toEqual([1]);

		// Mutate the underlying table, then signal a change.
		table.push(open(2, 5));
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(2, 5)
		});

		expect(diffs).toHaveLength(1);
		expect(diffs[0]?.added.map((row) => row.id)).toEqual([2]);
	});
});
