import { describe, expect, test } from 'bun:test';
import { prismaCollection } from '../src/adapters/prisma/collection';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Order = {
	id: number;
	userId: number;
	status: 'open' | 'closed';
	total: number;
};
type Params = { userId: number };
type Ctx = { userId: number };

const open = (id: number, userId: number, total = 0): Order => ({
	id,
	userId,
	status: 'open',
	total
});

const collectDiffs = () => {
	const diffs: ViewDiff<Order>[] = [];
	return { diffs, onDiff: (diff: ViewDiff<Order>) => diffs.push(diff) };
};

// "open orders for a user" — the WHERE written once.
const ordersCollection = (table: Order[]) =>
	prismaCollection<Order, Params, Ctx>({
		name: 'orders',
		where: (params) => ({ userId: params.userId, status: 'open' }),
		find: (where) =>
			table.filter((row) =>
				Object.entries(where).every(
					([field, value]) => row[field as keyof Order] === value
				)
			),
		authorize: (params, ctx) => params.userId === ctx.userId
	});

describe('prismaCollection', () => {
	test('hydrates from the shared where', async () => {
		const engine = createSyncEngine();
		engine.register(ordersCollection([open(1, 5), open(2, 5), open(3, 9)]));

		const sub = await engine.subscribe<Order, Params, Ctx>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff: () => {}
		});

		expect(sub.initial.map((row) => row.id)).toEqual([1, 2]);
	});

	test('matches incrementally using the derived predicate', async () => {
		const engine = createSyncEngine();
		engine.register(ordersCollection([open(1, 5)]));
		const { diffs, onDiff } = collectDiffs();

		await engine.subscribe<Order, Params, Ctx>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff
		});

		// entering row (open, user 5)
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(2, 5)
		});
		// another user's row — filtered out by the derived predicate
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(3, 9)
		});
		// row 1 leaves (no longer open)
		await engine.applyChange<Order>('orders', {
			op: 'update',
			row: { id: 1, userId: 5, status: 'closed', total: 0 }
		});

		expect(diffs.map((diff) => diff.added.map((row) => row.id))).toEqual([
			[2],
			[]
		]);
		// 2nd applyChange produced an empty diff (not pushed); 3rd removed row 1.
		expect(diffs[diffs.length - 1]?.removed.map((row) => row.id)).toEqual([
			1
		]);
	});

	test('an unsupported operator degrades to a refetch (still correct)', async () => {
		const table = [open(1, 5)];
		const collection = prismaCollection<Order, Params, Ctx>({
			name: 'orders',
			// `mode` is unsupported by the JS matcher -> match() throws -> refetch.
			where: (params) => ({
				userId: params.userId,
				status: { equals: 'open', mode: 'insensitive' }
			}),
			find: (_where, params) =>
				table.filter(
					(row) =>
						row.userId === params.userId && row.status === 'open'
				),
			authorize: () => true
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

		// Mutate the table, then signal — match() throws, so the engine refetches.
		table.push(open(2, 5));
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(2, 5)
		});

		expect(diffs).toHaveLength(1);
		expect(diffs[0]?.added.map((row) => row.id)).toEqual([2]);
	});
});
