import { describe, expect, test } from 'bun:test';
import { defineGraphCollection, query } from '../src/engine/graph';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { AggregateGroup } from '../src/engine/aggregate';
import type { ViewDiff } from '../src/engine/types';

type Order = { id: number; userId: number; status: string };
type User = { id: number; name: string };
type UserWithCount = { id: number; name: string; openOrders: number };

const order = (id: number, userId: number, status = 'open'): Order => ({
	id,
	userId,
	status
});

const build = () => {
	const orders: Order[] = [order(1, 5), order(2, 5), order(3, 9, 'closed')];
	const users: User[] = [
		{ id: 5, name: 'Ada' },
		{ id: 9, name: 'Bob' }
	];
	const engine = createSyncEngine();

	// RIGHT subquery: open orders, counted per user.
	const openOrderCounts = query<Order>({
		table: 'orders',
		hydrate: () => orders,
		key: (o) => o.id
	})
		.filter((o) => o.status === 'open')
		.groupBy({ key: (o) => o.id, groupBy: (o) => o.userId });

	// LEFT users ⋈ the derived counts subquery (inner join).
	const usersWithOpenOrders = query<User>({
		table: 'users',
		hydrate: () => users,
		key: (u) => u.id
	}).join<AggregateGroup, UserWithCount>(openOrderCounts, {
		on: (u) => u.id,
		rightOn: (g) => g.group,
		select: (u, g) => ({ id: u.id, name: u.name, openOrders: g.count }),
		key: (row) => row.id
	});

	engine.registerGraph(
		defineGraphCollection<UserWithCount>({
			name: 'usersWithOpenOrders',
			key: (row) => row.id,
			query: usersWithOpenOrders
		})
	);
	return { engine, orders, users };
};

const collect = () => {
	const diffs: ViewDiff<UserWithCount>[] = [];
	return {
		diffs,
		onDiff: (d: ViewDiff<UserWithCount>) => {
			diffs.push(d);
		}
	};
};

describe('join with a derived subquery on the right', () => {
	test('hydrates from the joined + aggregated subquery', async () => {
		const { engine } = build();
		const sub = await engine.subscribe<UserWithCount>({
			collection: 'usersWithOpenOrders',
			params: undefined,
			ctx: {},
			onDiff: () => {}
		});
		// only Ada has open orders (2); Bob's lone order is closed -> excluded.
		expect(sub.initial).toEqual([{ id: 5, name: 'Ada', openOrders: 2 }]);
	});

	test('a change to the subquery’s source propagates through the join', async () => {
		const { engine } = build();
		const { diffs, onDiff } = collect();
		await engine.subscribe<UserWithCount>({
			collection: 'usersWithOpenOrders',
			params: undefined,
			ctx: {},
			onDiff
		});

		// Bob gets an open order -> his count group appears -> he enters the join.
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: order(4, 9)
		});
		expect(diffs.at(-1)?.added).toEqual([
			{ id: 9, name: 'Bob', openOrders: 1 }
		]);

		// One of Ada's orders closes -> her count drops -> joined row changes.
		await engine.applyChange<Order>('orders', {
			op: 'update',
			row: order(1, 5, 'closed')
		});
		expect(diffs.at(-1)?.changed).toEqual([
			{ id: 5, name: 'Ada', openOrders: 1 }
		]);

		// Ada's last open order is deleted -> her group empties -> she leaves.
		await engine.applyChange<Order>('orders', {
			op: 'delete',
			row: order(2, 5)
		});
		expect(diffs.at(-1)?.removed.map((r) => r.id)).toEqual([5]);
	});

	test('a left-side (user) change updates the joined row', async () => {
		const { engine } = build();
		const { diffs, onDiff } = collect();
		await engine.subscribe<UserWithCount>({
			collection: 'usersWithOpenOrders',
			params: undefined,
			ctx: {},
			onDiff
		});

		await engine.applyChange<User>('users', {
			op: 'update',
			row: { id: 5, name: 'Ada Lovelace' }
		});
		expect(diffs.at(-1)?.changed).toEqual([
			{ id: 5, name: 'Ada Lovelace', openOrders: 2 }
		]);
	});
});
