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

	// RIGHT subquery: open orders, counted per user (no group for zero-open users).
	const openOrderCounts = query<Order>({
		table: 'orders',
		hydrate: () => orders,
		key: (o) => o.id
	})
		.filter((o) => o.status === 'open')
		.groupBy({ key: (o) => o.id, groupBy: (o) => o.userId });

	// LEFT join: every user, even those with no open-order group.
	const usersWithOpenOrders = query<User>({
		table: 'users',
		hydrate: () => users,
		key: (u) => u.id
	}).leftJoin<AggregateGroup, UserWithCount>(openOrderCounts, {
		on: (u) => u.id,
		rightOn: (g) => g.group,
		select: (u, g) => ({ id: u.id, name: u.name, openOrders: g.count }),
		selectUnmatched: (u) => ({ id: u.id, name: u.name, openOrders: 0 }),
		key: (row) => row.id
	});

	engine.registerGraph(
		defineGraphCollection<UserWithCount>({
			name: 'usersWithOpenOrders',
			key: (row) => row.id,
			query: usersWithOpenOrders
		})
	);
	return { engine };
};

/** Replay diffs into a keyed snapshot so we can assert net state. */
const snapshot = () => {
	const state = new Map<number, UserWithCount>();
	return {
		state,
		apply: (diff: ViewDiff<UserWithCount>) => {
			for (const row of diff.removed) {
				state.delete(row.id);
			}
			for (const row of [...diff.added, ...diff.changed]) {
				state.set(row.id, row);
			}
		},
		rows: () => [...state.values()].sort((a, b) => a.id - b.id)
	};
};

describe('left join over a derived subquery', () => {
	test('hydrate includes users with zero open orders', async () => {
		const { engine } = build();
		const sub = await engine.subscribe<UserWithCount>({
			collection: 'usersWithOpenOrders',
			params: undefined,
			ctx: {},
			onDiff: () => {}
		});
		// Ada has 2 open; Bob has 0 (closed order) but still appears.
		expect([...sub.initial].sort((a, b) => a.id - b.id)).toEqual([
			{ id: 5, name: 'Ada', openOrders: 2 },
			{ id: 9, name: 'Bob', openOrders: 0 }
		]);
	});

	test('a zero-count user gains a count, then drops back to zero', async () => {
		const { engine } = build();
		const snap = snapshot();
		const sub = await engine.subscribe<UserWithCount>({
			collection: 'usersWithOpenOrders',
			params: undefined,
			ctx: {},
			onDiff: snap.apply
		});
		for (const row of sub.initial) {
			snap.state.set(row.id, row);
		}

		// Bob gets an open order -> his group appears -> his row updates to 1.
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: order(4, 9)
		});
		expect(snap.state.get(9)).toEqual({
			id: 9,
			name: 'Bob',
			openOrders: 1
		});

		// Bob's only open order closes -> group empties -> back to the zero row.
		await engine.applyChange<Order>('orders', {
			op: 'update',
			row: order(4, 9, 'closed')
		});
		expect(snap.state.get(9)).toEqual({
			id: 9,
			name: 'Bob',
			openOrders: 0
		});
		// Still present (left join never drops the user).
		expect(snap.rows().map((r) => r.id)).toEqual([5, 9]);
	});

	test('a user with open orders reverts to zero when they all close', async () => {
		const { engine } = build();
		const snap = snapshot();
		const sub = await engine.subscribe<UserWithCount>({
			collection: 'usersWithOpenOrders',
			params: undefined,
			ctx: {},
			onDiff: snap.apply
		});
		for (const row of sub.initial) {
			snap.state.set(row.id, row);
		}

		await engine.applyChange<Order>('orders', {
			op: 'update',
			row: order(1, 5, 'closed')
		});
		expect(snap.state.get(5)?.openOrders).toBe(1);

		await engine.applyChange<Order>('orders', {
			op: 'delete',
			row: order(2, 5)
		});
		// Ada keeps her row at zero rather than disappearing.
		expect(snap.state.get(5)).toEqual({
			id: 5,
			name: 'Ada',
			openOrders: 0
		});
		expect(snap.rows().map((r) => r.id)).toEqual([5, 9]);
	});
});
