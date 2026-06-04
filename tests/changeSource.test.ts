import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ChangeSource, EmitChange, ViewDiff } from '../src/engine/types';

type Order = { id: number; userId: number };
type User = { id: number; name: string };
type OrderWithUser = { id: number; userId: number; userName: string };

const collectDiffs = <T>() => {
	const diffs: ViewDiff<T>[] = [];
	return { diffs, onDiff: (diff: ViewDiff<T>) => diffs.push(diff) };
};

describe('multi-table (join) collections', () => {
	const build = () => {
		const orders: Order[] = [{ id: 1, userId: 5 }];
		const users: User[] = [{ id: 5, name: 'Ada' }];
		const engine = createSyncEngine();
		engine.register(
			defineCollection<OrderWithUser>({
				name: 'ordersWithUser',
				tables: ['orders', 'users'], // a join over two tables -> refetch
				key: (row) => row.id,
				hydrate: () =>
					orders.map((order) => ({
						id: order.id,
						userId: order.userId,
						userName:
							users.find((u) => u.id === order.userId)?.name ??
							'?'
					}))
			})
		);
		return { engine, orders, users };
	};

	test('hydrates the joined result', async () => {
		const { engine } = build();
		const sub = await engine.subscribe<OrderWithUser>({
			collection: 'ordersWithUser',
			params: undefined,
			ctx: {},
			onDiff: () => {}
		});
		expect(sub.initial).toEqual([{ id: 1, userId: 5, userName: 'Ada' }]);
	});

	test('a change to either source table refetches the join', async () => {
		const { engine, orders, users } = build();
		const { diffs, onDiff } = collectDiffs<OrderWithUser>();
		await engine.subscribe<OrderWithUser>({
			collection: 'ordersWithUser',
			params: undefined,
			ctx: {},
			onDiff
		});

		// A new order (orders table) — refetch picks up the join row.
		orders.push({ id: 2, userId: 5 });
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: { id: 2, userId: 5 }
		});
		expect(diffs[0]?.added).toEqual([
			{ id: 2, userId: 5, userName: 'Ada' }
		]);

		// A user rename (users table) — the joined userName changes.
		users[0]!.name = 'Bob';
		await engine.applyChange<User>('users', {
			op: 'update',
			row: { id: 5, name: 'Bob' }
		});
		const last = diffs[diffs.length - 1];
		expect(last?.changed.map((row) => row.userName).sort()).toEqual([
			'Bob',
			'Bob'
		]);
	});

	test('a change to an unrelated table is ignored', async () => {
		const { engine } = build();
		const { diffs, onDiff } = collectDiffs<OrderWithUser>();
		await engine.subscribe<OrderWithUser>({
			collection: 'ordersWithUser',
			params: undefined,
			ctx: {},
			onDiff
		});

		await engine.applyChange('products', {
			op: 'insert',
			row: { id: 1 }
		});
		expect(diffs).toHaveLength(0);
	});
});

describe('a table feeding multiple collections', () => {
	test('one change fans to every collection on that table', async () => {
		const orders: Order[] = [];
		const engine = createSyncEngine();
		engine.register(
			defineCollection<Order, { userId: number }>({
				name: 'userOrders',
				tables: ['orders'], // name differs from the source table
				hydrate: (p) => orders.filter((o) => o.userId === p.userId),
				match: (o, p) => o.userId === p.userId
			})
		);
		engine.register(
			defineCollection<Order>({
				name: 'allOrders',
				tables: ['orders'],
				hydrate: () => orders
				// refetch fallback
			})
		);
		const a = collectDiffs<Order>();
		const b = collectDiffs<Order>();
		await engine.subscribe<Order, { userId: number }>({
			collection: 'userOrders',
			params: { userId: 5 },
			ctx: {},
			onDiff: a.onDiff
		});
		await engine.subscribe<Order>({
			collection: 'allOrders',
			params: undefined,
			ctx: {},
			onDiff: b.onDiff
		});

		orders.push({ id: 1, userId: 5 });
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: { id: 1, userId: 5 }
		});

		expect(a.diffs[0]?.added.map((o) => o.id)).toEqual([1]); // incremental
		expect(b.diffs[0]?.added.map((o) => o.id)).toEqual([1]); // refetch
	});
});

describe('connectSource', () => {
	test('routes a source change into the engine and disconnects', async () => {
		const orders: Order[] = [];
		const engine = createSyncEngine();
		engine.register(
			defineCollection<Order, { userId: number }>({
				name: 'orders',
				hydrate: (p) => orders.filter((o) => o.userId === p.userId),
				match: (o, p) => o.userId === p.userId
			})
		);
		const { diffs, onDiff } = collectDiffs<Order>();
		await engine.subscribe<Order, { userId: number }>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: {},
			onDiff
		});

		let emit: EmitChange | undefined;
		let stopped = false;
		const source: ChangeSource = {
			start: (fn) => {
				emit = fn;
			},
			stop: () => {
				stopped = true;
			}
		};

		const disconnect = await engine.connectSource(source);
		expect(emit).toBeDefined();

		await emit?.('orders', { op: 'insert', row: { id: 9, userId: 5 } });
		expect(diffs[0]?.added.map((o) => o.id)).toEqual([9]);

		await disconnect();
		expect(stopped).toBe(true);
	});

	test('metrics().source tracks connection + delivery liveness', async () => {
		const engine = createSyncEngine();
		engine.register(
			defineCollection<Order>({
				name: 'orders',
				hydrate: () => []
			})
		);

		// Wired but nothing connected yet.
		expect(engine.metrics().source).toEqual({
			changesReceived: 0,
			connected: 0,
			lastChangeAgeMs: null,
			lastChangeAt: null
		});

		let emit: EmitChange | undefined;
		const source: ChangeSource = {
			start: (fn) => {
				emit = fn;
			},
			stop: () => {}
		};
		const disconnect = await engine.connectSource(source);
		expect(engine.metrics().source.connected).toBe(1);

		await emit?.('orders', { op: 'insert', row: { id: 1, userId: 5 } });
		const after = engine.metrics().source;
		expect(after.changesReceived).toBe(1);
		expect(after.lastChangeAt).not.toBeNull();
		expect(after.lastChangeAgeMs).not.toBeNull();

		await disconnect();
		expect(engine.metrics().source.connected).toBe(0);
		// The delivery counters persist after disconnect (cumulative since start).
		expect(engine.metrics().source.changesReceived).toBe(1);
	});
});
