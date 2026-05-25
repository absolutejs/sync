import { describe, expect, test } from 'bun:test';
import { defineJoinCollection } from '../src/engine/collection';
import { createSyncEngine, UnauthorizedError } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Order = { id: number; userId: number; total: number };
type User = { id: number; name: string };
type Joined = { orderId: number; userId: number; userName: string };
type Ctx = { userId: number };

const order = (id: number, userId: number, total = 0): Order => ({
	id,
	userId,
	total
});
const user = (id: number, name: string): User => ({ id, name });

const build = () => {
	const orders: Order[] = [order(1, 5)];
	const users: User[] = [user(5, 'Ada')];
	const engine = createSyncEngine();
	engine.registerJoin(
		defineJoinCollection<Order, User, Joined, Ctx, Ctx>({
			name: 'ordersWithUser',
			left: {
				table: 'orders',
				hydrate: (p) => orders.filter((o) => o.userId === p.userId),
				key: (o) => o.id,
				on: (o) => o.userId,
				match: (o, p) => o.userId === p.userId
			},
			right: {
				table: 'users',
				hydrate: (p) => users.filter((u) => u.id === p.userId),
				key: (u) => u.id,
				on: (u) => u.id,
				match: (u, p) => u.id === p.userId
			},
			select: (o, u) => ({
				orderId: o.id,
				userId: o.userId,
				userName: u.name
			}),
			key: (out) => out.orderId,
			authorize: (p, ctx) => p.userId === ctx.userId
		})
	);
	return { engine, orders, users };
};

const collectDiffs = () => {
	const diffs: ViewDiff<Joined>[] = [];
	return { diffs, onDiff: (d: ViewDiff<Joined>) => diffs.push(d) };
};

describe('join collection (incremental)', () => {
	test('hydrates the joined result', async () => {
		const { engine } = build();
		const sub = await engine.subscribe<Joined, Ctx, Ctx>({
			collection: 'ordersWithUser',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff: () => {}
		});
		expect(sub.initial).toEqual([
			{ orderId: 1, userId: 5, userName: 'Ada' }
		]);
	});

	test('a change to the left table incrementally updates the join', async () => {
		const { engine } = build();
		const { diffs, onDiff } = collectDiffs();
		await engine.subscribe<Joined, Ctx, Ctx>({
			collection: 'ordersWithUser',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff
		});

		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: order(2, 5)
		});
		expect(diffs[0]?.added).toEqual([
			{ orderId: 2, userId: 5, userName: 'Ada' }
		]);
	});

	test('a change to the right table updates all joined rows', async () => {
		const { engine } = build();
		const { diffs, onDiff } = collectDiffs();
		await engine.subscribe<Joined, Ctx, Ctx>({
			collection: 'ordersWithUser',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff
		});
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: order(2, 5)
		});

		await engine.applyChange<User>('users', {
			op: 'update',
			row: user(5, 'Bob')
		});
		const last = diffs[diffs.length - 1];
		expect(last?.changed.map((r) => r.userName).sort()).toEqual([
			'Bob',
			'Bob'
		]);
	});

	test('a left row scoped out by match leaves the join', async () => {
		const { engine } = build();
		const { diffs, onDiff } = collectDiffs();
		await engine.subscribe<Joined, Ctx, Ctx>({
			collection: 'ordersWithUser',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff
		});
		// order 1 reassigned to another user -> fails left.match -> leaves.
		await engine.applyChange<Order>('orders', {
			op: 'update',
			row: order(1, 9)
		});
		expect(diffs[0]?.removed.map((r) => r.orderId)).toEqual([1]);
	});

	test('authorize denies a cross-user subscription', async () => {
		const { engine } = build();
		await expect(
			engine.subscribe<Joined, Ctx, Ctx>({
				collection: 'ordersWithUser',
				params: { userId: 9 },
				ctx: { userId: 5 },
				onDiff: () => {}
			})
		).rejects.toBeInstanceOf(UnauthorizedError);
	});
});
