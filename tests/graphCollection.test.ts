import { describe, expect, test } from 'bun:test';
import { defineGraphCollection, query } from '../src/engine/graph';
import { createSyncEngine, UnauthorizedError } from '../src/engine/syncEngine';
import type { AggregateGroup } from '../src/engine/aggregate';
import type { ViewDiff } from '../src/engine/types';

type Order = { id: number; userId: number; status: string; total: number };
type User = { id: number; region: string };
type Joined = { orderId: number; region: string; total: number };
type Ctx = { role: string };

const order = (
	id: number,
	userId: number,
	total: number,
	status = 'open'
): Order => ({
	id,
	userId,
	status,
	total
});

const build = () => {
	const orders: Order[] = [order(1, 5, 10)];
	const users: User[] = [
		{ id: 5, region: 'eu' },
		{ id: 9, region: 'us' }
	];
	const engine = createSyncEngine();

	// open orders ⋈ users → sum of totals by region
	const salesByRegion = query<Order, void, Ctx>({
		table: 'orders',
		hydrate: () => orders,
		key: (o) => o.id
	})
		.filter((o) => o.status === 'open')
		.join<User, Joined>(
			{ table: 'users', hydrate: () => users, key: (u) => u.id },
			{
				on: (o) => o.userId,
				rightOn: (u) => u.id,
				select: (o, u) => ({
					orderId: o.id,
					region: u.region,
					total: o.total
				}),
				key: (j) => j.orderId
			}
		)
		.groupBy({
			key: (j) => j.orderId,
			groupBy: (j) => j.region,
			value: (j) => j.total
		});

	engine.registerGraph(
		defineGraphCollection<AggregateGroup, void, Ctx>({
			name: 'salesByRegion',
			query: salesByRegion,
			key: (g) => g.group,
			authorize: (_params, ctx) => ctx.role === 'admin'
		})
	);
	return { engine, orders, users };
};

const sumOf = (rows: AggregateGroup[], region: string) =>
	rows.find((g) => g.group === region)?.sum;

const collect = () => {
	const diffs: ViewDiff<AggregateGroup>[] = [];
	return {
		diffs,
		onDiff: (d: ViewDiff<AggregateGroup>) => {
			diffs.push(d);
		}
	};
};

describe('graph collection (filter → join → groupBy)', () => {
	const subscribe = (
		engine: ReturnType<typeof build>['engine'],
		onDiff: (d: ViewDiff<AggregateGroup>) => void = () => {}
	) =>
		engine.subscribe<AggregateGroup, void, Ctx>({
			collection: 'salesByRegion',
			params: undefined,
			ctx: { role: 'admin' },
			onDiff
		});

	test('hydrates the aggregated joined result', async () => {
		const { engine } = build();
		const sub = await subscribe(engine);
		expect(sumOf(sub.initial, 'eu')).toBe(10);
	});

	test('an inserted order updates its region sum', async () => {
		const { engine } = build();
		const { diffs, onDiff } = collect();
		await subscribe(engine, onDiff);

		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: order(2, 9, 20)
		});
		const all = diffs.flatMap((d) => [...d.added, ...d.changed]);
		expect(all.find((g) => g.group === 'us')?.sum).toBe(20);
	});

	test('an order leaving the filter drops from its sum', async () => {
		const { engine } = build();
		const { diffs, onDiff } = collect();
		await subscribe(engine, onDiff);

		await engine.applyChange<Order>('orders', {
			op: 'update',
			row: order(1, 5, 10, 'closed') // no longer open -> leaves
		});
		const last = diffs[diffs.length - 1];
		expect(last?.removed.map((g) => g.group)).toEqual(['eu']); // eu group emptied
	});

	test('a user moving region rebalances sums', async () => {
		const { engine } = build();
		await subscribe(engine);
		const { diffs, onDiff } = collect();
		// re-subscribe to capture diffs cleanly
		const sub = await engine.subscribe<AggregateGroup, void, Ctx>({
			collection: 'salesByRegion',
			params: undefined,
			ctx: { role: 'admin' },
			onDiff
		});
		expect(sumOf(sub.initial, 'eu')).toBe(10);

		await engine.applyChange<User>('users', {
			op: 'update',
			row: { id: 5, region: 'us' } // order 1 moves eu -> us
		});
		const all = diffs.flatMap((d) => [
			...d.added,
			...d.changed,
			...d.removed.map((g) => ({ ...g, sum: undefined }))
		]);
		expect(all.find((g) => g.group === 'eu')).toBeDefined(); // eu touched (emptied)
		expect(
			diffs
				.flatMap((d) => [...d.added, ...d.changed])
				.find((g) => g.group === 'us')?.sum
		).toBe(10);
	});

	test('authorize denies a non-admin', async () => {
		const { engine } = build();
		await expect(
			engine.subscribe<AggregateGroup, void, Ctx>({
				collection: 'salesByRegion',
				params: undefined,
				ctx: { role: 'guest' },
				onDiff: () => {}
			})
		).rejects.toBeInstanceOf(UnauthorizedError);
	});
});
