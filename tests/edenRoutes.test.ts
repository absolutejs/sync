import { describe, expect, test } from 'bun:test';
import { treaty } from '@elysiajs/eden';
import { Elysia, t } from 'elysia';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { hydrateRoute, mutateRoute } from '../src/engine/routes';
import { createSyncEngine, UnauthorizedError } from '../src/engine/syncEngine';

type Order = { id: number; userId: number; status: string };
type Ctx = { userId: number };

const build = () => {
	const orders: Order[] = [
		{ id: 1, userId: 5, status: 'open' },
		{ id: 2, userId: 9, status: 'open' }
	];
	const engine = createSyncEngine();

	const ordersCollection = defineCollection<Order, { userId: number }, Ctx>({
		name: 'orders',
		hydrate: (params) =>
			orders.filter((order) => order.userId === params.userId),
		authorize: (params, ctx) => params.userId === ctx.userId
	});
	engine.register(ordersCollection);

	const createOrder = defineMutation<{ total: number }, Ctx, Order>({
		name: 'createOrder',
		handler: async (args, ctx, actions) => {
			const order: Order = {
				id: 3,
				userId: ctx.userId,
				status: `open:${args.total}`
			};
			orders.push(order);
			await actions.change<Order>('orders', { op: 'insert', row: order });
			return order;
		}
	});
	engine.registerMutation(createOrder);

	return { engine, ordersCollection, createOrder };
};

// One typed Elysia app whose routes Eden infers from the collection/mutation defs.
const makeApp = ({
	engine,
	ordersCollection,
	createOrder
}: ReturnType<typeof build>) =>
	new Elysia()
		.get(
			'/sync/orders',
			hydrateRoute(engine, ordersCollection, () => ({ userId: 5 })),
			{ query: t.Object({ userId: t.Numeric() }) }
		)
		.post(
			'/sync/createOrder',
			mutateRoute(engine, createOrder, () => ({ userId: 5 })),
			{ body: t.Object({ total: t.Number() }) }
		);

describe('engine.hydrate', () => {
	test('authorizes and returns the scoped rows', async () => {
		const { engine } = build();
		const rows = (await engine.hydrate(
			'orders',
			{ userId: 5 },
			{ userId: 5 }
		)) as Order[];
		expect(rows.map((row) => row.id)).toEqual([1]);
	});

	test('rejects an unknown collection', async () => {
		const { engine } = build();
		await expect(engine.hydrate('nope', {}, {})).rejects.toThrow(
			'Unknown collection'
		);
	});

	test('rejects when authorize denies', async () => {
		const { engine } = build();
		await expect(
			engine.hydrate('orders', { userId: 9 }, { userId: 5 })
		).rejects.toBeInstanceOf(UnauthorizedError);
	});
});

describe('Eden-typed routes (real treaty round trip)', () => {
	test('hydrate route returns typed rows over HTTP', async () => {
		const server = makeApp(build()).listen(0);
		const api = treaty<typeof server>(`localhost:${server.server?.port}`);

		const { data, error } = await api.sync.orders.get({
			query: { userId: 5 }
		});

		expect(error).toBeNull();
		// Type-flow proof: `data` is inferred as Order[] from the collection def,
		// not unknown[] — this line fails to compile if the type were lost.
		const typed: Order[] | null = data;
		expect(typed?.map((row) => row.id)).toEqual([1]);

		await server.stop(true);
	});

	test('mutate route returns the typed result and emits a change', async () => {
		const server = makeApp(build()).listen(0);
		const api = treaty<typeof server>(`localhost:${server.server?.port}`);

		const { data, error } = await api.sync.createOrder.post({ total: 42 });

		expect(error).toBeNull();
		const typed: Order | null = data; // inferred as Order from the mutation def
		expect(typed?.id).toBe(3);
		expect(typed?.status).toBe('open:42');

		await server.stop(true);
	});

	test('a denied hydrate surfaces as an error response', async () => {
		const server = makeApp(build()).listen(0);
		const api = treaty<typeof server>(`localhost:${server.server?.port}`);

		const { data } = await api.sync.orders.get({ query: { userId: 9 } });
		expect(data).toBeNull(); // authorize denied -> error response, no rows

		await server.stop(true);
	});
});
