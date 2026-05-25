import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncConnection } from '../src/engine/connection';
import type { ServerFrame } from '../src/engine/connection';
import { defineMutation } from '../src/engine/mutation';
import { createSyncEngine, UnauthorizedError } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Order = { id: number; userId: number; status: 'open' | 'closed' };
type Ctx = { userId: number };

const open = (id: number, userId: number): Order => ({
	id,
	userId,
	status: 'open'
});

const setup = () => {
	const table: Order[] = [];
	const engine = createSyncEngine();
	engine.register(
		defineCollection<Order, { userId: number }, Ctx>({
			name: 'orders',
			hydrate: (params) =>
				table.filter(
					(row) =>
						row.userId === params.userId && row.status === 'open'
				),
			match: (row, params) =>
				row.userId === params.userId && row.status === 'open',
			authorize: (params, ctx) => params.userId === ctx.userId
		})
	);
	engine.registerMutation(
		defineMutation<{ id: number }, Ctx, Order>({
			name: 'createOrder',
			handler: async (args, ctx, actions) => {
				const order = open(args.id, ctx.userId);
				table.push(order);
				await actions.change<Order>('orders', {
					op: 'insert',
					row: order
				});
				return order;
			}
		})
	);
	engine.registerMutation(
		defineMutation<unknown, Ctx, void>({
			name: 'adminOnly',
			authorize: (_args, ctx) => ctx.userId === 1,
			handler: () => {}
		})
	);
	engine.registerMutation(
		defineMutation<unknown, Ctx, void>({
			name: 'boom',
			handler: () => {
				throw new Error('handler failed');
			}
		})
	);
	return { engine, table };
};

describe('engine.runMutation', () => {
	test('runs the handler, emits diffs, and returns the result', async () => {
		const { engine } = setup();
		const diffs: ViewDiff<Order>[] = [];
		await engine.subscribe<Order, { userId: number }, Ctx>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff: (diff) => diffs.push(diff)
		});

		const result = (await engine.runMutation(
			'createOrder',
			{ id: 1 },
			{ userId: 5 }
		)) as Order;

		expect(result.id).toBe(1);
		expect(diffs).toHaveLength(1);
		expect(diffs[0]?.added.map((row) => row.id)).toEqual([1]);
	});

	test('an unknown mutation rejects', async () => {
		const { engine } = setup();
		await expect(
			engine.runMutation('nope', {}, { userId: 5 })
		).rejects.toThrow('Unknown mutation');
	});

	test('a denied mutation rejects with UnauthorizedError', async () => {
		const { engine } = setup();
		await expect(
			engine.runMutation('adminOnly', {}, { userId: 5 })
		).rejects.toBeInstanceOf(UnauthorizedError);
	});

	test('a handler throw propagates', async () => {
		const { engine } = setup();
		await expect(
			engine.runMutation('boom', {}, { userId: 5 })
		).rejects.toThrow('handler failed');
	});
});

describe('connection mutate frames', () => {
	const connect = (engine: ReturnType<typeof setup>['engine']) => {
		const frames: ServerFrame[] = [];
		const conn = createSyncConnection({
			engine,
			ctx: { userId: 5 },
			send: (frame) => frames.push(frame)
		});
		return { frames, conn };
	};

	test('a successful mutate acks (after its diff) with the result', async () => {
		const { engine } = setup();
		const { conn, frames } = connect(engine);
		// Subscribe so the mutation produces a diff on this connection.
		await conn.handle({
			type: 'subscribe',
			id: 's1',
			collection: 'orders',
			params: { userId: 5 }
		});
		frames.length = 0;

		await conn.handle({
			type: 'mutate',
			mutationId: 1,
			name: 'createOrder',
			args: { id: 7 }
		});

		// Diff precedes ack (ordered over one socket).
		expect(frames.map((frame) => frame.type)).toEqual(['diff', 'ack']);
		const ack = frames[1];
		if (ack?.type === 'ack') {
			expect(ack.mutationId).toBe(1);
			expect((ack.result as Order).id).toBe(7);
		}
	});

	test('a denied mutate rejects', async () => {
		const { engine } = setup();
		const { conn, frames } = connect(engine);
		await conn.handle({
			type: 'mutate',
			mutationId: 2,
			name: 'adminOnly',
			args: {}
		});

		const frame = frames[0];
		expect(frame?.type).toBe('reject');
		if (frame?.type === 'reject') {
			expect(frame.mutationId).toBe(2);
			expect(frame.message).toContain('Not authorized');
		}
	});

	test('a failing handler rejects with the message', async () => {
		const { engine } = setup();
		const { conn, frames } = connect(engine);
		await conn.handle({
			type: 'mutate',
			mutationId: 3,
			name: 'boom',
			args: {}
		});

		const frame = frames[0];
		expect(frame?.type).toBe('reject');
		if (frame?.type === 'reject') {
			expect(frame.message).toContain('handler failed');
		}
	});

	test('a malformed mutate frame (missing name) is rejected as malformed', async () => {
		const { engine } = setup();
		const { conn, frames } = connect(engine);
		await conn.handle({ type: 'mutate', mutationId: 4 });

		expect(frames[0]?.type).toBe('error');
	});
});
