import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncConnection } from '../src/engine/connection';
import type { ServerFrame } from '../src/engine/connection';
import { createSyncEngine } from '../src/engine/syncEngine';

type Order = { id: number; userId: number; status: 'open' | 'closed' };
type Params = { userId: number };
type Ctx = { userId: number };

const open = (id: number, userId: number): Order => ({
	id,
	userId,
	status: 'open'
});

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

const setup = (table: Order[] = []) => {
	const engine = createSyncEngine();
	engine.register(ordersCollection(table));
	const frames: ServerFrame[] = [];
	const conn = createSyncConnection({
		engine,
		ctx: { userId: 5 },
		send: (frame) => frames.push(frame)
	});
	return { engine, frames, conn };
};

const sub = (id: string, userId = 5) => ({
	type: 'subscribe' as const,
	id,
	collection: 'orders',
	params: { userId }
});

describe('createSyncConnection', () => {
	test('a subscribe replies with a snapshot frame', async () => {
		const { conn, frames } = setup([open(1, 5), open(2, 5)]);
		await conn.handle(sub('s1'));

		expect(frames).toHaveLength(1);
		const frame = frames[0];
		expect(frame?.type).toBe('snapshot');
		if (frame?.type === 'snapshot') {
			expect(frame.id).toBe('s1');
			expect((frame.rows as Order[]).map((row) => row.id)).toEqual([
				1, 2
			]);
		}
	});

	test('a change pushes a diff frame tagged with the subscription id', async () => {
		const { conn, frames, engine } = setup([open(1, 5)]);
		await conn.handle(sub('s1'));
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(2, 5)
		});

		const diff = frames.find((frame) => frame.type === 'diff');
		expect(diff?.type).toBe('diff');
		if (diff?.type === 'diff') {
			expect(diff.id).toBe('s1');
			expect((diff.added as Order[]).map((row) => row.id)).toEqual([2]);
		}
	});

	test('multiplexes independent subscriptions by id', async () => {
		const { conn, frames, engine } = setup([]);
		await conn.handle(sub('a'));
		await conn.handle(sub('b'));
		frames.length = 0; // drop the two snapshots

		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(7, 5)
		});

		// One change touching both subs is delivered as one consistent frame
		// (or, for a single sub, a plain diff) — collect ids from either form.
		const ids = frames.flatMap((frame) => {
			if (frame.type === 'diff') {
				return [frame.id];
			}
			if (frame.type === 'frame') {
				return frame.diffs.map((diff) => diff.id);
			}
			return [];
		});
		expect(ids.sort()).toEqual(['a', 'b']);
	});

	test('unsubscribe stops further diffs for that id', async () => {
		const { conn, frames, engine } = setup([]);
		await conn.handle(sub('s1'));
		await conn.handle({ type: 'unsubscribe', id: 's1' });
		frames.length = 0;

		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(1, 5)
		});

		expect(frames).toHaveLength(0);
	});

	test('a duplicate subscription id is rejected', async () => {
		const { conn, frames } = setup([]);
		await conn.handle(sub('s1'));
		frames.length = 0;
		await conn.handle(sub('s1'));

		expect(frames[0]?.type).toBe('error');
		if (frames[0]?.type === 'error') {
			expect(frames[0].id).toBe('s1');
			expect(frames[0].message).toContain('already in use');
		}
	});

	test('an unknown collection replies with an error frame', async () => {
		const { conn, frames } = setup([]);
		await conn.handle({
			type: 'subscribe',
			id: 's1',
			collection: 'nope',
			params: {}
		});

		expect(frames[0]?.type).toBe('error');
		if (frames[0]?.type === 'error') {
			expect(frames[0].message).toContain('Unknown collection');
		}
	});

	test('an unauthorized subscribe replies with an error frame', async () => {
		const { conn, frames } = setup([]);
		// ctx.userId is 5; asking for user 9's rows.
		await conn.handle(sub('s1', 9));

		expect(frames[0]?.type).toBe('error');
		if (frames[0]?.type === 'error') {
			expect(frames[0].message).toContain('Not authorized');
		}
	});

	test('a malformed frame replies with an error', async () => {
		const { conn, frames } = setup([]);
		await conn.handle('not json');
		await conn.handle({ type: 'wat' });

		expect(frames).toHaveLength(2);
		expect(frames.every((frame) => frame.type === 'error')).toBe(true);
	});

	test('accepts a raw JSON string frame', async () => {
		const { conn, frames } = setup([open(1, 5)]);
		await conn.handle(JSON.stringify(sub('s1')));

		expect(frames[0]?.type).toBe('snapshot');
	});

	test('close tears down every subscription', async () => {
		const { conn, frames, engine } = setup([]);
		await conn.handle(sub('a'));
		await conn.handle(sub('b'));
		expect(engine.subscriptionCount('orders')).toBe(2);

		conn.close();
		expect(engine.subscriptionCount('orders')).toBe(0);

		frames.length = 0;
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(1, 5)
		});
		expect(frames).toHaveLength(0);
	});
});
