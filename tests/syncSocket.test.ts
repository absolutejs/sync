import { describe, expect, test } from 'bun:test';
import { Elysia } from 'elysia';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';
import { createSyncSocketController, syncSocket } from '../src/engine/socket';

type Order = { id: number; userId: number; status: 'open' | 'closed' };
type Params = { userId: number };
type Ctx = { userId: number };

const open = (id: number, userId: number): Order => ({
	id,
	userId,
	status: 'open'
});

const waitFor = async (predicate: () => boolean, timeoutMs = 1000) => {
	const start = Date.now();
	while (!predicate()) {
		if (Date.now() - start > timeoutMs) {
			throw new Error('waitFor: timed out');
		}
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
};

const eventWithin = <T>(promise: Promise<T>, label: string) =>
	Promise.race([
		promise,
		new Promise<never>((_, reject) =>
			setTimeout(() => reject(new Error(`${label}: timed out`)), 1000)
		)
	]);

type AnyFrame = { type: string; [key: string]: unknown };

const connect = (port: number, frames: AnyFrame[]) => {
	const ws = new WebSocket(`ws://localhost:${port}/sync/ws`);
	ws.addEventListener('message', (event) => {
		frames.push(JSON.parse(event.data as string) as AnyFrame);
	});
	return ws;
};

describe('syncSocket (Elysia WebSocket)', () => {
	test('authenticates from a single-use first-frame ticket', async () => {
		const engine = createSyncEngine();
		engine.register(
			defineCollection<Order, Params, Ctx>({
				authorize: (params, ctx) => params.userId === ctx.userId,
				hydrate: () => [open(1, 5)],
				match: (row, params) => row.userId === params.userId,
				name: 'orders'
			})
		);
		const tickets = new Set(['once']);
		const app = new Elysia()
			.use(
				syncSocket({
					authenticate: (ticket) => {
						if (!tickets.delete(ticket))
							throw new Error('invalid ticket');
						return { userId: 5 };
					},
					engine
				})
			)
			.listen(0);
		const port = app.server?.port ?? 0;
		const frames: AnyFrame[] = [];
		const ws = connect(port, frames);
		await new Promise<void>((resolve, reject) => {
			ws.addEventListener('open', () => resolve());
			ws.addEventListener('error', () => reject(new Error('ws error')));
		});
		ws.send(JSON.stringify({ ticket: 'once', type: 'authenticate' }));
		ws.send(
			JSON.stringify({
				collection: 'orders',
				id: 's1',
				params: { userId: 5 },
				type: 'subscribe'
			})
		);
		await waitFor(() => frames.some((frame) => frame.type === 'snapshot'));
		expect(frames.find((frame) => frame.type === 'snapshot')?.rows).toEqual(
			[open(1, 5)]
		);

		const replay = connect(port, []);
		const replayClose = new Promise<CloseEvent>((resolve) =>
			replay.addEventListener('close', resolve)
		);
		await new Promise<void>((resolve) =>
			replay.addEventListener('open', () => resolve())
		);
		replay.send(JSON.stringify({ ticket: 'once', type: 'authenticate' }));
		expect((await eventWithin(replayClose, 'replay close')).code).toBe(
			4401
		);

		ws.close();
		replay.close();
		void app.stop(false);
	});

	test('rejects a non-authentication first frame when tickets are required', async () => {
		const engine = createSyncEngine();
		const app = new Elysia()
			.use(syncSocket({ authenticate: () => ({}), engine }))
			.listen(0);
		const port = app.server?.port ?? 0;
		const ws = connect(port, []);
		const closed = new Promise<CloseEvent>((resolve) =>
			ws.addEventListener('close', resolve)
		);
		await new Promise<void>((resolve) =>
			ws.addEventListener('open', () => resolve())
		);
		ws.send(JSON.stringify({ type: 'subscribe' }));
		const event = await eventWithin(closed, 'missing auth close');
		expect(event.code).toBe(4401);
		expect(event.reason).toBe('Authentication Required');
		ws.close();
		void app.stop(false);
	});

	test('times out sockets that never provide an authentication frame', async () => {
		const engine = createSyncEngine();
		const app = new Elysia()
			.use(
				syncSocket({
					authenticate: () => ({}),
					authenticationTimeoutMs: 20,
					engine
				})
			)
			.listen(0);
		const port = app.server?.port ?? 0;
		const ws = connect(port, []);
		const closed = new Promise<CloseEvent>((resolve) =>
			ws.addEventListener('close', resolve)
		);
		const event = await eventWithin(closed, 'authentication timeout');
		expect(event.code).toBe(4401);
		expect(event.reason).toBe('Authentication Timeout');
		ws.close();
		void app.stop(false);
	});

	test('streams a snapshot then diffs over a real socket', async () => {
		const table = [open(1, 5)];
		const engine = createSyncEngine();
		engine.register(
			defineCollection<Order, Params, Ctx>({
				name: 'orders',
				hydrate: (params) =>
					table.filter(
						(row) =>
							row.userId === params.userId &&
							row.status === 'open'
					),
				match: (row, params) =>
					row.userId === params.userId && row.status === 'open',
				authorize: (params, ctx) => params.userId === ctx.userId
			})
		);

		const app = new Elysia()
			.use(syncSocket({ engine, resolveContext: () => ({ userId: 5 }) }))
			.listen(0);
		const port = app.server?.port ?? 0;

		const frames: AnyFrame[] = [];
		const ws = connect(port, frames);
		await new Promise<void>((resolve, reject) => {
			ws.addEventListener('open', () => resolve());
			ws.addEventListener('error', () => reject(new Error('ws error')));
		});

		ws.send(
			JSON.stringify({
				type: 'subscribe',
				id: 's1',
				collection: 'orders',
				params: { userId: 5 }
			})
		);
		await waitFor(() => frames.some((frame) => frame.type === 'snapshot'));
		const snapshot = frames.find((frame) => frame.type === 'snapshot');
		expect((snapshot?.rows as Order[]).map((row) => row.id)).toEqual([1]);

		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(2, 5)
		});
		await waitFor(() => frames.some((frame) => frame.type === 'diff'));
		const diff = frames.find((frame) => frame.type === 'diff');
		expect((diff?.added as Order[]).map((row) => row.id)).toEqual([2]);

		ws.close();
		await app.stop(true);
	});

	test('reports an unauthorized subscribe as an error frame', async () => {
		const engine = createSyncEngine();
		engine.register(
			defineCollection<Order, Params, Ctx>({
				name: 'orders',
				hydrate: () => [],
				match: (row, params) => row.userId === params.userId,
				authorize: (params, ctx) => params.userId === ctx.userId
			})
		);

		const app = new Elysia()
			.use(syncSocket({ engine, resolveContext: () => ({ userId: 5 }) }))
			.listen(0);
		const port = app.server?.port ?? 0;

		const frames: AnyFrame[] = [];
		const ws = connect(port, frames);
		await new Promise<void>((resolve, reject) => {
			ws.addEventListener('open', () => resolve());
			ws.addEventListener('error', () => reject(new Error('ws error')));
		});

		ws.send(
			JSON.stringify({
				type: 'subscribe',
				id: 's1',
				collection: 'orders',
				params: { userId: 9 } // not the connection's user
			})
		);
		await waitFor(() => frames.some((frame) => frame.type === 'error'));
		const error = frames.find((frame) => frame.type === 'error');
		expect(error?.message).toContain('Not authorized');

		ws.close();
		await app.stop(true);
	});

	test('drains current and late sockets with a service-restart close', async () => {
		const engine = createSyncEngine();
		const controller = createSyncSocketController();
		const app = new Elysia()
			.use(syncSocket({ controller, engine }))
			.listen(0);
		const port = app.server?.port ?? 0;

		const first = connect(port, []);
		await new Promise<void>((resolve, reject) => {
			first.addEventListener('open', () => resolve());
			first.addEventListener('error', () =>
				reject(new Error('ws error'))
			);
		});
		await waitFor(() => controller.connectionCount() === 1);

		const firstClose = new Promise<CloseEvent>((resolve) => {
			first.addEventListener('close', (event) => resolve(event));
		});
		expect(controller.drain()).toBe(1);
		expect(controller.draining).toBe(true);
		const firstEvent = await eventWithin(firstClose, 'first close');
		expect(firstEvent.code).toBe(1012);
		expect(firstEvent.reason).toBe('Service Restart');
		await waitFor(() => controller.connectionCount() === 0);

		const late = connect(port, []);
		const lateClose = await eventWithin(
			new Promise<CloseEvent>((resolve) => {
				late.addEventListener('close', (event) => resolve(event));
			}),
			'late close'
		);
		expect(lateClose.code).toBe(1012);
		expect(lateClose.reason).toBe('Service Restart');
		expect(controller.connectionCount()).toBe(0);

		first.close();
		late.close();
		// Bun has already completed both close handshakes here, but awaiting a
		// second graceful server stop can hang on the just-rejected upgrade.
		// Initiate forced cleanup without making that runtime quirk the assertion.
		void app.stop(false);
	});
});
