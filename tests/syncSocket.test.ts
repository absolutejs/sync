import { describe, expect, test } from 'bun:test';
import { Elysia } from 'elysia';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';
import { syncSocket } from '../src/engine/socket';

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

type AnyFrame = { type: string; [key: string]: unknown };

const connect = (port: number, frames: AnyFrame[]) => {
	const ws = new WebSocket(`ws://localhost:${port}/sync/ws`);
	ws.addEventListener('message', (event) => {
		frames.push(JSON.parse(event.data as string) as AnyFrame);
	});
	return ws;
};

describe('syncSocket (Elysia WebSocket)', () => {
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
});
