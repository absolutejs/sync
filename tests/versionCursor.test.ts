import { afterEach, describe, expect, test } from 'bun:test';
import { createSyncCollection } from '../src/client/syncCollection';
import { defineCollection } from '../src/engine/collection';
import { createSyncConnection } from '../src/engine/connection';
import type { ServerFrame } from '../src/engine/connection';
import { createSyncEngine } from '../src/engine/syncEngine';

type Order = { id: number; userId: number; status: 'open' | 'closed' };
type Params = { userId: number };

const open = (id: number, userId: number): Order => ({
	id,
	userId,
	status: 'open'
});

const ordersCollection = (table: Order[] = []) =>
	defineCollection<Order, Params, Params>({
		name: 'orders',
		hydrate: (p) =>
			table.filter((o) => o.userId === p.userId && o.status === 'open'),
		match: (o, p) => o.userId === p.userId && o.status === 'open',
		authorize: (p, ctx) => p.userId === ctx.userId
	});

describe('engine versioning', () => {
	test('subscribe reports a version; diffs carry incrementing versions', async () => {
		const engine = createSyncEngine();
		engine.register(ordersCollection());
		const versions: number[] = [];
		const sub = await engine.subscribe<Order, Params>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			onDiff: (_diff, version) => versions.push(version)
		});
		expect(sub.version).toBe(0);
		expect(sub.catchup).toBeUndefined();

		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(1, 5)
		});
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(2, 5)
		});
		expect(versions).toEqual([1, 2]);
		sub.unsubscribe();
	});

	test('resume since a version returns a catch-up diff, not a snapshot', async () => {
		const engine = createSyncEngine();
		engine.register(ordersCollection());
		// A change happens with no subscribers — still logged.
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(1, 5)
		});

		const sub = await engine.subscribe<Order, Params>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			since: 0,
			onDiff: () => {}
		});

		expect(sub.initial).toEqual([]);
		expect(sub.catchup?.changed.map((r) => r.id)).toEqual([1]);
		expect(sub.version).toBe(1);
		sub.unsubscribe();
	});

	test('a delete since the version becomes a catch-up removal', async () => {
		const engine = createSyncEngine();
		engine.register(ordersCollection());
		await engine.applyChange<Order>('orders', {
			op: 'update',
			row: { id: 1, userId: 5, status: 'closed' } // leaves the set
		});
		const sub = await engine.subscribe<Order, Params>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			since: 0,
			onDiff: () => {}
		});
		expect(sub.catchup?.removed.map((r) => r.id)).toEqual([1]);
		sub.unsubscribe();
	});

	test('a too-old since falls back to a snapshot (log trimmed)', async () => {
		const engine = createSyncEngine({ changeLogSize: 2 });
		const table = [open(1, 5)];
		engine.register(ordersCollection(table));
		for (let i = 2; i <= 5; i += 1) {
			await engine.applyChange<Order>('orders', {
				op: 'insert',
				row: open(i, 5)
			});
		}
		const sub = await engine.subscribe<Order, Params>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: { userId: 5 },
			since: 0, // older than the trimmed log
			onDiff: () => {}
		});
		expect(sub.catchup).toBeUndefined();
		expect(sub.initial.map((r) => r.id)).toEqual([1]); // snapshot from hydrate
		sub.unsubscribe();
	});
});

describe('connection version frames', () => {
	test('snapshot/diff frames carry version; resume yields a catch-up diff', async () => {
		const engine = createSyncEngine();
		engine.register(ordersCollection());

		const frames: ServerFrame[] = [];
		const conn = createSyncConnection({
			engine,
			ctx: { userId: 5 },
			send: (f) => frames.push(f)
		});
		await conn.handle({
			type: 'subscribe',
			id: 's1',
			collection: 'orders',
			params: { userId: 5 }
		});
		const snapshot = frames.find((f) => f.type === 'snapshot');
		expect(snapshot?.type === 'snapshot' && snapshot.version).toBe(0);

		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: open(1, 5)
		});
		const diff = frames.find((f) => f.type === 'diff');
		expect(diff?.type === 'diff' && diff.version).toBe(1);

		// A fresh connection resuming from version 0 gets a catch-up diff.
		const resumed: ServerFrame[] = [];
		const conn2 = createSyncConnection({
			engine,
			ctx: { userId: 5 },
			send: (f) => resumed.push(f)
		});
		await conn2.handle({
			type: 'subscribe',
			id: 's1',
			collection: 'orders',
			params: { userId: 5 },
			since: 0
		});
		expect(resumed[0]?.type).toBe('diff');
		conn.close();
		conn2.close();
	});
});

class FakeWebSocket {
	static instances: FakeWebSocket[] = [];
	url: string;
	sent: string[] = [];
	onopen: ((e: unknown) => void) | null = null;
	onmessage: ((e: { data: string }) => void) | null = null;
	onclose: ((e: unknown) => void) | null = null;
	constructor(url: string) {
		this.url = url;
		FakeWebSocket.instances.push(this);
	}
	send(data: string) {
		this.sent.push(data);
	}
	close() {}
	open() {
		this.onopen?.({});
	}
	emit(frame: ServerFrame<Order>) {
		this.onmessage?.({ data: JSON.stringify(frame) });
	}
	fireClose() {
		this.onclose?.({});
	}
	subscribeFrame() {
		return this.sent
			.map((raw) => JSON.parse(raw))
			.find((f) => f.type === 'subscribe');
	}
}
const Impl = FakeWebSocket as unknown as typeof WebSocket;
const lastSocket = () => FakeWebSocket.instances.at(-1)!;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

afterEach(() => {
	FakeWebSocket.instances = [];
});

describe('client resume on reconnect', () => {
	test('sends `since` and applies catch-up onto retained state', async () => {
		const store = createSyncCollection<Order>({
			url: 'ws://x',
			collection: 'orders',
			webSocketImpl: Impl,
			reconnectMs: 5
		});
		const first = lastSocket();
		first.open();
		first.emit({
			type: 'snapshot',
			id: 's',
			rows: [open(1, 5)],
			version: 5
		});
		expect(store.get().data.map((r) => r.id)).toEqual([1]);
		expect(first.subscribeFrame().since).toBeUndefined(); // first connect: no resume

		first.fireClose();
		await sleep(20);

		const second = lastSocket();
		expect(second).not.toBe(first);
		second.open();
		expect(second.subscribeFrame().since).toBe(5); // resume from applied version

		// Catch-up diff is applied on top of retained confirmed (not cleared).
		second.emit({
			type: 'diff',
			id: 's',
			added: [open(2, 5)],
			removed: [],
			changed: [],
			version: 6
		});
		expect(
			store
				.get()
				.data.map((r) => r.id)
				.sort()
		).toEqual([1, 2]);
		store.close();
	});
});
