import { afterEach, describe, expect, test } from 'bun:test';
import { syncStore, unwrapEden } from '../src/client/syncStore';
import type { ServerFrame } from '../src/engine/connection';

type Order = { id: number; total: number };

class FakeWebSocket {
	static instances: FakeWebSocket[] = [];
	url: string;
	sent: string[] = [];
	onopen: ((event: unknown) => void) | null = null;
	onmessage: ((event: { data: string }) => void) | null = null;
	onclose: ((event: unknown) => void) | null = null;
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
}

const Impl = FakeWebSocket as unknown as typeof WebSocket;
const lastSocket = () => FakeWebSocket.instances.at(-1)!;
const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

afterEach(() => {
	FakeWebSocket.instances = [];
});

describe('syncStore reads', () => {
	test('WS snapshot populates confirmed and becomes ready', () => {
		const store = syncStore<Order>({
			url: 'ws://x',
			collection: 'orders',
			initialData: [] as Order[],
			webSocketImpl: Impl
		});
		expect(store.get().status).toBe('connecting');
		lastSocket().open();
		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [{ id: 1, total: 10 }]
		});

		expect(store.get().status).toBe('ready');
		expect(store.get().data.map((r) => r.id)).toEqual([1]);
		store.close();
	});

	test('initialData seeds immediately', () => {
		const store = syncStore<Order>({
			url: 'ws://x',
			collection: 'orders',
			initialData: [{ id: 9, total: 1 }],
			webSocketImpl: Impl
		});
		expect(store.get().data.map((r) => r.id)).toEqual([9]);
		store.close();
	});

	test('eager hydrate paints before the WS snapshot, then is refreshed', async () => {
		const store = syncStore<Order>({
			url: 'ws://x',
			collection: 'orders',
			hydrate: async () => [{ id: 1, total: 10 }],
			webSocketImpl: Impl
		});
		await tick();
		expect(store.get().data.map((r) => r.id)).toEqual([1]); // from hydrate

		lastSocket().open();
		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [{ id: 2, total: 20 }]
		});
		expect(store.get().data.map((r) => r.id)).toEqual([2]); // WS authoritative
		store.close();
	});
});

describe('syncStore mutations', () => {
	test('optimistic overlay, then reconcile when the diff reflects it (no flicker)', async () => {
		const store = syncStore({
			url: 'ws://x',
			collection: 'orders',
			initialData: [] as Order[],
			webSocketImpl: Impl,
			mutations: {
				createOrder: async (a: { total: number }) => ({
					id: 2,
					total: a.total
				})
			}
		});
		lastSocket().open();
		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [{ id: 1, total: 10 }]
		});

		const promise = store.mutate(
			'createOrder',
			{ total: 42 },
			{
				optimistic: (d) => d.set({ id: 2, total: 42 })
			}
		);
		expect(store.get().data.map((r) => r.id)).toEqual([1, 2]); // optimistic

		await promise; // server resolved
		expect(store.get().data.map((r) => r.id)).toEqual([1, 2]); // overlay still on

		// authoritative diff arrives → overlay dropped, row stays (no flicker)
		lastSocket().emit({
			type: 'diff',
			id: 's',
			added: [{ id: 2, total: 42 }],
			removed: [],
			changed: []
		});
		expect(store.get().data.map((r) => r.id)).toEqual([1, 2]);
		store.close();
	});

	test('returns the typed server result', async () => {
		const store = syncStore({
			url: 'ws://x',
			collection: 'orders',
			initialData: [] as Order[],
			webSocketImpl: Impl,
			mutations: {
				createOrder: async (a: { total: number }) => ({
					id: 7,
					total: a.total
				})
			}
		});
		lastSocket().open();
		const result = await store.mutate('createOrder', { total: 5 });
		const typed: { id: number; total: number } = result; // type-flow proof
		expect(typed.id).toBe(7);
		store.close();
	});

	test('grace timer drops an overlay no diff ever reflects', async () => {
		const store = syncStore({
			url: 'ws://x',
			collection: 'orders',
			initialData: [] as Order[],
			webSocketImpl: Impl,
			reconcileGraceMs: 10,
			mutations: { ping: async () => ({ ok: true }) }
		});
		lastSocket().open();
		lastSocket().emit({ type: 'snapshot', id: 's', rows: [] });

		await store.mutate('ping', undefined, {
			optimistic: (d) => d.set({ id: 99, total: 0 })
		});
		expect(store.get().data.map((r) => r.id)).toEqual([99]); // overlay
		await sleep(25);
		expect(store.get().data).toEqual([]); // dropped after grace
		store.close();
	});

	test('rolls back and rejects when the server rejects (online)', async () => {
		const store = syncStore({
			url: 'ws://x',
			collection: 'orders',
			initialData: [] as Order[],
			webSocketImpl: Impl,
			mutations: {
				createOrder: async () => {
					throw new Error('denied');
				}
			}
		});
		lastSocket().open();
		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [{ id: 1, total: 1 }]
		});

		const promise = store.mutate('createOrder', undefined, {
			optimistic: (d) => d.set({ id: 2, total: 0 })
		});
		expect(store.get().data.map((r) => r.id)).toEqual([1, 2]);

		await expect(promise).rejects.toThrow('denied');
		expect(store.get().data.map((r) => r.id)).toEqual([1]); // rolled back
		store.close();
	});

	test('offline: stays queued and retries on reconnect', async () => {
		let online = false;
		const store = syncStore({
			url: 'ws://x',
			collection: 'orders',
			initialData: [] as Order[],
			webSocketImpl: Impl,
			reconnectMs: 5,
			mutations: {
				createOrder: async (a: { total: number }) => {
					if (!online) {
						throw new Error('network');
					}
					return { id: 2, total: a.total };
				}
			}
		});
		// socket not opened yet -> offline
		const promise = store.mutate(
			'createOrder',
			{ total: 1 },
			{
				optimistic: (d) => d.set({ id: 2, total: 1 })
			}
		);
		await tick();
		expect(store.get().data.map((r) => r.id)).toEqual([2]); // overlay held

		online = true;
		lastSocket().open(); // reconnect/connect -> retry
		await expect(promise).resolves.toEqual({ id: 2, total: 1 });
		store.close();
	});

	test('close rejects pending mutations', async () => {
		const store = syncStore({
			url: 'ws://x',
			collection: 'orders',
			initialData: [] as Order[],
			webSocketImpl: Impl,
			mutations: { slow: () => new Promise<void>(() => {}) }
		});
		lastSocket().open();
		const promise = store.mutate('slow', undefined);
		store.close();
		await expect(promise).rejects.toThrow('closed');
	});
});

describe('unwrapEden', () => {
	test('returns data on success', async () => {
		expect(
			await unwrapEden(Promise.resolve({ data: 42, error: null }))
		).toBe(42);
	});
	test('throws the error', async () => {
		await expect(
			unwrapEden(Promise.resolve({ data: null, error: 'boom' }))
		).rejects.toBe('boom');
	});
});
