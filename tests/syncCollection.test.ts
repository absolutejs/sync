import { afterEach, describe, expect, test } from 'bun:test';
import { createSyncCollection } from '../src/client/syncCollection';
import type { SyncCollectionOptions } from '../src/client/syncCollection';
import type { ServerFrame } from '../src/engine/connection';

type Order = { id: number; status: string };

class FakeWebSocket {
	static instances: FakeWebSocket[] = [];

	url: string;
	sent: string[] = [];
	closed = false;
	onopen: ((event: unknown) => void) | null = null;
	onmessage: ((event: { data: string }) => void) | null = null;
	onclose: ((event: unknown) => void) | null = null;
	onerror: ((event: unknown) => void) | null = null;

	constructor(url: string) {
		this.url = url;
		FakeWebSocket.instances.push(this);
	}

	send(data: string) {
		this.sent.push(data);
	}

	close() {
		this.closed = true;
	}

	// --- test helpers ---
	open() {
		this.onopen?.({});
	}
	emit(frame: ServerFrame<Order>) {
		this.onmessage?.({ data: JSON.stringify(frame) });
	}
	fireClose() {
		this.onclose?.({});
	}
	lastSent() {
		const raw = this.sent.at(-1);
		return raw === undefined ? undefined : JSON.parse(raw);
	}
}

const Impl = FakeWebSocket as unknown as typeof WebSocket;
const lastSocket = () => {
	const socket = FakeWebSocket.instances.at(-1);
	if (!socket) {
		throw new Error('expected a FakeWebSocket');
	}
	return socket;
};

const make = (extra: Partial<SyncCollectionOptions<Order>> = {}) =>
	createSyncCollection<Order>({
		url: 'ws://localhost/sync/ws',
		collection: 'orders',
		params: { userId: 5 },
		webSocketImpl: Impl,
		...extra
	});

afterEach(() => {
	FakeWebSocket.instances = [];
});

describe('createSyncCollection', () => {
	test('sends a subscribe frame on open', () => {
		const store = make();
		expect(store.get().status).toBe('connecting');
		lastSocket().open();

		expect(lastSocket().lastSent()).toEqual({
			type: 'subscribe',
			id: 's',
			collection: 'orders',
			params: { userId: 5 }
		});
		store.close();
	});

	test('applies a snapshot frame and becomes ready', () => {
		const store = make();
		lastSocket().open();
		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [
				{ id: 1, status: 'open' },
				{ id: 2, status: 'open' }
			]
		});

		expect(store.get().status).toBe('ready');
		expect(store.get().data.map((row) => row.id)).toEqual([1, 2]);
		store.close();
	});

	test('applies diffs: added, changed, removed by key', () => {
		const store = make();
		lastSocket().open();
		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [{ id: 1, status: 'open' }]
		});

		lastSocket().emit({
			type: 'diff',
			id: 's',
			added: [{ id: 2, status: 'open' }],
			removed: [],
			changed: []
		});
		expect(store.get().data.map((row) => row.id)).toEqual([1, 2]);

		lastSocket().emit({
			type: 'diff',
			id: 's',
			added: [],
			removed: [],
			changed: [{ id: 1, status: 'shipped' }]
		});
		expect(store.get().data.find((row) => row.id === 1)?.status).toBe(
			'shipped'
		);

		lastSocket().emit({
			type: 'diff',
			id: 's',
			added: [],
			removed: [{ id: 2, status: 'open' }],
			changed: []
		});
		expect(store.get().data.map((row) => row.id)).toEqual([1]);
		store.close();
	});

	test('an error frame populates error and calls onError', () => {
		const errors: unknown[] = [];
		const store = make({ onError: (error) => errors.push(error) });
		lastSocket().open();
		lastSocket().emit({
			type: 'error',
			id: 's',
			message: 'Not authorized'
		});

		expect(store.get().error).toBe('Not authorized');
		expect(errors).toEqual(['Not authorized']);
		store.close();
	});

	test('notifies subscribers and stops after unsubscribe', () => {
		const store = make();
		const seen: number[][] = [];
		const off = store.subscribe((state) =>
			seen.push(state.data.map((row) => row.id))
		);
		lastSocket().open();
		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [{ id: 1, status: 'open' }]
		});
		expect(seen.at(-1)).toEqual([1]);

		off();
		const count = seen.length;
		lastSocket().emit({
			type: 'diff',
			id: 's',
			added: [{ id: 2, status: 'open' }],
			removed: [],
			changed: []
		});
		expect(seen.length).toBe(count);
		store.close();
	});

	test('close sends unsubscribe, closes the socket, and goes to closed', () => {
		const store = make();
		lastSocket().open();
		const socket = lastSocket();

		store.close();

		expect(socket.closed).toBe(true);
		expect(socket.lastSent()).toEqual({ type: 'unsubscribe', id: 's' });
		expect(store.get().status).toBe('closed');
	});

	test('reconnects after an unexpected close and re-subscribes', async () => {
		const store = make({ reconnectMs: 5 });
		const first = lastSocket();
		first.open();
		expect(FakeWebSocket.instances).toHaveLength(1);

		first.fireClose(); // unexpected drop
		await new Promise((resolve) => setTimeout(resolve, 20));

		expect(FakeWebSocket.instances).toHaveLength(2);
		const second = lastSocket();
		second.open();
		expect(second.lastSent()).toEqual({
			type: 'subscribe',
			id: 's',
			collection: 'orders',
			params: { userId: 5 }
		});
		store.close();
	});

	test('does not reconnect after an intentional close', async () => {
		const store = make({ reconnectMs: 5 });
		lastSocket().open();
		store.close();
		lastSocket().fireClose();
		await new Promise((resolve) => setTimeout(resolve, 20));

		expect(FakeWebSocket.instances).toHaveLength(1);
	});

	test('honours a custom key', () => {
		const store = createSyncCollection<{ sku: string; n: number }>({
			url: 'ws://localhost/sync/ws',
			collection: 'items',
			key: (row) => row.sku,
			webSocketImpl: Impl
		});
		lastSocket().open();
		lastSocket().onmessage?.({
			data: JSON.stringify({
				type: 'snapshot',
				id: 's',
				rows: [{ sku: 'a', n: 1 }]
			})
		});
		lastSocket().onmessage?.({
			data: JSON.stringify({
				type: 'diff',
				id: 's',
				added: [],
				removed: [],
				changed: [{ sku: 'a', n: 2 }]
			})
		});

		expect(store.get().data).toEqual([{ sku: 'a', n: 2 }]);
		store.close();
	});

	test('throws without a WebSocket implementation', () => {
		const saved = globalThis.WebSocket;
		// @ts-expect-error — simulate a runtime without WebSocket
		delete globalThis.WebSocket;
		try {
			expect(() =>
				createSyncCollection({ url: 'ws://x', collection: 'orders' })
			).toThrow('requires WebSocket');
		} finally {
			globalThis.WebSocket = saved;
		}
	});
});
