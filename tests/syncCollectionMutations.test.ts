import { afterEach, describe, expect, test } from 'bun:test';
import { createSyncCollection } from '../src/client/syncCollection';
import type { ServerFrame } from '../src/engine/connection';

type Order = { id: number; status: string };

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
	sentFrames() {
		return this.sent.map((raw) => JSON.parse(raw));
	}
	lastFrame() {
		return this.sentFrames().at(-1);
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
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const make = (reconnectMs = 0) =>
	createSyncCollection<Order>({
		url: 'ws://localhost/sync/ws',
		collection: 'orders',
		webSocketImpl: Impl,
		reconnectMs
	});

const ids = (store: ReturnType<typeof make>) =>
	store.get().data.map((row) => row.id);

afterEach(() => {
	FakeWebSocket.instances = [];
});

describe('createSyncCollection mutations', () => {
	test('applies an optimistic overlay immediately and sends a mutate frame', () => {
		const store = make();
		lastSocket().open();
		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [{ id: 1, status: 'open' }]
		});

		// close() will reject this never-acked mutation; swallow it.
		store
			.mutate({
				name: 'createOrder',
				args: { id: 2 },
				optimistic: (draft) => draft.set({ id: 2, status: 'open' })
			})
			.catch(() => {});

		expect(ids(store)).toEqual([1, 2]); // optimistic, before any server reply
		expect(lastSocket().lastFrame()).toEqual({
			type: 'mutate',
			mutationId: 1,
			name: 'createOrder',
			args: { id: 2 }
		});
		store.close();
	});

	test('ack resolves the promise and leaves the confirmed row (no flicker)', async () => {
		const store = make();
		lastSocket().open();
		lastSocket().emit({ type: 'snapshot', id: 's', rows: [] });

		const promise = store.mutate<Order>({
			name: 'createOrder',
			args: { id: 2 },
			optimistic: (draft) => draft.set({ id: 2, status: 'open' })
		});
		const mutationId = lastSocket().lastFrame().mutationId as number;

		// Authoritative diff arrives first, then the ack (ordered over one socket).
		lastSocket().emit({
			type: 'diff',
			id: 's',
			added: [{ id: 2, status: 'open' }],
			removed: [],
			changed: []
		});
		lastSocket().emit({
			type: 'ack',
			mutationId,
			result: { id: 2, status: 'open' }
		});

		await expect(promise).resolves.toEqual({ id: 2, status: 'open' });
		expect(ids(store)).toEqual([2]); // still present via confirmed
		store.close();
	});

	test('reject rolls back the overlay and rejects the promise', async () => {
		const store = make();
		lastSocket().open();
		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [{ id: 1, status: 'open' }]
		});

		const promise = store.mutate({
			name: 'createOrder',
			optimistic: (draft) => draft.set({ id: 9, status: 'open' })
		});
		expect(ids(store)).toContain(9);
		const mutationId = lastSocket().lastFrame().mutationId as number;

		lastSocket().emit({ type: 'reject', mutationId, message: 'denied' });

		await expect(promise).rejects.toThrow('denied');
		expect(ids(store)).toEqual([1]); // rolled back
		store.close();
	});

	test('a non-optimistic mutate updates only when the diff arrives', async () => {
		const store = make();
		lastSocket().open();
		lastSocket().emit({ type: 'snapshot', id: 's', rows: [] });

		const promise = store.mutate({ name: 'createOrder', args: { id: 3 } });
		expect(ids(store)).toEqual([]); // no optimistic overlay
		const mutationId = lastSocket().lastFrame().mutationId as number;

		lastSocket().emit({
			type: 'diff',
			id: 's',
			added: [{ id: 3, status: 'open' }],
			removed: [],
			changed: []
		});
		lastSocket().emit({ type: 'ack', mutationId });

		await promise;
		expect(ids(store)).toEqual([3]);
		store.close();
	});

	test('replays pending mutations on reconnect', async () => {
		const store = make(5);
		const first = lastSocket();
		first.open();
		first.emit({ type: 'snapshot', id: 's', rows: [] });

		store.mutate({ name: 'createOrder', args: { id: 2 } }).catch(() => {});
		expect(first.lastFrame().type).toBe('mutate');

		first.fireClose(); // drop before ack
		await sleep(20);

		const second = lastSocket();
		expect(second).not.toBe(first);
		second.open();

		const types = second.sentFrames().map((frame) => frame.type);
		expect(types).toEqual(['subscribe', 'mutate']); // re-subscribe + replay
		store.close();
	});

	test('close rejects still-pending mutations', async () => {
		const store = make();
		lastSocket().open();
		const promise = store.mutate({ name: 'createOrder', args: { id: 2 } });

		store.close();

		await expect(promise).rejects.toThrow('closed');
	});

	test('ignores an ack for an unknown mutation id', () => {
		const store = make();
		lastSocket().open();
		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [{ id: 1, status: 'open' }]
		});

		expect(() =>
			lastSocket().emit({ type: 'ack', mutationId: 999 })
		).not.toThrow();
		expect(ids(store)).toEqual([1]);
		store.close();
	});
});
