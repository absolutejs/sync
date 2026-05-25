import { afterEach, describe, expect, test } from 'bun:test';
import {
	createSyncCollection,
	localStorageMutationStorage
} from '../src/client/syncCollection';
import type {
	MutationStorage,
	PendingMutationRecord,
	ServerFrame
} from '../src/client/syncCollection';

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
	sentFrames() {
		return this.sent.map((raw) => JSON.parse(raw));
	}
}

const Impl = FakeWebSocket as unknown as typeof WebSocket;
const lastSocket = () => FakeWebSocket.instances.at(-1)!;
const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

const memoryStorage = (initial: PendingMutationRecord[] = []) => {
	let records = initial;
	const saved: PendingMutationRecord[][] = [];
	const storage: MutationStorage = {
		load: () => records,
		save: (next) => {
			records = next;
			saved.push(next);
		}
	};
	return { storage, saved };
};

afterEach(() => {
	FakeWebSocket.instances = [];
});

describe('createSyncCollection offline persistence', () => {
	test('persists the pending queue on mutate and clears it on ack', async () => {
		const { storage, saved } = memoryStorage();
		const store = createSyncCollection<Order>({
			url: 'ws://localhost/sync/ws',
			collection: 'orders',
			webSocketImpl: Impl,
			storage
		});
		lastSocket().open();
		await tick();

		store.mutate({ name: 'createOrder', args: { id: 1 } }).catch(() => {});
		expect(saved.at(-1)).toEqual([
			{ mutationId: 1, name: 'createOrder', args: { id: 1 } }
		]);

		lastSocket().emit({ type: 'ack', mutationId: 1 } as ServerFrame<Order>);
		expect(saved.at(-1)).toEqual([]); // dropped from the queue once acked
		store.close();
	});

	test('replays a persisted queue on connect (reload recovery)', async () => {
		const { storage } = memoryStorage([
			{ mutationId: 7, name: 'createOrder', args: { id: 7 } }
		]);
		const store = createSyncCollection<Order>({
			url: 'ws://localhost/sync/ws',
			collection: 'orders',
			webSocketImpl: Impl,
			storage
		});
		lastSocket().open();
		await tick(); // let the async load + replay run

		expect(lastSocket().sentFrames()).toContainEqual({
			type: 'mutate',
			mutationId: 7,
			name: 'createOrder',
			args: { id: 7 }
		});
		store.close();
	});

	test('a new mutation after a reload does not reuse a persisted id', async () => {
		const { storage } = memoryStorage([
			{ mutationId: 7, name: 'createOrder', args: { id: 7 } }
		]);
		const store = createSyncCollection<Order>({
			url: 'ws://localhost/sync/ws',
			collection: 'orders',
			webSocketImpl: Impl,
			storage
		});
		lastSocket().open();
		await tick();

		store.mutate({ name: 'createOrder', args: { id: 8 } }).catch(() => {});
		const mutateFrames = lastSocket()
			.sentFrames()
			.filter((frame) => frame.type === 'mutate');
		const newId = mutateFrames.at(-1).mutationId as number;
		expect(newId).toBeGreaterThan(7);
		store.close();
	});
});

describe('localStorageMutationStorage', () => {
	afterEach(() => {
		// @ts-expect-error — restore
		delete globalThis.localStorage;
	});

	test('round-trips records through localStorage', () => {
		const map = new Map<string, string>();
		globalThis.localStorage = {
			getItem: (key: string) => map.get(key) ?? null,
			setItem: (key: string, value: string) => map.set(key, value),
			removeItem: (key: string) => map.delete(key),
			clear: () => map.clear(),
			key: () => null,
			length: 0
		} as Storage;

		const storage = localStorageMutationStorage('pending');
		expect(storage.load()).toEqual([]);
		storage.save([{ mutationId: 1, name: 'm', args: { a: 1 } }]);
		expect(storage.load()).toEqual([
			{ mutationId: 1, name: 'm', args: { a: 1 } }
		]);
	});

	test('no-ops without localStorage', () => {
		const storage = localStorageMutationStorage('pending');
		expect(() => storage.save([])).not.toThrow();
		expect(storage.load()).toEqual([]);
	});
});
