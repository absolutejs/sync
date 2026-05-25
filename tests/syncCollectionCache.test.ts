import { afterEach, describe, expect, test } from 'bun:test';
import {
	createSyncCollection,
	localStorageCollectionCache
} from '../src/client/syncCollection';
import type {
	CollectionCache,
	CollectionCacheSnapshot,
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

const memoryCache = (initial?: CollectionCacheSnapshot<Order>) => {
	let snapshot = initial;
	const saved: CollectionCacheSnapshot<Order>[] = [];
	const cache: CollectionCache<Order> = {
		load: () => snapshot,
		save: (next) => {
			snapshot = next;
			saved.push(next);
		}
	};
	return { cache, saved };
};

afterEach(() => {
	FakeWebSocket.instances = [];
});

describe('createSyncCollection local-first cache', () => {
	test('hydrates rows from the cache before the socket connects', async () => {
		const { cache } = memoryCache({
			rows: [{ id: 1, status: 'new' }],
			version: 5
		});
		const store = createSyncCollection<Order>({
			url: 'ws://localhost/sync/ws',
			collection: 'orders',
			webSocketImpl: Impl,
			cache
		});
		await tick();

		// Cached rows are visible before the socket opens (instant/offline read).
		expect(store.get().data).toEqual([{ id: 1, status: 'new' }]);
		expect(store.get().status).toBe('connecting');
		store.close();
	});

	test('resumes the subscribe from the cached version', async () => {
		const { cache } = memoryCache({
			rows: [{ id: 1, status: 'new' }],
			version: 5
		});
		const store = createSyncCollection<Order>({
			url: 'ws://localhost/sync/ws',
			collection: 'orders',
			webSocketImpl: Impl,
			cache
		});
		await tick();
		lastSocket().open();

		const subscribe = lastSocket()
			.sentFrames()
			.find((frame) => frame.type === 'subscribe');
		expect(subscribe.since).toBe(5);
		store.close();
	});

	test('applies a catch-up diff on top of the cached rows', async () => {
		const { cache } = memoryCache({
			rows: [{ id: 1, status: 'new' }],
			version: 5
		});
		const store = createSyncCollection<Order>({
			url: 'ws://localhost/sync/ws',
			collection: 'orders',
			webSocketImpl: Impl,
			cache
		});
		await tick();
		lastSocket().open();
		// A resume replies with a catch-up diff, never a snapshot.
		lastSocket().emit({
			type: 'diff',
			id: 's',
			added: [{ id: 2, status: 'new' }],
			removed: [],
			changed: [{ id: 1, status: 'shipped' }],
			version: 6
		} as ServerFrame<Order>);
		await tick();

		expect(store.get().data).toEqual([
			{ id: 1, status: 'shipped' },
			{ id: 2, status: 'new' }
		]);
		// A catch-up diff flips the collection live (no stuck "connecting").
		expect(store.get().status).toBe('ready');
		store.close();
	});

	test('a full snapshot replaces a stale cache (changelog evicted)', async () => {
		const { cache } = memoryCache({
			rows: [{ id: 1, status: 'old' }],
			version: 5
		});
		const store = createSyncCollection<Order>({
			url: 'ws://localhost/sync/ws',
			collection: 'orders',
			webSocketImpl: Impl,
			cache
		});
		await tick();
		expect(store.get().data).toEqual([{ id: 1, status: 'old' }]);
		lastSocket().open();
		// Server can't resume from v5 (trimmed) → sends a fresh snapshot.
		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [{ id: 9, status: 'new' }],
			version: 12
		} as ServerFrame<Order>);
		await tick();

		expect(store.get().data).toEqual([{ id: 9, status: 'new' }]);
		expect(store.get().status).toBe('ready');
		store.close();
	});

	test('persists confirmed rows on snapshot and on each diff', async () => {
		const { cache, saved } = memoryCache();
		const store = createSyncCollection<Order>({
			url: 'ws://localhost/sync/ws',
			collection: 'orders',
			webSocketImpl: Impl,
			cache
		});
		await tick();
		lastSocket().open();

		lastSocket().emit({
			type: 'snapshot',
			id: 's',
			rows: [{ id: 1, status: 'new' }],
			version: 3
		} as ServerFrame<Order>);
		await tick();
		expect(saved.at(-1)).toEqual({
			rows: [{ id: 1, status: 'new' }],
			version: 3
		});

		lastSocket().emit({
			type: 'diff',
			id: 's',
			added: [{ id: 2, status: 'new' }],
			removed: [],
			changed: [],
			version: 4
		} as ServerFrame<Order>);
		await tick();
		expect(saved.at(-1)).toEqual({
			rows: [
				{ id: 1, status: 'new' },
				{ id: 2, status: 'new' }
			],
			version: 4
		});
		store.close();
	});

	test('a corrupt cache falls back to the server snapshot', async () => {
		const cache: CollectionCache<Order> = {
			load: () => {
				throw new Error('corrupt');
			},
			save: () => {}
		};
		const store = createSyncCollection<Order>({
			url: 'ws://localhost/sync/ws',
			collection: 'orders',
			webSocketImpl: Impl,
			cache
		});
		await tick();
		// No cached rows, and connect still proceeds despite the load throwing.
		expect(store.get().data).toEqual([]);
		lastSocket().open();
		const subscribe = lastSocket()
			.sentFrames()
			.find((frame) => frame.type === 'subscribe');
		expect(subscribe.since).toBeUndefined();
		store.close();
	});
});

describe('localStorageCollectionCache', () => {
	afterEach(() => {
		// @ts-expect-error — restore
		delete globalThis.localStorage;
	});

	test('round-trips a snapshot through localStorage', () => {
		const map = new Map<string, string>();
		globalThis.localStorage = {
			getItem: (key: string) => map.get(key) ?? null,
			setItem: (key: string, value: string) => map.set(key, value),
			removeItem: (key: string) => map.delete(key),
			clear: () => map.clear(),
			key: () => null,
			length: 0
		} as Storage;

		const cache = localStorageCollectionCache<Order>('orders');
		expect(cache.load()).toBeUndefined();
		cache.save({ rows: [{ id: 1, status: 'new' }], version: 2 });
		expect(cache.load()).toEqual({
			rows: [{ id: 1, status: 'new' }],
			version: 2
		});
		cache.clear?.();
		expect(cache.load()).toBeUndefined();
	});

	test('no-ops without localStorage', () => {
		const cache = localStorageCollectionCache<Order>('orders');
		expect(() => cache.save({ rows: [], version: 0 })).not.toThrow();
		expect(cache.load()).toBeUndefined();
	});
});
