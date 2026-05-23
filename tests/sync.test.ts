import { describe, expect, test } from 'bun:test';
import { createReactiveHub } from '../src/reactiveHub';
import { createWriteBehindCache } from '../src/writeBehindCache';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('createReactiveHub', () => {
	test('notifies exact subscribers and ignores other topics', () => {
		const hub = createReactiveHub();
		const seen: string[] = [];
		hub.subscribe(['orders'], (event) => seen.push(event.topic));

		hub.publish('orders', { id: 1 });
		hub.publish('users');

		expect(seen).toEqual(['orders']);
	});

	test('prefix wildcard matches by prefix', () => {
		const hub = createReactiveHub();
		const seen: string[] = [];
		hub.subscribe(['orders:*'], (event) => seen.push(event.topic));

		hub.publish('orders:42');
		hub.publish('orders:99');
		hub.publish('users:1');

		expect(seen).toEqual(['orders:42', 'orders:99']);
	});

	test('carries payload and timestamp', () => {
		const hub = createReactiveHub();
		let received: unknown;
		let at = 0;
		hub.subscribe(['t'], (event) => {
			received = event.payload;
			at = event.at;
		});

		const before = Date.now();
		hub.publish('t', { hello: 'world' });

		expect(received).toEqual({ hello: 'world' });
		expect(at).toBeGreaterThanOrEqual(before);
	});

	test('unsubscribe stops delivery and updates counts', () => {
		const hub = createReactiveHub();
		const seen: string[] = [];
		const unsubscribe = hub.subscribe(['orders'], (event) =>
			seen.push(event.topic)
		);

		expect(hub.subscriberCount('orders')).toBe(1);
		unsubscribe();
		hub.publish('orders');

		expect(seen).toEqual([]);
		expect(hub.subscriberCount('orders')).toBe(0);
	});
});

describe('createWriteBehindCache', () => {
	test('reads from memory and loads through on a miss', async () => {
		const durable = new Map<string, number>([['a', 1]]);
		let loads = 0;
		const cache = createWriteBehindCache<string, number>({
			load: (key) => {
				loads += 1;
				return durable.get(key);
			},
			persist: (key, value) => {
				durable.set(key, value);
			}
		});

		expect(await cache.get('a')).toBe(1); // load-through
		expect(await cache.get('a')).toBe(1); // cached
		expect(loads).toBe(1);
		expect(await cache.get('missing')).toBeUndefined();
	});

	test('set is synchronous in memory and coalesces durable writes', async () => {
		const durable = new Map<string, number>();
		let persists = 0;
		const cache = createWriteBehindCache<string, number>({
			load: (key) => durable.get(key),
			persist: (key, value) => {
				persists += 1;
				durable.set(key, value);
			},
			debounceMs: 20
		});

		cache.set('k', 1);
		cache.set('k', 2);
		cache.set('k', 3);

		expect(cache.peek('k')).toBe(3); // visible immediately
		expect(persists).toBe(0); // not yet persisted
		expect(durable.has('k')).toBe(false);

		await sleep(40);

		expect(persists).toBe(1); // three sets -> one write
		expect(durable.get('k')).toBe(3);
	});

	test('evicts terminal entries after persist', async () => {
		const durable = new Map<string, { status: string }>();
		const cache = createWriteBehindCache<string, { status: string }>({
			load: (key) => durable.get(key),
			persist: (key, value) => {
				durable.set(key, value);
			},
			debounceMs: 10,
			evict: (value) => value.status === 'closed'
		});

		cache.set('s', { status: 'closed' });
		await sleep(25);

		expect(cache.peek('s')).toBeUndefined(); // evicted from memory
		expect(durable.get('s')).toEqual({ status: 'closed' }); // still durable
	});

	test('flush persists pending writes immediately', async () => {
		const durable = new Map<string, number>();
		const cache = createWriteBehindCache<string, number>({
			load: (key) => durable.get(key),
			persist: (key, value) => {
				durable.set(key, value);
			},
			debounceMs: 10_000
		});

		cache.set('a', 1);
		cache.set('b', 2);
		await cache.flush();

		expect(durable.get('a')).toBe(1);
		expect(durable.get('b')).toBe(2);
	});

	test('delete removes from cache and durable store', async () => {
		const durable = new Map<string, number>([['a', 1]]);
		const cache = createWriteBehindCache<string, number>({
			load: (key) => durable.get(key),
			persist: (key, value) => {
				durable.set(key, value);
			},
			remove: (key) => {
				durable.delete(key);
			}
		});

		await cache.get('a');
		await cache.delete('a');

		expect(cache.peek('a')).toBeUndefined();
		expect(durable.has('a')).toBe(false);
	});

	test('a failed persist keeps the cache authoritative and reports', async () => {
		const errors: string[] = [];
		const cache = createWriteBehindCache<string, number>({
			load: () => undefined,
			persist: () => {
				throw new Error('durable down');
			},
			debounceMs: 10,
			onPersistError: (error) => errors.push((error as Error).message)
		});

		cache.set('k', 1);
		await sleep(25);

		expect(cache.peek('k')).toBe(1); // still in memory
		expect(errors).toEqual(['durable down']);
	});
});
