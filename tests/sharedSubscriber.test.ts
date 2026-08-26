import { describe, expect, test } from 'bun:test';
import {
	createLiveQuery,
	createSharedSyncSubscriber,
	createSyncSubscriber
} from '../src/client';
import { SYNC_OPEN_TOPIC } from '../src/reactiveHub';
import type { ReactiveEvent } from '../src/reactiveHub';

class FakeEventSource {
	static instances: FakeEventSource[] = [];

	url: string;
	withCredentials: boolean;
	closed = false;
	onmessage: ((event: { data: string }) => void) | null = null;
	onopen: (() => void) | null = null;
	onerror: ((event: unknown) => void) | null = null;

	constructor(url: string, init?: { withCredentials?: boolean }) {
		this.url = url;
		this.withCredentials = init?.withCredentials ?? false;
		FakeEventSource.instances.push(this);
	}

	close() {
		this.closed = true;
	}

	emit(event: ReactiveEvent) {
		this.onmessage?.({ data: JSON.stringify(event) });
	}

	get topics() {
		return new URL(this.url, 'http://localhost').searchParams.get('topics');
	}
}

const FakeImpl = FakeEventSource as unknown as typeof EventSource;
const tick = () => new Promise((resolve) => setTimeout(resolve, 5));
const live = () => FakeEventSource.instances.filter((s) => !s.closed);
const open = (topics: string[]): ReactiveEvent => ({
	topic: SYNC_OPEN_TOPIC,
	at: 1,
	payload: { topics }
});

// Each test uses its own endpoint so pools never bleed between tests.
let endpoint = 0;
const url = () => `/sync-${(endpoint += 1)}`;

describe('createSharedSyncSubscriber', () => {
	test('members on one endpoint share a single connection carrying the union of topics', async () => {
		const path = url();
		const seen: string[] = [];
		const a = createSharedSyncSubscriber({
			topics: ['orders'],
			onEvent: (event) => seen.push(`a:${event.topic}`),
			url: path,
			eventSourceImpl: FakeImpl
		});
		const b = createSharedSyncSubscriber({
			topics: ['quotes', 'orders'],
			onEvent: (event) => seen.push(`b:${event.topic}`),
			url: path,
			eventSourceImpl: FakeImpl
		});
		await tick();

		const sources = live().filter((s) => s.url.startsWith(path));
		expect(sources).toHaveLength(1);
		expect(sources[0]!.topics).toBe('orders,quotes');

		sources[0]!.emit({ topic: 'quotes', at: 2, payload: {} });
		sources[0]!.emit({ topic: 'orders', at: 3, payload: {} });
		expect(seen).toEqual(['b:quotes', 'a:orders', 'b:orders']);

		a.close();
		b.close();
		await tick();
		expect(live().filter((s) => s.url.startsWith(path))).toHaveLength(0);
	});

	test('re-opens once when the topic set changes and routes prefix patterns', async () => {
		const path = url();
		const seen: string[] = [];
		const a = createSharedSyncSubscriber({
			topics: ['orders:*'],
			onEvent: (event) => seen.push(`a:${event.topic}`),
			url: path,
			eventSourceImpl: FakeImpl
		});
		await tick();
		const first = live().find((s) => s.url.startsWith(path))!;
		expect(first.topics).toBe('orders:*');

		// Same topics again: no reconnect.
		const b = createSharedSyncSubscriber({
			topics: ['orders:*'],
			onEvent: (event) => seen.push(`b:${event.topic}`),
			url: path,
			eventSourceImpl: FakeImpl
		});
		await tick();
		expect(first.closed).toBe(false);

		// New topic: exactly one reconnect.
		const c = createSharedSyncSubscriber({
			topics: ['catalog'],
			onEvent: (event) => seen.push(`c:${event.topic}`),
			url: path,
			eventSourceImpl: FakeImpl
		});
		await tick();
		expect(first.closed).toBe(true);
		const second = live().filter((s) => s.url.startsWith(path));
		expect(second).toHaveLength(1);
		expect(second[0]!.topics).toBe('catalog,orders:*');

		second[0]!.emit({ topic: 'orders:42', at: 2, payload: {} });
		second[0]!.emit({ topic: 'catalog', at: 3, payload: {} });
		expect(seen).toEqual(['a:orders:42', 'b:orders:42', 'c:catalog']);
		a.close();
		b.close();
		c.close();
	});

	test('a topic-change reconnect only opens NEW members; a transport reconnect re-opens everyone', async () => {
		const path = url();
		const opens: string[] = [];
		const a = createSharedSyncSubscriber({
			topics: ['orders'],
			onEvent: (event) => {
				if (event.topic === SYNC_OPEN_TOPIC) opens.push('a');
			},
			url: path,
			eventSourceImpl: FakeImpl
		});
		await tick();
		live()
			.find((s) => s.url.startsWith(path))!
			.emit(open(['orders']));
		expect(opens).toEqual(['a']);

		const b = createSharedSyncSubscriber({
			topics: ['quotes'],
			onEvent: (event) => {
				if (event.topic === SYNC_OPEN_TOPIC) opens.push('b');
			},
			url: path,
			eventSourceImpl: FakeImpl
		});
		await tick();
		const source = live().find((s) => s.url.startsWith(path))!;
		source.emit(open(['orders', 'quotes']));
		// `a` was already hydrated — it must not be told to re-hydrate.
		expect(opens).toEqual(['a', 'b']);

		// EventSource auto-reconnect on the same object: a real reconnect.
		source.emit(open(['orders', 'quotes']));
		expect(opens).toEqual(['a', 'b', 'a', 'b']);
		a.close();
		b.close();
	});

	test('createSyncSubscriber({ shared: true }) and createLiveQuery({ shared: true }) join the pool', async () => {
		const path = url();
		let fetches = 0;
		const query = createLiveQuery<number>({
			topics: ['orders'],
			fetcher: async () => (fetches += 1),
			url: path,
			eventSourceImpl: FakeImpl,
			shared: true
		});
		const sub = createSyncSubscriber({
			topics: ['orders'],
			onEvent: () => undefined,
			url: path,
			eventSourceImpl: FakeImpl,
			shared: true
		});
		await tick();
		const sources = live().filter((s) => s.url.startsWith(path));
		expect(sources).toHaveLength(1);
		expect(fetches).toBe(1);

		sources[0]!.emit({ topic: 'orders', at: 2, payload: {} });
		await tick();
		expect(fetches).toBe(2);
		expect(query.get().data).toBe(2);
		query.close();
		sub.close();
		await tick();
		expect(sources[0]!.closed).toBe(true);
	});
});
