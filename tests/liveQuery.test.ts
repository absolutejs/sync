import { afterEach, describe, expect, test } from 'bun:test';
import { createLiveQuery, jsonFetcher } from '../src/client/liveQuery';
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
}

const FakeImpl = FakeEventSource as unknown as typeof EventSource;

const lastSource = () => {
	const source = FakeEventSource.instances.at(-1);
	if (!source) {
		throw new Error('expected a FakeEventSource to have been constructed');
	}
	return source;
};

const change: ReactiveEvent = { topic: 'users', at: 1, payload: { keys: [1] } };
const openFrame: ReactiveEvent = {
	topic: SYNC_OPEN_TOPIC,
	at: 1,
	payload: { topics: ['users'] }
};

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** A fetcher that resolves to an incrementing counter and records call count. */
const counterFetcher = () => {
	const calls = { count: 0 };
	const fetcher = async () => {
		calls.count += 1;
		return calls.count;
	};
	return { fetcher, calls };
};

afterEach(() => {
	FakeEventSource.instances = [];
});

describe('createLiveQuery', () => {
	test('hydrates on creation: loading then data', async () => {
		const { fetcher, calls } = counterFetcher();
		const live = createLiveQuery({
			topics: ['users'],
			fetcher,
			eventSourceImpl: FakeImpl
		});

		expect(live.get().loading).toBe(true);
		await tick();

		expect(live.get()).toEqual({
			data: 1,
			error: undefined,
			loading: false,
			fetching: false
		});
		expect(calls.count).toBe(1);
		live.close();
	});

	test('subscribes to the requested topics', () => {
		const { fetcher } = counterFetcher();
		const live = createLiveQuery({
			topics: ['users', 'users:5'],
			fetcher,
			eventSourceImpl: FakeImpl
		});
		expect(lastSource().url).toBe('/sync?topics=users%2Cusers%3A5');
		live.close();
	});

	test('refetches when a subscribed topic fires', async () => {
		const { fetcher, calls } = counterFetcher();
		const live = createLiveQuery({
			topics: ['users'],
			fetcher,
			eventSourceImpl: FakeImpl
		});
		await tick();
		expect(calls.count).toBe(1);

		lastSource().emit(change);
		await tick();

		expect(calls.count).toBe(2);
		expect(live.get().data).toBe(2);
		live.close();
	});

	test('ignores the first open frame but re-hydrates on reconnect', async () => {
		const { fetcher, calls } = counterFetcher();
		const live = createLiveQuery({
			topics: ['users'],
			fetcher,
			eventSourceImpl: FakeImpl
		});
		await tick();
		expect(calls.count).toBe(1);

		lastSource().emit(openFrame); // initial connect — no extra fetch
		await tick();
		expect(calls.count).toBe(1);

		lastSource().emit(openFrame); // reconnect — re-hydrate
		await tick();
		expect(calls.count).toBe(2);
		live.close();
	});

	test('retains stale data and reports when a refetch fails', async () => {
		let n = 0;
		const errors: unknown[] = [];
		const live = createLiveQuery<number>({
			topics: ['users'],
			fetcher: async () => {
				n += 1;
				if (n === 2) {
					throw new Error('boom');
				}
				return n;
			},
			eventSourceImpl: FakeImpl,
			onError: (error) => errors.push(error)
		});
		await tick();
		expect(live.get().data).toBe(1);

		lastSource().emit(change);
		await tick();

		expect(live.get().data).toBe(1); // stale data retained
		expect(live.get().error).toBeInstanceOf(Error);
		expect(live.get().fetching).toBe(false);
		expect(errors).toHaveLength(1);
		live.close();
	});

	test('debounce coalesces a burst of events into one refetch', async () => {
		const { fetcher, calls } = counterFetcher();
		const live = createLiveQuery({
			topics: ['users'],
			fetcher,
			eventSourceImpl: FakeImpl,
			debounceMs: 20
		});
		await tick();
		expect(calls.count).toBe(1); // initial hydrate is immediate

		const source = lastSource();
		source.emit(change);
		source.emit(change);
		source.emit(change);
		await tick();
		expect(calls.count).toBe(1); // still within the window

		await sleep(40);
		expect(calls.count).toBe(2); // three events -> one refetch
		live.close();
	});

	test('close stops further refetches and closes the source', async () => {
		const { fetcher, calls } = counterFetcher();
		const live = createLiveQuery({
			topics: ['users'],
			fetcher,
			eventSourceImpl: FakeImpl
		});
		await tick();
		const source = lastSource();

		live.close();
		source.emit(change);
		await tick();

		expect(calls.count).toBe(1);
		expect(source.closed).toBe(true);
	});

	test('notifies subscribers on change and stops after unsubscribe', async () => {
		const { fetcher } = counterFetcher();
		const live = createLiveQuery({
			topics: ['users'],
			fetcher,
			eventSourceImpl: FakeImpl
		});
		await tick();

		const states: number[] = [];
		const unsubscribe = live.subscribe((state) => {
			if (state.data !== undefined) {
				states.push(state.data);
			}
		});

		lastSource().emit(change);
		await tick();
		expect(states.at(-1)).toBe(2);

		const seen = states.length;
		unsubscribe();
		lastSource().emit(change);
		await tick();
		expect(states.length).toBe(seen);
		live.close();
	});

	test('initialData skips the initial fetch but events still refetch', async () => {
		const { fetcher, calls } = counterFetcher();
		const live = createLiveQuery({
			topics: ['users'],
			fetcher,
			eventSourceImpl: FakeImpl,
			initialData: 99
		});

		expect(live.get().loading).toBe(false);
		expect(live.get().data).toBe(99);
		await tick();
		expect(calls.count).toBe(0);

		lastSource().emit(change);
		await tick();
		expect(calls.count).toBe(1);
		expect(live.get().data).toBe(1);
		live.close();
	});

	test('manual skips the initial fetch until refetch is called', async () => {
		const { fetcher, calls } = counterFetcher();
		const live = createLiveQuery({
			topics: ['users'],
			fetcher,
			eventSourceImpl: FakeImpl,
			manual: true
		});

		expect(live.get().loading).toBe(false);
		await tick();
		expect(calls.count).toBe(0);

		await live.refetch();
		expect(calls.count).toBe(1);
		expect(live.get().data).toBe(1);
		live.close();
	});

	test('a superseded (slower) response never overwrites a newer one', async () => {
		const deferreds: Array<(value: number) => void> = [];
		const live = createLiveQuery<number>({
			topics: ['users'],
			fetcher: () =>
				new Promise<number>((resolve) => deferreds.push(resolve)),
			eventSourceImpl: FakeImpl
		});

		// Initial fetch is pending (deferreds[0]); an event starts a second.
		lastSource().emit(change);
		await tick();
		expect(deferreds).toHaveLength(2);

		const [first, second] = deferreds;
		second?.(200); // newer settles first
		await tick();
		expect(live.get().data).toBe(200);

		first?.(100); // older settles late — must be ignored
		await tick();
		expect(live.get().data).toBe(200);
		live.close();
	});
});

describe('jsonFetcher', () => {
	test('GETs and parses JSON', async () => {
		const original = globalThis.fetch;
		globalThis.fetch = (async () =>
			new Response(JSON.stringify({ ok: 1 }), {
				status: 200
			})) as unknown as typeof fetch;
		try {
			const data = await jsonFetcher<{ ok: number }>('/api')(
				new AbortController().signal
			);
			expect(data).toEqual({ ok: 1 });
		} finally {
			globalThis.fetch = original;
		}
	});

	test('throws on a non-2xx response', async () => {
		const original = globalThis.fetch;
		globalThis.fetch = (async () =>
			new Response('nope', {
				status: 500,
				statusText: 'Server Error'
			})) as unknown as typeof fetch;
		try {
			await expect(
				jsonFetcher('/api')(new AbortController().signal)
			).rejects.toThrow('500');
		} finally {
			globalThis.fetch = original;
		}
	});
});
