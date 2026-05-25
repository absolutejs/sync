import { afterEach, describe, expect, test } from 'bun:test';
import { createSyncSubscriber } from '../src/client/index';
import type { ReactiveEvent } from '../src/reactiveHub';

/** Minimal EventSource stand-in so the client can be driven deterministically. */
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

	emit(data: string) {
		this.onmessage?.({ data });
	}
}

const FakeImpl = FakeEventSource as unknown as typeof EventSource;

const lastInstance = () => {
	const source = FakeEventSource.instances.at(-1);
	if (!source) {
		throw new Error('expected a FakeEventSource to have been constructed');
	}
	return source;
};

afterEach(() => {
	FakeEventSource.instances = [];
});

describe('createSyncSubscriber', () => {
	test('builds the URL with comma-joined topics and credentials', () => {
		createSyncSubscriber({
			topics: ['orders', 'users:*'],
			onEvent: () => {},
			withCredentials: true,
			eventSourceImpl: FakeImpl
		});

		const source = lastInstance();
		expect(source.url).toBe('/sync?topics=orders%2Cusers%3A*');
		expect(source.withCredentials).toBe(true);
	});

	test('appends topics with & when the url already has a query string', () => {
		createSyncSubscriber({
			topics: ['orders'],
			onEvent: () => {},
			url: '/sync?token=abc',
			eventSourceImpl: FakeImpl
		});

		expect(lastInstance().url).toBe('/sync?token=abc&topics=orders');
	});

	test('parses JSON frames and forwards them to onEvent', () => {
		const seen: ReactiveEvent[] = [];
		createSyncSubscriber({
			topics: ['orders'],
			onEvent: (event) => seen.push(event),
			eventSourceImpl: FakeImpl
		});

		const event: ReactiveEvent = {
			topic: 'orders',
			at: 123,
			payload: { id: 1 }
		};
		lastInstance().emit(JSON.stringify(event));

		expect(seen).toEqual([event]);
	});

	test('ignores heartbeats and non-JSON frames without throwing', () => {
		const seen: ReactiveEvent[] = [];
		createSyncSubscriber({
			topics: ['orders'],
			onEvent: (event) => seen.push(event),
			eventSourceImpl: FakeImpl
		});

		const source = lastInstance();
		expect(() => source.emit(': ping')).not.toThrow();
		expect(() => source.emit('not json')).not.toThrow();
		expect(seen).toEqual([]);
	});

	test('wires onOpen and onError through to the source', () => {
		let opened = false;
		let errored = false;
		createSyncSubscriber({
			topics: ['orders'],
			onEvent: () => {},
			onOpen: () => {
				opened = true;
			},
			onError: () => {
				errored = true;
			},
			eventSourceImpl: FakeImpl
		});

		const source = lastInstance();
		source.onopen?.();
		source.onerror?.(new Event('error'));

		expect(opened).toBe(true);
		expect(errored).toBe(true);
	});

	test('close() closes the underlying source', () => {
		const subscriber = createSyncSubscriber({
			topics: ['orders'],
			onEvent: () => {},
			eventSourceImpl: FakeImpl
		});

		const source = lastInstance();
		expect(source.closed).toBe(false);
		subscriber.close();
		expect(source.closed).toBe(true);
	});

	test('throws a helpful error when no EventSource is available', () => {
		const saved = globalThis.EventSource;
		// @ts-expect-error — simulate a runtime without EventSource.
		delete globalThis.EventSource;
		try {
			expect(() =>
				createSyncSubscriber({ topics: ['orders'], onEvent: () => {} })
			).toThrow('requires EventSource');
		} finally {
			globalThis.EventSource = saved;
		}
	});
});
