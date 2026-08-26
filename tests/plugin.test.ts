import { describe, expect, test } from 'bun:test';
import { Elysia } from 'elysia';
import { createReactiveHub } from '../src/reactiveHub';
import { sync } from '../src/plugin';
import type { ReactiveEvent } from '../src/reactiveHub';

const decoder = new TextDecoder();

/**
 * Read SSE frames from a streaming Response until `count` `data:` frames have
 * arrived (heartbeat `: ping` comments are returned too, but don't count toward
 * the target). Frames are delimited by a blank line.
 */
const readFrames = async (response: Response, count: number) => {
	const reader = response.body!.getReader();
	const frames: string[] = [];
	let dataFrames = 0;
	let buffer = '';
	while (dataFrames < count) {
		const { value, done } = await reader.read();
		if (done) {
			break;
		}
		buffer += decoder.decode(value, { stream: true });
		let index = buffer.indexOf('\n\n');
		while (index !== -1) {
			const frame = buffer.slice(0, index);
			buffer = buffer.slice(index + 2);
			frames.push(frame);
			if (frame.startsWith('data:')) {
				dataFrames += 1;
			}
			index = buffer.indexOf('\n\n');
		}
	}
	reader.cancel().catch(() => {});
	return frames;
};

const parseData = (frame: string | undefined): ReactiveEvent => {
	if (frame === undefined) {
		throw new Error('expected an SSE data frame, got none');
	}
	return JSON.parse(frame.slice('data: '.length)) as ReactiveEvent;
};

const connect = (
	app: Elysia,
	url: string,
	controller = new AbortController()
) =>
	app.handle(
		new Request(`http://localhost${url}`, { signal: controller.signal })
	);

describe('sync plugin', () => {
	test('serves SSE headers and an open event with resolved topics', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(sync({ hub, heartbeatMs: 10_000 }));

		const response = await connect(app, '/sync?topics=orders,users');

		expect(response.headers.get('content-type')).toBe('text/event-stream');
		expect(response.headers.get('cache-control')).toBe(
			'no-cache, no-transform'
		);

		const [open] = await readFrames(response, 1);
		const event = parseData(open);
		expect(event.topic).toBe('@absolutejs/sync:open');
		expect(event.payload).toEqual({ topics: ['orders', 'users'] });
	});

	test('default resolveTopics parses and trims a comma-separated query', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(sync({ hub }));

		const response = await connect(app, '/sync?topics=%20a%20,b,,%20c%20');

		const [open] = await readFrames(response, 1);
		expect(parseData(open).payload).toEqual({ topics: ['a', 'b', 'c'] });
	});

	test('delivers a published event to a connected subscriber', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(sync({ hub, heartbeatMs: 10_000 }));

		const response = await connect(app, '/sync?topics=orders');
		// The subscription is registered synchronously while the stream starts, so
		// it is live by the time `handle` resolves.
		expect(hub.subscriberCount('orders')).toBe(1);
		hub.publish('orders', { id: 7 });

		const frames = await readFrames(response, 2);
		const delivered = parseData(frames[1]);
		expect(delivered.topic).toBe('orders');
		expect(delivered.payload).toEqual({ id: 7 });
	});

	test('a prefix-wildcard subscription streams matching topics', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(sync({ hub, heartbeatMs: 10_000 }));

		const response = await connect(app, '/sync?topics=orders:*');
		hub.publish('orders:42', { total: 1 });

		const frames = await readFrames(response, 2);
		expect(parseData(frames[1]).topic).toBe('orders:42');
	});

	test('registers a subscription on connect and removes it on abort', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(sync({ hub, heartbeatMs: 10_000 }));

		const controller = new AbortController();
		const response = await connect(app, '/sync?topics=orders', controller);
		expect(hub.subscriberCount('orders')).toBe(1);

		response.body!.cancel().catch(() => {});
		controller.abort();
		await new Promise((resolve) => setTimeout(resolve, 10));

		expect(hub.subscriberCount('orders')).toBe(0);
	});

	test('custom resolveTopics overrides client-provided topics', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(
			sync({
				hub,
				heartbeatMs: 10_000,
				// Derive from the (trusted) server side, ignore the query entirely.
				resolveTopics: () => ['session:abc']
			})
		);

		const response = await connect(app, '/sync?topics=admin:everything');

		expect(hub.subscriberCount('session:abc')).toBe(1);
		expect(hub.subscriberCount('admin:everything')).toBe(0);
		const [open] = await readFrames(response, 1);
		expect(parseData(open).payload).toEqual({ topics: ['session:abc'] });
	});

	test('awaits database-backed topic authorization before opening', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(
			sync({
				hub,
				resolveTopics: async ({ query }) => {
					await Promise.resolve();
					return query.project === 'owned' ? ['project:owned'] : [];
				}
			})
		);

		const response = await connect(app, '/sync?project=owned');
		const [open] = await readFrames(response, 1);
		expect(parseData(open).payload).toEqual({ topics: ['project:owned'] });
	});

	test('provides Elysia-decoded cookies to topic authorization', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(
			sync({
				hub,
				resolveTopics: ({ cookies }) =>
					cookies.session === 'authorized' ? ['private'] : []
			})
		);

		const response = await app.handle(
			new Request('http://localhost/sync', {
				headers: { cookie: 'session=authorized' }
			})
		);
		const [open] = await readFrames(response, 1);
		expect(parseData(open).payload).toEqual({ topics: ['private'] });
	});

	test('decodes cookie values read from the raw Cookie header', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(
			sync({
				hub,
				resolveTopics: ({ cookies }) =>
					cookies.staff === 'a b.c=d' ? ['staff'] : []
			})
		);

		const response = await app.handle(
			new Request('http://localhost/sync', {
				headers: { cookie: 'theme=dark; staff=a%20b.c%3Dd' }
			})
		);
		const [open] = await readFrames(response, 1);
		expect(parseData(open).payload).toEqual({ topics: ['staff'] });
	});

	test('a connection with no topics subscribes to nothing but still opens', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(sync({ hub, heartbeatMs: 10_000 }));

		const response = await connect(app, '/sync');

		expect(hub.subscriberCount()).toBe(0);
		const [open] = await readFrames(response, 1);
		expect(parseData(open).payload).toEqual({ topics: [] });
	});

	test('emits heartbeat comments to keep the stream alive', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(sync({ hub, heartbeatMs: 10 }));

		const response = await connect(app, '/sync?topics=orders');
		const reader = response.body!.getReader();
		let sawPing = false;
		// Open frame, then heartbeat comments while idle.
		for (let i = 0; i < 5 && !sawPing; i += 1) {
			const { value, done } = await reader.read();
			if (done) {
				break;
			}
			if (decoder.decode(value).includes(': ping')) {
				sawPing = true;
			}
		}
		reader.cancel().catch(() => {});
		expect(sawPing).toBe(true);
	});

	test('serves from a custom path', async () => {
		const hub = createReactiveHub();
		const app = new Elysia().use(
			sync({ hub, path: '/live', heartbeatMs: 10_000 })
		);

		const response = await connect(app, '/live?topics=orders');
		expect(response.status).toBe(200);
		expect(response.headers.get('content-type')).toBe('text/event-stream');
		const missing = await connect(app, '/sync?topics=orders');
		expect(missing.status).toBe(404);
	});
});
