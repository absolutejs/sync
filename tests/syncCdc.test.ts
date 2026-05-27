/**
 * `syncCdc` Elysia plugin — integration test against the SSE route.
 * Spins a real Elysia listener, opens an EventSource-style fetch with
 * `text/event-stream`, parses the events, and verifies the wire format.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Elysia } from 'elysia';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';
import { syncCdc } from '../src/engine/cdc';

type Item = { id: number; n: number };

type ParsedEvent = {
	id: string | null;
	event: string;
	data: unknown;
};

const setup = () => {
	const engine = createSyncEngine();
	engine.register(
		defineCollection<Item>({
			name: 'items',
			key: (row) => row.id,
			hydrate: () => [],
			match: () => true
		})
	);
	engine.registerMutation(
		defineMutation({
			name: 'add',
			handler: async (args: Item, _ctx, actions) => {
				await actions.change('items', {
					op: 'insert',
					row: args
				});
			}
		})
	);
	return engine;
};

let server: { stop: () => void; port: number } | null = null;

afterEach(() => {
	server?.stop();
	server = null;
});

const startServer = (app: Elysia): { url: string; stop: () => void } => {
	const handle = Bun.serve({
		port: 0,
		fetch: (request) => app.handle(request)
	});
	// Bun's Server type widens `port` to `number | undefined` even though
	// `Bun.serve()` always returns a bound port when port:0 succeeds.
	const port = handle.port ?? 0;
	server = { stop: () => handle.stop(true), port };
	return {
		url: `http://localhost:${port}`,
		stop: () => handle.stop(true)
	};
};

/** Parse N SSE events from the response body, abort after a budget. */
const readEvents = async (
	response: Response,
	want: number,
	timeoutMs = 2000
): Promise<ParsedEvent[]> => {
	const body = response.body;
	if (body === null) throw new Error('response has no body');
	const reader = body.getReader();
	const decoder = new TextDecoder();
	let buffer = '';
	const events: ParsedEvent[] = [];
	const deadline = Date.now() + timeoutMs;

	while (events.length < want) {
		if (Date.now() > deadline) {
			throw new Error(`readEvents timed out at ${events.length}/${want}`);
		}
		const result = await Promise.race([
			reader.read(),
			new Promise<ReadableStreamReadResult<Uint8Array>>((_, reject) =>
				setTimeout(
					() => reject(new Error('read timeout')),
					deadline - Date.now()
				)
			)
		]);
		if (result.done) break;
		buffer += decoder.decode(result.value);

		let separatorIndex: number;
		while ((separatorIndex = buffer.indexOf('\n\n')) !== -1) {
			const block = buffer.slice(0, separatorIndex);
			buffer = buffer.slice(separatorIndex + 2);
			if (block.startsWith(':')) continue; // heartbeat comment
			let id: string | null = null;
			let event = 'message';
			let data = '';
			for (const line of block.split('\n')) {
				if (line.startsWith('id: ')) id = line.slice(4);
				else if (line.startsWith('event: ')) event = line.slice(7);
				else if (line.startsWith('data: '))
					data += (data === '' ? '' : '\n') + line.slice(6);
			}
			events.push({
				id,
				event,
				data: data === '' ? '' : JSON.parse(data)
			});
		}
	}
	reader.cancel().catch(() => {
		// already done
	});
	return events;
};

describe('syncCdc', () => {
	test('streams committed changes as SSE events with id + event + data', async () => {
		const engine = setup();
		const app = new Elysia().use(syncCdc({ engine }));
		await engine.runMutation('add', { id: 1, n: 1 }, {});
		await engine.runMutation('add', { id: 2, n: 2 }, {});

		const { url } = startServer(app);
		const response = await fetch(`${url}/sync/cdc`, {
			headers: { accept: 'text/event-stream' }
		});
		expect(response.status).toBe(200);
		expect(response.headers.get('content-type')).toBe('text/event-stream');

		// open + 2 changes
		const events = await readEvents(response, 3);
		expect(events[0]?.event).toBe('open');
		expect(events[1]?.event).toBe('change');
		expect(events[1]?.id).toBe('1');
		expect((events[1]?.data as { version: number }).version).toBe(1);
		expect((events[1]?.data as { table: string }).table).toBe('items');
		expect(events[2]?.id).toBe('2');
	});

	test('resumes from ?since= query param', async () => {
		const engine = setup();
		const app = new Elysia().use(syncCdc({ engine }));
		await engine.runMutation('add', { id: 1, n: 1 }, {});
		await engine.runMutation('add', { id: 2, n: 2 }, {});
		await engine.runMutation('add', { id: 3, n: 3 }, {});

		const { url } = startServer(app);
		const response = await fetch(`${url}/sync/cdc?since=2`, {
			headers: { accept: 'text/event-stream' }
		});
		const events = await readEvents(response, 2);
		// open + just the v3 change
		expect(events[0]?.event).toBe('open');
		expect(events[1]?.event).toBe('change');
		expect(events[1]?.id).toBe('3');
	});

	test('resumes from Last-Event-ID header (EventSource auto-reconnect)', async () => {
		const engine = setup();
		const app = new Elysia().use(syncCdc({ engine }));
		await engine.runMutation('add', { id: 1, n: 1 }, {});
		await engine.runMutation('add', { id: 2, n: 2 }, {});

		const { url } = startServer(app);
		const response = await fetch(`${url}/sync/cdc`, {
			headers: {
				accept: 'text/event-stream',
				'last-event-id': '1'
			}
		});
		const events = await readEvents(response, 2);
		expect(events[0]?.event).toBe('open');
		expect(events[1]?.id).toBe('2');
	});

	test('emits an `error` SSE event with name + message on MissedChangesError', async () => {
		const engine = createSyncEngine({ changeLogSize: 3 });
		engine.register(
			defineCollection<Item>({
				name: 'items',
				key: (row) => row.id,
				hydrate: () => [],
				match: () => true
			})
		);
		engine.registerMutation(
			defineMutation({
				name: 'add',
				handler: async (args: Item, _ctx, actions) => {
					await actions.change('items', {
						op: 'insert',
						row: args
					});
				}
			})
		);
		for (let i = 1; i <= 5; i++) {
			await engine.runMutation('add', { id: i, n: i }, {});
		}

		const app = new Elysia().use(syncCdc({ engine }));
		const { url } = startServer(app);
		const response = await fetch(`${url}/sync/cdc?since=1`, {
			headers: { accept: 'text/event-stream' }
		});
		const events = await readEvents(response, 2);
		expect(events[0]?.event).toBe('open');
		expect(events[1]?.event).toBe('error');
		expect((events[1]?.data as { name: string }).name).toBe(
			'MissedChangesError'
		);
	});
});
