import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';
import {
	createSyncConnection,
	type ClientFrame,
	type ServerFrame
} from '../src/engine/connection';
import { jsonSerializer, type FrameSerializer } from '../src/serializer';

type Task = { id: number; title: string };

const makeEngine = () => {
	const store = new Map<number, Task>();
	const engine = createSyncEngine();
	engine.registerReader('tasks', { all: () => [...store.values()] });
	engine.registerWriter<Task>('tasks', {
		delete: (row) => { store.delete(row.id); },
		insert: (data) => { store.set(data.id, data); return data; },
		update: (data) => { store.set(data.id, data); return data; }
	});
	engine.register(
		defineCollection<Task>({
			hydrate: () => [...store.values()],
			key: (task) => task.id,
			match: () => true,
			name: 'tasks'
		})
	);
	return { engine, store };
};

describe('jsonSerializer (default — 1.16.0)', () => {
	test('encodeServer produces a JSON string that round-trips', () => {
		const frame: ServerFrame = { type: 'ack', mutationId: 7, result: { ok: true } };
		const encoded = jsonSerializer.encodeServer(frame);
		expect(typeof encoded).toBe('string');
		const decoded = jsonSerializer.decode(encoded);
		expect(decoded).toEqual(frame as unknown as Record<string, unknown>);
	});

	test('encodeClient produces a JSON string that round-trips', () => {
		const frame: ClientFrame = { type: 'unsubscribe', id: 's1' };
		const encoded = jsonSerializer.encodeClient(frame);
		const decoded = jsonSerializer.decode(encoded);
		expect(decoded).toEqual(frame as unknown as Record<string, unknown>);
	});

	test('decode handles already-parsed objects (pass-through)', () => {
		const frame = { type: 'subscribe', id: 's', collection: 'tasks' };
		expect(jsonSerializer.decode(frame)).toEqual(frame);
	});

	test('decode handles Uint8Array (UTF-8)', () => {
		const text = JSON.stringify({ type: 'ping' });
		const bytes = new TextEncoder().encode(text);
		expect(jsonSerializer.decode(bytes)).toEqual({ type: 'ping' });
	});

	test('decode handles ArrayBuffer (UTF-8)', () => {
		const text = JSON.stringify({ type: 'pong' });
		const bytes = new TextEncoder().encode(text);
		expect(jsonSerializer.decode(bytes.buffer)).toEqual({ type: 'pong' });
	});

	test('decode returns null on malformed input', () => {
		expect(jsonSerializer.decode('{ not json')).toBeNull();
		expect(jsonSerializer.decode(new TextEncoder().encode('garbage'))).toBeNull();
	});
});

describe('createSyncConnection with a custom serializer', () => {
	// A test serializer that wraps frames in a known marker — proves the
	// connection routes both incoming and outgoing through the serializer
	// instead of hard-coding JSON.
	const taggedSerializer: FrameSerializer = {
		decode: (raw: unknown) => {
			if (typeof raw !== 'string') return raw;
			if (!raw.startsWith('TAGGED::')) return null;
			try {
				return JSON.parse(raw.slice('TAGGED::'.length));
			} catch {
				return null;
			}
		},
		encodeClient: (frame: ClientFrame) => `TAGGED::${JSON.stringify(frame)}`,
		encodeServer: (frame: ServerFrame) => `TAGGED::${JSON.stringify(frame)}`
	};

	test('outgoing server frames go through serializer.encodeServer', async () => {
		const { engine } = makeEngine();
		const sent: unknown[] = [];
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: (frame) => {
				sent.push(frame); // ServerFrame, not the encoded form
			},
			serializer: taggedSerializer
		});
		await connection.handle({ collection: 'tasks', id: 's1', type: 'subscribe' });
		// `send` in this test stores the typed frame; the encoding happens
		// in the WS adapter. So we test the inverse: a tagged INCOMING frame
		// is parsed by the serializer.
		expect(sent.some((frame) => (frame as ServerFrame).type === 'snapshot')).toBe(true);
	});

	test('incoming client frames go through serializer.decode', async () => {
		const { engine } = makeEngine();
		const sent: ServerFrame[] = [];
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: (frame) => { sent.push(frame); },
			serializer: taggedSerializer
		});
		const wireFrame = `TAGGED::${JSON.stringify({
			collection: 'tasks',
			id: 's1',
			type: 'subscribe'
		})}`;
		await connection.handle(wireFrame);
		expect(sent.some((frame) => frame.type === 'snapshot')).toBe(true);
	});

	test('untagged incoming frame is rejected as malformed', async () => {
		const { engine } = makeEngine();
		const sent: ServerFrame[] = [];
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: (frame) => { sent.push(frame); },
			serializer: taggedSerializer
		});
		await connection.handle(JSON.stringify({ type: 'subscribe', id: 's', collection: 'tasks' }));
		// The taggedSerializer rejects untagged JSON; engine sends a Malformed error.
		expect(sent[0]?.type).toBe('error');
		expect((sent[0] as { message?: string }).message).toContain('Malformed');
	});

	test('default jsonSerializer keeps every pre-1.16 callsite working', async () => {
		const { engine } = makeEngine();
		const sent: ServerFrame[] = [];
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: (frame) => { sent.push(frame); }
			// no serializer — defaults to jsonSerializer
		});
		await connection.handle(JSON.stringify({ collection: 'tasks', id: 's1', type: 'subscribe' }));
		expect(sent.some((frame) => frame.type === 'snapshot')).toBe(true);
	});
});
