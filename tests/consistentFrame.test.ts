import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncConnection } from '../src/engine/connection';
import type { ServerFrame } from '../src/engine/connection';
import { defineMutation } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';
import { createSyncClient } from '../src/client/syncClient';

type Row = { id: number; v: number };

const tableWriter = (store: Map<number, Row>) => ({
	insert: (data: Row) => {
		store.set(data.id, data);

		return data;
	},
	update: (data: Row) => {
		store.set(data.id, data);

		return data;
	},
	delete: (row: { id: number }) => {
		store.delete(row.id);
	}
});

const makeEngine = () => {
	const a = new Map<number, Row>();
	const b = new Map<number, Row>();
	const engine = createSyncEngine();
	engine.register(
		defineCollection<Row>({
			name: 'a',
			key: (row) => row.id,
			hydrate: () => [...a.values()],
			match: () => true
		})
	);
	engine.register(
		defineCollection<Row>({
			name: 'b',
			key: (row) => row.id,
			hydrate: () => [...b.values()],
			match: () => true
		})
	);
	engine.registerWriter('a', tableWriter(a));
	engine.registerWriter('b', tableWriter(b));
	engine.registerMutation(
		defineMutation({
			name: 'writeBoth',
			handler: async (_args, _ctx, actions) => {
				await actions.insert('a', { id: 1, v: 1 });
				await actions.insert('b', { id: 1, v: 2 });
			}
		})
	);
	engine.registerMutation(
		defineMutation({
			name: 'writeOne',
			handler: (_args, _ctx, actions) =>
				actions.insert('a', { id: 2, v: 9 })
		})
	);

	return engine;
};

describe('connection frame coalescing', () => {
	test('a multi-collection mutation sends ONE frame (both diffs, one version) before the ack', async () => {
		const engine = makeEngine();
		const sent: ServerFrame[] = [];
		const conn = createSyncConnection({
			engine,
			ctx: {},
			send: (frame) => sent.push(frame)
		});
		await conn.handle({ type: 'subscribe', id: 'sa', collection: 'a' });
		await conn.handle({ type: 'subscribe', id: 'sb', collection: 'b' });
		sent.length = 0; // drop the two snapshots

		await conn.handle({ type: 'mutate', mutationId: 1, name: 'writeBoth' });

		const frames = sent.filter((frame) => frame.type === 'frame');
		expect(frames).toHaveLength(1);
		const frame = frames[0] as Extract<ServerFrame, { type: 'frame' }>;
		expect(frame.diffs.map((diff) => diff.id).sort()).toEqual(['sa', 'sb']);
		expect(typeof frame.version).toBe('number');

		// The ack arrives after the diffs.
		const frameIndex = sent.findIndex((frame) => frame.type === 'frame');
		const ackIndex = sent.findIndex((frame) => frame.type === 'ack');
		expect(ackIndex).toBeGreaterThan(frameIndex);
	});

	test('a single-collection mutation stays a plain diff (backward compatible)', async () => {
		const engine = makeEngine();
		const sent: ServerFrame[] = [];
		const conn = createSyncConnection({
			engine,
			ctx: {},
			send: (frame) => sent.push(frame)
		});
		await conn.handle({ type: 'subscribe', id: 'sa', collection: 'a' });
		sent.length = 0;

		await conn.handle({ type: 'mutate', mutationId: 1, name: 'writeOne' });

		expect(sent.filter((frame) => frame.type === 'frame')).toHaveLength(0);
		expect(sent.filter((frame) => frame.type === 'diff')).toHaveLength(1);
	});
});

class FakeWebSocket {
	static last: FakeWebSocket | undefined;
	onopen: (() => void) | undefined;
	onmessage: ((event: { data: string }) => void) | undefined;
	onclose: (() => void) | undefined;
	readonly sent: string[] = [];

	constructor(public url: string) {
		FakeWebSocket.last = this;
	}

	send(data: string) {
		this.sent.push(data);
	}

	close() {
		this.onclose?.();
	}

	// test driver helpers
	open() {
		this.onopen?.();
	}

	emit(frame: unknown) {
		this.onmessage?.({ data: JSON.stringify(frame) });
	}
}

describe('createSyncClient consistent frame', () => {
	test('a multi-collection frame updates all collections before any listener sees it', () => {
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: FakeWebSocket as unknown as typeof WebSocket,
			reconnectMs: 0
		});
		const ws = FakeWebSocket.last!;
		ws.open();

		const orders = client.collection<Row>({ collection: 'a' }); // id c0
		const counts = client.collection<Row>({ collection: 'b' }); // id c1
		ws.emit({ type: 'snapshot', id: 'c0', rows: [], version: 0 });
		ws.emit({ type: 'snapshot', id: 'c1', rows: [], version: 0 });

		// Whenever either store notifies, the two must already agree (both go 0→1
		// together) — a torn frame would catch one updated and the other not.
		let tornObserved = false;
		const check = () => {
			if (orders.get().data.length !== counts.get().data.length) {
				tornObserved = true;
			}
		};
		orders.subscribe(check);
		counts.subscribe(check);

		ws.emit({
			type: 'frame',
			version: 1,
			diffs: [
				{
					id: 'c0',
					added: [{ id: 1, v: 1 }],
					removed: [],
					changed: []
				},
				{ id: 'c1', added: [{ id: 1, v: 1 }], removed: [], changed: [] }
			]
		});

		expect(tornObserved).toBe(false);
		expect(orders.get().data).toHaveLength(1);
		expect(counts.get().data).toHaveLength(1);

		client.close();
	});

	test('a plain diff updates only its collection; snapshot marks it ready', () => {
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: FakeWebSocket as unknown as typeof WebSocket,
			reconnectMs: 0
		});
		const ws = FakeWebSocket.last!;
		ws.open();
		const orders = client.collection<Row>({ collection: 'a' }); // c0
		const counts = client.collection<Row>({ collection: 'b' }); // c1
		ws.emit({ type: 'snapshot', id: 'c0', rows: [], version: 0 });
		ws.emit({ type: 'snapshot', id: 'c1', rows: [], version: 0 });

		ws.emit({
			type: 'diff',
			id: 'c0',
			added: [{ id: 5, v: 5 }],
			removed: [],
			changed: [],
			version: 1
		});

		expect(orders.get().data).toEqual([{ id: 5, v: 5 }]);
		expect(orders.get().status).toBe('ready');
		expect(counts.get().data).toEqual([]);

		client.close();
	});

	test('optimistic mutate shows immediately and resolves on ack', async () => {
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: FakeWebSocket as unknown as typeof WebSocket,
			reconnectMs: 0
		});
		const ws = FakeWebSocket.last!;
		ws.open();
		const orders = client.collection<Row>({ collection: 'a' }); // c0
		ws.emit({ type: 'snapshot', id: 'c0', rows: [], version: 0 });

		const promise = orders.mutate({
			name: 'addOrder',
			args: { id: 3 },
			optimistic: (draft) => draft.set({ id: 3, v: 0 })
		});
		// Optimistic row is visible before the server confirms.
		expect(orders.get().data).toEqual([{ id: 3, v: 0 }]);

		// Authoritative diff then ack (ordered as the server sends them).
		ws.emit({
			type: 'diff',
			id: 'c0',
			added: [{ id: 3, v: 7 }],
			removed: [],
			changed: [],
			version: 1
		});
		ws.emit({ type: 'ack', mutationId: 1, result: { ok: true } });

		await expect(promise).resolves.toEqual({ ok: true });
		expect(orders.get().data).toEqual([{ id: 3, v: 7 }]);

		client.close();
	});
});
