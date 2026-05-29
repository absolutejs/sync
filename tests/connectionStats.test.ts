import { describe, expect, test } from 'bun:test';
import { createSyncConnection } from '../src/engine/connection';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ServerFrame } from '../src/engine/connection';

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

describe('connection.stats() — 1.14.0', () => {
	test('reports zero on a fresh connection', () => {
		const { engine } = makeEngine();
		const sent: ServerFrame[] = [];
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: (frame) => { sent.push(frame); }
		});
		const stats = connection.stats();
		expect(stats.subscriptionCount).toBe(0);
		expect(stats.presenceRoomCount).toBe(0);
		expect(stats.framesSent).toBe(0);
		expect(stats.slowSendsRecent).toBe(0);
	});

	test('counts subscriptions and frames after activity', async () => {
		const { engine } = makeEngine();
		const sent: ServerFrame[] = [];
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: (frame) => { sent.push(frame); }
		});
		await connection.handle({ collection: 'tasks', id: 's1', type: 'subscribe' });
		await connection.handle({ collection: 'tasks', id: 's2', type: 'subscribe' });
		const stats = connection.stats();
		expect(stats.subscriptionCount).toBe(2);
		expect(stats.framesSent).toBeGreaterThan(0);
	});

	test('accumulates slowSendsRecent when send returns -1', async () => {
		const { engine } = makeEngine();
		let pretendSlow = true;
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: () => (pretendSlow ? -1 : 0)
		});
		await connection.handle({ collection: 'tasks', id: 's1', type: 'subscribe' });
		// The subscribe path sends a `snapshot`; with -1 returns, slowSendsRecent climbs.
		expect(connection.stats().slowSendsRecent).toBeGreaterThan(0);
		expect(connection.stats().framesSent).toBe(0);

		// Healthy send resets slowSendsRecent on the next non-backpressure return.
		pretendSlow = false;
		await connection.handle({ collection: 'tasks', id: 's2', type: 'subscribe' });
		expect(connection.stats().slowSendsRecent).toBe(0);
		expect(connection.stats().framesSent).toBeGreaterThan(0);
	});

	test('unsubscribe drops the subscriptionCount', async () => {
		const { engine } = makeEngine();
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: () => {}
		});
		await connection.handle({ collection: 'tasks', id: 's1', type: 'subscribe' });
		expect(connection.stats().subscriptionCount).toBe(1);
		await connection.handle({ id: 's1', type: 'unsubscribe' });
		expect(connection.stats().subscriptionCount).toBe(0);
	});

	test('legacy void-returning send still works (no slow signal)', async () => {
		const { engine } = makeEngine();
		const connection = createSyncConnection({
			ctx: {},
			engine,
			send: () => {
				// Explicitly returns undefined (the 0.0.1 contract).
			}
		});
		await connection.handle({ collection: 'tasks', id: 's1', type: 'subscribe' });
		// Undefined return is treated as success; framesSent bumps; slow stays zero.
		expect(connection.stats().framesSent).toBeGreaterThan(0);
		expect(connection.stats().slowSendsRecent).toBe(0);
	});
});
