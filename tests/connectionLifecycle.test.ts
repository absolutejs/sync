import { expect, test } from 'bun:test';
import {
	createSyncConnection,
	type ServerFrame
} from '../src/engine/connection';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';

const fixture = () => {
	const engine = createSyncEngine();
	const started = Promise.withResolvers<void>();
	const hydrated = Promise.withResolvers<{ id: number }[]>();
	engine.register(
		defineCollection<{ id: number }>({
			name: 'items',
			key: (row) => row.id,
			match: () => true,
			hydrate: () => {
				started.resolve();
				return hydrated.promise;
			}
		})
	);
	const sent: ServerFrame[] = [];
	const connection = createSyncConnection({
		engine,
		ctx: {},
		send: (frame) => {
			sent.push(frame);
		}
	});
	return { engine, started, hydrated, sent, connection };
};
const subscribe = { type: 'subscribe', id: 'one', collection: 'items' };

test('close during hydration does not retain or send the late subscription', async () => {
	const f = fixture();
	const pending = f.connection.handle(subscribe);
	await f.started.promise;
	f.connection.close();
	f.hydrated.resolve([{ id: 1 }]);
	await pending;
	expect(f.connection.stats().subscriptionCount).toBe(0);
	expect(f.sent).toEqual([]);
	expect(f.engine.metrics().subscriptions.total).toBe(0);
});
test('unsubscribe during hydration cancels the pending subscription', async () => {
	const f = fixture();
	const pending = f.connection.handle(subscribe);
	await f.started.promise;
	await f.connection.handle({ type: 'unsubscribe', id: 'one' });
	f.hydrated.resolve([{ id: 1 }]);
	await pending;
	expect(f.connection.stats().subscriptionCount).toBe(0);
	expect(f.sent).toEqual([]);
	expect(f.engine.metrics().subscriptions.total).toBe(0);
});
test('duplicate pending subscription IDs are rejected', async () => {
	const f = fixture();
	const pending = f.connection.handle(subscribe);
	await f.started.promise;
	const duplicate = f.connection.handle(subscribe);
	f.hydrated.resolve([{ id: 1 }]);
	await Promise.all([pending, duplicate]);
	expect(f.sent.filter((frame) => frame.type === 'snapshot')).toHaveLength(1);
	expect(f.sent.filter((frame) => frame.type === 'error')).toHaveLength(1);
	f.connection.close();
});
test('closed connections ignore new work', async () => {
	const f = fixture();
	f.connection.close();
	f.hydrated.resolve([]);
	await f.connection.handle(subscribe);
	expect(f.connection.stats().subscriptionCount).toBe(0);
	expect(f.sent).toEqual([]);
	expect(f.engine.metrics().subscriptions.total).toBe(0);
});

test('a canceled pending ID can be reused without the old completion replacing it', async () => {
	const f = fixture();
	const first = f.connection.handle(subscribe);
	await f.started.promise;
	await f.connection.handle({ type: 'unsubscribe', id: 'one' });
	const replacement = f.connection.handle(subscribe);
	f.hydrated.resolve([{ id: 1 }]);
	await Promise.all([first, replacement]);
	expect(f.sent.filter((frame) => frame.type === 'snapshot')).toHaveLength(1);
	expect(f.engine.metrics().subscriptions.total).toBe(1);
	f.connection.close();
	expect(f.engine.metrics().subscriptions.total).toBe(0);
});
