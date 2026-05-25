import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { createInMemoryClusterBus } from '../src/engine/cluster';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { SyncEngine } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Task = { id: number; title: string };

const itemsCollection = () =>
	defineCollection<Task>({
		name: 'tasks',
		key: (task) => task.id,
		hydrate: () => [],
		match: () => true
	});

const collect = () => {
	const diffs: ViewDiff<Task>[] = [];
	return {
		diffs,
		onDiff: (diff: ViewDiff<Task>) => {
			diffs.push(diff);
		}
	};
};

const subscribe = (
	engine: SyncEngine,
	onDiff: (diff: ViewDiff<Task>) => void
) =>
	engine.subscribe<Task>({
		collection: 'tasks',
		params: undefined,
		ctx: {},
		onDiff
	});

// Fan-out to a peer is asynchronous (the bus callback applies on the peer's own
// tick), so let it settle before asserting on the other instance.
const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

describe('horizontal scale (cluster bus)', () => {
	test('a change on one instance reaches subscribers on another', async () => {
		const bus = createInMemoryClusterBus();
		const a = createSyncEngine();
		const b = createSyncEngine();
		a.register(itemsCollection());
		b.register(itemsCollection());
		await a.connectCluster(bus);
		await b.connectCluster(bus);

		const onA = collect();
		const onB = collect();
		await subscribe(a, onA.onDiff);
		await subscribe(b, onB.onDiff);

		// Applied on A only…
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'hi' }
		});
		await settle();

		// …reaches A's own subscriber and, via the bus, B's subscriber.
		expect(onA.diffs.at(-1)?.added).toEqual([{ id: 1, title: 'hi' }]);
		expect(onB.diffs.at(-1)?.added).toEqual([{ id: 1, title: 'hi' }]);
	});

	test('mutations fan out across instances', async () => {
		const bus = createInMemoryClusterBus();
		const a = createSyncEngine();
		const b = createSyncEngine();
		for (const engine of [a, b]) {
			engine.register(itemsCollection());
			engine.registerMutation(
				defineMutation({
					name: 'add',
					handler: async (args: Task, _ctx, actions) => {
						await actions.change('tasks', {
							op: 'insert',
							row: args
						});
					}
				})
			);
			await engine.connectCluster(bus);
		}

		const onB = collect();
		await subscribe(b, onB.onDiff);

		await a.runMutation('add', { id: 7, title: 'from A' }, {});
		await settle();
		expect(onB.diffs.at(-1)?.added).toEqual([{ id: 7, title: 'from A' }]);
	});

	test('an instance does not double-apply its own broadcast', async () => {
		const bus = createInMemoryClusterBus();
		const a = createSyncEngine();
		a.register(itemsCollection());
		await a.connectCluster(bus);

		const onA = collect();
		await subscribe(a, onA.onDiff);

		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'once' }
		});

		// Exactly one diff — the origin ignores its own echoed message.
		expect(onA.diffs).toHaveLength(1);
	});

	test('after disconnect, changes no longer fan out', async () => {
		const bus = createInMemoryClusterBus();
		const a = createSyncEngine();
		const b = createSyncEngine();
		a.register(itemsCollection());
		b.register(itemsCollection());
		await a.connectCluster(bus);
		const disconnectB = await b.connectCluster(bus);

		const onB = collect();
		await subscribe(b, onB.onDiff);
		await disconnectB();

		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'hi' }
		});
		await settle();
		expect(onB.diffs).toHaveLength(0);
	});
});
