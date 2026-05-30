import { describe, expect, test } from 'bun:test';
import { defineMutation } from '../src/engine/mutation';
import {
	createSyncEngine,
	MutationQueueOverflowError
} from '../src/engine/syncEngine';

type Row = { id: number; tag: string };

const wire = (instanceId: string, concurrency?: number, queueLimit?: number) => {
	const store = new Map<number, Row>();
	const engine = createSyncEngine({
		instanceId,
		mutationConcurrency: concurrency,
		mutationQueueLimit: queueLimit
	});
	engine.registerReader('rows', { all: () => [...store.values()] });
	engine.registerWriter<Row>('rows', {
		delete: (row) => {
			store.delete(row.id);
		},
		insert: (data) => {
			store.set(data.id, data);
			return data;
		},
		update: (data) => {
			store.set(data.id, data);
			return data;
		}
	});
	return { engine, store };
};

const makeGated = (
	engine: ReturnType<typeof wire>['engine']
): {
	gate: Promise<void>;
	release: () => void;
} => {
	let release!: () => void;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	engine.registerMutation(
		defineMutation({
			handler: async (args: Row, _ctx, actions) => {
				await gate;
				return actions.insert('rows', args);
			},
			name: 'gated'
		})
	);
	return { gate, release };
};

const freeMutation = (engine: ReturnType<typeof wire>['engine']) => {
	engine.registerMutation(
		defineMutation({
			handler: (args: Row, _ctx, actions) => actions.insert('rows', args),
			name: 'free'
		})
	);
};

describe('mutationConcurrency — 1.20.0', () => {
	test('cap = 2 keeps at most 2 in-flight; the rest queue', async () => {
		const { engine } = wire('e1', 2);
		const { release } = makeGated(engine);

		// Fire 5 concurrently — 2 should run, 3 should queue.
		const calls = [1, 2, 3, 4, 5].map((id) =>
			engine.runMutation('gated', { id, tag: 'x' }, {})
		);

		// Give the runtime a tick to schedule them.
		await Promise.resolve();
		await Promise.resolve();

		let m = engine.metrics();
		expect(m.mutations.inFlight).toBe(2);
		expect(m.mutations.queued).toBe(3);

		// Release the gate — all 5 should drain.
		release();
		await Promise.all(calls);

		m = engine.metrics();
		expect(m.mutations.inFlight).toBe(0);
		expect(m.mutations.queued).toBe(0);
		expect(m.mutations.completed).toBe(5);
	});

	test('queueLimit overflow throws MutationQueueOverflowError immediately', async () => {
		const { engine } = wire('e1', 1, 2);
		const { release } = makeGated(engine);

		// Slot taken by first call.
		const first = engine.runMutation('gated', { id: 1, tag: 'x' }, {});
		// Queue capacity 2 — these queue.
		const second = engine.runMutation('gated', { id: 2, tag: 'x' }, {});
		const third = engine.runMutation('gated', { id: 3, tag: 'x' }, {});
		// Beyond cap — should throw.
		await expect(
			engine.runMutation('gated', { id: 4, tag: 'x' }, {})
		).rejects.toThrow(MutationQueueOverflowError);

		release();
		await Promise.all([first, second, third]);
	});

	test('no concurrency setting → semaphore is a no-op', async () => {
		const { engine } = wire('e1');
		freeMutation(engine);
		await Promise.all(
			[1, 2, 3, 4, 5].map((id) =>
				engine.runMutation('free', { id, tag: 'x' }, {})
			)
		);
		const m = engine.metrics();
		expect(m.mutations.completed).toBe(5);
		expect(m.mutations.queued).toBe(0);
	});

	test('queued count returns to 0 even when a mutation throws', async () => {
		const { engine } = wire('e1', 1);
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		engine.registerMutation(
			defineMutation({
				handler: async () => {
					await gate;
					throw new Error('boom');
				},
				name: 'failsOnRelease'
			})
		);
		engine.registerMutation(
			defineMutation({
				handler: (_args, _ctx, actions) =>
					actions.insert('rows', { id: 99, tag: 'after' }),
				name: 'after'
			})
		);

		const first = engine.runMutation('failsOnRelease', {}, {});
		const queued = engine.runMutation('after', {}, {});

		await Promise.resolve();
		await Promise.resolve();
		expect(engine.metrics().mutations.queued).toBe(1);

		release();
		await expect(first).rejects.toThrow('boom');
		await queued;

		const m = engine.metrics();
		expect(m.mutations.queued).toBe(0);
		expect(m.mutations.inFlight).toBe(0);
		expect(m.mutations.failed).toBe(1);
		expect(m.mutations.completed).toBe(1);
	});

	test('FIFO order — queued mutations run in arrival order', async () => {
		const { engine } = wire('e1', 1);
		const order: number[] = [];
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		engine.registerMutation(
			defineMutation({
				handler: async (args: { id: number }) => {
					if (args.id === 0) {
						await gate;
					}
					order.push(args.id);
					return null;
				},
				name: 'tracked'
			})
		);

		const calls = [0, 1, 2, 3].map((id) =>
			engine.runMutation('tracked', { id }, {})
		);
		await Promise.resolve();
		await Promise.resolve();
		release();
		await Promise.all(calls);
		expect(order).toEqual([0, 1, 2, 3]);
	});

	test('runMutations batch counts as a single slot', async () => {
		const { engine } = wire('e1', 1);
		freeMutation(engine);
		// Hold the first slot with a gated single-mutation call.
		const { release } = makeGated(engine);
		const blocker = engine.runMutation('gated', { id: 0, tag: 'x' }, {});

		// A batch of 3 mutations through runMutations should queue as ONE slot.
		const batchPromise = engine.runMutations(
			[
				{ args: { id: 11, tag: 'a' }, name: 'free' },
				{ args: { id: 12, tag: 'b' }, name: 'free' },
				{ args: { id: 13, tag: 'c' }, name: 'free' }
			],
			{}
		);

		await Promise.resolve();
		await Promise.resolve();
		expect(engine.metrics().mutations.queued).toBe(1);

		release();
		await Promise.all([blocker, batchPromise]);
		expect(engine.metrics().mutations.queued).toBe(0);
		// gated + 3 from batch (batch counts as 1 for "completed" via mutationBatch).
		// Single-mutation completed = 1 (gated); batch tracks separately.
		expect(engine.metrics().mutations.inFlight).toBe(0);
	});

	test('unauthorized mutation does not consume a slot', async () => {
		const { engine } = wire('e1', 1);
		engine.registerMutation(
			defineMutation({
				authorize: () => false,
				handler: () => null,
				name: 'gated_auth'
			})
		);
		await expect(engine.runMutation('gated_auth', {}, {})).rejects.toThrow(
			/run mutation/
		);
		const m = engine.metrics();
		expect(m.mutations.inFlight).toBe(0);
		expect(m.mutations.queued).toBe(0);
	});
});
