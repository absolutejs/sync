import { describe, expect, test } from 'bun:test';
import {
	ConnectionBrokerDrainedError,
	createConnectionBroker,
	LeaseTimeoutError
} from '../src/connectionBroker';

type FakeConn = { id: number };

// Helper: an instrumented connection factory — plain objects with ids,
// recording every create/destroy so tests can assert pool behavior.
const makeFactory = () => {
	let nextId = 0;
	const created: number[] = [];
	const destroyed: number[] = [];
	return {
		create: (): FakeConn => {
			const conn = { id: nextId };
			nextId += 1;
			created.push(conn.id);
			return conn;
		},
		created,
		destroy: (conn: FakeConn) => {
			destroyed.push(conn.id);
		},
		destroyed
	};
};

// Let queued grants and floating destroys settle.
const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

describe('createConnectionBroker', () => {
	test('rejects a non-positive maxTotal', () => {
		const { create } = makeFactory();
		expect(() => createConnectionBroker({ create, maxTotal: 0 })).toThrow(
			RangeError
		);
	});

	test('caps in-use at maxTotal, queues FIFO, hands off on release', async () => {
		const factory = makeFactory();
		const broker = createConnectionBroker({
			create: factory.create,
			destroy: factory.destroy,
			maxTotal: 2
		});
		const first = await broker.lease('a');
		const second = await broker.lease('a');
		expect(broker.metrics().inUse).toBe(2);

		const order: string[] = [];
		const third = broker
			.lease('a')
			.then((lease) => (order.push('third'), lease));
		const fourth = broker
			.lease('a')
			.then((lease) => (order.push('fourth'), lease));
		await tick();
		expect(broker.metrics().queued).toBe(2);
		expect(factory.created).toHaveLength(2);

		// Release hands the SAME connection to the next queued waiter
		// immediately — no destroy/create churn at the cap.
		first.release();
		const thirdLease = await third;
		expect(thirdLease.conn).toBe(first.conn);
		expect(order).toEqual(['third']);

		second.release();
		const fourthLease = await fourth;
		expect(fourthLease.conn).toBe(second.conn);
		expect(order).toEqual(['third', 'fourth']);
		expect(factory.created).toHaveLength(2);
		await broker.dispose();
	});

	test('caps per tenant independently of the global cap', async () => {
		const factory = makeFactory();
		const broker = createConnectionBroker({
			create: factory.create,
			maxPerTenant: 1,
			maxTotal: 10
		});
		const first = await broker.lease('acme');
		let acmeSecondGranted = false;
		const acmeSecond = broker.lease('acme').then((lease) => {
			acmeSecondGranted = true;
			return lease;
		});
		await tick();
		// acme is at its budget, but the shard has plenty of room — a
		// different tenant leases straight through.
		expect(acmeSecondGranted).toBe(false);
		expect(broker.metrics().queued).toBe(1);
		const other = await broker.lease('globex');
		expect(other.conn.id).toBeDefined();

		first.release();
		const second = await acmeSecond;
		expect(acmeSecondGranted).toBe(true);
		expect(broker.metrics().byTenant).toEqual({ acme: 1, globex: 1 });
		second.release();
		other.release();
		await broker.dispose();
	});

	test('acquireTimeoutMs rejects with LeaseTimeoutError and dequeues the waiter', async () => {
		const factory = makeFactory();
		const broker = createConnectionBroker({
			acquireTimeoutMs: 20,
			create: factory.create,
			maxTotal: 1
		});
		const held = await broker.lease('a');
		const waiting = broker.lease('b');
		const error = await waiting.catch(
			(caught: LeaseTimeoutError) => caught
		);
		expect(error).toBeInstanceOf(LeaseTimeoutError);
		expect((error as LeaseTimeoutError).tenant).toBe('b');
		expect((error as LeaseTimeoutError).timeoutMs).toBe(20);
		// The timed-out waiter left the queue — releasing must not hand
		// it a connection or corrupt counts.
		expect(broker.metrics().queued).toBe(0);
		expect(broker.metrics().cumulative.timeouts).toBe(1);
		held.release();
		await tick();
		expect(broker.metrics().inUse).toBe(0);
		await broker.dispose();
	});

	test('withLease releases the connection even when fn throws', async () => {
		const factory = makeFactory();
		const broker = createConnectionBroker({
			create: factory.create,
			maxTotal: 1
		});
		await expect(
			broker.withLease('a', () => {
				throw new Error('query failed');
			})
		).rejects.toThrow('query failed');
		await tick();
		expect(broker.metrics().inUse).toBe(0);
		// The slot freed: the next lease resolves immediately.
		const reused = await broker.withLease('a', (conn) => conn.id);
		expect(reused).toBe(0);
		await broker.dispose();
	});

	test('reuses idle connections LIFO for cache warmth', async () => {
		const factory = makeFactory();
		const broker = createConnectionBroker({
			create: factory.create,
			maxTotal: 3
		});
		const leases = [
			await broker.lease('a'),
			await broker.lease('a'),
			await broker.lease('a')
		];
		for (const lease of leases) lease.release();
		// Released in id order 0, 1, 2 — the LAST released comes back first.
		const next = await broker.lease('a');
		expect(next.conn.id).toBe(2);
		next.release();
		await broker.dispose();
	});

	test('idleReleaseMs destroys stale idle connections (injected clock)', async () => {
		const factory = makeFactory();
		let clock = 0;
		const broker = createConnectionBroker({
			create: factory.create,
			destroy: factory.destroy,
			idleReleaseMs: 1_000,
			maxTotal: 2,
			now: () => clock
		});
		const stale = await broker.lease('a');
		stale.release();
		expect(broker.metrics().idle).toBe(1);

		// Advance the injected clock past the idle window; the next lease
		// sweeps lazily, destroys the stale connection, and creates fresh.
		clock = 1_001;
		const fresh = await broker.lease('a');
		expect(factory.destroyed).toEqual([0]);
		expect(fresh.conn.id).toBe(1);
		expect(broker.metrics().cumulative.destroyed).toBe(1);
		fresh.release();
		await broker.dispose();
	});

	test('validate=false destroys the pooled connection and creates fresh', async () => {
		const factory = makeFactory();
		const broker = createConnectionBroker({
			create: factory.create,
			destroy: factory.destroy,
			maxTotal: 1,
			validate: (conn) => conn.id !== 0
		});
		const first = await broker.lease('a');
		expect(first.conn.id).toBe(0);
		first.release();

		// Reuse health-checks the pooled connection: id 0 fails, gets
		// destroyed, and the lease is served by a fresh create.
		const second = await broker.lease('a');
		expect(second.conn.id).toBe(1);
		expect(factory.destroyed).toEqual([0]);
		expect(broker.metrics().cumulative.validationFailures).toBe(1);
		second.release();
		await broker.dispose();
	});

	test('release is idempotent — a double release never corrupts counts', async () => {
		const factory = makeFactory();
		const broker = createConnectionBroker({
			create: factory.create,
			maxTotal: 2
		});
		const lease = await broker.lease('a');
		lease.release();
		lease.release();
		const snapshot = broker.metrics();
		expect(snapshot.inUse).toBe(0);
		expect(snapshot.idle).toBe(1);
		expect(snapshot.byTenant).toEqual({});
		expect(snapshot.cumulative.releases).toBe(1);
		// The pool still behaves: two fresh leases fit under maxTotal and
		// only one of them reuses the single idle connection.
		const first = await broker.lease('a');
		const second = await broker.lease('a');
		expect(broker.metrics().inUse).toBe(2);
		expect(factory.created).toHaveLength(2);
		first.release();
		second.release();
		await broker.dispose();
	});

	test('drain stops new leases, serves the queue through, resolves at zero in-use', async () => {
		const factory = makeFactory();
		const broker = createConnectionBroker({
			create: factory.create,
			destroy: factory.destroy,
			maxTotal: 1
		});
		const held = await broker.lease('a');
		const queued = broker.lease('b');
		await tick();

		let drainResolved = false;
		const draining = broker.drain().then(() => {
			drainResolved = true;
		});
		await expect(broker.lease('c')).rejects.toBeInstanceOf(
			ConnectionBrokerDrainedError
		);
		await tick();
		expect(drainResolved).toBe(false);

		// The waiter queued BEFORE drain still gets served — drain is
		// graceful, it only walls off new leases.
		held.release();
		const queuedLease = await queued;
		await tick();
		expect(drainResolved).toBe(false);
		queuedLease.release();
		await draining;
		expect(drainResolved).toBe(true);

		// dispose() destroys the idle pool.
		expect(broker.metrics().idle).toBe(1);
		await broker.dispose();
		expect(broker.metrics().idle).toBe(0);
		expect(factory.destroyed).toEqual([0]);
	});

	test('dispose rejects queued waiters and destroys idle connections', async () => {
		const factory = makeFactory();
		const broker = createConnectionBroker({
			create: factory.create,
			destroy: factory.destroy,
			maxTotal: 1
		});
		const held = await broker.lease('a');
		const queued = broker.lease('b');
		await tick();
		await broker.dispose();
		await expect(queued).rejects.toBeInstanceOf(
			ConnectionBrokerDrainedError
		);
		// A connection still leased out at dispose() is destroyed on its
		// release instead of returning to the pool.
		held.release();
		await tick();
		expect(factory.destroyed).toEqual([0]);
		expect(broker.metrics().idle).toBe(0);
	});

	test('metrics returns the operator-shaped snapshot with cumulative counters', async () => {
		const factory = makeFactory();
		const broker = createConnectionBroker({
			create: factory.create,
			destroy: factory.destroy,
			maxTotal: 2
		});
		const first = await broker.lease('acme');
		const second = await broker.lease('globex');
		const queued = broker.lease('acme');
		await tick();

		const snapshot = broker.metrics();
		expect(snapshot.inUse).toBe(2);
		expect(snapshot.idle).toBe(0);
		expect(snapshot.queued).toBe(1);
		expect(snapshot.byTenant).toEqual({ acme: 1, globex: 1 });
		expect(snapshot.cumulative).toEqual({
			created: 2,
			destroyed: 0,
			leases: 2,
			releases: 0,
			timeouts: 0,
			validationFailures: 0
		});

		first.release();
		(await queued).release();
		second.release();
		const after = broker.metrics();
		expect(after.cumulative.leases).toBe(3);
		expect(after.cumulative.releases).toBe(3);
		expect(after.idle).toBe(2);
		await broker.dispose();
		expect(broker.metrics().cumulative.destroyed).toBe(2);
	});

	test('storm: 50 concurrent leases over maxTotal 5 — never more than 5 in flight, all served', async () => {
		const factory = makeFactory();
		const broker = createConnectionBroker({
			create: factory.create,
			maxTotal: 5
		});
		let active = 0;
		let peakActive = 0;
		const results = await Promise.all(
			Array.from({ length: 50 }, (_, index) =>
				broker.withLease('a', async (conn) => {
					active += 1;
					peakActive = Math.max(peakActive, active);
					expect(active).toBeLessThanOrEqual(5);
					await tick();
					active -= 1;
					return { held: conn.id, index };
				})
			)
		);
		expect(results).toHaveLength(50);
		expect(peakActive).toBe(5);
		// The whole storm rode 5 physical connections.
		expect(factory.created).toHaveLength(5);
		const snapshot = broker.metrics();
		expect(snapshot.inUse).toBe(0);
		expect(snapshot.cumulative.leases).toBe(50);
		expect(snapshot.cumulative.releases).toBe(50);
		await broker.dispose();
	});
});
