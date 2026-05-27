/**
 * Engine outbound CDC stream — `engine.streamChanges({ since, signal })`.
 * Validates history catch-up, live tail, resume from `since`, AbortSignal
 * shutdown, gap detection (MissedChangesError), and slow-consumer
 * overflow detection (CdcConsumerSlowError).
 */

import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import {
	CdcConsumerSlowError,
	createSyncEngine,
	MissedChangesError,
	type LoggedChange
} from '../src/engine/syncEngine';

type Item = { id: number; n: number };

const setup = (changeLogSize?: number) => {
	const engine = createSyncEngine({ changeLogSize });
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
			handler: async (args: { id: number; n: number }, _ctx, actions) => {
				await actions.change('items', {
					op: 'insert',
					row: { id: args.id, n: args.n }
				});
			}
		})
	);
	engine.registerMutation(
		defineMutation({
			name: 'addMany',
			handler: async (
				args: Array<{ id: number; n: number }>,
				_ctx,
				actions
			) => {
				for (const row of args) {
					await actions.change('items', { op: 'insert', row });
				}
			}
		})
	);
	return engine;
};

const collectN = async (
	iterable: AsyncIterable<LoggedChange>,
	n: number,
	timeoutMs = 2000
): Promise<LoggedChange[]> => {
	const collected: LoggedChange[] = [];
	const deadline = Date.now() + timeoutMs;
	const iterator = iterable[Symbol.asyncIterator]();
	while (collected.length < n) {
		const remaining = deadline - Date.now();
		if (remaining <= 0) {
			throw new Error(`collectN timed out at ${collected.length}/${n}`);
		}
		const next = await Promise.race([
			iterator.next(),
			new Promise<IteratorResult<LoggedChange>>((_, reject) =>
				setTimeout(() => reject(new Error('next() timeout')), remaining)
			)
		]);
		if (next.done) break;
		collected.push(next.value);
	}
	return collected;
};

describe('streamChanges', () => {
	test('phase 1: yields historical entries with version > since', async () => {
		const engine = setup();
		await engine.runMutation('add', { id: 1, n: 1 }, {});
		await engine.runMutation('add', { id: 2, n: 2 }, {});
		await engine.runMutation('add', { id: 3, n: 3 }, {});

		const controller = new AbortController();
		const stream = engine.streamChanges({
			since: 0,
			signal: controller.signal
		});
		const collected = await collectN(stream, 3);
		controller.abort();

		expect(collected).toHaveLength(3);
		expect(collected.map((entry) => entry.change.row)).toEqual([
			{ id: 1, n: 1 },
			{ id: 2, n: 2 },
			{ id: 3, n: 3 }
		]);
	});

	test('phase 1 + 2: tail picks up new commits after history drains', async () => {
		const engine = setup();
		await engine.runMutation('add', { id: 1, n: 1 }, {});

		const controller = new AbortController();
		const stream = engine.streamChanges({
			since: 0,
			signal: controller.signal
		});
		const collected: LoggedChange[] = [];
		const collector = (async () => {
			for await (const entry of stream) {
				collected.push(entry);
				if (collected.length === 3) {
					controller.abort();
					break;
				}
			}
		})();

		// Give phase 1 a tick to drain.
		await new Promise((resolve) => setTimeout(resolve, 10));
		await engine.runMutation('add', { id: 2, n: 2 }, {});
		await engine.runMutation('add', { id: 3, n: 3 }, {});
		await collector;

		expect(collected.map((entry) => entry.change.row)).toEqual([
			{ id: 1, n: 1 },
			{ id: 2, n: 2 },
			{ id: 3, n: 3 }
		]);
	});

	test('resume: since=X skips entries with version <= X', async () => {
		const engine = setup();
		await engine.runMutation('add', { id: 1, n: 1 }, {}); // v1
		await engine.runMutation('add', { id: 2, n: 2 }, {}); // v2
		await engine.runMutation('add', { id: 3, n: 3 }, {}); // v3

		const controller = new AbortController();
		const stream = engine.streamChanges({
			since: 2,
			signal: controller.signal
		});
		const collected = await collectN(stream, 1);
		controller.abort();
		expect(collected).toHaveLength(1);
		expect(collected[0]!.version).toBe(3);
	});

	test('AbortSignal cleanly stops the iterator', async () => {
		const engine = setup();
		await engine.runMutation('add', { id: 1, n: 1 }, {});
		const controller = new AbortController();
		const stream = engine.streamChanges({
			signal: controller.signal
		});
		const iterator = stream[Symbol.asyncIterator]();
		const first = await iterator.next();
		expect(first.done).toBe(false);
		// Abort while parked on phase 2.
		setTimeout(() => controller.abort(), 5);
		const second = await iterator.next();
		expect(second.done).toBe(true);
	});

	test('batch mutation: every row in the batch shares one version and arrives in order', async () => {
		const engine = setup();
		const rows = [
			{ id: 10, n: 1 },
			{ id: 20, n: 2 },
			{ id: 30, n: 3 }
		];
		await engine.runMutation('addMany', rows, {});

		const controller = new AbortController();
		const stream = engine.streamChanges({
			signal: controller.signal
		});
		const collected = await collectN(stream, 3);
		controller.abort();

		expect(collected.map((entry) => entry.version)).toEqual([1, 1, 1]);
		expect(collected.map((entry) => entry.change.row)).toEqual(rows);
	});

	test('MissedChangesError: since older than the oldest retained entry rejects the iterator', async () => {
		const engine = setup(3); // changeLogSize = 3
		// Commit 5 entries; the bounded log keeps only the last 3 (versions 3..5).
		for (let i = 1; i <= 5; i++) {
			await engine.runMutation('add', { id: i, n: i }, {});
		}

		const stream = engine.streamChanges({ since: 1 });
		const iterator = stream[Symbol.asyncIterator]();
		let caught: unknown;
		try {
			await iterator.next();
		} catch (error) {
			caught = error;
		}
		expect(caught).toBeInstanceOf(MissedChangesError);
		const err = caught as MissedChangesError;
		expect(err.requestedSince).toBe(1);
		expect(err.availableSince).toBeGreaterThan(2);
	});

	test('no gap if since=0 even when the log has rotated past', async () => {
		// since=0 is "give me what's in the log, then tail" — no gap check.
		const engine = setup(3);
		for (let i = 1; i <= 5; i++) {
			await engine.runMutation('add', { id: i, n: i }, {});
		}
		const controller = new AbortController();
		const stream = engine.streamChanges({
			since: 0,
			signal: controller.signal
		});
		const collected = await collectN(stream, 3);
		controller.abort();
		expect(collected).toHaveLength(3);
		// Bounded log retained the last 3.
		expect(collected.map((entry) => entry.version)).toEqual([3, 4, 5]);
	});

	test('CdcConsumerSlowError fires when the in-flight buffer overflows', async () => {
		const engine = setup();
		// Tiny maxBuffer; the consumer never iterates, so phase 2 overflows.
		const controller = new AbortController();
		const stream = engine.streamChanges({
			signal: controller.signal,
			maxBuffer: 5
		});

		// Drain phase 1 (empty) before flooding phase 2.
		const iterator = stream[Symbol.asyncIterator]();

		// Kick off the iterator so it gets to phase 2.
		const firstNext = iterator.next();
		// Now flood — exceed the buffer.
		for (let i = 1; i <= 50; i++) {
			await engine.runMutation('add', { id: i, n: i }, {});
		}

		// The buffer accepted up to maxBuffer entries; the next overflow flips
		// the flag. The iterator drains its buffer, then throws.
		let caught: unknown;
		try {
			// Drain whatever fits in buffer first.
			let result = await firstNext;
			while (!result.done) {
				result = await iterator.next();
			}
		} catch (error) {
			caught = error;
		}
		controller.abort();
		expect(caught).toBeInstanceOf(CdcConsumerSlowError);
	});

	test('multiple concurrent streams each get every entry independently', async () => {
		const engine = setup();
		await engine.runMutation('add', { id: 1, n: 1 }, {});

		const controllerA = new AbortController();
		const controllerB = new AbortController();
		const a = engine.streamChanges({ signal: controllerA.signal });
		const b = engine.streamChanges({ signal: controllerB.signal });
		const collectedA: number[] = [];
		const collectedB: number[] = [];

		const runner = async (
			stream: AsyncIterable<LoggedChange>,
			into: number[],
			until: number
		) => {
			for await (const entry of stream) {
				into.push(entry.version);
				if (into.length >= until) break;
			}
		};
		const taskA = runner(a, collectedA, 3);
		const taskB = runner(b, collectedB, 3);

		await new Promise((resolve) => setTimeout(resolve, 10));
		await engine.runMutation('add', { id: 2, n: 2 }, {});
		await engine.runMutation('add', { id: 3, n: 3 }, {});

		await Promise.all([taskA, taskB]);
		controllerA.abort();
		controllerB.abort();

		expect(collectedA).toEqual([1, 2, 3]);
		expect(collectedB).toEqual([1, 2, 3]);
	});
});
