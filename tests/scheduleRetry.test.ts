/**
 * Schedule retry — `defineSchedule`'s opt-in `retry` policy. Mirrors the
 * mutation retry path: a retryable thrown error retries up to
 * `maxAttempts`, with the configured backoff and elapsed budget. Designed
 * to be symmetric with `defineMutation.retry` so consumers who learn one
 * understand the other.
 */

import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineReactiveQuery } from '../src/engine/reactive';
import { defineSchedule } from '../src/engine/schedule';
import { RetriesExhaustedError } from '../src/engine/retry';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { EngineActivity } from '../src/engine/devtools';

type Tick = { id: number; n: number };

const rejection = async (promise: Promise<unknown>): Promise<unknown> => {
	try {
		await promise;
	} catch (error) {
		return error;
	}
	throw new Error('promise did not reject');
};

const pgSerializationFailure = () =>
	Object.assign(new Error('serialization_failure'), { code: '40001' });

const makeEngine = () => {
	const ticks = new Map<number, Tick>();
	const engine = createSyncEngine();
	engine.registerReader('ticks', { all: () => [...ticks.values()] });
	engine.registerWriter<Tick>('ticks', {
		insert: (data: Tick) => {
			ticks.set(data.id, data);
			return data;
		},
		update: (data: Tick) => {
			ticks.set(data.id, data);
			return data;
		},
		delete: (row: { id: number }) => {
			ticks.delete(row.id);
		}
	});
	engine.registerReactive(
		defineReactiveQuery<Tick>({
			name: 'ticks',
			key: (tick) => tick.id,
			run: ({ db }) => db.all<Tick>('ticks')
		})
	);
	return { engine, ticks };
};

describe('schedule retry', () => {
	test('without `retry`: a thrown error rejects on the first attempt', async () => {
		const { engine } = makeEngine();
		let attempts = 0;
		engine.registerSchedule(
			defineSchedule({
				name: 'flaky',
				pattern: '* * * * *',
				run: () => {
					attempts++;
					throw pgSerializationFailure();
				}
			})
		);
		const error = (await rejection(engine.runSchedule('flaky'))) as Error;
		expect(error.message).toBe('serialization_failure');
		expect(attempts).toBe(1);
	});

	test('with `retry`: a serialization failure is retried up to maxAttempts', async () => {
		const { engine } = makeEngine();
		let attempts = 0;
		engine.registerSchedule(
			defineSchedule({
				name: 'flaky',
				pattern: '* * * * *',
				retry: { maxAttempts: 3, backoff: () => 0 },
				run: () => {
					attempts++;
					throw pgSerializationFailure();
				}
			})
		);
		const error = (await rejection(
			engine.runSchedule('flaky')
		)) as RetriesExhaustedError;
		expect(error).toBeInstanceOf(RetriesExhaustedError);
		expect(error.attempts).toBe(3);
		expect(attempts).toBe(3);
	});

	test('with `retry`: succeeds on a later attempt and commits normally', async () => {
		const { engine, ticks } = makeEngine();
		let attempts = 0;
		engine.registerSchedule(
			defineSchedule({
				name: 'flaky',
				pattern: '* * * * *',
				retry: { maxAttempts: 5, backoff: () => 0 },
				run: ({ actions }) => {
					attempts++;
					if (attempts < 3) throw pgSerializationFailure();
					actions.insert('ticks', { id: 1, n: attempts });
				}
			})
		);
		await engine.runSchedule('flaky');
		expect(attempts).toBe(3);
		expect(ticks.get(1)).toEqual({ id: 1, n: 3 });
	});

	test('non-retryable errors throw immediately (no retry)', async () => {
		const { engine } = makeEngine();
		let attempts = 0;
		engine.registerSchedule(
			defineSchedule({
				name: 'broken',
				pattern: '* * * * *',
				retry: { maxAttempts: 5, backoff: () => 0 },
				run: () => {
					attempts++;
					throw new Error('not retryable');
				}
			})
		);
		const error = (await rejection(engine.runSchedule('broken'))) as Error;
		expect(error.message).toBe('not retryable');
		expect(attempts).toBe(1);
	});

	test('emits `scheduleRetry` and `schedule` activity events', async () => {
		const { engine } = makeEngine();
		let attempts = 0;
		engine.registerSchedule(
			defineSchedule({
				name: 'flaky',
				pattern: '* * * * *',
				retry: { maxAttempts: 3, backoff: () => 0 },
				run: () => {
					attempts++;
					if (attempts < 2) throw pgSerializationFailure();
				}
			})
		);

		const events: EngineActivity[] = [];
		engine.onActivity((event) => events.push(event));
		await engine.runSchedule('flaky');

		const retries = events.filter((e) => e.type === 'scheduleRetry');
		const completes = events.filter((e) => e.type === 'schedule');
		expect(retries.length).toBe(1);
		expect(completes.length).toBe(1);
		expect(completes[0]).toMatchObject({
			type: 'schedule',
			name: 'flaky',
			status: 'ok'
		});
	});

	test('a successful retry emits its writes fresh — buffered changes do NOT leak across attempts', async () => {
		const engine = createSyncEngine();
		engine.register(
			defineCollection<Tick>({
				name: 'ticks',
				key: (row) => row.id,
				hydrate: () => [],
				match: () => true
			})
		);
		let attempts = 0;
		engine.registerSchedule(
			defineSchedule({
				name: 'addOneAfterRetry',
				pattern: '* * * * *',
				retry: { maxAttempts: 3, backoff: () => 0 },
				run: async ({ actions }) => {
					attempts++;
					// Always buffer a change; the throw on attempt 1 must
					// discard it. If buffered leaks across attempts a
					// subscriber would see duplicates.
					await actions.change('ticks', {
						op: 'insert',
						row: { id: 7, n: 1 }
					});
					if (attempts === 1) throw pgSerializationFailure();
				}
			})
		);

		const seenIds: number[] = [];
		await engine.subscribe<Tick>({
			collection: 'ticks',
			params: undefined,
			ctx: {},
			onDiff: (diff) => {
				for (const row of diff.added) seenIds.push(row.id);
			}
		});

		await engine.runSchedule('addOneAfterRetry');
		expect(seenIds).toEqual([7]);
	});
});
