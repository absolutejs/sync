/**
 * Mutation retry — `defineMutation`'s opt-in `retry` policy. Validates the
 * loop wired around the handler call in `runMutation` (syncEngine.ts).
 *
 * `await expect(p).rejects.toThrow(...)` is replaced by a `rejection()`
 * helper everywhere here — same Bun 1.3 test-runner hang we hit in
 * isolated-jsc (oven-sh/bun#31462). For these tests, where mutation
 * handlers throw synchronously, the matcher works fine, but staying
 * consistent across the suite makes the workaround pattern obvious.
 */

import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import {
	exponentialBackoff,
	isSerializationFailure,
	RetriesExhaustedError
} from '../src/engine/retry';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { EngineActivity } from '../src/engine/devtools';

type Item = { id: number; n: number };

const items = (name: string) =>
	defineCollection<Item>({
		name,
		key: (row) => row.id,
		hydrate: () => [],
		match: () => true
	});

const rejection = async (promise: Promise<unknown>): Promise<unknown> => {
	try {
		await promise;
	} catch (error) {
		return error;
	}
	throw new Error('promise did not reject');
};

/** Fabricate a "Postgres serialization failure" error. The default
 * `isRetryable` is {@link isSerializationFailure}, which keys on
 * `error.code === '40001'`. */
const pgSerializationFailure = () =>
	Object.assign(new Error('serialization_failure'), { code: '40001' });

describe('mutation retry', () => {
	test('without `retry`: a thrown error rejects on the first attempt', async () => {
		const engine = createSyncEngine();
		engine.register(items('items'));
		let attempts = 0;
		engine.registerMutation(
			defineMutation({
				name: 'fails',
				handler: () => {
					attempts++;
					throw pgSerializationFailure();
				}
			})
		);

		const err = (await rejection(
			engine.runMutation('fails', {}, {})
		)) as Error & { code?: string };
		expect(attempts).toBe(1);
		expect(err.code).toBe('40001');
		expect(err).not.toBeInstanceOf(RetriesExhaustedError);
	});

	test('with `retry`: a serialization failure is retried up to maxAttempts', async () => {
		const engine = createSyncEngine();
		engine.register(items('items'));
		let attempts = 0;
		engine.registerMutation(
			defineMutation({
				name: 'flaky',
				retry: {
					maxAttempts: 4,
					backoff: () => 0
				},
				handler: () => {
					attempts++;
					throw pgSerializationFailure();
				}
			})
		);

		const err = (await rejection(
			engine.runMutation('flaky', {}, {})
		)) as RetriesExhaustedError;
		expect(attempts).toBe(4);
		expect(err).toBeInstanceOf(RetriesExhaustedError);
		expect(err.attempts).toBe(4);
		// Underlying error is preserved on `cause`.
		expect((err.cause as { code?: string }).code).toBe('40001');
	});

	test('with `retry`: succeeds on a later attempt and returns normally', async () => {
		const engine = createSyncEngine();
		engine.register(items('items'));
		let attempts = 0;
		engine.registerMutation(
			defineMutation({
				name: 'eventually',
				retry: {
					maxAttempts: 5,
					backoff: () => 0
				},
				handler: () => {
					attempts++;
					if (attempts < 3) throw pgSerializationFailure();
					return 'committed';
				}
			})
		);

		const result = await engine.runMutation('eventually', {}, {});
		expect(attempts).toBe(3);
		expect(result).toBe('committed');
	});

	test('with `retry`: non-retryable errors throw immediately (no retry)', async () => {
		const engine = createSyncEngine();
		engine.register(items('items'));
		let attempts = 0;
		engine.registerMutation(
			defineMutation({
				name: 'businessError',
				retry: {
					maxAttempts: 5,
					backoff: () => 0
				},
				handler: () => {
					attempts++;
					// Plain Error, no PG code — not retryable by default.
					throw new Error('insufficient_funds');
				}
			})
		);

		const err = (await rejection(
			engine.runMutation('businessError', {}, {})
		)) as Error;
		expect(attempts).toBe(1);
		expect(err.message).toBe('insufficient_funds');
		expect(err).not.toBeInstanceOf(RetriesExhaustedError);
	});

	test('custom isRetryable: caller can retry on app-defined conditions', async () => {
		const engine = createSyncEngine();
		engine.register(items('items'));
		let attempts = 0;
		engine.registerMutation(
			defineMutation({
				name: 'customRetry',
				retry: {
					maxAttempts: 4,
					backoff: () => 0,
					isRetryable: (error) =>
						error instanceof Error && error.message === 'try again'
				},
				handler: () => {
					attempts++;
					if (attempts < 3) throw new Error('try again');
					return attempts;
				}
			})
		);

		const result = await engine.runMutation('customRetry', {}, {});
		expect(result).toBe(3);
	});

	test('maxElapsedMs caps total time even when maxAttempts would allow more', async () => {
		const engine = createSyncEngine();
		engine.register(items('items'));
		let attempts = 0;
		engine.registerMutation(
			defineMutation({
				name: 'slow',
				retry: {
					maxAttempts: 100,
					// 50 ms per backoff
					backoff: () => 50,
					maxElapsedMs: 120
				},
				handler: () => {
					attempts++;
					throw pgSerializationFailure();
				}
			})
		);

		const startedAt = Date.now();
		const err = (await rejection(
			engine.runMutation('slow', {}, {})
		)) as RetriesExhaustedError;
		const elapsed = Date.now() - startedAt;

		expect(err).toBeInstanceOf(RetriesExhaustedError);
		// 120 ms budget / 50 ms backoff ≈ 2-3 attempts; should not hit 100.
		expect(attempts).toBeLessThan(10);
		// Generous upper bound — should be well under 1 second.
		expect(elapsed).toBeLessThan(1000);
	});

	test('emits `mutationRetry` activity events between attempts', async () => {
		const engine = createSyncEngine();
		engine.register(items('items'));
		let attempts = 0;
		engine.registerMutation(
			defineMutation({
				name: 'observed',
				retry: { maxAttempts: 3, backoff: () => 0 },
				handler: () => {
					attempts++;
					if (attempts < 3) throw pgSerializationFailure();
					return 'ok';
				}
			})
		);

		const events: EngineActivity[] = [];
		engine.onActivity((event) => events.push(event));

		await engine.runMutation('observed', {}, {});
		const retries = events.filter(
			(event) => event.type === 'mutationRetry'
		);
		expect(retries).toHaveLength(2);
		expect(retries[0]).toMatchObject({
			type: 'mutationRetry',
			name: 'observed',
			attempt: 1,
			errorName: 'Error',
			errorMessage: 'serialization_failure'
		});
		expect(retries[1]).toMatchObject({
			type: 'mutationRetry',
			attempt: 2
		});
		const final = events.find((event) => event.type === 'mutation');
		expect(final).toMatchObject({ name: 'observed', status: 'ok' });
	});

	test('a successful retry still emits its writes (the buffered changes are fresh each attempt)', async () => {
		const engine = createSyncEngine();
		engine.register(items('items'));
		let attempts = 0;
		engine.registerMutation(
			defineMutation({
				name: 'addOneAfterRetry',
				retry: { maxAttempts: 3, backoff: () => 0 },
				handler: async (_args, _ctx, actions) => {
					attempts++;
					// Always emit a change; the throw on attempt 1 must
					// discard it. If buffered leaks across attempts the
					// subscriber would see duplicates.
					await actions.change('items', {
						op: 'insert',
						row: { id: 7, n: 1 }
					});
					if (attempts === 1) throw pgSerializationFailure();
				}
			})
		);

		const seenIds: number[] = [];
		await engine.subscribe<Item>({
			collection: 'items',
			params: undefined,
			ctx: {},
			onDiff: (diff) => {
				for (const row of diff.added) seenIds.push(row.id);
			}
		});

		await engine.runMutation('addOneAfterRetry', {}, {});
		expect(seenIds).toEqual([7]);
	});

	test('exponentialBackoff produces increasing-with-jitter delays', () => {
		const fn = exponentialBackoff({
			baseMs: 10,
			factor: 2,
			maxMs: 100,
			jitter: 0
		});
		expect(fn(1)).toBe(10);
		expect(fn(2)).toBe(20);
		expect(fn(3)).toBe(40);
		expect(fn(10)).toBe(100); // capped at maxMs
	});

	test('isSerializationFailure matches PG 40001/40P01 codes', () => {
		expect(
			isSerializationFailure(
				Object.assign(new Error(), { code: '40001' })
			)
		).toBe(true);
		expect(
			isSerializationFailure(
				Object.assign(new Error(), { code: '40P01' })
			)
		).toBe(true);
		expect(
			isSerializationFailure(
				Object.assign(new Error(), { code: '23505' })
			)
		).toBe(false);
		expect(isSerializationFailure(new Error('plain'))).toBe(false);
		// Wrapped on .cause
		expect(
			isSerializationFailure(
				Object.assign(new Error('wrap'), {
					cause: Object.assign(new Error(), { code: '40001' })
				})
			)
		).toBe(true);
	});
});
