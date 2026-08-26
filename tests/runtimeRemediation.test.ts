import { afterEach, describe, expect, test } from 'bun:test';
import type { LocalMutationRecord } from '../src/client/localStore';
import {
	discardSyncRuntimeDeadLetter,
	inspectSyncRuntime,
	rebaseSyncRuntimeDeadLetter,
	registerSyncRuntimeClient,
	retrySyncRuntimeDeadLetter,
	type SyncRuntimeClient
} from '../src/client/runtimeTransport';

const removals: Array<() => void> = [];

afterEach(() => {
	for (const remove of removals.splice(0)) remove();
});

const deadLetter: LocalMutationRecord = {
	args: { secret: 'never expose this' },
	attempts: 2,
	createdAt: 10,
	deadLetteredAt: 20,
	inverse: [],
	name: 'tasks:update',
	operationId: 'install:operation',
	optimistic: [],
	rejection: {
		code: 'STALE',
		details: { current: { private: true } },
		kind: 'conflict',
		message: 'The task changed.'
	},
	state: 'dead-letter'
};

describe('framework-neutral runtime remediation', () => {
	test('aggregates redacted diagnostics and dispatches explicit actions', async () => {
		const calls: Array<{ action: string; value?: unknown }> = [];
		const client: SyncRuntimeClient = {
			discardDeadLetter: async (operationId) => {
				calls.push({ action: 'discard', value: operationId });
			},
			flush: async () => ({
				deadLetters: 1,
				pending: 2,
				timedOut: false
			}),
			listDeadLetters: async () => [deadLetter],
			rebaseDeadLetter: async (_operationId, args) => {
				calls.push({ action: 'rebase', value: args });
				return 'install:rebased';
			},
			reconnect: () => undefined,
			retryDeadLetter: async (operationId) => {
				calls.push({ action: 'retry', value: operationId });
			},
			status: () => ({
				automaticResolutions: 3,
				conflicts: 1,
				connection: 'offline',
				deadLetters: 1,
				lastError: 'offline',
				lastSuccessfulPullAt: 8,
				oldestDeadLetterAt: 20,
				oldestPendingAt: 5,
				pending: 2
			}),
			subscribeStatus: () => () => undefined
		};
		removals.push(registerSyncRuntimeClient(client));

		const inspection = await inspectSyncRuntime();
		expect(inspection).toEqual(
			expect.objectContaining({
				automaticResolutions: 3,
				clients: 1,
				conflicts: 1,
				pending: 2
			})
		);
		expect(inspection.deadLetters).toEqual([
			{
				attempts: 2,
				code: 'STALE',
				createdAt: 10,
				deadLetteredAt: 20,
				kind: 'conflict',
				message: 'The task changed.',
				name: 'tasks:update',
				operationId: 'install:operation'
			}
		]);
		expect(JSON.stringify(inspection)).not.toContain('never expose this');
		expect(JSON.stringify(inspection)).not.toContain('private');

		await retrySyncRuntimeDeadLetter('install:operation');
		await discardSyncRuntimeDeadLetter('install:operation');
		expect(
			await rebaseSyncRuntimeDeadLetter('install:operation', {
				version: 2
			})
		).toBe('install:rebased');
		expect(calls).toEqual([
			{ action: 'retry', value: 'install:operation' },
			{ action: 'discard', value: 'install:operation' },
			{ action: 'rebase', value: { version: 2 } }
		]);
	});
});
