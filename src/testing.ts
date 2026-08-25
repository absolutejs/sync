/**
 * `@absolutejs/sync/testing` — small helpers for testing sync engines and
 * sync packs. Importable from a focused subpath so pack repos don't pull
 * the whole sync barrel into their test surface area.
 *
 * What's here is intentionally tiny. Sync's own tests use
 * `createSyncEngine` directly, and that's still the right call for engine-
 * internal tests. This subpath is for *pack* tests and *consumer* tests
 * where the boilerplate of "boot an engine, expect an async throw, drive
 * a mutation with a labelled actor" repeats across every file.
 *
 * No new test framework is introduced — these are plain helpers you can
 * use with `bun:test`, `vitest`, or anything else.
 */

import {
	createSyncEngine,
	type SyncEngine,
	type SyncEngineOptions
} from './engine/syncEngine';
import type { LocalMutationRecord, SyncLocalStore } from './client/localStore';

/**
 * Construct a {@link SyncEngine} for tests. Today this is a documented
 * re-export of {@link createSyncEngine} — the wrapper exists so callers
 * can clearly signal "this engine is test-scoped" at the call site, and
 * so future test-mode defaults (e.g. a synchronous in-memory transaction
 * runner) can be added without churning every test.
 *
 * Pass options through if you need a real transaction runner, schemas,
 * permissions, etc. — same shape as {@link createSyncEngine}.
 */
export const createTestEngine = (options?: SyncEngineOptions): SyncEngine =>
	createSyncEngine(options);

/**
 * Await `work` and return the thrown value. Throws if `work` resolves —
 * use it inside tests that expect a specific rejection without relying
 * on `expect(...).rejects.toThrow(...)`, which has been flaky on Bun
 * 1.3.x (oven-sh/bun#31462). Equivalent to the `rejection()` helper
 * sync's own tests use.
 *
 * @example
 * const error = await expectRejection(() =>
 *   engine.runMutation('comments:edit', { commentId, body }, { userId: 'eve' }),
 * );
 * expect((error as Error).message).toMatch(/not author/);
 */
export const expectRejection = async (
	work: () => unknown
): Promise<unknown> => {
	try {
		await work();
	} catch (error) {
		return error;
	}
	throw new Error('expected `work` to reject but it resolved');
};

/**
 * Convenience: call `engine.runMutation` with an actor-id-shaped ctx.
 * Matches the standard pack convention (`getActorId: (ctx) => ctx.userId`).
 * Tests that don't need to vary other ctx fields can avoid the
 * `{ userId: 'alice' }` boilerplate.
 *
 * @example
 * await runAsActor(engine, 'alice', 'comments:create', {
 *   resourceId: 'doc-1',
 *   body: 'first',
 * });
 */
export const runAsActor = (
	engine: SyncEngine,
	actorId: string,
	mutation: string,
	args: unknown,
	extraCtx?: Record<string, unknown>
): Promise<unknown> =>
	engine.runMutation(mutation, args, { ...extraCtx, userId: actorId });

export type SyncLocalStoreConformanceOptions = {
	/** A fresh adapter instance. The harness may delete its test namespaces. */
	store: SyncLocalStore;
	/** Optional prefix when a test database is shared with other processes. */
	namespacePrefix?: string;
};

const conformanceMutation = (operationId: string): LocalMutationRecord => ({
	args: { title: operationId },
	attempts: 0,
	createdAt: 1,
	inverse: [],
	name: 'tasks:create',
	operationId,
	optimistic: []
});

/**
 * Runs the portable durability contract used by Sync's memory, IndexedDB,
 * and native SQLite adapters. An empty result means the adapter preserves
 * atomic rows/outbox state, rollback, readonly behavior, and principal
 * isolation.
 */
export const inspectSyncLocalStoreConformance = async ({
	store,
	namespacePrefix = `sync-conformance-${crypto.randomUUID()}`
}: SyncLocalStoreConformanceOptions): Promise<string[]> => {
	const issues: string[] = [];
	const accountA = `${namespacePrefix}:account-a`;
	const accountB = `${namespacePrefix}:account-b`;
	try {
		await store.transaction(accountA, 'readwrite', async (tx) => {
			await tx.setInstallationId('install-a');
			await tx.putCollection('tasks', {
				cursor: 'cursor-7',
				rows: [{ id: 1, title: 'offline' }],
				version: 7
			});
			await tx.putMutation(conformanceMutation('install-a:op-1'));
		});
		const committed = await store.transaction(
			accountA,
			'readonly',
			async (tx) => ({
				collection: await tx.getCollection('tasks'),
				installationId: await tx.getInstallationId(),
				mutations: await tx.listMutations()
			})
		);
		if (
			committed.installationId !== 'install-a' ||
			committed.collection?.cursor !== 'cursor-7' ||
			committed.collection?.version !== 7 ||
			committed.mutations[0]?.operationId !== 'install-a:op-1'
		)
			issues.push('atomic state did not round-trip');

		await store.transaction(accountA, 'readwrite', async (tx) => {
			const pending = await tx.getMutation('install-a:op-1');
			if (pending === undefined)
				throw new Error('missing conformance mutation');
			await tx.putMutation({
				...pending,
				deadLetteredAt: 2,
				lastError: 'stale row',
				rejection: {
					kind: 'conflict',
					message: 'stale row',
					code: 'STALE_ROW',
					details: { version: 8 }
				},
				state: 'dead-letter'
			});
		});
		const deadLetter = await store.transaction(accountA, 'readonly', (tx) =>
			tx.getMutation('install-a:op-1')
		);
		if (
			deadLetter?.state !== 'dead-letter' ||
			deadLetter.rejection?.kind !== 'conflict' ||
			deadLetter.rejection.code !== 'STALE_ROW'
		)
			issues.push('dead-letter metadata did not round-trip');

		try {
			await store.transaction(accountB, 'readwrite', async (tx) => {
				await tx.putCollection('tasks', {
					rows: [{ id: 2 }],
					version: 1
				});
				await tx.putMutation(conformanceMutation('install-b:rollback'));
				throw new Error('conformance rollback');
			});
			issues.push(
				'throwing transaction committed instead of rolling back'
			);
		} catch (error) {
			if (
				!(error instanceof Error) ||
				error.message !== 'conformance rollback'
			)
				throw error;
		}
		const rolledBack = await store.transaction(
			accountB,
			'readonly',
			async (tx) => ({
				collection: await tx.getCollection('tasks'),
				mutations: await tx.listMutations()
			})
		);
		if (rolledBack.collection !== undefined || rolledBack.mutations.length)
			issues.push('transaction rollback left partial state');

		try {
			await store.transaction(accountA, 'readonly', (tx) =>
				tx.putMutation(conformanceMutation('readonly-write'))
			);
			issues.push('readonly transaction accepted a write');
		} catch {
			// Expected.
		}

		await store.transaction(accountB, 'readwrite', (tx) =>
			tx.putMutation(conformanceMutation('install-b:op-1'))
		);
		await store.deleteNamespace(accountA);
		const [deleted, retained] = await Promise.all([
			store.transaction(accountA, 'readonly', (tx) => tx.listMutations()),
			store.transaction(accountB, 'readonly', (tx) => tx.listMutations())
		]);
		if (deleted.length !== 0)
			issues.push('deleteNamespace retained signed-out account state');
		if (retained[0]?.operationId !== 'install-b:op-1')
			issues.push('deleteNamespace crossed a principal boundary');
	} finally {
		await Promise.all([
			store.deleteNamespace(accountA),
			store.deleteNamespace(accountB)
		]);
	}

	return issues;
};

export const assertSyncLocalStoreConformance = async (
	options: SyncLocalStoreConformanceOptions
): Promise<void> => {
	const issues = await inspectSyncLocalStoreConformance(options);
	if (issues.length)
		throw new Error(
			`Sync local-store conformance failed: ${issues.join('; ')}`
		);
};
