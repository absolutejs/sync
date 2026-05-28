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
