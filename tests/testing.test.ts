/**
 * Smoke test for the `@absolutejs/sync/testing` subpath helpers —
 * createTestEngine, expectRejection, runAsActor. The helpers are
 * intentionally small; this test mostly proves the subpath builds
 * and the API stays as documented.
 */

import { describe, expect, test } from 'bun:test';
import { defineCollection, defineMutation } from '../src/engine';
import { createTestEngine, expectRejection, runAsActor } from '../src/testing';

type Row = { id: string; n: number };

describe('@absolutejs/sync/testing', () => {
	test('createTestEngine returns a working SyncEngine', () => {
		const engine = createTestEngine();
		engine.register(
			defineCollection<Row>({
				hydrate: () => [],
				key: (row) => row.id,
				match: () => true,
				name: 'rows'
			})
		);
		expect(engine.inspect().collections.map((c) => c.name)).toContain(
			'rows'
		);
	});

	test('expectRejection returns the thrown error', async () => {
		const error = await expectRejection(() => {
			throw new Error('boom');
		});
		expect((error as Error).message).toBe('boom');
	});

	test('expectRejection throws if `work` resolves', async () => {
		let resolved = false;
		try {
			await expectRejection(() => 'fine');
			resolved = true;
		} catch {
			// expected
		}
		expect(resolved).toBe(false);
	});

	test('runAsActor runs a mutation with userId on ctx', async () => {
		const engine = createTestEngine();
		let seenCtx: { userId?: string } | undefined;
		engine.registerMutation(
			defineMutation({
				handler: (_args, ctx) => {
					seenCtx = ctx as { userId?: string };
				},
				name: 'echo'
			})
		);
		await runAsActor(engine, 'alice', 'echo', {});
		expect(seenCtx?.userId).toBe('alice');
	});

	test('runAsActor merges extraCtx but userId wins', async () => {
		const engine = createTestEngine();
		let seenCtx: { userId?: string; role?: string } | undefined;
		engine.registerMutation(
			defineMutation({
				handler: (_args, ctx) => {
					seenCtx = ctx as { userId?: string; role?: string };
				},
				name: 'echo'
			})
		);
		await runAsActor(engine, 'alice', 'echo', {}, { role: 'editor' });
		expect(seenCtx?.userId).toBe('alice');
		expect(seenCtx?.role).toBe('editor');
	});
});
