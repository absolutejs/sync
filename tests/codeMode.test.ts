/**
 * Tests for the `@absolutejs/sync/code-mode` subpath. We don't import
 * `@absolutejs/ai` here — the host-tool map is shape-compatible with
 * `codeModeTool({ tools })`, and these tests exercise the map
 * directly by invoking the handler the same way a Code Mode runner
 * would (positional args from the sandbox).
 */

import { describe, expect, test } from 'bun:test';
import { engineMutationsAsHostTools } from '../src/codeMode';
import { defineMutation } from '../src/engine';
import { createTestEngine } from '../src/testing';

type Ctx = { userId?: string };

describe('@absolutejs/sync/code-mode', () => {
	test('engineMutationsAsHostTools wraps a registered mutation, threads ctx, returns the result', async () => {
		const engine = createTestEngine();
		const seen: Array<{ name: string; payload: unknown; ctx: Ctx }> = [];
		engine.registerMutation(
			defineMutation<{ note: string }, Ctx, { id: string }>({
				handler: (args, ctx) => {
					seen.push({ ctx, name: 'demo:do', payload: args });
					return { id: `note-${args.note}` };
				},
				name: 'demo:do'
			})
		);

		const hostTools = engineMutationsAsHostTools<Ctx>({
			ctx: () => ({ userId: 'alice' }),
			engine,
			mutations: [
				{
					description: 'Do the demo thing.',
					name: 'demo:do',
					tsSignature:
						'(args: { note: string }) => Promise<{ id: string }>'
				}
			]
		});

		// `:` is not a valid JS identifier, so the default host-fn name is `demo_do`.
		expect(Object.keys(hostTools)).toEqual(['demo_do']);
		expect(hostTools.demo_do!.tsSignature).toBe(
			'(args: { note: string }) => Promise<{ id: string }>'
		);

		const result = await hostTools.demo_do!.handler({ note: 'hello' });
		expect(result).toEqual({ id: 'note-hello' });
		expect(seen).toEqual([
			{ ctx: { userId: 'alice' }, name: 'demo:do', payload: { note: 'hello' } }
		]);
	});

	test('ctx factory is invoked PER CALL (handler-level threading, not factory-level caching)', async () => {
		const engine = createTestEngine();
		const seen: Ctx[] = [];
		engine.registerMutation(
			defineMutation<{ tag: string }, Ctx, null>({
				handler: (_args, ctx) => {
					seen.push(ctx);
					return null;
				},
				name: 'demo:do'
			})
		);
		let counter = 0;
		const hostTools = engineMutationsAsHostTools<Ctx>({
			ctx: () => ({ userId: `user-${++counter}` }),
			engine,
			mutations: [{ description: 'demo', name: 'demo:do' }]
		});

		await hostTools.demo_do!.handler({ tag: 'a' });
		await hostTools.demo_do!.handler({ tag: 'b' });
		expect(seen).toEqual([{ userId: 'user-1' }, { userId: 'user-2' }]);
	});

	test('default tsSignature is the generic shape when none provided', () => {
		const engine = createTestEngine();
		engine.registerMutation(
			defineMutation<unknown, Ctx, null>({
				handler: () => null,
				name: 'm:any'
			})
		);
		const hostTools = engineMutationsAsHostTools<Ctx>({
			ctx: () => ({}),
			engine,
			mutations: [{ description: 'any', name: 'm:any' }]
		});
		expect(hostTools.m_any!.tsSignature).toBe(
			'(args: any) => Promise<any>'
		);
	});

	test('hostFnName override wins over the auto-derived name', () => {
		const engine = createTestEngine();
		engine.registerMutation(
			defineMutation<unknown, Ctx, null>({
				handler: () => null,
				name: 'comments:create'
			})
		);
		const hostTools = engineMutationsAsHostTools<Ctx>({
			ctx: () => ({}),
			engine,
			mutations: [
				{
					description: 'Post a comment.',
					hostFnName: 'post_comment',
					name: 'comments:create'
				}
			]
		});
		expect(Object.keys(hostTools)).toEqual(['post_comment']);
	});

	test('throws at build time if a descriptor names an unregistered mutation', () => {
		const engine = createTestEngine();
		expect(() =>
			engineMutationsAsHostTools<Ctx>({
				ctx: () => ({}),
				engine,
				mutations: [
					{ description: 'unknown', name: 'never:registered' }
				]
			})
		).toThrow(/never:registered/);
	});

	test('throws at build time on duplicate host-fn names', () => {
		const engine = createTestEngine();
		engine.registerMutation(
			defineMutation<unknown, Ctx, null>({
				handler: () => null,
				name: 'comments:create'
			})
		);
		engine.registerMutation(
			defineMutation<unknown, Ctx, null>({
				handler: () => null,
				name: 'comments_create' // collides with the auto-derived host-fn name from above
			})
		);
		expect(() =>
			engineMutationsAsHostTools<Ctx>({
				ctx: () => ({}),
				engine,
				mutations: [
					{ description: 'a', name: 'comments:create' },
					{ description: 'b', name: 'comments_create' }
				]
			})
		).toThrow(/duplicate host-fn name/);
	});

	test('mutation errors propagate from the host-tool handler (caller can catch)', async () => {
		const engine = createTestEngine();
		engine.registerMutation(
			defineMutation<unknown, Ctx, null>({
				handler: () => {
					throw new Error('handler boom');
				},
				name: 'm:fail'
			})
		);
		const hostTools = engineMutationsAsHostTools<Ctx>({
			ctx: () => ({}),
			engine,
			mutations: [{ description: 'fail', name: 'm:fail' }]
		});
		let caught: unknown;
		try {
			await hostTools.m_fail!.handler({});
		} catch (error) {
			caught = error;
		}
		expect((caught as Error).message).toMatch(/handler boom/);
	});

	test('partial-failure semantics: prior mutations commit even when a later one throws', async () => {
		// The v0.1 documented contract — exercise it end-to-end.
		const engine = createTestEngine();
		const writes: string[] = [];
		engine.registerMutation(
			defineMutation<{ tag: string }, Ctx, null>({
				handler: (args) => {
					writes.push(args.tag);
					return null;
				},
				name: 'm:ok'
			})
		);
		engine.registerMutation(
			defineMutation<unknown, Ctx, null>({
				handler: () => {
					throw new Error('second boom');
				},
				name: 'm:boom'
			})
		);
		const hostTools = engineMutationsAsHostTools<Ctx>({
			ctx: () => ({}),
			engine,
			mutations: [
				{ description: 'ok', name: 'm:ok' },
				{ description: 'fail', name: 'm:boom' }
			]
		});
		await hostTools.m_ok!.handler({ tag: 'first' });
		await expect(hostTools.m_boom!.handler({})).rejects.toThrow(
			/second boom/
		);
		// First call committed before the second threw.
		expect(writes).toEqual(['first']);
	});
});
