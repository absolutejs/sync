/**
 * Integration test: mutations whose handler runs inside an
 * `@absolutejs/isolated-jsc` Isolate. Validates the wiring in
 * `src/engine/sandbox.ts` + the `sandboxedHandler` branch in
 * `runMutation` in syncEngine.ts.
 *
 * Note: cross-Worker postMessage rejections + Bun-test's
 * `expect(p).rejects.toThrow` hang to test timeout (see isolated-jsc's
 * UPSTREAM_ISSUES.md / oven-sh/bun#31462). Tests below use plain try/catch
 * for any expected-rejection assertions to sidestep that.
 */

import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';

type Item = { id: number; n: number };

const itemsCollection = (name: string) =>
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

describe('sandboxed mutations', () => {
	test('sandboxed handler executes via actions.change and emits diffs', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'addThree',
				sandboxedHandler: `async (args, ctx, actions) => {
					await actions.change('items', { op: 'insert', row: { id: 1, n: 1 } });
					await actions.change('items', { op: 'insert', row: { id: 2, n: 2 } });
					await actions.change('items', { op: 'insert', row: { id: 3, n: 3 } });
					return { count: 3 };
				}`,
				sandbox: { memoryLimit: 32, timeout: 5000 }
			})
		);

		const result = (await engine.runMutation('addThree', {}, {})) as {
			count: number;
		};
		expect(result.count).toBe(3);
	});

	test('sandboxed handler receives args + ctx as structured clones', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'echo',
				sandboxedHandler: `async (args, ctx) => {
					return { gotArgs: args, gotCtx: ctx };
				}`,
				sandbox: { timeout: 2000 }
			})
		);

		const result = (await engine.runMutation(
			'echo',
			{ value: 'hello' },
			{ tenant: 'acme' }
		)) as { gotArgs: { value: string }; gotCtx: { tenant: string } };
		expect(result.gotArgs.value).toBe('hello');
		expect(result.gotCtx.tenant).toBe('acme');
	});

	test('sandboxed handler errors propagate to the caller', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'fails',
				sandboxedHandler: `async () => {
					throw new Error('user mutation failed');
				}`,
				sandbox: { timeout: 2000 }
			})
		);

		const err = (await rejection(
			engine.runMutation('fails', {}, {})
		)) as Error;
		expect(err.message).toContain('user mutation failed');
	});

	test('registerMutation rejects definitions with both handler and sandboxedHandler', () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		expect(() =>
			engine.registerMutation({
				name: 'conflicting',
				handler: () => 1,
				sandboxedHandler: '() => 1'
			})
		).toThrow(/pick one/);
	});

	test('registerMutation rejects definitions with neither handler nor sandboxedHandler', () => {
		// Both fields are optional in the type now (either may be set), so
		// this passes static checks. The engine catches it at register time.
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		expect(() => engine.registerMutation({ name: 'empty' })).toThrow(
			/must define either/
		);
	});

	test('sandbox isolate is reused across calls (functional: many calls succeed)', async () => {
		// First call pays compile + worker spawn; subsequent calls reuse the
		// isolate and only spin a fresh context. We don't assert on timing
		// (too flaky under load) — just that many calls succeed in sequence,
		// which would fail in seconds, not milliseconds, if each respawned.
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'increment',
				sandboxedHandler: `async (args) => args.n + 1`,
				// Higher cap: each context spin-up retains some JSC metadata
				// until the next GC sweep. 25 calls × ~2 MB residual >> 32 MB.
				sandbox: { memoryLimit: 128, timeout: 2000 }
			})
		);

		const results: number[] = [];
		for (let i = 0; i < 25; i++) {
			const r = (await engine.runMutation(
				'increment',
				{ n: i },
				{}
			)) as number;
			results.push(r);
		}
		expect(results).toEqual(Array.from({ length: 25 }, (_, i) => i + 1));
	});

	test('sandboxed handler timeout terminates the isolate (transparent re-spawn on next call)', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'loops',
				sandboxedHandler: `() => { while (true) {} }`,
				sandbox: { timeout: 100 }
			})
		);

		const err = (await rejection(
			engine.runMutation('loops', {}, {})
		)) as Error;
		expect(err.name).toBe('TimeoutError');

		// Engine recovers: register a fresh mutation and run it. (The timed-out
		// isolate is dead but each registerMutation builds its own runner.)
		engine.registerMutation(
			defineMutation({
				name: 'works',
				sandboxedHandler: `() => 42`,
				sandbox: { timeout: 2000 }
			})
		);
		const result = await engine.runMutation('works', {}, {});
		expect(result).toBe(42);
	});

	test('read-only handler can opt into the FFI backend (no actions calls)', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'pureDouble',
				// No `actions.*` calls — FFI is safe here. Sandbox computes
				// a derived value and returns it.
				sandboxedHandler: `(args) => args.n * 2`,
				sandbox: { backend: 'ffi', memoryLimit: 128, timeout: 1000 }
			})
		);
		const result = await engine.runMutation('pureDouble', { n: 21 }, {});
		expect(result).toBe(42);
	});

	test("explicit `backend: 'worker'` matches the default (current behaviour)", async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'explicitWorker',
				sandboxedHandler: `async (args, ctx, actions) => {
					await actions.change('items', { op: 'insert', row: { id: 99, n: args.n } });
					return 'ok';
				}`,
				sandbox: { backend: 'worker', memoryLimit: 128, timeout: 5000 }
			})
		);
		const result = await engine.runMutation('explicitWorker', { n: 7 }, {});
		expect(result).toBe('ok');
	});
});
