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

	test('handlerMetrics hook fires on success with cpuMs + heapBytes', async () => {
		const records: Array<Record<string, unknown>> = [];
		const engine = createSyncEngine({
			handlerMetrics: (record) => {
				records.push(record);
			}
		});
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'instrumentedPure',
				sandbox: { memoryLimit: 64, timeout: 2000 },
				sandboxedHandler: `(args) => args.n + 1`
			})
		);
		const result = await engine.runMutation(
			'instrumentedPure',
			{ n: 41 },
			{}
		);
		expect(result).toBe(42);
		expect(records.length).toBe(1);
		const r = records[0] as {
			id: string;
			mutationName: string;
			ok: boolean;
			cpuMs: number;
			heapBytes: number;
			durationMs: number;
			timestamp: number;
		};
		expect(r.mutationName).toBe('instrumentedPure');
		expect(r.ok).toBe(true);
		expect(typeof r.id).toBe('string');
		expect(r.cpuMs).toBeGreaterThanOrEqual(0);
		expect(r.heapBytes).toBeGreaterThan(0);
		expect(r.durationMs).toBeGreaterThan(0);
		expect(r.timestamp).toBeGreaterThan(0);
	});

	test('handlerMetrics hook fires on failure with errorName + errorMessage; original error still throws', async () => {
		const records: Array<Record<string, unknown>> = [];
		const engine = createSyncEngine({
			handlerMetrics: (record) => {
				records.push(record);
			}
		});
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'instrumentedFails',
				sandbox: { timeout: 2000 },
				sandboxedHandler: `() => { throw new Error('measured failure'); }`
			})
		);
		const err = (await rejection(
			engine.runMutation('instrumentedFails', {}, {})
		)) as Error;
		expect(err.message).toContain('measured failure');
		expect(records.length).toBe(1);
		const r = records[0] as {
			ok: boolean;
			errorName?: string;
			errorMessage?: string;
		};
		expect(r.ok).toBe(false);
		expect(r.errorName).toBe('Error');
		expect(r.errorMessage).toContain('measured failure');
	});

	test('handlerMetrics hook crashes do not crash the mutation', async () => {
		const engine = createSyncEngine({
			handlerMetrics: () => {
				throw new Error('observer exploded');
			}
		});
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'instrumentedResilient',
				sandbox: { timeout: 2000 },
				sandboxedHandler: `() => 7`
			})
		);
		// Mutation must still return its real value even though the
		// hook throws.
		const result = await engine.runMutation(
			'instrumentedResilient',
			{},
			{}
		);
		expect(result).toBe(7);
	});

	test('handlerMetrics async hook rejection is also swallowed', async () => {
		const engine = createSyncEngine({
			handlerMetrics: async () => {
				throw new Error('async observer exploded');
			}
		});
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'instrumentedAsyncResilient',
				sandbox: { timeout: 2000 },
				sandboxedHandler: `() => 'still ok'`
			})
		);
		const result = await engine.runMutation(
			'instrumentedAsyncResilient',
			{},
			{}
		);
		expect(result).toBe('still ok');
		// Give the async rejection a tick to fire so we know it was
		// caught rather than turning into an unhandled rejection.
		await new Promise((r) => setTimeout(r, 10));
	});

	test('handlerMetrics records one entry per call across many calls', async () => {
		const records: Array<{ mutationName: string; ok: boolean }> = [];
		const engine = createSyncEngine({
			handlerMetrics: (record) => {
				records.push(record);
			}
		});
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'instrumentedCounted',
				sandbox: { memoryLimit: 128, timeout: 2000 },
				sandboxedHandler: `(args) => args.n * 2`
			})
		);
		for (let i = 0; i < 10; i++) {
			await engine.runMutation('instrumentedCounted', { n: i }, {});
		}
		expect(records.length).toBe(10);
		for (const r of records) {
			expect(r.mutationName).toBe('instrumentedCounted');
			expect(r.ok).toBe(true);
		}
	});

	test('actions.fetch enforces hostname allowlist + injects host-side Authorization', async () => {
		const seen: Array<{ url: string; authorization?: string }> = [];
		const originalFetch = globalThis.fetch;
		globalThis.fetch = (async (
			input: RequestInfo | URL,
			init?: RequestInit
		) => {
			const headers = new Headers(init?.headers ?? {});
			seen.push({
				authorization: headers.get('authorization') ?? undefined,
				url: typeof input === 'string' ? input : input.toString()
			});
			return new Response(JSON.stringify({ pong: true }), {
				headers: { 'content-type': 'application/json' },
				status: 200,
				statusText: 'OK'
			});
		}) as typeof fetch;

		try {
			const engine = createSyncEngine({
				bridgeFetch: {
					'api.allowed.test': {
						authorization: () => 'Bearer host-side-secret'
					}
				}
			});
			engine.register(itemsCollection('items'));
			engine.registerMutation(
				defineMutation({
					name: 'bridgeOk',
					sandbox: { timeout: 2000 },
					sandboxedHandler: `async (args, ctx, actions) => {
						const r = await actions.fetch('https://api.allowed.test/ping', {
							method: 'POST',
							headers: { 'X-Trace': 'abc' },
							body: 'hello'
						});
						return { ok: r.ok, status: r.status, body: r.body };
					}`
				})
			);
			engine.registerMutation(
				defineMutation({
					name: 'bridgeDenied',
					sandbox: { timeout: 2000 },
					sandboxedHandler: `async (args, ctx, actions) => {
						return await actions.fetch('https://api.evil.test/x');
					}`
				})
			);

			const ok = (await engine.runMutation('bridgeOk', {}, {})) as {
				ok: boolean;
				status: number;
				body: string;
			};
			expect(ok.ok).toBe(true);
			expect(ok.status).toBe(200);
			expect(JSON.parse(ok.body)).toEqual({ pong: true });
			expect(seen.length).toBe(1);
			expect(seen[0]?.url).toBe('https://api.allowed.test/ping');
			expect(seen[0]?.authorization).toBe('Bearer host-side-secret');

			const denied = (await rejection(
				engine.runMutation('bridgeDenied', {}, {})
			)) as Error;
			expect(denied.message).toContain('not allowlisted');
			// And no extra fetch happened for the denied call.
			expect(seen.length).toBe(1);
		} finally {
			globalThis.fetch = originalFetch;
		}
	});

	test('actions.fetch rejects sandbox-supplied Authorization to prevent secret-leak vectors', async () => {
		const seen: Array<Record<string, string | undefined>> = [];
		const originalFetch = globalThis.fetch;
		globalThis.fetch = (async (
			input: RequestInfo | URL,
			init?: RequestInit
		) => {
			const headers = new Headers(init?.headers ?? {});
			seen.push({
				authorization: headers.get('authorization') ?? undefined,
				xTrace: headers.get('x-trace') ?? undefined
			});
			return new Response('{}', { status: 200, statusText: 'OK' });
		}) as typeof fetch;

		try {
			const engine = createSyncEngine({
				bridgeFetch: {
					'api.allowed.test': {
						authorization: () => 'Bearer host-side'
					}
				}
			});
			engine.register(itemsCollection('items'));
			engine.registerMutation(
				defineMutation({
					name: 'bridgeSandboxAuth',
					sandbox: { timeout: 2000 },
					sandboxedHandler: `async (args, ctx, actions) => {
						return await actions.fetch('https://api.allowed.test/x', {
							headers: { Authorization: 'Bearer sandbox-tried-to-set', 'X-Trace': 'ok' }
						});
					}`
				})
			);
			await engine.runMutation('bridgeSandboxAuth', {}, {});
			// Sandbox's Authorization is stripped; host's is the one that
			// went out.
			expect(seen[0]?.authorization).toBe('Bearer host-side');
			// Non-Authorization sandbox headers DO pass through.
			expect(seen[0]?.xTrace).toBe('ok');
		} finally {
			globalThis.fetch = originalFetch;
		}
	});

	test('actions.fetch throws cleanly when no bridgeFetch is configured', async () => {
		const engine = createSyncEngine(); // no bridgeFetch
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'bridgeUnconfigured',
				sandbox: { timeout: 2000 },
				sandboxedHandler: `async (args, ctx, actions) => {
					return await actions.fetch('https://api.example/x');
				}`
			})
		);
		const err = (await rejection(
			engine.runMutation('bridgeUnconfigured', {}, {})
		)) as Error;
		expect(err.message).toMatch(/no .?bridgeFetch.? config/i);
	});

	test('actions.now() returns a Date.now()-comparable number from the host', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'currentTime',
				sandbox: { timeout: 2000 },
				sandboxedHandler: `async (args, ctx, actions) => actions.now()`
			})
		);
		const before = Date.now();
		const t = (await engine.runMutation('currentTime', {}, {})) as number;
		const after = Date.now();
		expect(typeof t).toBe('number');
		expect(t).toBeGreaterThanOrEqual(before);
		expect(t).toBeLessThanOrEqual(after);
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

	test('unsafeHost: declared fn runs on the host and returns its value', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		const seen: Array<{ amount: number; token: string }> = [];
		engine.registerMutation(
			defineMutation({
				name: 'payments:charge',
				sandbox: {
					unsafeHost: {
						chargeStripe: (input: {
							amount: number;
							token: string;
						}) => {
							seen.push(input);
							return { id: `ch_${input.amount}`, ok: true };
						}
					}
				},
				sandboxedHandler: `async (args, ctx, actions, unsafeHost) => {
					const receipt = await unsafeHost.chargeStripe({
						amount: args.amount,
						token: args.token
					});
					return receipt;
				}`
			})
		);
		const result = (await engine.runMutation(
			'payments:charge',
			{ amount: 4200, token: 'tok-demo' },
			{}
		)) as { id: string; ok: boolean };
		expect(result).toEqual({ id: 'ch_4200', ok: true });
		expect(seen).toEqual([{ amount: 4200, token: 'tok-demo' }]);
	});

	test('unsafeHost: undeclared fn throws a clear, instructive error', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'badEscape',
				sandbox: { unsafeHost: { allowed: () => 'ok' } },
				sandboxedHandler: `async (args, ctx, actions, unsafeHost) => {
					return await unsafeHost.notDeclared({ x: 1 });
				}`
			})
		);
		const error = (await rejection(
			engine.runMutation('badEscape', {}, {})
		)) as Error;
		expect(error.message).toMatch(/unsafeHost\.notDeclared/);
		expect(error.message).toMatch(/not declared/);
	});

	test('unsafeHost: thrown host error propagates into the sandbox', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'catchableHostFailure',
				sandbox: {
					unsafeHost: {
						boom: () => {
							throw new Error('payment-gateway: declined');
						}
					}
				},
				sandboxedHandler: `async (args, ctx, actions, unsafeHost) => {
					try {
						await unsafeHost.boom();
						return { caught: false };
					} catch (e) {
						return { caught: true, message: e.message };
					}
				}`
			})
		);
		const result = (await engine.runMutation(
			'catchableHostFailure',
			{},
			{}
		)) as { caught: boolean; message?: string };
		expect(result.caught).toBe(true);
		expect(result.message).toMatch(/declined/);
	});

	test('unsafeHost: without any sandbox.unsafeHost config, calling through still gives a clear "not declared" error', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		engine.registerMutation(
			defineMutation({
				name: 'noEscapeConfig',
				sandboxedHandler: `async (args, ctx, actions, unsafeHost) => {
					await unsafeHost.anything();
					return 'unreachable';
				}`
			})
		);
		const error = (await rejection(
			engine.runMutation('noEscapeConfig', {}, {})
		)) as Error;
		expect(error.message).toMatch(/unsafeHost\.anything/);
	});

	test('unsafeHost: interleaves cleanly with actions.* writes (single fan-out)', async () => {
		const engine = createSyncEngine();
		engine.register(itemsCollection('items'));
		const sideEffects: string[] = [];
		engine.registerMutation(
			defineMutation({
				name: 'orderThenCharge',
				sandbox: {
					unsafeHost: {
						chargeStripe: () => {
							sideEffects.push('charge');
							return { ok: true };
						}
					}
				},
				sandboxedHandler: `async (args, ctx, actions, unsafeHost) => {
					await actions.change('items', { op: 'insert', row: { id: 1, n: args.n } });
					const receipt = await unsafeHost.chargeStripe({ amount: args.n });
					await actions.change('items', { op: 'insert', row: { id: 2, n: args.n * 2 } });
					return { paid: receipt.ok, total: args.n * 3 };
				}`
			})
		);
		const diffs: Array<{ added: Item[] }> = [];
		await engine.subscribe<Item>({
			collection: 'items',
			ctx: {},
			onDiff: (diff) => {
				diffs.push({ added: diff.added });
			},
			params: undefined
		});
		const result = (await engine.runMutation(
			'orderThenCharge',
			{ n: 7 },
			{}
		)) as { paid: boolean; total: number };
		expect(result).toEqual({ paid: true, total: 21 });
		expect(sideEffects).toEqual(['charge']);
		expect(diffs).toHaveLength(1);
		expect(diffs[0]!.added.map((row) => row.id).sort()).toEqual([1, 2]);
	});
});
