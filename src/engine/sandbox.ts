/**
 * Sandboxed mutation runner — executes a string-form mutation handler inside
 * an `@absolutejs/isolated-jsc` Isolate. Use it when the handler source is
 * not fully trusted (multi-tenant PaaS, plugin systems, AI-generated logic),
 * or when you want to cap CPU/memory per mutation defensively.
 *
 * Trade-offs vs an ordinary `handler`:
 *
 * - Handler must be a string. It evaluates inside the isolate's JSC VM, with
 *   no access to the host's modules, closures, or globals — only the
 *   `args` / `ctx` clones and the `actions` References we pass in.
 * - First call per mutation pays a Worker spawn + compile (~30 ms). Every
 *   subsequent call reuses the isolate and only spends ~0.5 ms creating a
 *   fresh context.
 * - Timeout terminates the isolate (the sandbox runner detects this and
 *   lazily re-spawns on the next call). On the FFI backend timeouts throw
 *   a TerminationException without killing the isolate; sync's runner
 *   treats both shapes the same.
 * - Each per-call context retains some JSC metadata until the isolate's
 *   next GC sweep. Empirically ~2 MB residual per call (Worker backend).
 *   For long-lived mutations choose `memoryLimit` ≥ 128 (the default 32
 *   trips after a few dozen calls without pressure for GC).
 *
 * **Backend default: `'auto'`** — isolated-jsc 0.4 added an async host-fn
 * pump on the FFI backend (alternates Bun event-loop yields with JSC
 * microtask drains, bounded by `Script.run`'s `timeout`), so the
 * `actions.insert/update/delete/change` async References settle on FFI
 * just like they do on Worker. `'auto'` picks FFI when libJSC is reachable
 * (~300 KB cold heap, interrupt-driven CPU timeouts) and falls back to
 * Worker (~46 MB cold heap, postMessage round-trips) otherwise. Pin to
 * `'worker'` if you specifically need Web APIs (`URL`, `TextEncoder`,
 * `WebSocket`) inside your handler — those live in the Bun-Worker
 * environment, not the bare JSC C API.
 *
 * The runner is built lazily per-mutation: nothing is spawned until the
 * mutation actually runs for the first time. No engine teardown hook is
 * needed — the OS reaps the workers when the engine's host process exits.
 */

// Type-only import — erased at runtime, so the engine still loads when
// `@absolutejs/isolated-jsc` isn't installed. The actual module is loaded
// lazily via dynamic `import()` inside `loadIsolatedJsc`.
import type { Context, Isolate, Script } from '@absolutejs/isolated-jsc';
import type { MutationActions } from './mutation';

type IsolatedJscModule = typeof import('@absolutejs/isolated-jsc');

/** Per-mutation sandbox configuration. */
export type SandboxConfig = {
	/** Heap memory cap (MB). Default 32. */
	memoryLimit?: number;
	/** Wall-clock cap per call (ms). Default 5000. */
	timeout?: number;
	/**
	 * isolated-jsc backend. Defaults to `'auto'` (FFI when libJSC is
	 * reachable, Worker otherwise) since isolated-jsc 0.4 added async
	 * host-fn support on FFI — `actions.insert/update/delete/change`
	 * now settle on both backends.
	 *
	 * Pin to `'worker'` if your handler needs Web APIs (`URL`,
	 * `TextEncoder`, `WebSocket`) — those live in the Bun-Worker
	 * environment, not the bare JSC C API.
	 *
	 * Pin to `'ffi'` for hot-path read-only handlers (~300 KB cold heap
	 * vs ~46 MB on Worker, interrupt-driven CPU timeouts).
	 */
	backend?: 'auto' | 'ffi' | 'worker';
};

let isolatedJscModule: IsolatedJscModule | undefined;
const loadIsolatedJsc = async (): Promise<IsolatedJscModule> => {
	if (isolatedJscModule !== undefined) return isolatedJscModule;
	try {
		isolatedJscModule = await import('@absolutejs/isolated-jsc');
		return isolatedJscModule;
	} catch (error) {
		throw new Error(
			'sandboxedHandler requires the optional peer "@absolutejs/isolated-jsc". ' +
				'Install it with: bun add @absolutejs/isolated-jsc',
			{ cause: error }
		);
	}
};

/**
 * Wraps user source as a callable. The user supplies an expression that
 * evaluates to `(args, ctx, actions) => Result` (sync or async); we evaluate
 * it, build `actions` as a thin in-VM shim that dispatches through the
 * `__syncAction` router Reference (installed once per isolate, shared
 * across calls), and invoke. One Reference instead of four, installed
 * once instead of per-call.
 *
 * The wrapper is a SYNC IIFE — not `(async () => ...)()`. If `userFn` is
 * sync, the IIFE returns a primitive directly, and the FFI backend's
 * `unwrapResultPromise` short-circuits on `!JSValueIsObject` (no setup
 * eval, no read eval). If `userFn` is async, the IIFE returns its Promise
 * and the unwrap pump fires normally. Either shape works; only the sync
 * shape gets the fast path. This shaves ~1.5 ms off pure-handler warm
 * dispatch on FFI.
 *
 * The user source is interpolated directly — backticks and `${...}` in it
 * are fine because JSC's eval consumes the raw text, not a template literal.
 * We rely on the isolate boundary, not source-level sanitisation, for safety.
 */
const wrap = (source: string): string => `
	(() => {
		const userFn = (${source});
		if (typeof userFn !== 'function') {
			throw new Error(
				'sandboxedHandler must evaluate to (args, ctx, actions) => result; got ' +
					typeof userFn
			);
		}
		const actions = {
			insert: (table, data) => __syncAction('insert', table, data),
			update: (table, data) => __syncAction('update', table, data),
			delete: (table, row) => __syncAction('delete', table, row),
			change: (collection, change) => __syncAction('change', collection, change)
		};
		return userFn(args, ctx, actions);
	})()
`;

/**
 * How many sandboxed calls a single context can serve before we recycle
 * it (dispose + create + re-install the router). Per-call JSC metadata
 * accumulates in the reused context until GC sweeps it; recycling caps
 * that without paying per-call context-create cost. Empirical: ~2 MB
 * residual per call (Worker backend); 256 calls × 2 MB ≈ 512 MB before
 * sweep, well under the 1 GB cap most workloads run with.
 */
const DEFAULT_RECYCLE_CONTEXT_AFTER = 256;

type CompiledMutation = {
	context: Context;
	/** Shared slot the router Reference reads on each `__syncAction` call. */
	currentActions: { value: MutationActions | undefined };
	isolate: Isolate;
	/** Promise queue for serialising calls — keeps the shared slot coherent. */
	runQueue: Promise<unknown>;
	script: Script;
	/** Calls served by the current `context`; recycled at the threshold. */
	servedCalls: number;
	timeoutMs: number;
};

const installRouter = async (
	context: Context,
	currentActions: { value: MutationActions | undefined },
	Reference: typeof import('@absolutejs/isolated-jsc').Reference
): Promise<void> => {
	const router = new Reference(((
		op: unknown,
		...rest: unknown[]
	): Promise<unknown> | unknown => {
		const a = currentActions.value;
		if (a === undefined) {
			throw new Error(
				'__syncAction invoked outside an active sandboxed call (shared-slot router)'
			);
		}
		switch (op) {
			case 'insert':
				return a.insert(rest[0] as string, rest[1]);
			case 'update':
				return a.update(rest[0] as string, rest[1]);
			case 'delete':
				return a.delete(rest[0] as string, rest[1]);
			case 'change':
				return a.change(rest[0] as string, rest[1] as never);
			default:
				throw new Error(`unknown sandbox action op: ${String(op)}`);
		}
	}) as (...rawArgs: unknown[]) => unknown);
	await context.setGlobal('__syncAction', router);
};

const compile = async (
	source: string,
	config: SandboxConfig
): Promise<CompiledMutation> => {
	const { createIsolate, Reference } = await loadIsolatedJsc();
	const isolate = await createIsolate({
		// `'auto'` since isolated-jsc 0.4 (async host-fn pump on FFI).
		// See SandboxConfig.backend JSDoc for the per-backend trade-offs.
		backend: config.backend ?? 'auto',
		memoryLimit: config.memoryLimit ?? 32
	});
	const script = await isolate.compileScript(wrap(source));
	const context = await isolate.createContext();
	const currentActions: { value: MutationActions | undefined } = {
		value: undefined
	};
	await installRouter(context, currentActions, Reference);
	return {
		context,
		currentActions,
		isolate,
		runQueue: Promise.resolve(undefined),
		script,
		servedCalls: 0,
		timeoutMs: config.timeout ?? 5000
	};
};

/**
 * Build a lazy runner for one mutation's sandboxed source. The first call
 * compiles + spawns; subsequent calls reuse the isolate AND the context
 * (the router Reference is installed once on isolate creation; per-call
 * cost is just two `setGlobal`s for `args` + `ctx`). Calls are serialised
 * via a promise queue so the shared-slot router stays coherent. The
 * context is recycled every {@link DEFAULT_RECYCLE_CONTEXT_AFTER} calls
 * to bound JSC per-call metadata accumulation. If the isolate has been
 * disposed (timeout, memory cap), the next call re-spawns transparently.
 */
export const makeSandboxedHandler = (
	source: string,
	config: SandboxConfig = {}
): ((
	args: unknown,
	ctx: unknown,
	actions: MutationActions
) => Promise<unknown>) => {
	let pending: Promise<CompiledMutation> | undefined;

	const getCompiled = async (): Promise<CompiledMutation> => {
		if (pending !== undefined) {
			const compiled = await pending;
			if (!compiled.isolate.isDisposed) return compiled;
			pending = undefined; // dead — re-spawn below
		}
		pending = compile(source, config);
		return pending;
	};

	const recycleContextIfNeeded = async (
		compiled: CompiledMutation
	): Promise<void> => {
		if (compiled.servedCalls < DEFAULT_RECYCLE_CONTEXT_AFTER) return;
		const { Reference } = await loadIsolatedJsc();
		await compiled.context.dispose().catch(() => {});
		compiled.context = await compiled.isolate.createContext();
		await installRouter(compiled.context, compiled.currentActions, Reference);
		compiled.servedCalls = 0;
	};

	return async (args, ctx, actions) => {
		const compiled = await getCompiled();
		// Chain on the queue — the shared slot is only valid during the
		// active call, so the next call must wait for the current one to
		// finish setting + reading + clearing it. `.catch(() => undefined)`
		// keeps the queue alive after a rejection.
		const prev = compiled.runQueue;
		const turn = prev.then(async () => {
			await recycleContextIfNeeded(compiled);
			compiled.currentActions.value = actions;
			try {
				await compiled.context.setGlobal('args', args);
				await compiled.context.setGlobal('ctx', ctx);
				const result = await compiled.script.run(compiled.context, {
					timeout: compiled.timeoutMs
				});
				return result;
			} finally {
				compiled.currentActions.value = undefined;
				compiled.servedCalls += 1;
			}
		});
		compiled.runQueue = turn.catch(() => undefined);
		return turn;
	};
};
