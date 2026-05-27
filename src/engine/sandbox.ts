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

type CompiledMutation = {
	isolate: Isolate;
	script: Script;
	timeoutMs: number;
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
 * it, build `actions` from the individually-installed References (sidestepping
 * isolated-jsc not yet supporting nested Reference values), and invoke.
 *
 * The user source is interpolated directly — backticks and `${...}` in it
 * are fine because JSC's eval consumes the raw text, not a template literal.
 * We rely on the isolate boundary, not source-level sanitisation, for safety.
 */
const wrap = (source: string): string => `
	(async () => {
		const userFn = (${source});
		if (typeof userFn !== 'function') {
			throw new Error(
				'sandboxedHandler must evaluate to (args, ctx, actions) => result; got ' +
					typeof userFn
			);
		}
		const actions = {
			insert: __syncActionInsert,
			update: __syncActionUpdate,
			delete: __syncActionDelete,
			change: __syncActionChange
		};
		return await userFn(args, ctx, actions);
	})()
`;

const compile = async (
	source: string,
	config: SandboxConfig
): Promise<CompiledMutation> => {
	const { createIsolate } = await loadIsolatedJsc();
	const isolate = await createIsolate({
		// `'auto'` since isolated-jsc 0.4 (async host-fn pump on FFI).
		// See SandboxConfig.backend JSDoc for the per-backend trade-offs.
		backend: config.backend ?? 'auto',
		memoryLimit: config.memoryLimit ?? 32
	});
	const script = await isolate.compileScript(wrap(source));
	return { isolate, script, timeoutMs: config.timeout ?? 5000 };
};

/**
 * Build a lazy runner for one mutation's sandboxed source. The first call
 * compiles + spawns; subsequent calls reuse the isolate. If the isolate has
 * been disposed (timeout, memory cap), the next call re-spawns transparently.
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

	return async (args, ctx, actions) => {
		const { Reference } = await loadIsolatedJsc();
		const compiled = await getCompiled();
		const context = await compiled.isolate.createContext();
		try {
			await context.setGlobal('args', args);
			await context.setGlobal('ctx', ctx);
			await context.setGlobal(
				'__syncActionInsert',
				new Reference(((table: unknown, data: unknown) =>
					actions.insert(table as string, data)) as (
					...args: unknown[]
				) => unknown)
			);
			await context.setGlobal(
				'__syncActionUpdate',
				new Reference(((table: unknown, data: unknown) =>
					actions.update(table as string, data)) as (
					...args: unknown[]
				) => unknown)
			);
			await context.setGlobal(
				'__syncActionDelete',
				new Reference(((table: unknown, row: unknown) =>
					actions.delete(table as string, row)) as (
					...args: unknown[]
				) => unknown)
			);
			await context.setGlobal(
				'__syncActionChange',
				new Reference(((collection: unknown, change: unknown) =>
					actions.change(collection as string, change as never)) as (
					...args: unknown[]
				) => unknown)
			);
			return await compiled.script.run(context, {
				timeout: compiled.timeoutMs
			});
		} finally {
			// Best-effort context dispose. If the isolate self-died on
			// timeout/memory, this is a no-op that swallows the rejection.
			await context.dispose().catch(() => {});
		}
	};
};
