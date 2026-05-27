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
 * **Backend default: `'worker'`** — required so far because the `actions`
 * machinery (insert/update/delete/change) crosses the host boundary as
 * **async** References (they return Promises that go through the engine's
 * writer + diff path). The isolated-jsc FFI backend only supports SYNC
 * host fns today (per its 0.3 documented limit); calling
 * `actions.insert(...)` from a sandboxed handler on FFI would surface as
 * a Promise-cannot-unwrap error. Pin to Worker.
 *
 * Read-only sandboxed mutations that don't call `actions.*` (e.g. compute
 * a derived value from `args` + `ctx` and `return` it) CAN opt into FFI
 * via `sandbox: { backend: 'ffi' }` — they get the ~300 KB cold heap and
 * interrupt-driven timeouts. Document this clearly when you do.
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
	 * isolated-jsc backend. Defaults to `'worker'` because the engine's
	 * `actions.insert/update/delete/change` cross the sandbox boundary as
	 * async References — and isolated-jsc's FFI backend doesn't pump
	 * async host fns (its 0.3 documented limit). The Worker backend
	 * supports both sync and async host fns.
	 *
	 * Opt into `'ffi'` only for **read-only** sandboxed handlers — ones
	 * that compute a derived value from `args` + `ctx` and return it
	 * without calling any `actions.*`. Those get the FFI cold-heap
	 * (~300 KB vs ~46 MB) + interrupt-driven timeout benefits.
	 *
	 * `'auto'` resolves to FFI when libJSC is reachable and Worker
	 * otherwise; same async-actions caveat applies on the FFI path.
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
		// Pinned to Worker by default. See SandboxConfig.backend JSDoc for
		// why FFI is opt-in (actions References are async).
		backend: config.backend ?? 'worker',
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
