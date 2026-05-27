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
 *   `args` / `ctx` clones and the `actions` Reference we pass in.
 * - First call per mutation pays an isolate spawn + compile (~3–25 ms
 *   depending on backend). Every subsequent call is a single
 *   `JSObjectCallAsFunction` (FFI) or one postMessage (Worker) — no
 *   per-call eval, no per-call `setGlobal`.
 * - Timeout terminates the isolate (the sandbox runner detects this and
 *   lazily re-spawns on the next call). On the FFI backend timeouts throw
 *   a TerminationException without killing the isolate; sync's runner
 *   treats both shapes the same.
 *
 * **Backend default: `'auto'`** — `'auto'` picks FFI when libJSC is reachable
 * (~300 KB cold heap, interrupt-driven CPU timeouts) and falls back to
 * Worker (~46 MB cold heap, postMessage round-trips) otherwise. Pin to
 * `'worker'` if you specifically need Web APIs (`URL`, `TextEncoder`,
 * `WebSocket`) inside your handler — those live in the Bun-Worker
 * environment, not the bare JSC C API.
 *
 * **Per-call hot path (since 1.7.4 / isolated-jsc 0.6).** Each mutation is
 * compiled to a {@link Callable} once — a precompiled function expression
 * the sandbox owns by reference. Per call we invoke
 * `callable.call([args, ctx, dispatch])` where `dispatch` is a Reference
 * that bridges `actions.*` back to the host. No globals, no eval per call,
 * no shared-slot serialization machinery.
 *
 * The runner is built lazily per-mutation: nothing is spawned until the
 * mutation actually runs for the first time. No engine teardown hook is
 * needed — the OS reaps the workers when the engine's host process exits.
 */

// Type-only import — erased at runtime, so the engine still loads when
// `@absolutejs/isolated-jsc` isn't installed. The actual module is loaded
// lazily via dynamic `import()` inside `loadIsolatedJsc`.
import type {
	Callable,
	Context,
	Isolate
} from '@absolutejs/isolated-jsc';
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
	 * reachable, Worker otherwise). Both backends now run the same
	 * `Context.compileCallable`-based hot path; the choice trades cold
	 * spawn (FFI wins ~6×) against Web API availability (Worker only).
	 *
	 * Pin to `'worker'` if your handler needs Web APIs (`URL`,
	 * `TextEncoder`, `WebSocket`) — those live in the Bun-Worker
	 * environment, not the bare JSC C API.
	 *
	 * Pin to `'ffi'` to bypass the auto-probe when you know libJSC is
	 * reachable (e.g. CI with a known image).
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
 * Wrap user source as a function expression for `compileCallable`. The
 * compiled function takes `(__callId, args, ctx)` — `__callId` keys the
 * per-call `actions` lookup, so the dispatch Reference (installed once
 * per isolate as a global) can route `actions.*` calls back to the
 * correct call's host-side `actions` instance. The in-VM `actions`
 * object is a thin shim that closes the call id over a global
 * `__dispatch` Reference.
 *
 * The wrapper is SYNC. If `userFn` is sync the return is a primitive
 * (FFI's `unwrapResultPromise` short-circuits on `!JSValueIsObject` —
 * zero unwrap evals). If `userFn` is async the return is a Promise and
 * the unwrap pump fires normally.
 */
const wrap = (source: string): string => `
	function (__callId, args, ctx) {
		const userFn = (${source});
		if (typeof userFn !== 'function') {
			throw new Error(
				'sandboxedHandler must evaluate to (args, ctx, actions) => result; got ' +
					typeof userFn
			);
		}
		const actions = {
			insert: (table, data) => __dispatch(__callId, 'insert', table, data),
			update: (table, data) => __dispatch(__callId, 'update', table, data),
			delete: (table, row) => __dispatch(__callId, 'delete', table, row),
			change: (collection, change) => __dispatch(__callId, 'change', collection, change)
		};
		return userFn(args, ctx, actions);
	}
`;

type CompiledMutation = {
	callable: Callable;
	/** Per-call actions instances, keyed by callId. Lives for the
	 * duration of each call. */
	callMap: Map<number, MutationActions>;
	context: Context;
	isolate: Isolate;
	nextCallId: number;
	timeoutMs: number;
};

const compile = async (
	source: string,
	config: SandboxConfig
): Promise<CompiledMutation> => {
	const { createIsolate, Reference } = await loadIsolatedJsc();
	const isolate = await createIsolate({
		backend: config.backend ?? 'auto',
		memoryLimit: config.memoryLimit ?? 32
	});
	const context = await isolate.createContext();

	// Dispatch installed ONCE per isolate as a global. Closes over the
	// per-mutation callMap; each in-VM `actions.*` call hands its
	// callId back so we look up the right `actions` instance. This is
	// concurrent-safe: every call has its own callId → its own slot,
	// no shared-mutable-state races.
	const callMap = new Map<number, MutationActions>();
	const dispatch = new Reference(((
		callId: unknown,
		op: unknown,
		...rest: unknown[]
	): Promise<unknown> | unknown => {
		const a = callMap.get(callId as number);
		if (a === undefined) {
			throw new Error(
				`__dispatch invoked for orphan callId ${String(callId)}`
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
	await context.setGlobal('__dispatch', dispatch);

	const callable = await context.compileCallable(wrap(source));
	return {
		callable,
		callMap,
		context,
		isolate,
		nextCallId: 1,
		timeoutMs: config.timeout ?? 5000
	};
};

/**
 * Build a lazy runner for one mutation's sandboxed source. The first call
 * compiles the isolate + context + dispatch Reference + callable;
 * subsequent calls only generate a fresh callId, register the per-call
 * `actions` in the callMap, and invoke `callable.call([callId, args,
 * ctx])`. Per-call cost on FFI: one JSObjectCallAsFunction + three
 * cheap primitive packings. No per-call Reference allocation, no
 * setGlobal, no eval.
 *
 * Concurrency-safe by construction: each call has its own callId →
 * its own actions slot in the callMap.
 *
 * If the isolate has been disposed (timeout, memory cap), the next
 * call re-spawns transparently.
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
		const compiled = await getCompiled();
		const callId = compiled.nextCallId++;
		compiled.callMap.set(callId, actions);
		try {
			return await compiled.callable.call([callId, args, ctx], {
				timeout: compiled.timeoutMs
			});
		} finally {
			compiled.callMap.delete(callId);
		}
	};
};
