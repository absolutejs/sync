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
 * - First call per mutation pays an isolated-jsc runner warmup + callable
 *   compile. Every subsequent call is served from the runner's keyed
 *   callable cache — no per-call eval, no per-call `setGlobal`.
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
 * **Per-call hot path (since isolated-jsc 0.8).** Each mutation owns a
 * `createIsolatedRunner()` instance. The runner applies the `tenant-script`
 * policy, keeps a keyed isolate pool, and caches the wrapped mutation as a
 * precompiled callable. Per call we invoke `runner.call(name, source, args)`
 * with a call id that routes `actions.*` back through a host Reference.
 *
 * The runner is built lazily per-mutation: nothing is spawned until the
 * mutation actually runs for the first time. No engine teardown hook is
 * needed — the OS reaps the workers when the engine's host process exits.
 */

// Type-only import — erased at runtime, so the engine still loads when
// `@absolutejs/isolated-jsc` isn't installed. The actual module is loaded
// lazily via dynamic `import()` inside `loadIsolatedJsc`.
import type { IsolatedRunner, RunMetrics } from '@absolutejs/isolated-jsc';
import type { MutationActions } from './mutation';

type IsolatedJscModule = typeof import('@absolutejs/isolated-jsc');

/**
 * Per-call metrics record emitted by `sandboxedHandler` when the engine
 * is configured with {@link SyncEngineOptions.handlerMetrics}. One record
 * per invocation, fired AFTER the call completes (success or failure).
 *
 * Use this for per-tenant dashboards ("which tenant is burning the most
 * CPU?"), runtime alerting ("this handler is timing out repeatedly"),
 * cost attribution, and post-mortem replay of slow / failed mutations.
 *
 * Sample wiring pattern — publish to a sync collection users can
 * subscribe to like any other:
 *
 * ```ts
 * const engine = createSyncEngine({
 *   handlerMetrics: (record) => {
 *     metricsCollection.insert(record);  // your own collection / sink
 *   },
 * });
 * ```
 */
export type HandlerMetricsRecord = {
	/** Globally-unique id for this call (random). Useful as a join key. */
	id: string;
	/** Name passed to `defineMutation`. */
	mutationName: string;
	/** Wall-clock duration from call entry to result resolution (ms). */
	durationMs: number;
	/**
	 * CPU time spent inside the JSC sandbox (ms). Comes from
	 * isolated-jsc runner metrics — does NOT include host-side message-passing
	 * overhead on the Worker backend. Sub-millisecond runs round to 0.
	 */
	cpuMs: number;
	/** isolated-jsc backend that executed the call. */
	backend?: 'ffi' | 'worker';
	/**
	 * Heap size (bytes) measured immediately after the script returned.
	 * Not the run's peak — a true peak needs continuous polling.
	 */
	heapBytes: number;
	/** `true` if the handler returned normally; `false` if it threw. */
	ok: boolean;
	/** Error name (`TimeoutError`, `MemoryLimitError`, `Error`, …) on failure. */
	errorName?: string;
	/** Error message on failure. */
	errorMessage?: string;
	/** `Date.now()` at the moment the call ended. */
	timestamp: number;
};

/**
 * Per-call hook invoked once each `sandboxedHandler` invocation finishes
 * (success or failure). Synchronous return is the common case; an async
 * return is awaited but its rejection is swallowed (a metrics hook that
 * crashes must NOT also crash the caller's mutation path).
 */
export type HandlerMetricsHook = (
	record: HandlerMetricsRecord
) => void | Promise<void>;

/**
 * Per-host configuration for an entry in {@link BridgeFetchConfig}.
 * Auth is computed on the host side per call; the secret never enters
 * the sandbox's JSC heap.
 */
export type BridgeFetchEndpoint = {
	/**
	 * Header values to add to every request to this host. Static — read
	 * once at engine construction. Use {@link authorization} for tokens
	 * that need to be computed per call.
	 */
	headers?: Record<string, string>;
	/**
	 * Compute the `Authorization` header value on each call. Synchronous
	 * or async. Throwing rejects the in-sandbox call without revealing
	 * the underlying error (the sandbox sees a generic
	 * "authorization callback failed").
	 */
	authorization?: () => string | Promise<string>;
};

/**
 * `actions.fetch(url, init)` allowlist + auth-injection config keyed by
 * hostname. A request whose URL parses to a hostname NOT in this map is
 * rejected before any network call. A request whose hostname IS in the
 * map gets the configured static headers + the computed authorization
 * stitched in on the host side. The sandbox source never sees the auth
 * value.
 *
 * Hostname keys are exact (`'api.example.com'`). The special key `'*'`
 * is a wildcard (use sparingly — it disables allowlisting).
 *
 * ```ts
 * createSyncEngine({
 *   bridgeFetch: {
 *     'api.stripe.com': {
 *       authorization: () => `Bearer ${process.env.STRIPE_KEY}`,
 *     },
 *     'api.openai.com': {
 *       authorization: () => `Bearer ${process.env.OPENAI_KEY}`,
 *       headers: { 'OpenAI-Beta': 'assistants=v2' },
 *     },
 *   },
 * });
 * ```
 */
export type BridgeFetchConfig = Record<string, BridgeFetchEndpoint>;

/**
 * Response shape `actions.fetch` resolves to inside the sandbox. The
 * body is materialised as text on the host (so it crosses the JSC
 * boundary as a structured-cloned string). Users parse it themselves
 * with `JSON.parse(res.body)` for JSON responses.
 */
export type BridgeFetchResponse = {
	ok: boolean;
	status: number;
	statusText: string;
	url: string;
	headers: Record<string, string>;
	body: string;
};

/** Per-mutation sandbox configuration. */
export type SandboxConfig = {
	/** Heap memory cap (MB). Default 32. */
	memoryLimit?: number;
	/** Wall-clock cap per call (ms). Default 5000. */
	timeout?: number;
	/**
	 * isolated-jsc backend. Defaults to `'auto'` (FFI when libJSC is
	 * reachable, Worker otherwise). Both backends now run through the same
	 * `createIsolatedRunner().call()` hot path; the choice trades cold spawn
	 * (FFI wins ~6×) against Web API availability (Worker only).
	 *
	 * Pin to `'worker'` if your handler needs Web APIs (`URL`,
	 * `TextEncoder`, `WebSocket`) — those live in the Bun-Worker
	 * environment, not the bare JSC C API.
	 *
	 * Pin to `'ffi'` to bypass the auto-probe when you know libJSC is
	 * reachable (e.g. CI with a known image).
	 */
	backend?: 'auto' | 'ffi' | 'worker';
	/**
	 * **Escape hatch.** Map of host functions the sandboxed handler may
	 * call as `unsafeHost.fnName(...args)`. The name is deliberately
	 * loud: anyone reading the source must see immediately that a
	 * sandboxed mutation is reaching through to non-deterministic host
	 * code (third-party API, queue push, email send, anything that
	 * touches the outside world).
	 *
	 * Without this option, the sandbox is hermetic — only `args`,
	 * `ctx`, and `actions` are reachable. Declare an entry here, name
	 * it visibly (e.g. `chargeStripe`, `sendSlackPing`), and the engine
	 * exposes it on the sandbox-side `unsafeHost` Proxy. The fn runs on
	 * the host; its return value is structured-cloned back across the
	 * isolate boundary; thrown errors propagate into the sandbox as
	 * normal JS errors the handler can catch.
	 *
	 * The deterministic-mutation guarantees stop at the call site —
	 * retries WILL re-fire these host fns (they're outside the
	 * transaction). Treat them as side effects and either make them
	 * idempotent or pair them with explicit compensation in the
	 * handler. Convex's actions are the same model; this is the same
	 * trade.
	 *
	 * @example
	 * ```ts
	 * defineMutation({
	 *   name: 'payments:checkout',
	 *   sandboxedHandler: `async (args, ctx, actions, unsafeHost) => {
	 *     const order = await actions.insert('orders', { ...args, status: 'pending' });
	 *     const receipt = await unsafeHost.chargeStripe({
	 *       amount: order.amount,
	 *       token: args.token,
	 *     });
	 *     await actions.update('orders', { id: order.id, status: 'paid', receipt });
	 *     return order;
	 *   }`,
	 *   sandbox: {
	 *     unsafeHost: {
	 *       chargeStripe: ({ amount, token }) =>
	 *         stripe.charges.create({ amount, source: token }),
	 *     },
	 *   },
	 * });
	 * ```
	 */
	// `any[]` (not `unknown[]`) for ergonomics — consumers write typed
	// fns like `(input: { amount: number }) => …` and would otherwise
	// have to cast inside every body to satisfy contravariance. The
	// sandbox-side dispatch doesn't enforce arg types anyway (they
	// cross structured-clone); validate inside the fn.
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	unsafeHost?: Record<string, (...args: any[]) => unknown | Promise<unknown>>;
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
 * Wrap user source as a function expression for `runner.call`. The compiled
 * function takes `(__callId, args, ctx)` — `__callId` keys the
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
				'sandboxedHandler must evaluate to (args, ctx, actions, unsafeHost) => result; got ' +
					typeof userFn
			);
		}
		const actions = {
			insert: (table, data) => __dispatch(__callId, 'insert', table, data),
			update: (table, data) => __dispatch(__callId, 'update', table, data),
			delete: (table, row) => __dispatch(__callId, 'delete', table, row),
			change: (collection, change) => __dispatch(__callId, 'change', collection, change),
			now: () => __dispatch(__callId, 'now'),
			fetch: (url, init) => __dispatch(__callId, 'fetch', url, init)
		};
		// Escape hatch — host fns the mutation explicitly opted in to.
		// The Proxy means every property access is a host call; the
		// engine throws if the property name isn't declared in the
		// mutation's sandbox.unsafeHost map.
		const unsafeHost = new Proxy({}, {
			get: (_target, fnName) => {
				if (typeof fnName !== 'string') return undefined;
				return (...callArgs) =>
					__dispatch(__callId, 'unsafeHost', fnName, callArgs);
			}
		});
		return userFn(args, ctx, actions, unsafeHost);
	}
`;

type CompiledMutation = {
	/** Per-call actions instances, keyed by callId. Lives for the
	 * duration of each call. */
	callMap: Map<number, MutationActions>;
	nextCallId: number;
	runner: IsolatedRunner;
	source: string;
	timeoutMs: number;
};

const compile = async (
	source: string,
	config: SandboxConfig,
	bridgeFetch: BridgeFetchConfig | undefined
): Promise<CompiledMutation> => {
	const { Reference, createIsolatedRunner, resolveIsolatePolicy } =
		await loadIsolatedJsc();

	// Dispatch installed ONCE per runner context as a global. Closes over the
	// per-mutation callMap; each in-VM `actions.*` call hands its
	// callId back so we look up the right `actions` instance. This is
	// concurrent-safe: every call has its own callId → its own slot,
	// no shared-mutable-state races.
	const callMap = new Map<number, MutationActions>();
	const unsafeHost = config.unsafeHost;
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
			case 'now':
				return a.now();
			case 'fetch':
				return runBridgeFetch(
					bridgeFetch,
					rest[0] as string,
					rest[1] as RequestInit | undefined
				);
			case 'unsafeHost': {
				const fnName = rest[0] as string;
				const callArgs = (rest[1] as unknown[]) ?? [];
				if (
					unsafeHost === undefined ||
					typeof unsafeHost[fnName] !== 'function'
				) {
					throw new Error(
						`sandboxedHandler called unsafeHost.${fnName}() but it was not declared in the mutation's sandbox.unsafeHost config. Declare it (and only the host fns you intend to expose) to opt in to the escape hatch.`
					);
				}
				return unsafeHost[fnName](...callArgs);
			}
			default:
				throw new Error(`unknown sandbox action op: ${String(op)}`);
		}
	}) as (...rawArgs: unknown[]) => unknown);

	const timeoutMs = config.timeout ?? 5000;
	const sourceToCall = wrap(source);
	const policy = resolveIsolatePolicy('tenant-script', {
		allowWorkerFallback: true,
		backend: config.backend ?? 'auto',
		memoryLimit: config.memoryLimit ?? 32,
		timeout: timeoutMs
	});
	const runner = createIsolatedRunner({
		globals: { __dispatch: dispatch },
		policy
	});
	await runner.precompile('sandboxedHandler', sourceToCall);

	return {
		callMap,
		nextCallId: 1,
		runner,
		source: sourceToCall,
		timeoutMs
	};
};

/**
 * Host-side implementation of `actions.fetch(url, init)`. Enforces the
 * `BridgeFetchConfig` allowlist (rejecting otherwise-unknown hostnames
 * before any network call), computes the authorization on the host
 * side (so the secret never crosses into JSC), and returns a
 * structured-cloneable {@link BridgeFetchResponse} the sandbox can
 * pick apart.
 */
const runBridgeFetch = async (
	config: BridgeFetchConfig | undefined,
	url: string,
	init: RequestInit | undefined
): Promise<BridgeFetchResponse> => {
	if (config === undefined) {
		throw new Error(
			'actions.fetch called but the engine has no `bridgeFetch` config — ' +
				'pass `bridgeFetch: { ... }` to createSyncEngine.'
		);
	}
	let parsed: URL;
	try {
		parsed = new URL(url);
	} catch {
		throw new Error(`actions.fetch: invalid URL "${String(url)}"`);
	}
	const endpoint =
		config[parsed.hostname] ??
		(Object.prototype.hasOwnProperty.call(config, '*')
			? config['*']
			: undefined);
	if (endpoint === undefined) {
		throw new Error(
			`actions.fetch: hostname "${parsed.hostname}" is not allowlisted in bridgeFetch config`
		);
	}
	const headers: Record<string, string> = { ...(endpoint.headers ?? {}) };
	// Carry user-supplied headers from the sandbox last so an explicit
	// override wins — except we never let the sandbox set Authorization,
	// because that would reveal the auth pattern the host injected.
	if (init?.headers !== undefined) {
		const incoming = init.headers as Record<string, string>;
		for (const [name, value] of Object.entries(incoming)) {
			if (name.toLowerCase() === 'authorization') continue;
			headers[name] = value;
		}
	}
	if (endpoint.authorization !== undefined) {
		let auth: string;
		try {
			auth = await endpoint.authorization();
		} catch {
			throw new Error('actions.fetch: authorization callback failed');
		}
		headers.Authorization = auth;
	}
	const response = await fetch(url, { ...init, headers });
	const responseHeaders: Record<string, string> = {};
	response.headers.forEach((value, name) => {
		responseHeaders[name] = value;
	});
	const body = await response.text();
	return {
		body,
		headers: responseHeaders,
		ok: response.ok,
		status: response.status,
		statusText: response.statusText,
		url: response.url
	};
};

/**
 * Build a lazy runner for one mutation's sandboxed source. The first call
 * compiles the runner + dispatch Reference + callable;
 * subsequent calls only generate a fresh callId, register the per-call
 * `actions` in the callMap, and invoke `runner.call(...)`. Per-call cost on
 * FFI: one JSObjectCallAsFunction + three cheap primitive packings. No
 * per-call Reference allocation, no setGlobal, no eval.
 *
 * Concurrency-safe by construction: each call has its own callId →
 * its own actions slot in the callMap.
 *
 * If the isolate has been disposed (timeout, memory cap), the next
 * call re-spawns transparently.
 */
export const makeSandboxedHandler = (
	source: string,
	config: SandboxConfig = {},
	/**
	 * Engine-level extras the per-mutation config doesn't carry:
	 *  - `metricsHook` enables per-call telemetry via
	 *    `runner.call(..., { withMetrics: true })` (small cost; off without
	 *    the hook).
	 *  - `bridgeFetch` enables `actions.fetch(url, init)` inside the
	 *    sandbox with host-side allowlist + auth injection.
	 */
	engineExtras?: {
		metricsHook?: {
			mutationName: string;
			onMetrics: HandlerMetricsHook;
		};
		bridgeFetch?: BridgeFetchConfig;
	}
): ((
	args: unknown,
	ctx: unknown,
	actions: MutationActions
) => Promise<unknown>) => {
	let pending: Promise<CompiledMutation> | undefined;
	const metricsHook = engineExtras?.metricsHook;
	const bridgeFetch = engineExtras?.bridgeFetch;

	const getCompiled = async (): Promise<CompiledMutation> => {
		if (pending !== undefined) {
			return pending;
		}
		pending = compile(source, config, bridgeFetch);
		return pending;
	};

	return async (args, ctx, actions) => {
		const compiled = await getCompiled();
		const callId = compiled.nextCallId++;
		compiled.callMap.set(callId, actions);

		// Fast path: no metrics hook → no per-call overhead.
		if (metricsHook === undefined) {
			try {
				return await compiled.runner.call(
					'sandboxedHandler',
					compiled.source,
					[callId, args, ctx],
					{ run: { timeout: compiled.timeoutMs } }
				);
			} catch (error) {
				if (isIsolateDisposalError(error)) {
					pending = undefined;
					await disposeCompiled(compiled);
				}
				throw error;
			} finally {
				compiled.callMap.delete(callId);
			}
		}

		// Metrics path: switch to runner metrics so we get backend, cpuMs,
		// and heapBytes from the isolate side. Errors get a synthesized record
		// before re-throwing so the caller still sees the error.
		const startedAt = performance.now();
		const id = makeRandomId();
		try {
			const { result, metrics } = await compiled.runner.call(
				'sandboxedHandler',
				compiled.source,
				[callId, args, ctx],
				{ run: { timeout: compiled.timeoutMs }, withMetrics: true }
			);
			fireMetrics(metricsHook.onMetrics, {
				backend: metrics.backend,
				cpuMs: metrics.cpuMs,
				durationMs: performance.now() - startedAt,
				heapBytes: metrics.heapBytes,
				id,
				mutationName: metricsHook.mutationName,
				ok: true,
				timestamp: Date.now()
			});
			return result;
		} catch (error) {
			if (isIsolateDisposalError(error)) {
				pending = undefined;
				await disposeCompiled(compiled);
			}
			fireMetrics(metricsHook.onMetrics, {
				cpuMs: 0,
				durationMs: performance.now() - startedAt,
				errorMessage:
					error instanceof Error ? error.message : String(error),
				errorName: error instanceof Error ? error.name : 'Error',
				heapBytes: 0,
				id,
				mutationName: metricsHook.mutationName,
				ok: false,
				timestamp: Date.now()
			});
			throw error;
		} finally {
			compiled.callMap.delete(callId);
		}
	};
};

const makeRandomId = (): string =>
	`hm_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`;

const isIsolateDisposalError = (error: unknown): boolean =>
	error instanceof Error &&
	(error.name === 'TimeoutError' ||
		error.name === 'MemoryLimitError' ||
		error.name === 'IsolateDisposedError');

const disposeCompiled = async (compiled: CompiledMutation): Promise<void> => {
	try {
		await compiled.runner.dispose();
	} catch {
		// The isolate/pool may already have been torn down by the failing run.
	}
};

const fireMetrics = (
	hook: HandlerMetricsHook,
	record: HandlerMetricsRecord
): void => {
	let outcome: void | Promise<void>;
	try {
		outcome = hook(record);
	} catch {
		// Hook threw synchronously — swallow. The caller's mutation must
		// not fail because the metrics sink failed.
		return;
	}
	if (outcome instanceof Promise) {
		outcome.catch(() => {
			// Async rejection — same policy: swallow.
		});
	}
};
