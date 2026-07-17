import {
	ABS_ATTRS,
	tracerOrNoop,
	type Span as TelemetrySpan,
	type TracerProvider as TelemetryTracerProvider
} from '@absolutejs/telemetry';

/**
 * Thrown by `broker.lease` when `acquireTimeoutMs` elapses before a
 * connection frees up. The tenant and timeout carry through so operators
 * can correlate timeouts to the tenant that starved.
 */
export class LeaseTimeoutError extends Error {
	readonly tenant: string;
	readonly timeoutMs: number;
	constructor(tenant: string, timeoutMs: number) {
		super(
			`Lease for tenant "${tenant}" timed out after ${timeoutMs}ms; ` +
				`the broker is at its connection caps and nothing freed up ` +
				`in time. Raise maxTotal/maxPerTenant or shed load upstream.`
		);
		this.name = 'LeaseTimeoutError';
		this.tenant = tenant;
		this.timeoutMs = timeoutMs;
	}
}

/**
 * Thrown by `broker.lease` after `drain()`/`dispose()` — the broker is
 * winding down and no new leases are issued. Queued waiters that were
 * still pending at `dispose()` are rejected with this too.
 */
export class ConnectionBrokerDrainedError extends Error {
	constructor() {
		super(
			'Connection broker is drained; no new leases are issued. ' +
				'Construct a fresh broker to resume leasing.'
		);
		this.name = 'ConnectionBrokerDrainedError';
	}
}

export type ConnectionBrokerOptions<Conn> = {
	/**
	 * Whether idle connections may be reused by every tenant or only by the
	 * tenant that created them. Use `tenant` for BYO credentials or databases.
	 * In tenant mode `maxTotal` caps physical active + idle connections.
	 */
	affinity?: 'shared' | 'tenant';
	/**
	 * Open one upstream connection. Called lazily — only when a lease needs
	 * a connection and the idle pool is empty. The broker never imports a
	 * DB driver; bring postgres-js, `Bun.sql`, or anything else.
	 */
	create: (tenant: string) => Promise<Conn> | Conn;
	/**
	 * Global in-use cap — the whole point of the broker. One shard hosting
	 * many tenants shares this many upstream connections, total, no matter
	 * how many tenants lease concurrently. Required, must be > 0.
	 */
	maxTotal: number;
	/**
	 * Give up on a queued lease after this many milliseconds and reject
	 * with {@link LeaseTimeoutError}. Defaults to waiting forever.
	 */
	acquireTimeoutMs?: number;
	/**
	 * Close one upstream connection. Called for validation failures, idle
	 * releases, and `dispose()`. Defaults to dropping the reference.
	 */
	destroy?: (conn: Conn) => Promise<void> | void;
	/**
	 * Destroy pooled-idle connections that have sat unused for this many
	 * milliseconds, so a burst doesn't pin connections against the managed
	 * provider's limit forever. Swept on an unref'd timer and lazily on
	 * every `lease`. Defaults to keeping idle connections indefinitely.
	 */
	idleReleaseMs?: number;
	/**
	 * Per-tenant in-use cap, independent of `maxTotal` — one noisy tenant
	 * queues against its own budget instead of starving the shard.
	 * Defaults to no per-tenant cap.
	 */
	maxPerTenant?: number;
	/**
	 * Clock used for idle-age accounting. Injectable so idle-release tests
	 * run without real timers. Defaults to `Date.now`.
	 */
	now?: () => number;
	/**
	 * Called when `create`, `destroy`, or `validate` throws. The broker
	 * stays consistent either way (a failed create rejects that lease and
	 * frees the slot; a failed destroy drops the connection reference).
	 * Defaults to a no-op.
	 */
	onError?: (
		error: unknown,
		phase: 'create' | 'destroy' | 'validate'
	) => void;
	/**
	 * Optional OpenTelemetry tracer provider. When set, every lease is
	 * traced as a `sync.broker_lease` span carrying `abs.tenant` and the
	 * queue wait in milliseconds. Noop when unset.
	 */
	tracerProvider?: TelemetryTracerProvider;
	/**
	 * Health-check a pooled connection before reuse. Return false (or
	 * throw) and the broker destroys it and tries the next idle connection
	 * — or creates a fresh one — so a server-side idle disconnect never
	 * reaches a caller. Defaults to trusting pooled connections.
	 */
	validate?: (conn: Conn) => Promise<boolean> | boolean;
};

export type ConnectionLease<Conn> = {
	conn: Conn;
	/** Return the connection to the pool. Idempotent. */
	release: () => void;
};

export type ConnectionBrokerMetrics = {
	/** Connections currently leased out (or reserved for an in-flight create). */
	inUse: number;
	/** Connections sitting in the idle pool. */
	idle: number;
	/** Lease calls waiting for a connection to free up. */
	queued: number;
	/** In-use count per tenant — only tenants with at least one lease appear. */
	byTenant: Record<string, number>;
	cumulative: {
		leases: number;
		releases: number;
		timeouts: number;
		created: number;
		destroyed: number;
		validationFailures: number;
	};
};

export type ConnectionBroker<Conn> = {
	/**
	 * Lease a connection for a tenant. Resolves immediately when under
	 * both caps (reusing the most-recently-released idle connection first,
	 * for cache warmth); otherwise queues FIFO until a release frees the
	 * cap that blocked it.
	 */
	lease: (tenant: string) => Promise<ConnectionLease<Conn>>;
	/** Lease, run `fn`, release in `finally` — even when `fn` throws. */
	withLease: <Result>(
		tenant: string,
		fn: (conn: Conn) => Promise<Result> | Result
	) => Promise<Result>;
	/** Operator-shaped snapshot, same spirit as `engine.metrics()`. */
	metrics: () => ConnectionBrokerMetrics;
	/**
	 * Stop issuing new leases ({@link ConnectionBrokerDrainedError}) and
	 * resolve once every outstanding lease has been released and the queue
	 * has drained through. Follow with `dispose()` to close idle
	 * connections.
	 */
	drain: () => Promise<void>;
	/**
	 * Drain-if-needed, reject queued waiters, destroy every idle
	 * connection, and stop the sweep timer. Connections still leased out
	 * are destroyed on their release instead of returning to the pool.
	 */
	dispose: () => Promise<void>;
};

type IdleEntry<Conn> = {
	conn: Conn;
	idleSince: number;
	tenant: string;
};

type Waiter<Conn> = {
	enqueuedAt: number;
	reject: (error: unknown) => void;
	resolve: (lease: ConnectionLease<Conn>) => void;
	span: TelemetrySpan;
	tenant: string;
	timer: ReturnType<typeof setTimeout> | undefined;
};

const QUEUE_WAIT_ATTR = 'abs.broker.queue_wait_ms';

/**
 * A generic connection lease broker: multiplex ONE upstream connection
 * budget across many tenants.
 *
 * The BYO-Postgres problem this solves: one shard hosting 50 customers,
 * each spawning its own PG pool, instantly exceeds a managed provider's
 * connection limit. Instead, give every tenant `broker.lease(tenant)`
 * over a single caller-supplied `create`/`destroy` pair — the broker
 * enforces a global in-use cap (`maxTotal`), optional per-tenant budgets
 * (`maxPerTenant`), FIFO queueing with timeouts when at cap, LIFO idle
 * reuse for cache warmth, and idle harvesting (`idleReleaseMs`) so a
 * burst doesn't pin connections forever.
 *
 * The broker never imports a DB driver — `Conn` is whatever `create`
 * returns (a postgres-js `sql`, a `Bun.sql`, an HTTP client, …).
 */
export const createConnectionBroker = <Conn>(
	options: ConnectionBrokerOptions<Conn>
): ConnectionBroker<Conn> => {
	if (!Number.isFinite(options.maxTotal) || options.maxTotal <= 0) {
		throw new RangeError(
			`createConnectionBroker requires maxTotal > 0 (got ${options.maxTotal})`
		);
	}
	const now = options.now ?? Date.now;
	const affinity = options.affinity ?? 'shared';
	const tracer = tracerOrNoop(options.tracerProvider, '@absolutejs/sync');

	// LIFO stack (push/pop at the end) — the most-recently-released
	// connection is reused first, so its driver-side caches stay warm.
	// Push order is time order, so index 0 is always the stalest entry.
	const idle: IdleEntry<Conn>[] = [];
	// One FIFO queue for both caps: on every release the broker scans from
	// the front and grants the first waiter whose caps now have room, so a
	// per-tenant-blocked waiter never holds up a differently-blocked one.
	const queue: Waiter<Conn>[] = [];
	const inUseByTenant = new Map<string, number>();
	let inUse = 0;
	let drained = false;
	let disposed = false;
	let drainPromise: Promise<void> | undefined;
	let resolveDrain: (() => void) | undefined;

	const cumulative = {
		created: 0,
		destroyed: 0,
		leases: 0,
		releases: 0,
		timeouts: 0,
		validationFailures: 0
	};

	const destroyConn = async (conn: Conn) => {
		cumulative.destroyed += 1;
		try {
			await options.destroy?.(conn);
		} catch (error) {
			options.onError?.(error, 'destroy');
		}
	};

	// Harvest idle connections older than idleReleaseMs. Runs on the
	// unref'd interval below and lazily on every lease, so injected-`now`
	// tests can advance the clock and trigger a sweep with a plain lease.
	const sweepIdle = () => {
		const idleReleaseMs = options.idleReleaseMs;
		if (idleReleaseMs === undefined) return;
		const cutoff = now() - idleReleaseMs;
		while (idle.length > 0 && idle[0] !== undefined) {
			const oldest = idle[0];
			if (oldest.idleSince > cutoff) break;
			idle.shift();
			void destroyConn(oldest.conn);
		}
	};

	const sweepTimer =
		options.idleReleaseMs === undefined
			? undefined
			: setInterval(sweepIdle, options.idleReleaseMs);
	sweepTimer?.unref?.();

	const canGrant = (tenant: string) =>
		inUse < options.maxTotal &&
		(options.maxPerTenant === undefined ||
			(inUseByTenant.get(tenant) ?? 0) < options.maxPerTenant);

	// In-use counts are reserved BEFORE the (async) create/validate runs,
	// so concurrent leases can never oversubscribe the caps.
	const reserve = (tenant: string) => {
		inUse += 1;
		inUseByTenant.set(tenant, (inUseByTenant.get(tenant) ?? 0) + 1);
	};

	const unreserve = (tenant: string) => {
		inUse -= 1;
		const count = inUseByTenant.get(tenant) ?? 0;
		if (count <= 1) inUseByTenant.delete(tenant);
		else inUseByTenant.set(tenant, count - 1);
		if (drained && inUse === 0 && queue.length === 0) {
			resolveDrain?.();
		}
	};

	// Pop idle connections (validating each when configured) until one
	// passes, else open a fresh one. The caller has already reserved a
	// slot, so a create here can never breach the caps.
	const takeIdle = (tenant: string) => {
		if (affinity === 'shared') return idle.pop();
		for (let index = idle.length - 1; index >= 0; index -= 1) {
			const entry = idle[index];
			if (entry?.tenant !== tenant) continue;
			idle.splice(index, 1);

			return entry;
		}

		return undefined;
	};
	const acquireConn = async (tenant: string): Promise<Conn> => {
		let entry = takeIdle(tenant);
		while (entry !== undefined) {
			if (options.validate === undefined) return entry.conn;
			let healthy = false;
			try {
				healthy = await options.validate(entry.conn);
			} catch (error) {
				options.onError?.(error, 'validate');
			}
			if (healthy) return entry.conn;
			cumulative.validationFailures += 1;
			await destroyConn(entry.conn);
			entry = takeIdle(tenant);
		}
		if (affinity === 'tenant')
			while (inUse + idle.length > options.maxTotal) {
				const displaced = idle.shift();
				if (displaced === undefined) break;
				await destroyConn(displaced.conn);
			}
		try {
			const conn = await options.create(tenant);
			cumulative.created += 1;
			return conn;
		} catch (error) {
			options.onError?.(error, 'create');
			throw error;
		}
	};

	const endSpanWithError = (span: TelemetrySpan, error: unknown) => {
		const spanError =
			error instanceof Error ? error : new Error(String(error));
		span.recordException(spanError);
		span.setStatus({
			code: 2 /* SpanStatusCode.ERROR */,
			message: spanError.message
		});
		span.end();
	};

	const makeLease = (tenant: string, conn: Conn): ConnectionLease<Conn> => {
		cumulative.leases += 1;
		let released = false;
		return {
			conn,
			release: () => {
				// Idempotent: a double release must not corrupt the counts
				// or hand the same connection to two callers.
				if (released) return;
				released = true;
				cumulative.releases += 1;
				if (disposed) {
					void destroyConn(conn);
				} else {
					idle.push({ conn, idleSince: now(), tenant });
				}
				unreserve(tenant);
				pump();
			}
		};
	};

	// Deliver a connection to a waiter whose slot is already reserved.
	const grantTo = async (waiter: Waiter<Conn>) => {
		try {
			const conn = await acquireConn(waiter.tenant);
			waiter.span.setAttribute(
				QUEUE_WAIT_ATTR,
				now() - waiter.enqueuedAt
			);
			waiter.span.end();
			waiter.resolve(makeLease(waiter.tenant, conn));
		} catch (error) {
			unreserve(waiter.tenant);
			endSpanWithError(waiter.span, error);
			waiter.reject(error);
			pump();
		}
	};

	// Scan the FIFO queue from the front and grant every waiter whose caps
	// now have room. Reservation is synchronous, so one release hands its
	// slot to exactly one waiter.
	const pump = () => {
		if (disposed) return;
		let index = 0;
		while (index < queue.length) {
			const waiter = queue[index];
			if (waiter === undefined) break;
			if (!canGrant(waiter.tenant)) {
				index += 1;
				continue;
			}
			queue.splice(index, 1);
			if (waiter.timer !== undefined) clearTimeout(waiter.timer);
			reserve(waiter.tenant);
			void grantTo(waiter);
		}
	};

	const rejectQueued = (error: () => Error) => {
		const waiting = queue.splice(0, queue.length);
		for (const waiter of waiting) {
			if (waiter.timer !== undefined) clearTimeout(waiter.timer);
			const rejection = error();
			endSpanWithError(waiter.span, rejection);
			waiter.reject(rejection);
		}
	};

	const lease = async (tenant: string): Promise<ConnectionLease<Conn>> => {
		if (drained) throw new ConnectionBrokerDrainedError();
		sweepIdle();
		const span = tracer.startSpan('sync.broker_lease', {
			attributes: { [ABS_ATTRS.tenant]: tenant }
		});
		const startedAt = now();
		if (canGrant(tenant)) {
			reserve(tenant);
			try {
				const conn = await acquireConn(tenant);
				span.setAttribute(QUEUE_WAIT_ATTR, now() - startedAt);
				span.end();
				return makeLease(tenant, conn);
			} catch (error) {
				unreserve(tenant);
				endSpanWithError(span, error);
				pump();
				throw error;
			}
		}
		return new Promise<ConnectionLease<Conn>>((resolve, reject) => {
			const waiter: Waiter<Conn> = {
				enqueuedAt: startedAt,
				reject,
				resolve,
				span,
				tenant,
				timer: undefined
			};
			if (options.acquireTimeoutMs !== undefined) {
				const timeoutMs = options.acquireTimeoutMs;
				waiter.timer = setTimeout(() => {
					const position = queue.indexOf(waiter);
					if (position === -1) return;
					queue.splice(position, 1);
					cumulative.timeouts += 1;
					const timeout = new LeaseTimeoutError(tenant, timeoutMs);
					endSpanWithError(span, timeout);
					reject(timeout);
				}, timeoutMs);
			}
			queue.push(waiter);
		});
	};

	const drain = () => {
		drained = true;
		if (drainPromise === undefined) {
			drainPromise =
				inUse === 0 && queue.length === 0
					? Promise.resolve()
					: new Promise<void>((resolve) => {
							resolveDrain = resolve;
						});
		}
		return drainPromise;
	};

	return {
		dispose: async () => {
			drained = true;
			disposed = true;
			if (sweepTimer !== undefined) clearInterval(sweepTimer);
			rejectQueued(() => new ConnectionBrokerDrainedError());
			const pooled = idle.splice(0, idle.length);
			await Promise.all(pooled.map((entry) => destroyConn(entry.conn)));
			if (inUse === 0) resolveDrain?.();
		},
		drain,
		lease,
		metrics: () => ({
			byTenant: Object.fromEntries(inUseByTenant),
			cumulative: { ...cumulative },
			idle: idle.length,
			inUse,
			queued: queue.length
		}),
		withLease: async (tenant, fn) => {
			const { conn, release } = await lease(tenant);
			try {
				return await fn(conn);
			} finally {
				release();
			}
		}
	};
};
