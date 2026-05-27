/**
 * Mutation retry policy — opt-in conflict retry for `defineMutation`. Convex
 * auto-retries every mutation on OCC conflict because their handlers are pure
 * deterministic functions; sync handlers are arbitrary user code, so retry is
 * opt-in and the user owns the idempotency contract. When a handler throws a
 * classified-as-retryable error (default: PG serialization failures, codes
 * 40001 / 40P01), the engine discards the buffered changes, awaits a backoff,
 * and re-runs the handler with a fresh transaction.
 */

/** What the engine receives + uses to schedule the next attempt. */
export type RetryPolicy = {
	/** Inclusive max attempts (1 = no retry). Defaults to 5. */
	maxAttempts?: number;
	/** Compute the delay before attempt `n+1` (n is 1-indexed). */
	backoff?: (attempt: number) => number;
	/**
	 * Return `true` for errors that should trigger a retry. Defaults to
	 * {@link isSerializationFailure} — PG `40001` (serialization_failure) +
	 * `40P01` (deadlock_detected). Override to add MySQL `1213`, app-level
	 * "version conflict" sentinels, CRDT merge conflicts, etc.
	 */
	isRetryable?: (error: unknown) => boolean;
	/**
	 * Hard ceiling on total elapsed time across all attempts (ms). A
	 * still-failing mutation past this gives up regardless of `maxAttempts`.
	 * Defaults to 30_000.
	 */
	maxElapsedMs?: number;
};

const PG_RETRY_CODES = new Set(['40001', '40P01']);

/**
 * Default retryability predicate — Postgres serialization_failure (`40001`)
 * and deadlock_detected (`40P01`). `postgres` (porsager) and `pg` both expose
 * the SQLSTATE on `error.code`. We also accept the codes on a wrapping
 * `error.cause` to be friendly to libraries that wrap errors.
 */
export const isSerializationFailure = (error: unknown): boolean => {
	if (error === null || typeof error !== 'object') return false;
	const code = (error as { code?: unknown }).code;
	if (typeof code === 'string' && PG_RETRY_CODES.has(code)) return true;
	const cause = (error as { cause?: unknown }).cause;
	if (cause !== undefined) return isSerializationFailure(cause);

	return false;
};

export type ExponentialBackoffOptions = {
	/** Initial delay (ms). Defaults to 25. */
	baseMs?: number;
	/** Multiplier per attempt. Defaults to 2. */
	factor?: number;
	/** Hard cap on a single delay (ms). Defaults to 1000. */
	maxMs?: number;
	/** Random jitter as a fraction of the computed delay. Defaults to 0.2. */
	jitter?: number;
};

/**
 * Exponential backoff with optional jitter — matches the shape used in
 * `@absolutejs/queue` so a user who's already learned one knows the other.
 *
 * @example
 * defineMutation({
 *   name: 'transfer',
 *   retry: { maxAttempts: 5, backoff: exponentialBackoff() },
 *   handler: async (args, ctx, actions) => { ... }
 * });
 */
export const exponentialBackoff =
	(options: ExponentialBackoffOptions = {}) =>
	(attempt: number): number => {
		const base = options.baseMs ?? 25;
		const factor = options.factor ?? 2;
		const max = options.maxMs ?? 1000;
		const jitter = options.jitter ?? 0.2;
		const raw = Math.min(max, base * factor ** Math.max(0, attempt - 1));
		const spread = raw * jitter;

		return raw + (Math.random() * 2 - 1) * spread;
	};

/** Internal: thrown when retries exhaust without success. Re-throws the
 * underlying error so the caller's existing error handling still works; the
 * `RetriesExhaustedError` form is only used inside the engine for typing. */
export class RetriesExhaustedError extends Error {
	readonly attempts: number;
	readonly elapsedMs: number;
	readonly cause: unknown;
	constructor(attempts: number, elapsedMs: number, cause: unknown) {
		const message = cause instanceof Error ? cause.message : String(cause);
		super(
			`retries exhausted after ${attempts} attempts (${elapsedMs}ms): ${message}`
		);
		this.name = 'RetriesExhaustedError';
		this.attempts = attempts;
		this.elapsedMs = elapsedMs;
		this.cause = cause;
	}
}
