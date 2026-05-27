import type { CollectionContext } from './collection';
import type { RetryPolicy } from './retry';
import type { SandboxConfig } from './sandbox';
import type { RowChange } from './types';

/**
 * How to persist a table — register one with {@link SyncEngine.registerWriter} so
 * `insert`/`update`/`delete` on the mutation actions write to your store (any
 * ORM) and emit the live change in one step. Each function returns the stored
 * row so the emitted diff carries DB-generated fields (ids, timestamps).
 *
 * The third argument is the transaction handle from the engine's
 * {@link TransactionRunner} (or `undefined` if none is configured) — write
 * through it so a mutation's writes commit all-or-nothing.
 */
export type TableWriter<Row = any, Ctx = unknown, Tx = unknown> = {
	insert: (data: any, ctx: Ctx, tx: Tx) => Promise<Row> | Row;
	update: (data: any, ctx: Ctx, tx: Tx) => Promise<Row> | Row;
	/** Persist the delete; receives the row/identifier passed to `actions.delete`. */
	delete: (row: any, ctx: Ctx, tx: Tx) => Promise<unknown> | unknown;
};

/**
 * Runs a function inside your database's transaction, threading the transaction
 * handle to each {@link TableWriter}, so a mutation's writes commit
 * all-or-nothing and the engine emits its diff only after the commit. Configure
 * it on {@link createSyncEngine}. Examples:
 *
 *   `(run) => db.transaction(run)`        // Drizzle
 *   `(run) => prisma.$transaction(run)`   // Prisma
 */
export type TransactionRunner = <R>(
	run: (tx: unknown) => Promise<R>
) => Promise<R>;

/**
 * Actions a mutation handler uses to write and publish changes.
 *
 * Prefer `insert`/`update`/`delete`: they persist via the table's registered
 * {@link TableWriter} and emit the live change in one fused call, so you can't
 * forget to go live (and the change always reflects the stored row). `change` is
 * the lower-level escape hatch for when you wrote some other way.
 */
export type MutationActions = {
	/** Persist a new row to `table` and emit it. Returns the stored row. */
	insert: <Row = unknown>(table: string, data: unknown) => Promise<Row>;
	/** Persist an update to `table` and emit it. Returns the stored row. */
	update: <Row = unknown>(table: string, data: unknown) => Promise<Row>;
	/** Persist a delete to `table` (pass the row or its key) and emit it. */
	delete: (table: string, row: unknown) => Promise<void>;
	/** Escape hatch: emit a change you persisted yourself (no writer call). */
	change: <T>(collection: string, change: RowChange<T>) => Promise<void>;
	/**
	 * Wall-clock timestamp the handler should use instead of `Date.now()`.
	 * Returns a `number` (ms since epoch).
	 *
	 * Why an injected clock? Two forward-looking reasons:
	 *
	 *   1. **Replay / rebase determinism.** When the engine re-runs a
	 *      mutation against an updated state (Replicache-style mutator
	 *      replay), `actions.now()` returns the ORIGINAL call's timestamp
	 *      instead of the current wall clock. `Date.now()` would silently
	 *      diverge between client-optimistic and server-canonical runs;
	 *      `actions.now()` doesn't.
	 *
	 *   2. **Test determinism.** Test harnesses can pin time by passing a
	 *      custom `now()` through {@link MutationActions} — the handler
	 *      observes whatever the test wants.
	 *
	 * If the engine doesn't override it (the common case today), it just
	 * returns `Date.now()`. Use it everywhere you'd reach for
	 * `Date.now()` inside a mutation handler.
	 */
	now: () => number;
};

export type MutationHandler<Args, Ctx, Result> = (
	args: Args,
	ctx: Ctx,
	actions: MutationActions
) => Promise<Result> | Result;

export type MutationDefinition<
	Args = unknown,
	Ctx = CollectionContext,
	Result = unknown
> = {
	/** Mutation name the client invokes. */
	name: string;
	/** Access control: return false (or throw) to reject the mutation. */
	authorize?: (args: Args, ctx: Ctx) => boolean | Promise<boolean>;
	/**
	 * Apply the mutation: write to your durable store, then call
	 * `actions.change(...)` for each affected row. Return value (e.g. the created
	 * record) is sent back to the caller in the ack.
	 *
	 * Optional **only** when {@link sandboxedHandler} is set instead.
	 */
	handler?: MutationHandler<Args, Ctx, Result>;
	/**
	 * Run the mutation inside an `@absolutejs/isolated-jsc` sandbox. Provide
	 * the handler as a string expression that evaluates to
	 * `(args, ctx, actions) => result` (sync or async).
	 *
	 * The string is compiled into a JSC isolate on first call; per-call cost
	 * is ~0.5 ms after that. The isolate has no access to the host's globals,
	 * filesystem, or network — only `args`, `ctx` (passed via structured
	 * clone), and `actions` (exposed as cross-boundary Reference functions).
	 *
	 * Use this when the handler source is not fully trusted (multi-tenant
	 * PaaS, plugin systems, AI-generated logic), or as defense-in-depth to
	 * cap CPU/memory of even your own handlers. Configure limits via
	 * {@link sandbox}.
	 *
	 * Mutually exclusive with {@link handler}. Requires the optional peer
	 * `@absolutejs/isolated-jsc` to be installed.
	 */
	sandboxedHandler?: string;
	/** Limits + memory cap for the sandboxed handler. Ignored without {@link sandboxedHandler}. */
	sandbox?: SandboxConfig;
	/**
	 * Opt-in conflict retry. When the handler throws a classified-as-retryable
	 * error (default: PG `40001` / `40P01`) the engine discards the buffered
	 * changes, awaits a backoff, and re-runs the handler with a fresh
	 * transaction. Handlers MUST be idempotent under retry — external side
	 * effects (HTTP, email) will fire more than once. See {@link RetryPolicy}.
	 */
	retry?: RetryPolicy;
};

/**
 * Define a server mutation. Identity at runtime — it exists for type inference
 * (args/ctx/result flow through). Register it with {@link SyncEngine.registerMutation}
 * and invoke it from the client; the engine authorizes, runs the handler, fans
 * the resulting diffs to subscribers, and acks the caller.
 */
export const defineMutation = <
	Args = unknown,
	Ctx = CollectionContext,
	Result = unknown
>(
	definition: MutationDefinition<Args, Ctx, Result>
): MutationDefinition<Args, Ctx, Result> => definition;
