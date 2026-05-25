import type { CollectionContext } from './collection';
import type { RowKey } from './types';

/**
 * Read-set tracking — the reactor. You write a plain query function that reads
 * through an instrumented `ctx.db`; the engine records which tables it touched
 * and re-runs it (diffing old vs new) whenever any of those tables change. No
 * hand-written `match`, no operator graph, no manual change emission for reads:
 * write a query, it stays live. This is the BYO-database analogue of Convex's
 * automatic read-set tracking — it works on your own DB because your reads go
 * through the registered {@link TableReader}s.
 *
 * Granularity is table-level for now (a query that read a table re-runs on any
 * change to it) — the full developer experience, and always correct; key/range
 * precision is a later optimization.
 */

/** How to read a table for reactive queries — register with `registerReader`. */
export type TableReader<Ctx = unknown> = {
	/** All rows of the table (the common case; filter in JS in your query). */
	all: (ctx: Ctx) => Promise<Iterable<unknown>> | Iterable<unknown>;
	/** Optional point lookup by key. */
	get?: (key: RowKey, ctx: Ctx) => Promise<unknown> | unknown;
};

/** The instrumented data handle passed to a reactive query — reads are tracked. */
export type ReadHandle = {
	/** Read all rows of `table` (records a dependency on it). */
	all: <T = unknown>(table: string) => Promise<T[]>;
	/** Read one row of `table` by key (records a dependency on `table`). */
	get: <T = unknown>(table: string, key: RowKey) => Promise<T | undefined>;
};

export type ReactiveQueryContext<P, Ctx> = {
	/** Tracked reads — anything you read here becomes a live dependency. */
	db: ReadHandle;
	params: P;
	ctx: Ctx;
};

export type ReactiveQueryDefinition<T, P = void, Ctx = CollectionContext> = {
	name: string;
	kind: 'reactive';
	/** Compute the result set by reading through `ctx.db`; re-run on change. */
	run: (context: ReactiveQueryContext<P, Ctx>) => Promise<T[]> | T[];
	/** Result-row identity (used to diff re-runs). */
	key: (row: T) => RowKey;
	/** Access control; return false (or throw) to deny the subscription. */
	authorize?: (params: P, ctx: Ctx) => boolean | Promise<boolean>;
};

/**
 * Define a reactive query: a function that reads through `ctx.db` and is kept
 * live automatically by read-set tracking. Register it with
 * {@link SyncEngine.registerReactive} (and the tables it reads with
 * `registerReader`).
 */
export const defineReactiveQuery = <T, P = void, Ctx = CollectionContext>(
	definition: Omit<ReactiveQueryDefinition<T, P, Ctx>, 'kind'>
): ReactiveQueryDefinition<T, P, Ctx> => ({ ...definition, kind: 'reactive' });
