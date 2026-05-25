import type { RowKey } from './types';

/**
 * App-provided context for a subscription — typically the authenticated session
 * (user id, roles). Passed to `authorize`, `hydrate`, and `match` so a
 * collection can scope its rows to the caller.
 */
export type CollectionContext = Record<string, unknown>;

export type CollectionDefinition<T, P = void, Ctx = CollectionContext> = {
	/** Collection name — its identity for subscribe (e.g. `orders`). */
	name: string;
	/**
	 * Source tables this collection reads. A committed change to any of them
	 * updates the collection. Defaults to `[name]`. List several for a join /
	 * aggregate collection — which uses the refetch fallback, since a single
	 * table's row can't be matched into a multi-table result.
	 */
	tables?: string[];
	/**
	 * Fetch the initial result set from your database (any ORM). Receives the
	 * subscription's params and context so it can filter to the caller.
	 */
	hydrate: (params: P, ctx: Ctx) => Promise<Iterable<T>> | Iterable<T>;
	/** Row identity. Defaults to `row.id`. */
	key?: (row: T) => RowKey;
	/**
	 * The query's filter as a JS predicate, for incremental matching. Omit to use
	 * the refetch fallback (re-hydrate on every change to the collection).
	 *
	 * It MUST encode the same row filter as `hydrate`/`authorize`: a change that
	 * the predicate accepts is pushed to the subscriber, so a too-loose predicate
	 * leaks rows. (Deriving `match` from the same filter as `hydrate` keeps the
	 * two in lockstep — the planned adapter convenience.)
	 */
	match?: (row: T, params: P, ctx: Ctx) => boolean;
	/**
	 * Access control: return `false` (or throw) to deny the subscription. Runs
	 * before `hydrate`. Without it a collection is world-readable, so treat it as
	 * mandatory for any non-public data.
	 */
	authorize?: (params: P, ctx: Ctx) => boolean | Promise<boolean>;
};

/**
 * Define a syncable collection. Identity at runtime — it exists for type
 * inference, so `params`/`ctx`/row types flow through `hydrate`/`match`/
 * `authorize` without restating them. Register it with a {@link SyncEngine}.
 */
export const defineCollection = <T, P = void, Ctx = CollectionContext>(
	definition: CollectionDefinition<T, P, Ctx>
): CollectionDefinition<T, P, Ctx> => definition;
