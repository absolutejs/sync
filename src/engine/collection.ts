import type { RowChange, RowKey } from './types';

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
	 * Refetch-fallback gate (multi-table / shape-mismatched collections that can't
	 * use `match`). Without it, ANY change to a read table re-hydrates EVERY
	 * subscription — even ones the change can't touch. Given the RAW change (the
	 * source row, which may differ from `T` and be partial), return `false` ONLY
	 * when the change provably can't affect this subscription's result, to skip
	 * its re-hydrate; the fan-out drops from O(all subscribers) to O(affected).
	 *
	 * Conservative: a `false` that should have been `true` DROPS an update, so
	 * default to `true` when unsure (e.g. a delete whose row lacks your scope
	 * field). Ignored when `match` is set (incremental routing is already exact).
	 */
	affects?: (change: RowChange<unknown>, params: P, ctx: Ctx) => boolean;
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

/** One input of a join collection. */
export type JoinSide<Row, P, Ctx> = {
	/** Source table name (the change feed routes its changes to this side). */
	table: string;
	/** Fetch this side's rows for the subscription (scoped to the caller). */
	hydrate: (params: P, ctx: Ctx) => Promise<Iterable<Row>> | Iterable<Row>;
	/** Row identity within this side. */
	key: (row: Row) => RowKey;
	/** Join value — matched for equality against the other side's `on`. */
	on: (row: Row) => RowKey;
	/**
	 * Access/predicate filter for incremental changes on this side. A changed row
	 * that fails it is treated as a leave (removed from the join), so a row that
	 * becomes invisible drops out. Omit only for an unscoped side.
	 */
	match?: (row: Row, params: P, ctx: Ctx) => boolean;
};

/**
 * A collection that is the incremental inner equi-join of two tables. The engine
 * maintains it with an {@link createEquiJoin} operator — a change to either side
 * moves only the affected pairs, instead of re-hydrating the whole join.
 */
export type JoinCollectionDefinition<
	L,
	R,
	Out,
	P = void,
	Ctx = CollectionContext
> = {
	name: string;
	kind: 'join';
	left: JoinSide<L, P, Ctx>;
	right: JoinSide<R, P, Ctx>;
	/** Combine a matched pair into an output row. */
	select: (left: L, right: R) => Out;
	/** Output row identity (must be unique per emitted row). */
	key: (out: Out) => RowKey;
	/** Access control; return false (or throw) to deny the subscription. */
	authorize?: (params: P, ctx: Ctx) => boolean | Promise<boolean>;
};

/**
 * Define an incremental equi-join collection (see {@link JoinCollectionDefinition}).
 * For a many-to-one join the output can key by the left id; for many-to-many,
 * include both ids in the output and key on the pair.
 */
export const defineJoinCollection = <
	L,
	R,
	Out,
	P = void,
	Ctx = CollectionContext
>(
	definition: Omit<JoinCollectionDefinition<L, R, Out, P, Ctx>, 'kind'>
): JoinCollectionDefinition<L, R, Out, P, Ctx> => ({
	...definition,
	kind: 'join'
});
