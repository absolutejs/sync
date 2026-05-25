import type { CollectionContext } from './collection';
import type { RowKey } from './types';

/**
 * A scored search result: the matched row and its relevance score (higher is
 * more relevant). A search collection sorts hits descending and tags each
 * emitted row with its score (see {@link SEARCH_SCORE_FIELD}).
 */
export type SearchHit<T> = { row: T; score: number };

/**
 * An incremental search index over a row set, queried by `Q` (a string for
 * full-text, a vector for similarity). Maintained as rows are added/removed, so
 * the collection that owns it stays live as the corpus changes.
 * {@link createTextIndex} and {@link createVectorIndex} implement it.
 */
export type SearchIndex<T, Q> = {
	/** Add or replace a row (upsert by key). */
	add: (row: T) => void;
	/** Remove a row by key. */
	remove: (key: RowKey) => void;
	/** Top-`limit` hits for `query`, sorted by descending score. */
	search: (query: Q, limit: number) => SearchHit<T>[];
	/** Number of indexed rows. */
	size: () => number;
	/** Drop every indexed row. */
	clear: () => void;
};

/** The field a search collection adds to each emitted row carrying its score. */
export const SEARCH_SCORE_FIELD = '_score';

export type SearchCollectionDefinition<
	T,
	Query = string,
	Ctx = CollectionContext
> = {
	/** Collection name — its identity for subscribe. */
	name: string;
	kind: 'search';
	/** Source table whose committed changes keep the index live. */
	table: string;
	/** Build the (empty) index — e.g. `() => createTextIndex({ ... })`. */
	index: () => SearchIndex<T, Query>;
	/** The full corpus to index on first subscribe (e.g. a DB read). */
	source: () => Promise<Iterable<T>> | Iterable<T>;
	/** Row identity. */
	key: (row: T) => RowKey;
	/** Max results returned. Defaults to 20. */
	limit?: number;
	/** Access control: return `false` (or throw) to deny the subscription. */
	authorize?: (query: Query, ctx: Ctx) => boolean | Promise<boolean>;
};

/**
 * Define a live search collection: an index (full-text via {@link createTextIndex}
 * or vector via {@link createVectorIndex}) maintained from a source table's
 * change feed. The subscription's `params` *are* the query — a string for
 * full-text, a vector for similarity. Register it with
 * {@link SyncEngine.registerSearch}; the client receives the ranked top-K as a
 * normal collection, re-ranked live as rows change. Each emitted row carries its
 * relevance under {@link SEARCH_SCORE_FIELD}, so the client can sort by it.
 *
 * The corpus is the whole table; a row-level read permission on the table (see
 * {@link definePermissions}) still filters a caller's hits.
 */
export const defineSearchCollection = <
	T,
	Query = string,
	Ctx = CollectionContext
>(
	definition: Omit<SearchCollectionDefinition<T, Query, Ctx>, 'kind'>
): SearchCollectionDefinition<T, Query, Ctx> => ({
	...definition,
	kind: 'search'
});
