/**
 * @absolutejs/sync engine (Tier 3 — sync engine MVP, server-side).
 *
 * Row-level reactive query results on your own database. Hydrate a query's
 * result set once, then maintain it incrementally as rows change and push the
 * diffs to subscribers — instead of refetching the whole query.
 *
 * This entry currently exposes the predicate-matching IVM core
 * ({@link createMaterializedView}); the collection registry, view syncer, diff
 * transport, and client store land here as the read and write paths fill in.
 */

export { createMaterializedView, isEmptyViewDiff } from './materializedView';
export type {
	MaterializedView,
	MaterializedViewOptions
} from './materializedView';
export type { RowChange, RowKey, RowOp, ViewDiff } from './types';
