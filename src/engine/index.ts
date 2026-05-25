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
export { createAggregate } from './aggregate';
export type { Aggregate, AggregateGroup, AggregateOptions } from './aggregate';
export { createEquiJoin } from './equiJoin';
export type { EquiJoin, EquiJoinOptions } from './equiJoin';
export type {
	ChangeSource,
	EmitChange,
	RowChange,
	RowKey,
	RowOp,
	ViewDiff
} from './types';

export { defineCollection } from './collection';
export type { CollectionContext, CollectionDefinition } from './collection';

export { defineMutation } from './mutation';
export type {
	MutationActions,
	MutationDefinition,
	MutationHandler
} from './mutation';

export { createSyncEngine, UnauthorizedError } from './syncEngine';
export type { SubscribeArgs, Subscription, SyncEngine } from './syncEngine';

export { hydrateRoute, mutateRoute } from './routes';
export type { SyncRouteContext } from './routes';

export { createSyncConnection } from './connection';
export type {
	ClientFrame,
	ServerFrame,
	SyncConnection,
	SyncConnectionOptions
} from './connection';
