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
export {
	aggregateOp,
	chain,
	filterOp,
	fromRowChange,
	joinNode,
	mapOp,
	materialize,
	orderByOp
} from './dataflow';
export type {
	AggregateOpOptions,
	Change,
	JoinNode,
	JoinNodeOptions,
	Materializer,
	Operator,
	OrderByOptions
} from './dataflow';
export type {
	ChangeSource,
	EmitChange,
	ParsedChange,
	RowChange,
	RowKey,
	RowOp,
	ViewDiff
} from './types';
export { createPollingChangeSource, parseOutboxRow } from './pollingSource';
export type { OutboxRow, PollingChangeSourceOptions } from './pollingSource';
export { createInMemoryClusterBus } from './cluster';
export type { ClusterBus, ClusterChange, ClusterMessage } from './cluster';
export { createPresenceHub } from './presence';
export type {
	PresenceDiff,
	PresenceHandle,
	PresenceHub,
	PresenceMember
} from './presence';

export { defineCollection, defineJoinCollection } from './collection';
export type {
	CollectionContext,
	CollectionDefinition,
	JoinCollectionDefinition,
	JoinSide
} from './collection';
export { defineReactiveQuery } from './reactive';
export type {
	ReactiveQueryContext,
	ReactiveQueryDefinition,
	ReadHandle,
	TableReader
} from './reactive';
export { defineGraphCollection, query } from './graph';
export type {
	GraphCollectionDefinition,
	GraphInstance,
	GraphSource,
	GroupByOptions,
	JoinOptions,
	OrderByQueryOptions,
	Query
} from './graph';

export { definePermissions } from './permissions';
export type {
	PermissionsDefinition,
	ReadRule,
	TablePermissions,
	WriteRule
} from './permissions';

export { defineSearchCollection, SEARCH_SCORE_FIELD } from './search';
export type {
	SearchCollectionDefinition,
	SearchHit,
	SearchIndex
} from './search';
export { createTextIndex } from './textIndex';
export type { TextIndexOptions } from './textIndex';
export { createVectorIndex } from './vectorIndex';
export type { VectorIndexOptions, VectorMetric } from './vectorIndex';

export { defineSchedule } from './schedule';
export type { ScheduleContext, ScheduleDefinition } from './schedule';

export { defineMutation } from './mutation';
export type {
	MutationActions,
	MutationDefinition,
	MutationHandler,
	TableWriter,
	TransactionRunner
} from './mutation';

export { createSyncEngine, UnauthorizedError } from './syncEngine';
export type { SubscribeArgs, Subscription, SyncEngine } from './syncEngine';

export { hydrateRoute, mutateRoute } from './routes';
export type { SyncRouteContext } from './routes';

export { createSyncConnection } from './connection';
export type {
	ClientFrame,
	FrameDiff,
	ServerFrame,
	SyncConnection,
	SyncConnectionOptions
} from './connection';
