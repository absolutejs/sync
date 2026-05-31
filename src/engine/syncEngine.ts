import {
	ABS_ATTRS,
	tracerOrNoop,
	type TracerProvider as TelemetryTracerProvider
} from '@absolutejs/telemetry';
import type {
	CollectionContext,
	CollectionDefinition,
	JoinCollectionDefinition
} from './collection';
import { createEquiJoin } from './equiJoin';
import type { EquiJoin } from './equiJoin';
import type { GraphCollectionDefinition, GraphInstance } from './graph';
import { createMaterializedView, isEmptyViewDiff } from './materializedView';
import type { MaterializedView } from './materializedView';
import type {
	MutationActions,
	MutationDefinition,
	TableWriter,
	TransactionRunner
} from './mutation';
import type {
	ReactiveQueryDefinition,
	ReadHandle,
	TableReader
} from './reactive';
import {
	exponentialBackoff,
	isSerializationFailure,
	RetriesExhaustedError
} from './retry';
import {
	type BridgeFetchConfig,
	type HandlerMetricsHook,
	type HandlerMetricsRecord,
	makeSandboxedHandler
} from './sandbox';
import type {
	PermissionsDefinition,
	ReadRule,
	TablePermissions,
	WriteRule
} from './permissions';
import { PackMissingDependencyError, PackTableConflictError } from './pack';
import type { RegisteredPack, SyncPack } from './pack';
import type { SearchCollectionDefinition, SearchIndex } from './search';
import { SEARCH_SCORE_FIELD } from './search';
import type { ScheduleDefinition } from './schedule';
import type {
	CollectionKind,
	EngineActivity,
	EngineInspection,
	EngineMetrics
} from './devtools';
import type { SchemaDefinition, TableSchema } from './schema';
import {
	EngineFencedError,
	type EngineSnapshot,
	type ExportSnapshotOptions,
	type FenceHandle,
	type ImportSnapshotOptions,
	type MigrationImportResult
} from './migrate';
import type { CrdtMergeable } from '../crdt';
import type { ClusterBus } from './cluster';
import type { ChangeSource, RowChange, RowKey, ViewDiff } from './types';

/**
 * Which fields of a `Row` are CRDT values, and the {@link CrdtMergeable} backend
 * (e.g. `rgaText`, or `yjsText` from `@absolutejs/sync-yjs`) used to merge each.
 * Pass to `engine.registerCrdt` so the engine merges those fields on write.
 */
export type CrdtFields<Row> = {
	[Field in keyof Row]?: CrdtMergeable<Row[Field]>;
};

/**
 * Thrown when `authorize` denies a subscribe or a mutation. The message names
 * the denied action; the message always starts with "Not authorized".
 */
export class UnauthorizedError extends Error {
	constructor(subject: string) {
		super(`Not authorized: ${subject}`);
		this.name = 'UnauthorizedError';
	}
}

/**
 * Thrown by `engine.subscribe` / `engine.hydrate` (1.15.0+) when the caller's
 * `AbortSignal` fires before the operation reaches a `Subscription` /
 * resolved value. The `name` is `'AbortError'` to match the DOM-standard
 * spelling so existing `catch (error) { if (error.name === 'AbortError') ... }`
 * patterns work unchanged.
 */
export class AbortError extends Error {
	constructor(reason?: string) {
		super(reason ?? 'Aborted');
		this.name = 'AbortError';
	}
}

const checkAborted = (signal?: AbortSignal): void => {
	if (signal?.aborted) {
		throw new AbortError(
			signal.reason instanceof Error
				? signal.reason.message
				: typeof signal.reason === 'string'
					? signal.reason
					: 'Aborted'
		);
	}
};

const linkAbortToUnsubscribe = (
	signal: AbortSignal | undefined,
	unsubscribe: () => void
): void => {
	if (signal === undefined) return;
	if (signal.aborted) {
		unsubscribe();
		return;
	}
	const handler = () => {
		try {
			unsubscribe();
		} catch {
			/* idempotent unsubscribes shouldn't surface here */
		}
	};
	signal.addEventListener('abort', handler, { once: true });
};

/**
 * Thrown when a mutation's write fails its table's schema (see
 * {@link defineSchema}). The message names the offending field.
 */
export class SchemaError extends Error {
	constructor(table: string, fieldName: string) {
		super(`Schema violation on "${table}": invalid field "${fieldName}"`);
		this.name = 'SchemaError';
	}
}

export type SubscribeArgs<T, P, Ctx> = {
	/** Registered collection name. */
	collection: string;
	/** Query params (e.g. a filter value); passed to hydrate/match/authorize. */
	params: P;
	/** Caller context (e.g. session); passed to hydrate/match/authorize. */
	ctx: Ctx;
	/**
	 * Receives every non-empty diff (with its version) after the initial
	 * reply. 1.18.0+: a third optional `cursor` argument carries the
	 * cross-instance resume cursor as of this diff. Callers that ignore
	 * the 3rd arg keep working unchanged.
	 */
	onDiff: (diff: ViewDiff<T>, version: number, cursor?: string) => void;
	/**
	 * Resume from a point the client already applied. When the change log still
	 * covers `(since, now]` for a single-table collection, the engine replies
	 * with a catch-up diff instead of a full snapshot; otherwise it falls back
	 * to a snapshot.
	 *
	 * Accepts `number` (legacy pre-1.17 — interpreted as the version of THIS
	 * engine instance) or a `string` cursor (1.17.0+ — opaque vector of
	 * `(instanceId, version)` per origin, returned by the engine on every
	 * subscription/diff and round-tripped by the client unmodified). Use the
	 * cursor form for cross-instance resume.
	 */
	since?: number | string;
	/**
	 * Cancellation handle (1.15.0). Two effects:
	 *  1. If the signal is already aborted when `subscribe` is called, the
	 *     engine throws {@link AbortError} immediately — no authorize, no
	 *     hydrate, no subscription.
	 *  2. If the signal fires AFTER the subscription is live, the engine
	 *     auto-calls `unsubscribe()`. The consumer never has to thread two
	 *     handles for the same lifetime.
	 *
	 * Backwards-compatible — omit `signal` and the engine behaves exactly
	 * as in pre-1.15.0.
	 */
	signal?: AbortSignal;
};

export type Subscription<T> = {
	/** The result set at subscribe time — a snapshot (empty when resuming). */
	initial: T[];
	/** Catch-up diff when resuming via `since` (instead of `initial`). */
	catchup?: ViewDiff<T>;
	/** The engine's local version this reply brings the client up to. */
	version: number;
	/**
	 * Opaque cross-instance resume cursor (1.17.0+). Encodes the per-origin
	 * vector of `(instanceId, version)` the client is now up-to-date with;
	 * pass it back as `SubscribeArgs.since` on reconnect. Works for resume
	 * against ANY instance in a cluster, not just the one that issued it —
	 * the receiving instance decodes the cursor, walks its log for entries
	 * the client hasn't seen yet, and either replies with a catch-up diff
	 * or a fresh snapshot.
	 */
	cursor: string;
	/** Stop receiving diffs and release the view. */
	unsubscribe: () => void;
};

export type SyncEngine = {
	/** Register a collection definition (see {@link defineCollection}). */
	register: <T, P = void, Ctx = CollectionContext>(
		collection: CollectionDefinition<T, P, Ctx>
	) => void;
	/** Register an incremental join collection (see {@link defineJoinCollection}). */
	registerJoin: <L, R, Out, P = void, Ctx = CollectionContext>(
		collection: JoinCollectionDefinition<L, R, Out, P, Ctx>
	) => void;
	/** Register an operator-graph collection (see {@link defineGraphCollection}). */
	registerGraph: <Out, P = void, Ctx = CollectionContext>(
		collection: GraphCollectionDefinition<Out, P, Ctx>
	) => void;
	/**
	 * Register a live search collection (see {@link defineSearchCollection}): a
	 * full-text or vector index maintained from a source table's change feed and
	 * queried by the subscription's params, returning the ranked top-K live.
	 */
	registerSearch: <T, Query = string, Ctx = CollectionContext>(
		collection: SearchCollectionDefinition<T, Query, Ctx>
	) => void;
	/**
	 * Register a scheduled function (see {@link defineSchedule}): server-triggered
	 * work whose `actions` writes go live through the change feed. Wire the cron
	 * triggers with the `scheduled` Elysia plugin.
	 */
	registerSchedule: (schedule: ScheduleDefinition) => void;
	/**
	 * Run a registered schedule's handler now: its writes commit (in the
	 * configured transaction) and emit as one live batch. The `scheduled` plugin
	 * calls this on each cron fire; call it directly to trigger on demand.
	 */
	runSchedule: (name: string) => Promise<void>;
	/** Registered schedules (name + cron pattern) — used by the `scheduled` plugin. */
	listSchedules: () => ScheduleDefinition[];
	/**
	 * Open a live subscription: authorize, hydrate the initial set, and stream
	 * diffs as changes arrive. Rejects with {@link UnauthorizedError} on deny.
	 */
	subscribe: <T, P = void, Ctx = CollectionContext>(
		args: SubscribeArgs<T, P, Ctx>
	) => Promise<Subscription<T>>;
	/**
	 * One-shot read: authorize and return a collection's current rows without
	 * subscribing. Powers an Eden-typed HTTP hydrate route (and SSR). Rejects
	 * with {@link UnauthorizedError} on deny.
	 *
	 * Pass `options.signal` (1.15.0+) to cancel the operation mid-flight —
	 * the engine throws {@link AbortError} after the next await point if
	 * the signal has fired.
	 */
	hydrate: (
		collection: string,
		params: unknown,
		ctx: unknown,
		options?: { signal?: AbortSignal }
	) => Promise<unknown[]>;
	/**
	 * Feed a committed change to `table` into the engine, fanning the resulting
	 * diff to every live subscription of every collection that reads that table.
	 * Call after a mutation, or wire a {@link ChangeSource} via `connectSource`.
	 * Single-table subscriptions diff the row; multi-table / refetch ones
	 * re-hydrate.
	 */
	applyChange: <T>(table: string, change: RowChange<T>) => Promise<void>;
	/**
	 * Connect a change source (e.g. a CDC adapter): its emitted changes flow into
	 * `applyChange`. Resolves to a disconnect function that stops the source.
	 */
	connectSource: (source: ChangeSource) => Promise<() => Promise<void>>;
	/**
	 * Join a cluster (see {@link ClusterBus}): broadcast this instance's committed
	 * changes to peers and apply theirs locally, so subscribers on every instance
	 * stay live. Resolves to a disconnect function. Run once per instance.
	 */
	connectCluster: (bus: ClusterBus) => Promise<() => Promise<void>>;
	/** Active subscription count, optionally for one collection. */
	subscriptionCount: (collection?: string) => number;
	/** Register a mutation definition (see {@link defineMutation}). */
	registerMutation: <Args, Ctx = CollectionContext, Result = unknown>(
		mutation: MutationDefinition<Args, Ctx, Result>
	) => void;
	/**
	 * Register how to persist a `table` (any ORM), so a mutation's
	 * `actions.insert/update/delete` write to your store and emit the live change
	 * in one step — you can't write without going live. See {@link TableWriter}.
	 */
	registerWriter: <Row = unknown, Ctx = CollectionContext, Tx = unknown>(
		table: string,
		writer: TableWriter<Row, Ctx, Tx>
	) => void;
	/**
	 * Register a read-set-tracked reactive query (see {@link defineReactiveQuery}):
	 * it re-runs and re-pushes whenever any table it read changes — no `match`, no
	 * operator graph, no manual change emission.
	 */
	registerReactive: <T, P = void, Ctx = CollectionContext>(
		query: ReactiveQueryDefinition<T, P, Ctx>
	) => void;
	/**
	 * Teach the engine how to read a table for reactive queries' `ctx.db` (any
	 * ORM). Required for every table a reactive query reads.
	 */
	registerReader: <Ctx = CollectionContext>(
		table: string,
		reader: TableReader<Ctx>
	) => void;
	/**
	 * Register declarative, row-level permissions for a `table` (see
	 * {@link definePermissions}). Read rules filter every row the engine emits for
	 * the table; write rules gate `actions.insert/update/delete`. Equivalent to a
	 * `permissions` entry on {@link createSyncEngine}.
	 */
	registerPermissions: <Row = unknown, Ctx = CollectionContext>(
		table: string,
		rules: TablePermissions<Row, Ctx>
	) => void;
	/**
	 * Register a `table`'s schema (see {@link defineSchema}): writes are validated
	 * against it (a bad write rejects the mutation with {@link SchemaError}), and
	 * its `migrate` lazily upcasts rows on read. Equivalent to a `schemas` entry
	 * on {@link createSyncEngine}.
	 */
	registerSchema: <Row = unknown>(
		table: string,
		schema: TableSchema<Row>
	) => void;
	/**
	 * Declare which fields on a `table` are CRDT values (see {@link CrdtMergeable}
	 * — e.g. `rgaText` from `@absolutejs/sync/crdt`, or `yjsText` from
	 * `@absolutejs/sync-yjs`). The engine then MERGES those fields on
	 * `actions.insert/update` instead of overwriting them, so concurrent writers
	 * converge with no clobbering — conflict-free collaborative editing with no
	 * merge code in your mutation. It also registers a ready-made
	 * `"<table>:merge"` mutation that upserts a row patch, so a client (e.g. the
	 * `useCollaborativeText` framework hooks) needs no custom server mutation.
	 */
	registerCrdt: <Row = Record<string, unknown>>(
		table: string,
		fields: CrdtFields<Row>
	) => void;
	/**
	 * Apply a table's schema `migrate` to a raw/stored row (identity when there's
	 * no schema or migration). Use it wherever you read raw rows the engine
	 * doesn't (e.g. a search collection's `source`); the engine already migrates
	 * reactive `ctx.db` reads, view hydrates, and the one-shot hydrate.
	 */
	migrate: <Row = unknown>(table: string, row: Row) => Row;
	/**
	 * Run a registered mutation: authorize, invoke its handler (which writes and
	 * emits changes via `applyChange`), and resolve with the handler's result.
	 * Rejects with {@link UnauthorizedError} on deny, or an error for an unknown
	 * mutation / a handler throw. Drive this from the transport's mutate frame.
	 */
	runMutation: (
		name: string,
		args: unknown,
		ctx: unknown
	) => Promise<unknown>;
	/**
	 * Atomically run N mutations in a single transaction (sync 1.11+).
	 * Each `{ name, args }` spec is authorized, then handlers fire in
	 * order against shared buffered changes. If any handler throws, the
	 * entire transaction rolls back — no partial commits, no fanned-out
	 * diffs. On success the accumulated changes apply as ONE live batch
	 * and the per-mutation results return in order.
	 *
	 * No retry policy applies to batches in v0.2; configure per-mutation
	 * retries on individual `runMutation` calls when atomicity isn't
	 * needed. A failed batch passes the original error through with no
	 * wrapping.
	 *
	 * Requires `transaction` to be set in {@link SyncEngineOptions} for
	 * actual DB-level atomicity; without it the batch still buffers
	 * changes into one fan-out but the underlying adapter writes
	 * piecemeal.
	 */
	runMutations: (
		specs: Array<{ name: string; args: unknown }>,
		ctx: unknown
	) => Promise<unknown[]>;
	/**
	 * A point-in-time snapshot of the engine for devtools: registered collections
	 * (+ kind, tables, live subscription counts), mutations, schedules, readers,
	 * writers, the change-feed version, and recent changes. See `syncDevtools`.
	 */
	inspect: () => EngineInspection;
	/**
	 * Operator-shaped engine metrics — counters + memory estimates + throughput
	 * totals since engine start. Distinct from {@link SyncEngine.inspect}: this
	 * is what a PaaS host scrapes on an interval to answer "is this engine
	 * healthy" and "what's its resource footprint." Feed it to
	 * `@absolutejs/metering` for per-engine cost attribution.
	 *
	 * Added in 1.13.0.
	 */
	metrics: () => EngineMetrics;
	/**
	 * Capture the engine's change log + version as a serializable
	 * {@link ChangeLogSnapshot} the host can persist (disk, S3, the cluster
	 * bus) and restore on the next boot via
	 * {@link SyncEngineOptions.initialChangeLog} or
	 * {@link SyncEngine.importChangeLog}. The receiving engine MUST share this
	 * engine's `instanceId` — otherwise the resume contract silently breaks.
	 *
	 * Cheap: the snapshot's `entries` is a shallow copy of the bounded log
	 * (capped by `changeLogSize` / `changeLogRetainMs`). Call on a timer or on
	 * graceful shutdown — both are fine; the snapshot is monotonic in commit
	 * order, so a partial roll-forward (apply entries newer than the snapshot
	 * from another source) is safe.
	 *
	 * Added in 1.19.0.
	 */
	exportChangeLog: () => ChangeLogSnapshot;
	/**
	 * Adopt a {@link ChangeLogSnapshot} into a running engine that has not yet
	 * committed any local changes (its `version` is 0). The snapshot's
	 * `instanceId` MUST match this engine's `instanceId`. Throws otherwise.
	 *
	 * Convenience for hosts that want to set up the engine, register surfaces,
	 * AND THEN restore. Equivalent to passing the snapshot via
	 * `createSyncEngine({ initialChangeLog })` if you have it at construction
	 * time. Returns the number of entries imported.
	 *
	 * Added in 1.19.0.
	 */
	importChangeLog: (snapshot: ChangeLogSnapshot) => number;
	/**
	 * Reconstruct the state of registered tables as of a target
	 * timestamp by walking the change log forward and folding each op
	 * into a per-table view. Useful for forensic incident response
	 * ("what did the tenant see at 14:32?") and the "I deleted prod
	 * — restore us to 2h ago" recovery story.
	 *
	 * The reconstruction is exact when the log spans `targetAt` (i.e.
	 * the log's oldest entry is at version 1). When the log has been
	 * trimmed (`changeLogSize` / `changeLogRetainMs` evicted older
	 * entries) AND `targetAt` falls in the gap, the result is
	 * best-effort: state walked forward from the OLDEST retained
	 * entry, with `truncated: true` so the caller knows.
	 *
	 * Added in 1.22.0.
	 *
	 * @example
	 * ```ts
	 * const twoHoursAgo = Date.now() - 2 * 60 * 60 * 1000;
	 * const result = await engine.replayTo({ at: twoHoursAgo, tables: ['orders'] });
	 * if (result.truncated) {
	 *   console.warn('Replay truncated — log retention window too short.');
	 * }
	 * console.log(result.rows.orders); // orders as of two hours ago
	 * ```
	 */
	replayTo: (options: ReplayOptions) => Promise<ReplayResult>;
	/**
	 * Pause new mutations on the engine — the source half of the G7 tenant
	 * migration contract. While at least one fence is held, `runMutation`
	 * rejects with {@link EngineFencedError}; subscribe/hydrate continue to
	 * work, so live readers stay served while the snapshot is in flight.
	 *
	 * Multiple fence handles compose — the engine stays fenced until every
	 * handle has been `lift()`-ed. Lifting is idempotent.
	 *
	 * Out of scope: out-of-band writes (CDC drivers, raw SQL). The caller
	 * is responsible for halting those before fencing, otherwise the
	 * snapshot will drift between `exportSnapshot` and import on the target.
	 *
	 * Added in 1.24.0.
	 */
	fence: (options: { reason: string }) => FenceHandle;
	/**
	 * Capture the engine's current per-table state into a portable
	 * {@link EngineSnapshot}. Walks every registered reader's `all(ctx)`
	 * and collects the rows. Used to ship a tenant between engines (G7).
	 *
	 * Pair with `fence()` on the source to stop drift, then
	 * `importSnapshot()` on the target. The shape is intentionally
	 * detached from `ChangeLogSnapshot` — snapshots carry live state, not
	 * history. Use `exportChangeLog()` separately if you need forensic
	 * continuity at the target instanceId.
	 *
	 * Added in 1.24.0.
	 *
	 * @example
	 * const fence = source.fence({ reason: 'tenant move' });
	 * try {
	 *   const snapshot = await source.exportSnapshot();
	 *   await target.importSnapshot(snapshot);
	 * } finally { fence.lift(); }
	 */
	exportSnapshot: (options?: ExportSnapshotOptions) => Promise<EngineSnapshot>;
	/**
	 * Bulk-load an {@link EngineSnapshot} into this engine via each table's
	 * registered writer. Tables present in the snapshot but missing a
	 * writer here are surfaced in `result.skipped` so the operator can
	 * detect a misconfigured target. The target half of the G7 migration
	 * contract.
	 *
	 * Inserts do NOT emit change events to subscribers — the import is
	 * meant to land on a fresh target whose clients will re-hydrate after
	 * the DNS cutover. If you need to fan changes out (e.g. mid-flight
	 * cutover), drain the change log via `streamChanges()` and
	 * `applyChange()` separately.
	 *
	 * Added in 1.24.0.
	 */
	importSnapshot: (
		snapshot: EngineSnapshot,
		options?: ImportSnapshotOptions
	) => Promise<MigrationImportResult>;
	/**
	 * Subscribe to the live engine activity stream (changes, mutation outcomes,
	 * subscribe/unsubscribe). Returns an unsubscribe. Powers the devtools feed.
	 */
	onActivity: (listener: (event: EngineActivity) => void) => () => void;
	/**
	 * Outbound CDC stream — yield every committed change as a {@link LoggedChange},
	 * historical first (entries with `version > since`) then continuously tailing
	 * live commits. Use it to feed downstream pipelines (Kafka, search indexers,
	 * audit logs, analytics warehouses).
	 *
	 * The iterator is notify-driven (no polling): it parks on a Promise that
	 * resolves the instant a new commit lands.
	 *
	 * If `since` falls before the oldest entry retained in the bounded change
	 * log, the iterator throws {@link MissedChangesError} so the consumer
	 * notices the gap instead of silently skipping commits. Resubscribe with
	 * `since = engine.inspect().recentChanges[0].version` after re-bootstrapping.
	 *
	 * If the consumer iterates slower than the engine commits and the in-flight
	 * buffer overflows (`maxBuffer`, default 10000), the iterator throws
	 * {@link CdcConsumerSlowError} for the same reason.
	 *
	 * @example
	 * for await (const entry of engine.streamChanges({ since: lastCursor })) {
	 *   await kafka.send('sync.changes', JSON.stringify(entry));
	 *   lastCursor = entry.version;
	 * }
	 */
	streamChanges: (
		options?: StreamChangesOptions
	) => AsyncIterable<LoggedChange>;
	/**
	 * Register a {@link SyncPack} — a self-contained bundle of schemas,
	 * permissions, readers/writers, collections, mutations, and schedules.
	 * Dispatches each field to the matching `register*` method. Rejects
	 * with {@link PackTableConflictError} if the pack claims a table
	 * another registered pack already owns; with
	 * {@link PackMissingDependencyError} if `requireDependencies` is set
	 * and a `readsTables` entry has no registered reader.
	 *
	 * See `syncPacks.design.md` for the rationale.
	 */
	registerPack: (pack: SyncPack) => void;
};

/**
 * 1.18.0: `OnDiff` receives an opaque `cursor` string alongside the version.
 * The cursor is the engine's cross-instance resume cursor as of this batch
 * — the connection layer forwards it to the client on the wire so a
 * reconnect can resume across shards. Pre-1.18 callers that ignore the 3rd
 * arg keep working unchanged.
 */
type OnDiff = (diff: ViewDiff<unknown>, version: number, cursor?: string) => void;

type JoinState = {
	op: EquiJoin<unknown, unknown, unknown>;
	leftTable: string;
	rightTable: string;
	/** Per-side filters (bound to params/ctx) — a failing change leaves the join. */
	leftMatch?: (row: unknown) => boolean;
	rightMatch?: (row: unknown) => boolean;
};

type ActiveSubscription =
	| {
			kind: 'view';
			collection: string;
			view: MaterializedView<unknown>;
			/** Incremental (has a predicate) vs refetch fallback. */
			incremental: boolean;
			/** Re-run the bound hydrate for the refetch fallback. */
			rehydrate: () => Promise<Iterable<unknown>>;
			/** Result-row identity (used to net a batch's diffs). */
			key: (row: unknown) => RowKey;
			onDiff: OnDiff;
	  }
	| {
			kind: 'join';
			collection: string;
			join: JoinState;
			key: (row: unknown) => RowKey;
			onDiff: OnDiff;
	  }
	| {
			kind: 'graph';
			collection: string;
			instance: GraphInstance<unknown>;
			key: (row: unknown) => RowKey;
			onDiff: OnDiff;
	  }
	| {
			kind: 'reactive';
			collection: string;
			key: (row: unknown) => RowKey;
			/** Re-run; returns the new rows and the read set (tables/keys/ranges). */
			rerun: () => Promise<{
				rows: unknown[];
				readTables: Set<string>;
				readKeys: Set<string>;
				rangeDeps: RangeDep[];
			}>;
			/**
			 * Stable key over `(collection, params, ctx)`. Subscriptions sharing
			 * the same key are equivalent on the read side, so a single rerun
			 * per change batch can serve all of them (see `reactivePairs`).
			 */
			rerunKey: string;
			/** Current result set, keyed (diffed against the next re-run). */
			current: Map<RowKey, unknown>;
			/** Full-table dependencies (from `db.all`). */
			readTables: Set<string>;
			/** Row-level dependencies `table\0key` (from `db.get`). */
			readKeys: Set<string>;
			/** Range dependencies (from `db.where`) — predicate + matched keys. */
			rangeDeps: RangeDep[];
			onDiff: OnDiff;
	  }
	| {
			kind: 'search';
			collection: string;
			key: (row: unknown) => RowKey;
			/** Re-run the search against the (now-updated) shared index. */
			rerun: () => unknown[];
			/** Current ranked result set, keyed (diffed against the next re-run). */
			current: Map<RowKey, unknown>;
			onDiff: OnDiff;
	  };

/** A `db.where` dependency: the predicate plus the keys that matched at read. */
type RangeDep = {
	table: string;
	predicate: (row: unknown) => boolean;
	keys: Set<RowKey>;
};

/**
 * A single committed change as it appears in the engine's change log and on
 * the {@link SyncEngine.streamChanges} CDC stream. Versions are monotonic
 * across the engine: a single mutation that writes N rows emits N entries
 * all sharing the same `version`.
 */
export type LoggedChange = {
	/** This engine's local monotonic version when the change was logged. */
	version: number;
	table: string;
	change: RowChange<unknown>;
	/**
	 * Wall-clock when this change was logged (Date.now()). Used by the
	 * engine's time-based retention sweep (`changeLogRetainMs`) and
	 * surfaced as the change-log age in {@link SyncEngine.metrics}.
	 * Added in 1.13.0; pre-1.13.0 consumers of `LoggedChange` ignore it.
	 */
	at: number;
	/**
	 * Instance id that originated this change. For locally-committed changes
	 * this is the engine's own `instanceId`; for cluster-received changes,
	 * the originating peer's id.
	 *
	 * Added in 1.17.0; pre-1.17 consumers ignore it.
	 */
	origin: string;
	/**
	 * The ORIGINATOR's local version at commit time. For locally-committed
	 * changes this equals `version`; for cluster-received changes, the
	 * peer's version. Resume cursors (1.17.0+) carry `(origin, originVersion)`
	 * pairs so a client's last-seen point matches against peer entries this
	 * engine has logged via the bus.
	 *
	 * Added in 1.17.0; pre-1.17 consumers ignore it.
	 */
	originVersion: number;
};

/** Thrown by {@link SyncEngine.streamChanges} when `since` is older than the
 * oldest entry retained in the bounded change log (i.e. the consumer was
 * disconnected long enough that the engine has lost the diff). The consumer
 * should re-bootstrap from a fresh hydrate and resume from `availableSince`. */
export class MissedChangesError extends Error {
	readonly requestedSince: number;
	readonly availableSince: number;
	constructor(requestedSince: number, availableSince: number) {
		super(
			`Change log no longer covers version ${requestedSince}; oldest available is ${availableSince}. ` +
				`Re-bootstrap and resume from ${availableSince}.`
		);
		this.name = 'MissedChangesError';
		this.requestedSince = requestedSince;
		this.availableSince = availableSince;
	}
}

/** Options for {@link SyncEngine.streamChanges}. */
export type StreamChangesOptions = {
	/**
	 * Last version the consumer has already processed. The stream yields
	 * entries with `version > since`. Defaults to `0` (replay everything in
	 * the log, then tail).
	 */
	since?: number;
	/**
	 * Cancel the stream cleanly. When the signal aborts, the iterator
	 * resolves to `done` on its next yield and unregisters its subscriber.
	 */
	signal?: AbortSignal;
	/**
	 * Hard cap on the in-flight buffer for this consumer. If the engine
	 * commits faster than the consumer iterates and the buffer overflows,
	 * the stream rejects so the consumer notices instead of silently
	 * skipping entries. Defaults to 10000.
	 */
	maxBuffer?: number;
};

/** Thrown by {@link SyncEngine.streamChanges} when the consumer fell so far
 * behind that the in-flight buffer overflowed. Resubscribe from the last
 * cursor the consumer successfully processed. */
export class CdcConsumerSlowError extends Error {
	readonly maxBuffer: number;
	readonly lastDeliveredVersion: number;
	constructor(maxBuffer: number, lastDeliveredVersion: number) {
		super(
			`CDC stream buffer overflowed (max ${maxBuffer}); consumer fell behind. ` +
				`Last delivered version: ${lastDeliveredVersion}. Resubscribe with since=${lastDeliveredVersion}.`
		);
		this.name = 'CdcConsumerSlowError';
		this.maxBuffer = maxBuffer;
		this.lastDeliveredVersion = lastDeliveredVersion;
	}
}

/**
 * Thrown by `runMutation` / `runMutations` when `mutationConcurrency` is
 * saturated AND the waiting queue is already at `mutationQueueLimit`. The
 * caller sees this immediately (no queue time) so the host can shed load
 * with a clean 429 instead of letting the queue grow unboundedly. Added
 * in 1.20.0.
 */
export class MutationQueueOverflowError extends Error {
	readonly queueLimit: number;
	constructor(queueLimit: number) {
		super(
			`Mutation queue overflowed (limit ${queueLimit}); the engine is at ` +
				`its mutationConcurrency cap and the waiting queue is full. ` +
				`Retry later or shed load at the gateway.`
		);
		this.name = 'MutationQueueOverflowError';
		this.queueLimit = queueLimit;
	}
}

/**
 * Thrown by `engine.subscribe` when the calling tenant's active-subscription
 * count is already at the configured `subscriptionLimit.max`. The caller sees
 * this immediately — BEFORE authorize, hydrate, or any subscription state
 * allocation — so a rejected call leaks nothing. Added in 1.20.1.
 */
export class SubscriptionLimitError extends Error {
	readonly tenantKey: string;
	readonly limit: number;
	readonly active: number;
	constructor(tenantKey: string, limit: number, active: number) {
		super(
			`Tenant "${tenantKey}" is at the subscription cap ` +
				`(${active}/${limit}). Close an existing subscription before opening another.`
		);
		this.name = 'SubscriptionLimitError';
		this.tenantKey = tenantKey;
		this.limit = limit;
		this.active = active;
	}
}

/**
 * Serializable snapshot of an engine's change log + monotonic version, returned
 * by {@link SyncEngine.exportChangeLog} and consumed by
 * {@link SyncEngineOptions.initialChangeLog} or
 * {@link SyncEngine.importChangeLog}.
 *
 * The PaaS host persists this on shard rotation (every N seconds or on graceful
 * shutdown) and hands it back to the replacement engine so resume cursors
 * referencing this `instanceId` keep working past the restart. Bounded by the
 * receiving engine's `changeLogSize` + `changeLogRetainMs` policies — entries
 * that exceed either cap on import are trimmed exactly as if they had been
 * logged live.
 *
 * Added in 1.19.0.
 */
export type ChangeLogSnapshot = {
	/** The exporting engine's `instanceId`. Receiver MUST match. */
	instanceId: string;
	/** The exporting engine's monotonic version at snapshot time. */
	version: number;
	/** Every retained log entry, in commit order (oldest first). */
	entries: ReadonlyArray<LoggedChange>;
	/**
	 * Optional version-stamp the host may use to compare snapshots without
	 * deserializing the entries (e.g. for incremental persistence). Set to
	 * `Date.now()` at export time. Receivers ignore this field.
	 */
	exportedAt?: number;
};

/**
 * Options for {@link SyncEngine.replayTo}. Added in 1.22.0.
 */
export type ReplayOptions = {
	/**
	 * Target timestamp (`Date.now()`-shaped). The engine walks the
	 * change log forward, applying entries with `at <= targetAt`. The
	 * result is the state as-of `targetAt` (or as close as the log
	 * permits — see `truncated`).
	 */
	at: number;
	/**
	 * Optional table filter. When set, only entries whose `table` is
	 * in this list are folded into the result; entries for other
	 * tables are skipped. Useful for "show me what `tasks` looked
	 * like at T" without paying to reconstruct every table.
	 */
	tables?: ReadonlyArray<string>;
};

/**
 * Returned by {@link SyncEngine.replayTo}. Added in 1.22.0.
 *
 * - `rows` — per-table arrays of rows that existed as of `asOfAt`.
 *   Keys are table names; values are the row objects (in last-write
 *   order — last write wins for duplicate-keyed inserts).
 * - `asOfVersion` / `asOfAt` — the version + wall-clock of the LAST
 *   entry folded into the result. May be earlier than `targetAt` if
 *   no entries existed between the last-included entry and the
 *   target.
 * - `truncated` — `true` when the log has been trimmed past the
 *   target window (`changeLog[0].version > 1 && changeLog[0].at >
 *   targetAt`). In this case, `rows` represents the state walked
 *   forward from the OLDEST retained entry — NOT the actual state
 *   at `targetAt`. The caller should treat the result as
 *   "best-effort given retention window" and warn the operator.
 */
export type ReplayResult = {
	asOfVersion: number;
	asOfAt: number;
	rows: Record<string, ReadonlyArray<unknown>>;
	truncated: boolean;
};

export type SyncEngineOptions = {
	/**
	 * Stable identifier for this engine instance. Defaults to a per-process
	 * random UUID. Pass a stable value (e.g. `${hostname}:${shardId}`) when
	 * running a fleet of engines behind a cluster bus — 1.17.0+ resume
	 * cursors carry the originating `instanceId`, so a client that reconnects
	 * to a different shard can request a catch-up against the original's
	 * change feed only if that instance's id matches a peer the new shard
	 * knows about.
	 */
	instanceId?: string;
	/**
	 * How many recent changes to retain for resumable reconnects. A client that
	 * reconnects within this window gets a catch-up diff; beyond it, a fresh
	 * snapshot. Defaults to 1024.
	 */
	changeLogSize?: number;
	/**
	 * Time-based change-log retention: drop entries older than this many ms,
	 * in addition to the count cap above. Lets a high-throughput engine keep
	 * a SHORT log (e.g. "60s of changes") regardless of count, which both
	 * bounds memory and bounds the catch-up work on reconnect. Defaults to
	 * `null` — only the count cap (`changeLogSize`) applies.
	 *
	 * Added in 1.13.0.
	 */
	changeLogRetainMs?: number | null;
	/**
	 * Run every mutation inside your database's transaction (see
	 * {@link TransactionRunner}): the handler's writes commit all-or-nothing, and
	 * the engine emits the resulting diff only after the commit. Omit to run
	 * mutations without a transaction (each writer call is its own DB op).
	 */
	transaction?: TransactionRunner;
	/**
	 * Declarative, row-level permissions keyed by table (see
	 * {@link definePermissions}). Read rules filter every row the engine emits;
	 * write rules gate `actions.insert/update/delete`. Add more later with
	 * {@link SyncEngine.registerPermissions}. Type the rules at the
	 * `definePermissions<YourCtx>(...)` call site; the engine accepts any context
	 * (it threads `ctx` untyped to your rules).
	 */
	permissions?: PermissionsDefinition<any>;
	/**
	 * Declarative row schemas keyed by table (see {@link defineSchema}): writes
	 * are validated against them, and `migrate` lazily upcasts rows on read. Add
	 * more later with {@link SyncEngine.registerSchema}.
	 */
	schemas?: SchemaDefinition;
	/**
	 * Cross-client cache for reactive query results, keyed by
	 * `(collection, params, ctx)` — equivalent subscribers reuse a single
	 * cached snapshot on initial subscribe instead of each re-running the
	 * query body. Per-batch dedup (already in `reactivePairs` since 1.1) is
	 * unchanged; this adds *cross-batch* sharing.
	 *
	 * Entries are invalidated when a write overlaps their read set (same
	 * `isReactiveAffected` check live subscriptions use), and bounded by an
	 * LRU + an optional TTL.
	 *
	 * Defaults: `{ max: 256, ttlMs: 60_000 }`. Pass `{ max: 0 }` to disable.
	 */
	reactiveCache?: {
		max?: number;
		ttlMs?: number;
	};
	/**
	 * Per-call telemetry for `sandboxedHandler` mutations. When set, every
	 * sandboxed call fires `onMetrics(record)` after completion with
	 * `{ id, mutationName, durationMs, cpuMs, heapBytes, ok, errorName,
	 * errorMessage, timestamp }`. Wire to a sync collection, your
	 * observability backend, a Drizzle table, anything you want.
	 *
	 * Hook failures are swallowed (a misbehaving metrics sink must NOT
	 * crash the caller's mutation). Adding the hook switches the runner
	 * to `callable.callWithMetrics`, which is a small per-call cost
	 * (~0.05 ms) — disable for hot-path mutations that don't need it.
	 *
	 * Off by default.
	 *
	 * @see {@link HandlerMetricsRecord}
	 */
	handlerMetrics?: HandlerMetricsHook;
	/**
	 * Allowlist + auth-injection map for `actions.fetch(url, init)` calls
	 * issued from inside a `sandboxedHandler`. Each entry is keyed by
	 * hostname (`'api.stripe.com'`); the value's `authorization` is a
	 * sync or async callback computed on the host so the secret never
	 * crosses into the JSC sandbox. Requests to non-allowlisted hosts
	 * are rejected before any network call.
	 *
	 * Without this set, `actions.fetch` throws "no bridgeFetch config."
	 * Plain (non-sandboxed) handlers don't use this — they can just call
	 * `fetch` directly since they run in the host process.
	 *
	 * @see {@link BridgeFetchConfig}
	 */
	bridgeFetch?: BridgeFetchConfig;
	/**
	 * Seed the engine's change log on boot from a prior snapshot — produced by
	 * {@link SyncEngine.exportChangeLog} on the previous instance, persisted by
	 * the host across a shard reboot, then handed back here. Cursors that
	 * referenced this engine's `instanceId` stay resumable past the restart
	 * (provided their last-seen point still lives in the retained log).
	 *
	 * The snapshot's `instanceId` MUST match `options.instanceId` (otherwise
	 * `createSyncEngine` throws — a wrong-id restore would silently break the
	 * resume contract). Snapshot `version` becomes this engine's local
	 * monotonic version; entries are inserted in version order. Subscribers,
	 * permissions, schemas, schedules, packs, mutations, and the reactive
	 * cache are NOT in the snapshot — re-register them as normal after
	 * `createSyncEngine` returns. Added in 1.19.0.
	 */
	initialChangeLog?: ChangeLogSnapshot;
	/**
	 * Maximum concurrent in-flight mutations (`runMutation` + `runMutations`).
	 * Calls beyond the limit wait in a FIFO queue and run as slots free up;
	 * `engine.metrics().mutations.queued` surfaces the queue depth.
	 *
	 * A single tenant flooding `runMutation` can otherwise drive unbounded
	 * memory growth (per-mutation `actions` buffers, retry timers, sandbox
	 * invocations queued against the isolate pool). Set this to a value
	 * appropriate for the host's tenant tier — e.g. `32` for a free tier,
	 * `256` for paid. Without this option the engine is unbounded
	 * (matching pre-1.20 behavior).
	 *
	 * Sandboxed mutations are gated by the same semaphore. If you need
	 * finer-grained control (sandbox-only throttling), see
	 * `@absolutejs/isolated-jsc`'s pool size — that's the lower layer.
	 *
	 * Added in 1.20.0.
	 */
	mutationConcurrency?: number;
	/**
	 * Cap on the queue of waiting mutations once `mutationConcurrency` is
	 * saturated. Calls beyond this cap throw {@link MutationQueueOverflowError}
	 * immediately instead of queueing — the host can surface a clean 429 or
	 * apply a tenant-specific shed policy. Defaults to unbounded (queue
	 * never rejects). Only meaningful when `mutationConcurrency` is set.
	 *
	 * Added in 1.20.0.
	 */
	mutationQueueLimit?: number;
	/**
	 * Per-tenant active-subscription cap. Symmetric to
	 * {@link SyncEngineOptions.mutationConcurrency} on the read side: a
	 * single tenant opening thousands of subscriptions would otherwise
	 * exhaust the engine's per-subscription bookkeeping
	 * (`active`/`tableIndex` Maps, the reactive cache, per-row diff
	 * computation cost).
	 *
	 * `key` derives a tenant identifier from `(ctx, args)`; returning
	 * `undefined` skips the cap for that call (e.g. internal/system
	 * subscriptions). When the active count for a key reaches `max`, the
	 * next `subscribe` throws {@link SubscriptionLimitError} BEFORE any
	 * authorize, hydrate, or state allocation — so a denied call leaks
	 * nothing.
	 *
	 * Active counts are surfaced through `engine.metrics().subscriptions.byTenant`
	 * for tier monitoring. Added in 1.20.1.
	 */
	subscriptionLimit?: {
		max: number;
		key: (ctx: unknown, args: { collection: string }) => string | undefined;
	};
	/**
	 * Optional OpenTelemetry tracer provider. When set, the engine
	 * wraps `subscribe`, `runMutation`, `runMutations`, and cluster
	 * fan-out in spans named `sync.<op>` with `ABS_ATTRS` semantic
	 * conventions (`abs.engine.id`, `abs.collection`, `abs.mutation`,
	 * etc.). When absent, all tracing is a zero-allocation noop —
	 * existing call sites pay nothing. Added in 1.21.0.
	 *
	 * Pass any `@opentelemetry/api`-compatible `TracerProvider`. See
	 * `@absolutejs/telemetry` for the type shape — sync re-uses its
	 * helpers but doesn't peer-dep `@opentelemetry/api` directly.
	 *
	 * @example
	 * ```ts
	 * import { NodeTracerProvider } from '@opentelemetry/sdk-node';
	 * const tp = new NodeTracerProvider({ ... });
	 * tp.register();
	 * const engine = createSyncEngine({ tracerProvider: tp });
	 * ```
	 */
	tracerProvider?: TelemetryTracerProvider;
};

const defaultKey = (row: unknown): RowKey => (row as { id: RowKey }).id;

const shallowEqual = (a: unknown, b: unknown): boolean => {
	if (a === b) {
		return true;
	}
	if (
		typeof a !== 'object' ||
		typeof b !== 'object' ||
		a === null ||
		b === null
	) {
		return false;
	}
	const aKeys = Object.keys(a);
	const bKeys = Object.keys(b);
	return (
		aKeys.length === bKeys.length &&
		aKeys.every(
			(k) =>
				(a as Record<string, unknown>)[k] ===
				(b as Record<string, unknown>)[k]
		)
	);
};

/**
 * Per-object stable identifier — paired with {@link stableSubKey} so two
 * subscriptions that share the same `(collection, params, ctx)` get the same
 * key and have their reactive rerun deduplicated within a change batch (the
 * fan-out fix in {@link reactivePairs}). Falls back to an incrementing id
 * stored on a WeakMap for values JSON can't represent (functions, classes,
 * cyclic structures), so identity-equal ctxs still share a key while
 * different-identity ones don't accidentally merge.
 */
const subKeyIds = new WeakMap<object, string>();
let subKeyCounter = 0;
const stableValueKey = (value: unknown): string => {
	if (value === undefined) return 'u';
	if (value === null) return 'n';
	const tag = typeof value;
	if (tag === 'string') return `s:${value as string}`;
	if (tag === 'number' || tag === 'boolean' || tag === 'bigint') {
		return `${tag[0]}:${String(value)}`;
	}
	if (tag !== 'object') return `${tag[0]}:fn`;
	try {
		// Stable ordering: sort keys before serialising so { a, b } and { b, a }
		// produce the same string. JSON.stringify with a replacer keeps it tight.
		return `o:${JSON.stringify(value, (_k, v): unknown => {
			if (v === null || typeof v !== 'object' || Array.isArray(v))
				return v;
			const record = v as Record<string, unknown>;
			const sorted: Record<string, unknown> = {};
			for (const key of Object.keys(record).sort()) {
				sorted[key] = record[key];
			}

			return sorted;
		})}`;
	} catch {
		// Cyclic or unserializable — fall back to per-object identity.
		const obj = value as object;
		let id = subKeyIds.get(obj);
		if (id === undefined) {
			subKeyCounter += 1;
			id = `i${subKeyCounter}`;
			subKeyIds.set(obj, id);
		}

		return `i:${id}`;
	}
};

const stableSubKey = (
	collection: string,
	params: unknown,
	ctx: unknown
): string => `${collection}|${stableValueKey(params)}|${stableValueKey(ctx)}`;

/** Shallow-equal ignoring the search score field — used to suppress re-emitting
 * a search result whose only change is BM25 score drift as the corpus grows. */
const equalsIgnoringScore = (a: unknown, b: unknown): boolean => {
	if (
		typeof a !== 'object' ||
		typeof b !== 'object' ||
		a === null ||
		b === null
	) {
		return a === b;
	}
	const strip = (value: Record<string, unknown>) =>
		Object.keys(value).filter((k) => k !== SEARCH_SCORE_FIELD);
	const aKeys = strip(a as Record<string, unknown>);
	const bKeys = strip(b as Record<string, unknown>);
	return (
		aKeys.length === bKeys.length &&
		aKeys.every(
			(k) =>
				(a as Record<string, unknown>)[k] ===
				(b as Record<string, unknown>)[k]
		)
	);
};

/**
 * The Tier 3 sync engine: a registry of collections plus the view syncer. It is
 * transport-agnostic — `subscribe` returns the initial snapshot and an
 * `onDiff` stream, which an Elysia/SSE layer wires to a connection, and
 * `applyChange` is the change feed you drive from your mutations.
 *
 * Access control is first-class: every subscribe runs the collection's
 * `authorize`, and a collection's `match`/`hydrate` scope rows to the caller, so
 * a change to a row the caller can't see never reaches them.
 */
export const createSyncEngine = (
	options: SyncEngineOptions = {}
): SyncEngine => {
	// Heterogeneous registry: `any` here is what lets collections of different
	// row/param/context types share one map (the public `register`/`subscribe`
	// surface stays fully typed).
	const registry = new Map<
		string,
		| CollectionDefinition<any, any, any>
		| JoinCollectionDefinition<any, any, any, any, any>
		| GraphCollectionDefinition<any, any, any>
		| ReactiveQueryDefinition<any, any, any>
		| SearchCollectionDefinition<any, any, any>
	>();
	const mutations = new Map<string, MutationDefinition<any, any, any>>();
	// Lazy sandbox runners keyed by mutation name. Built on first call to a
	// mutation that has `sandboxedHandler` set; reused thereafter. Engine has
	// no teardown; the OS reaps the isolate workers on process exit.
	const sandboxRunners = new Map<
		string,
		(
			args: unknown,
			ctx: unknown,
			actions: MutationActions
		) => Promise<unknown>
	>();
	const writers = new Map<string, TableWriter>();
	const readers = new Map<string, TableReader>();
	const schedules = new Map<string, ScheduleDefinition>();
	// Pack registry — table -> owning pack name, and the list of registered
	// packs for engine.inspect().packs.
	const packTableOwners = new Map<string, string>();
	const registeredPacks: RegisteredPack[] = [];
	// Declarative row-level permissions, keyed by table. Stored with an `unknown`
	// context — the engine threads ctx untyped — while the public
	// `definePermissions`/`registerPermissions` surface stays fully typed.
	const permissions = new Map<string, TablePermissions<unknown, unknown>>();
	for (const [table, rules] of Object.entries(options.permissions ?? {})) {
		permissions.set(table, rules as TablePermissions<unknown, unknown>);
	}
	const readRuleFor = (
		table: string
	): ReadRule<unknown, unknown> | undefined => permissions.get(table)?.read;
	const writeRuleFor = (
		table: string,
		op: 'insert' | 'update' | 'delete'
	): WriteRule<unknown, unknown> | undefined => {
		const rules = permissions.get(table);
		return rules?.[op] ?? rules?.write;
	};
	// Declarative row schemas, keyed by table.
	const schemas = new Map<string, TableSchema<unknown>>();
	for (const [table, schema] of Object.entries(options.schemas ?? {})) {
		schemas.set(table, schema as TableSchema<unknown>);
	}
	// CRDT fields, keyed by table: field name -> mergeable backend. Set via
	// registerCrdt; consulted in makeActions to merge (not overwrite) on write.
	const crdtFields = new Map<
		string,
		Record<string, CrdtMergeable<unknown>>
	>();
	// Validate a write against its table's schema: every field on insert; only
	// the supplied fields on update. Throws SchemaError naming the bad field.
	const validateWrite = (
		table: string,
		op: 'insert' | 'update',
		row: unknown
	) => {
		const schema = schemas.get(table);
		if (schema === undefined || typeof row !== 'object' || row === null) {
			return;
		}
		const record = row as Record<string, unknown>;
		for (const [fieldName, validate] of Object.entries(schema.fields)) {
			const present = fieldName in record;
			if (op === 'update' && !present) {
				continue;
			}
			if (!validate(record[fieldName])) {
				throw new SchemaError(table, fieldName);
			}
		}
	};
	// Lazily upcast a stored/raw row to the current shape (identity if no migrate).
	const migrateRow = (table: string, row: unknown): unknown => {
		const migrate = schemas.get(table)?.migrate;
		return migrate ? migrate(row) : row;
	};
	// Reactive (read-set-tracked) subscriptions, scanned on each change since
	// their dependencies (the tables they read) are dynamic, not in tableIndex.
	const reactiveSubs = new Set<
		Extract<ActiveSubscription, { kind: 'reactive' }>
	>();
	// Search subscriptions + one shared index per search collection, kept live
	// from the source table's change feed (like reactiveSubs, not in tableIndex).
	const searchSubs = new Set<
		Extract<ActiveSubscription, { kind: 'search' }>
	>();
	const searchIndexes = new Map<
		string,
		{
			index: SearchIndex<unknown, unknown>;
			definition: SearchCollectionDefinition<unknown, unknown, unknown>;
			hydrated: boolean;
		}
	>();
	const active = new Map<string, Set<ActiveSubscription>>();
	// Which collections read each table — so a table change fans to all of them.
	const tableIndex = new Map<string, Set<string>>();

	// Monotonic change feed: every applyChange bumps `version` and appends to a
	// bounded log, so a client can resume from the version it last applied.
	const changeLogSize = options.changeLogSize ?? 1024;
	const changeLogRetainMs = options.changeLogRetainMs ?? null;
	const changeLog: LoggedChange[] = [];
	let version = 0;
	// Engine-level counters surfaced via `engine.metrics()` (1.13.0).
	const engineStartedAt = Date.now();
	let mutationsCompleted = 0;
	let mutationsFailed = 0;
	let mutationsRetried = 0;
	let mutationsInFlight = 0;

	// 1.20.0: optional FIFO semaphore gating mutation entry. Capacity is
	// `mutationConcurrency`; waiters queue with optional `mutationQueueLimit`
	// rejection. New arrivals that find ANY queued waiter also queue (FIFO
	// preservation), so a steady arrival rate can't starve an early waiter.
	const mutationWaiters: (() => void)[] = [];
	let mutationsQueued = 0;

	// 1.24.0: G7 migration fence. While `activeFences` is non-empty,
	// `runMutation` rejects with EngineFencedError. Reads remain
	// available so the snapshot transport doesn't block subscribers.
	// Multi-fence compose: every handle must lift() before the engine
	// unfences. The reason of the OLDEST fence is reported on rejection.
	const activeFences = new Set<FenceHandle>();

	const acquireMutationSlot = async (): Promise<void> => {
		const limit = options.mutationConcurrency;
		if (limit === undefined) {
			// No semaphore — still bump the metric so `inFlight` is
			// always accurate, but skip all the queue plumbing.
			mutationsInFlight += 1;
			return;
		}
		// FIFO: if a slot is open AND no waiters are queued, take it
		// synchronously. The increment is part of the same synchronous
		// step as the check, so two arrivals can't both pass when only
		// one slot remains.
		if (mutationsInFlight < limit && mutationWaiters.length === 0) {
			mutationsInFlight += 1;
			return;
		}
		// Queue, or reject if the queue is also capped.
		const queueLimit = options.mutationQueueLimit;
		if (queueLimit !== undefined && mutationsQueued >= queueLimit) {
			throw new MutationQueueOverflowError(queueLimit);
		}
		mutationsQueued += 1;
		try {
			await new Promise<void>((resolve) => {
				mutationWaiters.push(resolve);
			});
		} finally {
			mutationsQueued -= 1;
		}
		// Wake means a slot just opened up FOR US (`releaseMutationSlot`
		// only resolves one waiter per release). Claim it now — atomic
		// with the wake step.
		mutationsInFlight += 1;
	};

	const releaseMutationSlot = (): void => {
		mutationsInFlight -= 1;
		if (options.mutationConcurrency === undefined) return;
		const next = mutationWaiters.shift();
		if (next !== undefined) next();
	};

	// 1.20.1: per-tenant subscription cap. Active count keyed by the
	// host-supplied `subscriptionLimit.key(ctx, args)`. `undefined` from
	// `key()` means "exempt this call from the cap" — internal / system
	// subscriptions can skip the bookkeeping entirely.
	const subscriptionsByTenant = new Map<string, number>();

	const acquireSubscriptionSlot = (
		ctx: unknown,
		args: { collection: string }
	): string | undefined => {
		const cap = options.subscriptionLimit;
		if (cap === undefined) return undefined;
		const tenantKey = cap.key(ctx, args);
		if (tenantKey === undefined) return undefined;
		const active = subscriptionsByTenant.get(tenantKey) ?? 0;
		if (active >= cap.max) {
			throw new SubscriptionLimitError(tenantKey, cap.max, active);
		}
		subscriptionsByTenant.set(tenantKey, active + 1);
		return tenantKey;
	};

	const releaseSubscriptionSlot = (tenantKey: string | undefined): void => {
		if (tenantKey === undefined) return;
		const active = subscriptionsByTenant.get(tenantKey);
		if (active === undefined || active <= 1) {
			subscriptionsByTenant.delete(tenantKey);
		} else {
			subscriptionsByTenant.set(tenantKey, active - 1);
		}
	};

	// Cross-client reactive query cache (1.3+). Keyed by stableSubKey, holds
	// the result + read set so a fresh subscribe with the same key reuses the
	// rerun instead of hitting the DB again. Per-batch dedup (since 1.1) is
	// already in `reactivePairs`; this lifts the sharing across batches.
	//
	// Entries are invalidated when a write overlaps the cached read set (same
	// `isReactiveAffected` check live subs use). LRU-bounded; optional TTL.
	const reactiveCacheMax = options.reactiveCache?.max ?? 256;
	const reactiveCacheTtlMs = options.reactiveCache?.ttlMs ?? 60_000;
	type CachedRerun = {
		rerunKey: string;
		rows: unknown[];
		readTables: Set<string>;
		readKeys: Set<string>;
		rangeDeps: RangeDep[];
		version: number;
		expiresAt: number;
	};
	// `Map` preserves insertion order — re-set on access for LRU semantics.
	const cachedReruns = new Map<string, CachedRerun>();
	const touchCacheEntry = (key: string, entry: CachedRerun) => {
		cachedReruns.delete(key);
		cachedReruns.set(key, entry);
	};
	const readCacheEntry = (key: string): CachedRerun | undefined => {
		if (reactiveCacheMax <= 0) return undefined;
		const entry = cachedReruns.get(key);
		if (entry === undefined) return undefined;
		if (reactiveCacheTtlMs > 0 && entry.expiresAt < Date.now()) {
			cachedReruns.delete(key);

			return undefined;
		}
		touchCacheEntry(key, entry);

		return entry;
	};
	const writeCacheEntry = (entry: CachedRerun) => {
		if (reactiveCacheMax <= 0) return;
		cachedReruns.set(entry.rerunKey, entry);
		// LRU eviction: oldest insertion wins out when over budget.
		while (cachedReruns.size > reactiveCacheMax) {
			const oldest = cachedReruns.keys().next().value;
			if (oldest === undefined) break;
			cachedReruns.delete(oldest);
		}
	};
	// Devtools activity stream — listeners are notified of changes, mutation
	// outcomes, and subscribe/unsubscribe. Cheap (a no-op) when no one's watching.
	const activityListeners = new Set<(event: EngineActivity) => void>();
	const emitActivity = (event: EngineActivity) => {
		for (const listener of activityListeners) {
			listener(event);
		}
	};
	// Outbound CDC stream subscribers — `streamChanges()` adds itself here.
	// Notifications fire from `logChange` so every appended log entry reaches
	// every active streamer atomically with the log push.
	const streamSubscribers = new Set<(entry: LoggedChange) => void>();
	const runInTransaction = options.transaction;
	// Cluster fan-out: a stable id so we ignore our own broadcasts, and the bus
	// (set by connectCluster) we publish locally-committed changes to. Pass
	// `options.instanceId` for stable cross-process identity (e.g. the
	// hostname or a config-supplied UUID) — 1.17.0+ resume cursors travel
	// across instances, so the id needs to be stable across restarts if you
	// want resume to keep working past a reboot.
	const instanceId =
		options.instanceId ??
		globalThis.crypto?.randomUUID?.() ??
		`i${Math.random()}`;
	let clusterBus: ClusterBus | undefined;

	// 1.21.0: OTel tracer (noop when options.tracerProvider is unset).
	// All hot-path tracing flows through this — zero allocations when
	// the provider is absent because the noop tracer is a singleton.
	const tracer = tracerOrNoop(options.tracerProvider, '@absolutejs/sync');

	// 1.19.0: optional boot-time restore from a prior engine's snapshot. Must
	// happen BEFORE any local writes — we validate by checking version === 0
	// inside importChangeLog and call it once here from the construction path.
	const importChangeLog = (snapshot: ChangeLogSnapshot): number => {
		if (version !== 0) {
			throw new Error(
				`[sync] importChangeLog: engine already has version ${version}; ` +
					`restore must happen before any local writes commit.`
			);
		}
		if (snapshot.instanceId !== instanceId) {
			throw new Error(
				`[sync] importChangeLog: snapshot instanceId "${snapshot.instanceId}" ` +
					`does not match this engine's instanceId "${instanceId}". ` +
					`Pass options.instanceId = "${snapshot.instanceId}" to createSyncEngine.`
			);
		}
		// Adopt version + entries. logChange's retention sweeps still apply,
		// so over-large snapshots get trimmed exactly like live logs would.
		version = snapshot.version;
		for (const entry of snapshot.entries) {
			changeLog.push(entry);
		}
		// Apply count cap once after the bulk push (cheaper than per-entry).
		while (changeLog.length > changeLogSize) {
			changeLog.shift();
		}
		// Apply time-based retention to the imported tail.
		if (changeLogRetainMs !== null && changeLogRetainMs > 0) {
			const cutoff = Date.now() - changeLogRetainMs;
			while (changeLog.length > 0 && changeLog[0]!.at < cutoff) {
				changeLog.shift();
			}
		}
		return snapshot.entries.length;
	};

	if (options.initialChangeLog !== undefined) {
		importChangeLog(options.initialChangeLog);
	}

	const broadcast = (
		changes: { table: string; change: RowChange<unknown> }[],
		// 1.17.0 — the local version at the moment of this broadcast, so
		// peers can log the changes against `(instanceId, originVersion)`
		// and serve cross-instance resume from their own log.
		originVersion: number
	) => {
		if (clusterBus !== undefined && changes.length > 0) {
			void clusterBus.publish({ changes, origin: instanceId, originVersion });
		}
	};

	const subsFor = (collection: string) => {
		let set = active.get(collection);
		if (set === undefined) {
			set = new Set();
			active.set(collection, set);
		}
		return set;
	};

	const addTableIndex = (table: string, name: string) => {
		let set = tableIndex.get(table);
		if (set === undefined) {
			set = new Set();
			tableIndex.set(table, set);
		}
		set.add(name);
	};

	/** A side change that fails its filter becomes a leave (delete from the join). */
	const sideChange = (
		change: RowChange<unknown>,
		match?: (row: unknown) => boolean
	): RowChange<unknown> =>
		change.op !== 'delete' && match !== undefined && !match(change.row)
			? { op: 'delete', row: change.row }
			: change;

	const EMPTY_DIFF: ViewDiff<unknown> = {
		added: [],
		removed: [],
		changed: []
	};

	/** Apply one change to a subscription's state and return its diff (no emit). */
	const subscriptionDiff = async (
		subscription: ActiveSubscription,
		table: string,
		change: RowChange<unknown>
	): Promise<ViewDiff<unknown>> => {
		if (subscription.kind === 'graph') {
			return subscription.instance.applyChange(table, change);
		}
		if (subscription.kind === 'join') {
			const js = subscription.join;
			if (table === js.leftTable) {
				return js.op.applyLeft(sideChange(change, js.leftMatch));
			}
			if (table === js.rightTable) {
				return js.op.applyRight(sideChange(change, js.rightMatch));
			}
			return EMPTY_DIFF;
		}
		if (subscription.kind === 'reactive') {
			// Reactive subs re-run as a whole (see reactivePairs), not per change.
			return EMPTY_DIFF;
		}
		if (subscription.kind === 'search') {
			// Search subs re-rank as a whole (see searchPairs), not per change.
			return EMPTY_DIFF;
		}
		if (subscription.incremental) {
			try {
				return subscription.view.apply(change);
			} catch {
				// The predicate couldn't decide this change (e.g. an operator the
				// inferred matcher doesn't support) — degrade to a correct refetch
				// rather than a wrong diff.
				return subscription.view.reset(await subscription.rehydrate());
			}
		}
		return subscription.view.reset(await subscription.rehydrate());
	};

	/** Active subscriptions whose collection reads `table`. */
	const subscriptionsForTable = function* (
		table: string
	): Generator<ActiveSubscription> {
		const names = tableIndex.get(table);
		if (names === undefined) {
			return;
		}
		for (const name of names) {
			const set = active.get(name);
			if (set === undefined) {
				continue;
			}
			yield* set;
		}
	};

	/**
	 * Net a batch's per-change diffs by key, relative to the pre-batch state, so a
	 * mutation that touches the same row twice collapses to one coherent change:
	 * add-then-remove cancels, add-then-update stays an add, remove-then-add
	 * becomes a change.
	 */
	const mergeViewDiffs = (
		diffs: ViewDiff<unknown>[],
		key: (row: unknown) => RowKey
	): ViewDiff<unknown> => {
		type Net = { state: 'added' | 'changed' | 'removed'; row: unknown };
		const net = new Map<RowKey, Net>();
		for (const diff of diffs) {
			for (const row of diff.removed) {
				const previous = net.get(key(row));
				if (previous?.state === 'added') {
					net.delete(key(row));
				} else {
					net.set(key(row), { state: 'removed', row });
				}
			}
			for (const row of diff.added) {
				const previous = net.get(key(row));
				net.set(key(row), {
					state: previous?.state === 'removed' ? 'changed' : 'added',
					row
				});
			}
			for (const row of diff.changed) {
				const previous = net.get(key(row));
				net.set(key(row), {
					state: previous?.state === 'added' ? 'added' : 'changed',
					row
				});
			}
		}
		const added: unknown[] = [];
		const changed: unknown[] = [];
		const removed: unknown[] = [];
		for (const { state, row } of net.values()) {
			if (state === 'added') {
				added.push(row);
			} else if (state === 'changed') {
				changed.push(row);
			} else {
				removed.push(row);
			}
		}
		return { added, changed, removed };
	};

	type ReactiveSub = Extract<ActiveSubscription, { kind: 'reactive' }>;

	const depKey = (table: string, key: RowKey): string => `${table} ${key}`;

	/** The key of a changed row under its table's reader key (if one is set). */
	const changedKeyFor = (
		table: string,
		change: RowChange<unknown>
	): RowKey | undefined => readers.get(table)?.key?.(change.row);

	/**
	 * An instrumented read handle: `all` records a full-table dependency; `get`
	 * records a precise row-key dependency when the table's reader has a `key`
	 * (else falls back to a table dependency).
	 */
	const makeReadHandle = (
		ctx: unknown,
		readTables: Set<string>,
		readKeys: Set<string>,
		rangeDeps: RangeDep[],
		// Schedules read unscoped (trusted server code); subscriptions apply rules.
		applyRules = true
	): ReadHandle => {
		const readerFor = (table: string): TableReader => {
			const reader = readers.get(table);
			if (reader === undefined) {
				throw new Error(
					`No reader registered for table "${table}" — register one with engine.registerReader`
				);
			}
			return reader;
		};
		const ruleFor = (table: string) =>
			applyRules ? readRuleFor(table) : undefined;

		return {
			all: async (table) => {
				readTables.add(table);
				// Migrate raw rows to the current shape, then scope by read rule.
				const rows = [...(await readerFor(table).all(ctx))].map((row) =>
					migrateRow(table, row)
				);
				const rule = ruleFor(table);
				return (
					rule ? rows.filter((row) => rule(ctx, row)) : rows
				) as never[];
			},
			get: async (table, key) => {
				const reader = readerFor(table);
				if (reader.get === undefined) {
					throw new Error(
						`Reader for table "${table}" has no get(); use db.all() or add get`
					);
				}
				if (reader.key !== undefined) {
					readKeys.add(depKey(table, key));
				} else {
					readTables.add(table);
				}
				const raw = await reader.get(key, ctx);
				const row =
					raw === undefined ? undefined : migrateRow(table, raw);
				const rule = ruleFor(table);
				// A row the caller can't read reads as absent.
				return (
					rule && row !== undefined && !rule(ctx, row)
						? undefined
						: row
				) as never;
			},
			where: async (table, predicate) => {
				const reader = readerFor(table);
				const rule = ruleFor(table);
				// Fold the read rule into the range predicate, so an unreadable row
				// never matches and a visibility flip still re-runs the query.
				const effective = (
					rule
						? (row: unknown) =>
								(predicate as (r: unknown) => boolean)(row) &&
								rule(ctx, row)
						: (predicate as (row: unknown) => boolean)
				) as (row: unknown) => boolean;
				const matched = [...(await reader.all(ctx))]
					.map((row) => migrateRow(table, row))
					.filter(effective);
				if (reader.key !== undefined) {
					// Remember which rows matched, so an update/delete that pulls a
					// row out of the range still re-runs (it's in this key set).
					const key = reader.key;
					rangeDeps.push({
						table,
						predicate: effective,
						keys: new Set(matched.map(key))
					});
				} else {
					readTables.add(table);
				}
				return matched as never[];
			}
		};
	};

	const writerFor = (table: string): TableWriter => {
		const writer = writers.get(table);
		if (writer === undefined) {
			throw new Error(
				`No writer registered for table "${table}" — register one with engine.registerWriter, or use actions.change`
			);
		}
		return writer;
	};

	// Load the committed row a write targets (by the table's reader), if any — so
	// authorization and CRDT merge both reflect committed state, not the payload.
	const readExisting = async (
		table: string,
		value: unknown,
		ctx: unknown
	): Promise<unknown> => {
		const reader = readers.get(table);
		if (reader?.get === undefined) {
			return undefined;
		}
		const id = reader.key
			? reader.key(value)
			: (value as { id?: RowKey }).id;
		return id === undefined ? undefined : reader.get(id, ctx);
	};

	// Enforce a table's declarative write rule before the writer runs (so a deny
	// rolls the transaction back). For update/delete, evaluate the rule against the
	// *existing* row when a reader can load it — so the check reflects committed
	// state, not a client-supplied payload.
	const authorizeWrite = async (
		table: string,
		op: 'insert' | 'update' | 'delete',
		value: unknown,
		ctx: unknown
	) => {
		const rule = writeRuleFor(table, op);
		if (rule === undefined) {
			return;
		}
		let subject = value;
		if (op !== 'insert') {
			const existing = await readExisting(table, value, ctx);
			if (existing !== undefined) {
				subject = existing;
			}
		}
		if (!rule(ctx, subject)) {
			throw new UnauthorizedError(`${op} on table "${table}"`);
		}
	};

	// Merge a write's CRDT fields into the committed row (so concurrent writers
	// converge) instead of overwriting them. A no-op for tables without CRDT
	// fields. On insert the base is the empty state; on update it's the stored
	// field value. Returns the row patch the writer should persist.
	const mergeCrdtFields = async (
		table: string,
		op: 'insert' | 'update',
		data: unknown,
		ctx: unknown
	): Promise<unknown> => {
		const fields = crdtFields.get(table);
		if (fields === undefined || data === null || typeof data !== 'object') {
			return data;
		}
		const incoming = data as Record<string, unknown>;
		const existing =
			op === 'update' ? await readExisting(table, data, ctx) : undefined;
		const base =
			existing !== null && typeof existing === 'object'
				? (existing as Record<string, unknown>)
				: undefined;
		const merged: Record<string, unknown> = { ...incoming };
		for (const [field, adapter] of Object.entries(fields)) {
			if (incoming[field] === undefined) {
				continue;
			}
			merged[field] = adapter.merge(
				base?.[field] ?? adapter.empty(),
				incoming[field]
			);
		}
		return merged;
	};

	/**
	 * Build the write actions a mutation or schedule handler uses, collecting its
	 * changes into a fresh buffer (so a transaction that retries/rolls back never
	 * double-emits). `tx` threads to each writer. `enforce` applies write
	 * permission rules (mutations); schedules run trusted, so they pass `false`.
	 */
	const makeActions = (tx: unknown, ctx: unknown, enforce: boolean) => {
		const buffered: { table: string; change: RowChange<unknown> }[] = [];
		const actions: MutationActions = {
			change: (collection, change) => {
				buffered.push({
					table: collection,
					change: change as RowChange<unknown>
				});
				return Promise.resolve();
			},
			insert: async (table, data) => {
				// Schema is data integrity — validated for trusted schedules too.
				validateWrite(table, 'insert', data);
				if (enforce) {
					await authorizeWrite(table, 'insert', data, ctx);
				}
				const merged = await mergeCrdtFields(
					table,
					'insert',
					data,
					ctx
				);
				const row = await writerFor(table).insert(merged, ctx, tx);
				buffered.push({ table, change: { op: 'insert', row } });
				return row;
			},
			update: async (table, data) => {
				validateWrite(table, 'update', data);
				if (enforce) {
					await authorizeWrite(table, 'update', data, ctx);
				}
				const merged = await mergeCrdtFields(
					table,
					'update',
					data,
					ctx
				);
				const row = await writerFor(table).update(merged, ctx, tx);
				buffered.push({ table, change: { op: 'update', row } });
				return row;
			},
			delete: async (table, row) => {
				if (enforce) {
					await authorizeWrite(table, 'delete', row, ctx);
				}
				await writerFor(table).delete(row, ctx, tx);
				buffered.push({ table, change: { op: 'delete', row } });
			},
			// Default wall clock. Replay / rebase paths can wrap and pin
			// this; today it's just Date.now().
			now: () => Date.now()
		};
		return { actions, buffered };
	};

	/** Diff a re-run against a sub's current set; updates `current`. Shared by the
	 * reactive and search kinds (both re-run wholesale and diff). `equals` decides
	 * whether a still-present row counts as changed. */
	const diffRerun = (
		sub: {
			key: (row: unknown) => RowKey;
			current: Map<RowKey, unknown>;
		},
		rows: unknown[],
		equals: (a: unknown, b: unknown) => boolean = shallowEqual
	): ViewDiff<unknown> => {
		const next = new Map<RowKey, unknown>();
		for (const row of rows) {
			next.set(sub.key(row), row);
		}
		const added: unknown[] = [];
		const removed: unknown[] = [];
		const changed: unknown[] = [];
		for (const [rowKey, row] of next) {
			const previous = sub.current.get(rowKey);
			if (previous === undefined) {
				added.push(row);
			} else if (!equals(previous, row)) {
				changed.push(row);
			}
		}
		for (const [rowKey, row] of sub.current) {
			if (!next.has(rowKey)) {
				removed.push(row);
			}
		}
		sub.current = next;
		return { added, removed, changed };
	};

	/** Re-run every reactive query whose read set intersects the changed tables. */
	type ReactiveChange = {
		table: string;
		key: RowKey | undefined;
		row: unknown;
	};

	/** Does a change fall in a range dep — matched now, or a member at last read? */
	const inRange = (dep: RangeDep, change: ReactiveChange): boolean =>
		dep.table === change.table &&
		((change.key !== undefined && dep.keys.has(change.key)) ||
			dep.predicate(change.row));

	/** Does any change in the batch overlap this read set? Used for both live
	 * sub invalidation and cross-client cache invalidation. */
	const readSetOverlaps = (
		readTables: Set<string>,
		readKeys: Set<string>,
		rangeDeps: RangeDep[],
		changes: ReactiveChange[]
	): boolean =>
		changes.some(
			(change) =>
				readTables.has(change.table) ||
				(change.key !== undefined &&
					readKeys.has(depKey(change.table, change.key))) ||
				rangeDeps.some((dep) => inRange(dep, change))
		);

	/** Did this batch touch a table, row key, or range the sub read? */
	const isReactiveAffected = (
		sub: ReactiveSub,
		changes: ReactiveChange[]
	): boolean =>
		readSetOverlaps(sub.readTables, sub.readKeys, sub.rangeDeps, changes);

	/** Drop cached reruns whose read set overlaps this write batch. Cheap walk —
	 * the cache is bounded by `reactiveCache.max` (default 256). */
	const invalidateCacheForChanges = (changes: ReactiveChange[]) => {
		if (cachedReruns.size === 0) return;
		for (const [key, entry] of cachedReruns) {
			if (
				readSetOverlaps(
					entry.readTables,
					entry.readKeys,
					entry.rangeDeps,
					changes
				)
			) {
				cachedReruns.delete(key);
			}
		}
	};

	const reactivePairs = async (
		changes: ReactiveChange[]
	): Promise<[ActiveSubscription, ViewDiff<unknown>][]> => {
		// Drop now-stale cache entries before reruns — otherwise a fresh
		// subscriber landing during the batch could read the OLD value.
		invalidateCacheForChanges(changes);

		const pairs: [ActiveSubscription, ViewDiff<unknown>][] = [];
		// Dedupe: subscriptions sharing the same `(collection, params, ctx)`
		// only need ONE rerun per change batch. With 1000 subs on the same
		// query, this drops per-change CPU from O(N) reruns to O(1) — every
		// sub then diffs the shared result against its own `current` and
		// receives its own per-sub frame (which the transport still writes
		// per-WS, see #22 batch-frame fan-out for the next step).
		const sharedRuns = new Map<
			string,
			Promise<Awaited<ReturnType<ReactiveSub['rerun']>>>
		>();
		for (const sub of reactiveSubs) {
			if (!isReactiveAffected(sub, changes)) {
				continue;
			}
			let runPromise = sharedRuns.get(sub.rerunKey);
			if (runPromise === undefined) {
				runPromise = sub.rerun();
				sharedRuns.set(sub.rerunKey, runPromise);
			}
			const { rows, readTables, readKeys, rangeDeps } = await runPromise;
			sub.readTables = readTables;
			sub.readKeys = readKeys;
			sub.rangeDeps = rangeDeps;
			const diff = diffRerun(sub, rows);
			if (!isEmptyViewDiff(diff)) {
				pairs.push([sub, diff]);
			}
		}
		// Refresh cache entries with the freshly-computed rows so subsequent
		// subscribers reuse them without hitting the DB.
		for (const [key, runPromise] of sharedRuns) {
			runPromise
				.then(({ rows, readTables, readKeys, rangeDeps }) => {
					writeCacheEntry({
						expiresAt: Date.now() + reactiveCacheTtlMs,
						rangeDeps,
						readKeys,
						readTables,
						rerunKey: key,
						rows,
						version
					});
				})
				.catch(() => {
					// rerun threw — leave cache as-is (already invalidated above)
				});
		}

		return pairs;
	};

	/** Lazily build + hydrate a search collection's shared index (once). */
	const ensureSearchIndex = async (
		definition: SearchCollectionDefinition<unknown, unknown, unknown>
	) => {
		let entry = searchIndexes.get(definition.name);
		if (entry === undefined) {
			entry = { index: definition.index(), definition, hydrated: false };
			searchIndexes.set(definition.name, entry);
		}
		if (!entry.hydrated) {
			for (const row of await definition.source()) {
				entry.index.add(row);
			}
			entry.hydrated = true;
		}
		return entry;
	};

	/**
	 * Keep search indexes live and re-rank affected search subs: apply each change
	 * to its collection's index, then re-run every sub whose collection changed.
	 * Synchronous — the index ops and re-ranks don't touch the DB.
	 */
	const searchPairs = (
		changes: { table: string; change: RowChange<unknown> }[]
	): [ActiveSubscription, ViewDiff<unknown>][] => {
		const touched = new Set<string>();
		for (const { table, change } of changes) {
			for (const entry of searchIndexes.values()) {
				if (!entry.hydrated || entry.definition.table !== table) {
					continue;
				}
				if (change.op === 'delete') {
					entry.index.remove(entry.definition.key(change.row));
				} else {
					entry.index.add(change.row);
				}
				touched.add(entry.definition.name);
			}
		}
		const pairs: [ActiveSubscription, ViewDiff<unknown>][] = [];
		for (const sub of searchSubs) {
			if (!touched.has(sub.collection)) {
				continue;
			}
			// Ignore pure score drift (BM25 idf shifts as the corpus grows), so a
			// result only re-emits when it enters/leaves or its content changes —
			// not on every unrelated insert.
			const diff = diffRerun(sub, sub.rerun(), equalsIgnoringScore);
			if (!isEmptyViewDiff(diff)) {
				pairs.push([sub, diff]);
			}
		}
		return pairs;
	};

	const logChange = (changeVersion: number, entry: LoggedChange) => {
		changeLog.push(entry);
		// Count-based cap.
		if (changeLog.length > changeLogSize) {
			changeLog.shift();
		}
		// Time-based retention (1.13.0): drop entries older than the
		// configured window. Cheap when the log is small or the head is
		// fresh — we stop the moment we find a young-enough entry.
		if (changeLogRetainMs !== null && changeLogRetainMs > 0) {
			const cutoff = entry.at - changeLogRetainMs;
			while (changeLog.length > 0 && changeLog[0]!.at < cutoff) {
				changeLog.shift();
			}
		}
		// Atomic with the log push — every active CDC streamer sees every
		// entry exactly once, in version order, with no chance of a missed
		// commit between phase-1 catch-up and phase-2 tail.
		for (const subscriber of streamSubscribers) {
			subscriber(entry);
		}
	};

	// 1.17.0 cross-instance cursor encode/decode. Opaque to clients —
	// shaped as base64-ish JSON internally. The client must round-trip
	// what the server returned, unmodified.
	const encodeCursor = (versions: Record<string, number>): string =>
		// Plain JSON is fine; clients treat it as opaque. We don't base64
		// because the cursor lives in a JSON payload anyway (snapshot frame).
		JSON.stringify(versions);
	const decodeCursor = (cursor: string): Record<string, number> | null => {
		try {
			const parsed = JSON.parse(cursor);
			if (typeof parsed !== 'object' || parsed === null) return null;
			const out: Record<string, number> = {};
			for (const [k, v] of Object.entries(parsed)) {
				if (typeof v === 'number') out[k] = v;
			}
			return out;
		} catch {
			return null;
		}
	};
	const currentCursor = (): string => {
		// Snapshot the highest local version + each peer's highest origin
		// version seen so far. Cheap O(log) — single backwards walk grabs
		// the most-recent originVersion per peer.
		const versions: Record<string, number> = { [instanceId]: version };
		for (let i = changeLog.length - 1; i >= 0; i--) {
			const entry = changeLog[i]!;
			if (versions[entry.origin] === undefined) {
				versions[entry.origin] = entry.originVersion;
			}
		}
		return encodeCursor(versions);
	};

	/** Apply a single committed change at its own version (CDC / direct writes). */
	const applyChange = async (
		table: string,
		change: RowChange<unknown>,
		shouldBroadcast = true
	) => {
		version += 1;
		const changeVersion = version;
		const at = Date.now();
		logChange(changeVersion, {
			version: changeVersion,
			table,
			change,
			at,
			origin: instanceId,
			originVersion: changeVersion
		});
		emitActivity({
			type: 'change',
			at,
			table,
			op: change.op,
			version: changeVersion
		});
		// Collect, then emit once at the end: reactive re-runs are async, and
		// emitting before they finish would let the transport flush a partial frame.
		const emissions: [ActiveSubscription, ViewDiff<unknown>][] = [];
		for (const subscription of subscriptionsForTable(table)) {
			const diff = await subscriptionDiff(subscription, table, change);
			if (!isEmptyViewDiff(diff)) {
				emissions.push([subscription, diff]);
			}
		}
		emissions.push(
			...(await reactivePairs([
				{ table, key: changedKeyFor(table, change), row: change.row }
			]))
		);
		emissions.push(...searchPairs([{ table, change }]));
		const cursorForBatch = currentCursor();
		for (const [subscription, diff] of emissions) {
			subscription.onDiff(diff, changeVersion, cursorForBatch);
		}
		if (shouldBroadcast) {
			broadcast([{ table, change }], changeVersion);
		}
	};

	/**
	 * Apply a set of changes atomically: one version bump for the whole batch and
	 * a single net-merged diff per affected subscription. Used by mutations so a
	 * client never renders a torn intermediate state mid-mutation.
	 */
	const applyChangeBatch = async (
		changes: { table: string; change: RowChange<unknown> }[],
		shouldBroadcast = true,
		/**
		 * 1.17.0 — peer-relayed batches override the change-log entry's
		 * `origin` + `originVersion` so a cross-instance client cursor can
		 * later match the entry. Local batches leave this `undefined` and
		 * the entry inherits the engine's own identity.
		 */
		peerOrigin?: { origin: string; originVersion: number }
	) => {
		if (changes.length === 0) {
			return;
		}
		version += 1;
		const batchVersion = version;
		const perSubscription = new Map<
			ActiveSubscription,
			ViewDiff<unknown>[]
		>();
		const reactiveChanges: ReactiveChange[] = [];
		const batchAt = Date.now();
		// 1.17.0: peer-relayed batches override origin/originVersion via
		// `peerOrigin` (set when applyChangeBatch is called from the cluster
		// subscribe path). Defaults to this engine's identity.
		const batchOrigin = peerOrigin?.origin ?? instanceId;
		const batchOriginVersion = peerOrigin?.originVersion ?? batchVersion;
		for (const { table, change } of changes) {
			logChange(batchVersion, {
				version: batchVersion,
				table,
				change,
				at: batchAt,
				origin: batchOrigin,
				originVersion: batchOriginVersion
			});
			emitActivity({
				type: 'change',
				at: batchAt,
				table,
				op: change.op,
				version: batchVersion
			});
			reactiveChanges.push({
				table,
				key: changedKeyFor(table, change),
				row: change.row
			});
			for (const subscription of subscriptionsForTable(table)) {
				// Apply in order to keep operator state correct; collect to merge.
				const diff = await subscriptionDiff(
					subscription,
					table,
					change
				);
				const list = perSubscription.get(subscription);
				if (list === undefined) {
					perSubscription.set(subscription, [diff]);
				} else {
					list.push(diff);
				}
			}
		}
		// Gather all emissions before sending any, so the whole batch — view diffs
		// and reactive re-runs (async) — leaves as one coalesced frame.
		const emissions: [ActiveSubscription, ViewDiff<unknown>][] = [];
		for (const [subscription, diffs] of perSubscription) {
			const merged =
				diffs.length === 1
					? diffs[0]!
					: mergeViewDiffs(diffs, subscription.key);
			if (!isEmptyViewDiff(merged)) {
				emissions.push([subscription, merged]);
			}
		}
		emissions.push(...(await reactivePairs(reactiveChanges)));
		emissions.push(...searchPairs(changes));
		const cursorForBatch = currentCursor();
		for (const [subscription, diff] of emissions) {
			subscription.onDiff(diff, batchVersion, cursorForBatch);
		}
		if (shouldBroadcast) {
			broadcast(changes, batchVersion);
		}
	};

	/**
	 * Normalize a `since` value (number or cursor string) into a per-origin
	 * version vector. A bare `number` is treated as legacy 1.16- form — the
	 * version of THIS instance. A cursor string is the 1.17.0+ multi-origin
	 * shape encoded by `currentCursor()`.
	 */
	const normalizeSince = (since: number | string): Record<string, number> | null => {
		if (typeof since === 'number') {
			return { [instanceId]: since };
		}
		return decodeCursor(since);
	};

	/**
	 * Can we replay `(since, now]` from the log for `tables`? With a cursor,
	 * this is a per-origin coverage check — every entry the client hasn't
	 * seen MUST be present in our log. Pre-1.16 `number` form matches when
	 * the local log covers `(since.version, now]`. Returns `false` for
	 * non-incremental subs (refetch/join/graph/search), since those can't be
	 * replayed precisely from a row-change log.
	 */
	const canResume = (since: number | string, incremental: boolean): boolean => {
		if (!incremental) {
			return false;
		}
		const sinceVec = normalizeSince(since);
		if (sinceVec === null) {
			return false;
		}

		// Walk the log backwards: every entry with `origin === O` and
		// `originVersion > sinceVec[O]` MUST appear in the log. If the log
		// has been trimmed past any such entry, we can't catch up.
		// Per-origin: for each origin O we've seen, check that the oldest
		// entry with that origin is no newer than `sinceVec[O] + 1`. For
		// an unknown origin, we fall back to "no coverage" (caller gets a
		// snapshot, just like pre-1.17 behavior).
		const oldestPerOrigin = new Map<string, number>();
		for (const entry of changeLog) {
			const current = oldestPerOrigin.get(entry.origin);
			if (current === undefined || entry.originVersion < current) {
				oldestPerOrigin.set(entry.origin, entry.originVersion);
			}
		}

		// Log-wide watermark: the smallest version still in the log. If this
		// is past `lastSeen + 1`, ANY entries between the cursor and oldestLogVersion
		// were trimmed (regardless of origin).
		const oldestLogVersion = changeLog[0]?.version;
		for (const [origin, lastSeen] of Object.entries(sinceVec)) {
			// Special case: if we've never seen any entry from this origin,
			// but the client claims to have seen up to `lastSeen` from it,
			// we DEFINITELY can't reconstruct — snapshot it.
			if (origin === instanceId) {
				// Local origin: standard check.
				if (lastSeen >= version) continue; // nothing newer
				const oldestLocal = oldestPerOrigin.get(instanceId);
				// If we have local entries, walk them: they must reach back
				// to lastSeen + 1.
				if (oldestLocal !== undefined) {
					if (oldestLocal > lastSeen + 1) return false;
					continue;
				}
				// No local entries — the version bumps since mint were all
				// from peer broadcasts. Resume is safe ONLY if the log itself
				// hasn't been trimmed past the mint point (otherwise some
				// local entries existed but were retired).
				if (
					oldestLogVersion !== undefined &&
					oldestLogVersion > lastSeen + 1
				) {
					return false;
				}
			} else {
				// Peer origin: same check against the peer's entries.
				const oldestPeer = oldestPerOrigin.get(origin);
				if (oldestPeer === undefined) {
					// We've never logged any change from this peer. If the client
					// has seen entries from this peer, we can't help.
					if (lastSeen > 0) return false;
				} else if (oldestPeer > lastSeen + 1) {
					return false;
				}
			}
		}
		return true;
	};

	/**
	 * Build a catch-up diff from the log for one subscription (last op per
	 * key wins). Multi-origin aware (1.17.0+): walks every entry whose
	 * `(origin, originVersion)` is newer than the client's last-seen for
	 * that origin.
	 */
	const buildCatchup = (
		since: number | string,
		tables: string[],
		key: (row: unknown) => RowKey,
		match: (row: unknown) => boolean
	): ViewDiff<unknown> => {
		const sinceVec = normalizeSince(since) ?? {};
		const latest = new Map<
			RowKey,
			{ op: 'upsert' | 'remove'; row: unknown }
		>();
		for (const entry of changeLog) {
			if (!tables.includes(entry.table)) continue;
			// Skip entries the client has already seen for this origin.
			const lastSeen = sinceVec[entry.origin];
			if (lastSeen !== undefined && entry.originVersion <= lastSeen) continue;
			const row = entry.change.row;
			const present =
				entry.change.op !== 'delete' && match(row)
					? 'upsert'
					: 'remove';
			latest.set(key(row), { op: present, row });
		}
		const changed: unknown[] = [];
		const removed: unknown[] = [];
		for (const { op, row } of latest.values()) {
			(op === 'upsert' ? changed : removed).push(row);
		}
		return { added: [], removed, changed };
	};

	const subscribeJoin = async (
		collection: string,
		definition: JoinCollectionDefinition<
			unknown,
			unknown,
			unknown,
			unknown,
			unknown
		>,
		params: unknown,
		ctx: unknown,
		onDiff: OnDiff,
		set: Set<ActiveSubscription>
	): Promise<Subscription<unknown>> => {
		if (definition.authorize !== undefined) {
			const allowed = await definition.authorize(params, ctx);
			if (!allowed) {
				throw new UnauthorizedError(
					`subscribe to collection "${collection}"`
				);
			}
		}
		const { left, right } = definition;
		const op = createEquiJoin<unknown, unknown, unknown>({
			leftKey: left.key,
			rightKey: right.key,
			leftOn: left.on,
			rightOn: right.on,
			select: definition.select
		});
		op.hydrate(
			[...(await left.hydrate(params, ctx))],
			[...(await right.hydrate(params, ctx))]
		);
		const atVersion = version;

		const subscription: ActiveSubscription = {
			kind: 'join',
			collection,
			join: {
				op,
				leftTable: left.table,
				rightTable: right.table,
				leftMatch: left.match
					? (row) => left.match!(row, params, ctx)
					: undefined,
				rightMatch: right.match
					? (row) => right.match!(row, params, ctx)
					: undefined
			},
			key: definition.key as (row: unknown) => RowKey,
			onDiff
		};
		set.add(subscription);

		return {
			initial: op.rows(),
			cursor: currentCursor(),
			version: atVersion,
			unsubscribe: () => {
				set.delete(subscription);
			}
		};
	};

	const subscribeGraph = async (
		collection: string,
		definition: GraphCollectionDefinition<unknown, unknown, unknown>,
		params: unknown,
		ctx: unknown,
		onDiff: OnDiff,
		set: Set<ActiveSubscription>
	): Promise<Subscription<unknown>> => {
		if (definition.authorize !== undefined) {
			const allowed = await definition.authorize(params, ctx);
			if (!allowed) {
				throw new UnauthorizedError(
					`subscribe to collection "${collection}"`
				);
			}
		}
		const instance = definition.query.instantiate(params, ctx);
		const initial = await instance.hydrate();
		const atVersion = version;
		const subscription: ActiveSubscription = {
			kind: 'graph',
			collection,
			instance,
			key: definition.key as (row: unknown) => RowKey,
			onDiff
		};
		set.add(subscription);
		return {
			initial,
			cursor: currentCursor(),
			version: atVersion,
			unsubscribe: () => {
				set.delete(subscription);
			}
		};
	};

	const subscribeReactive = async (
		collection: string,
		definition: ReactiveQueryDefinition<unknown, unknown, unknown>,
		params: unknown,
		ctx: unknown,
		onDiff: OnDiff,
		set: Set<ActiveSubscription>
	): Promise<Subscription<unknown>> => {
		if (definition.authorize !== undefined) {
			const allowed = await definition.authorize(params, ctx);
			if (!allowed) {
				throw new UnauthorizedError(
					`subscribe to collection "${collection}"`
				);
			}
		}
		// Each run gets a fresh read set; the handle records tables + row keys read.
		const rerun = async () => {
			const readTables = new Set<string>();
			const readKeys = new Set<string>();
			const rangeDeps: RangeDep[] = [];
			const db = makeReadHandle(ctx, readTables, readKeys, rangeDeps);
			const rows = [...(await definition.run({ ctx, db, params }))];
			return { rangeDeps, readKeys, readTables, rows };
		};
		const rerunKey = stableSubKey(collection, params, ctx);
		// Cross-client cache hit (1.3+): a previous subscriber with the same
		// (collection, params, ctx) ran the query body recently and its
		// result is still valid (no overlapping write since). Reuse it
		// instead of hitting the DB again. Cache misses fall through to
		// `rerun()` and populate the cache for the next subscriber.
		const cached = readCacheEntry(rerunKey);
		const first =
			cached !== undefined
				? {
						rangeDeps: cached.rangeDeps,
						readKeys: cached.readKeys,
						readTables: cached.readTables,
						rows: cached.rows
					}
				: await rerun();
		if (cached === undefined) {
			writeCacheEntry({
				expiresAt: Date.now() + reactiveCacheTtlMs,
				rangeDeps: first.rangeDeps,
				readKeys: first.readKeys,
				readTables: first.readTables,
				rerunKey,
				rows: first.rows,
				version
			});
		}
		const current = new Map<RowKey, unknown>();
		for (const row of first.rows) {
			current.set(definition.key(row), row);
		}
		const atVersion = version;
		const subscription: ReactiveSub = {
			kind: 'reactive',
			collection,
			key: definition.key,
			rerun,
			rerunKey,
			current,
			readTables: first.readTables,
			readKeys: first.readKeys,
			rangeDeps: first.rangeDeps,
			onDiff
		};
		set.add(subscription);
		reactiveSubs.add(subscription);
		return {
			initial: first.rows,
			cursor: currentCursor(),
			version: atVersion,
			unsubscribe: () => {
				set.delete(subscription);
				reactiveSubs.delete(subscription);
			}
		};
	};

	const subscribeSearch = async (
		collection: string,
		definition: SearchCollectionDefinition<unknown, unknown, unknown>,
		params: unknown,
		ctx: unknown,
		onDiff: OnDiff,
		set: Set<ActiveSubscription>
	): Promise<Subscription<unknown>> => {
		// The subscription params are the query (a string for text, a vector for
		// similarity).
		const query = params;
		if (definition.authorize !== undefined) {
			const allowed = await definition.authorize(query, ctx);
			if (!allowed) {
				throw new UnauthorizedError(
					`subscribe to collection "${collection}"`
				);
			}
		}
		const entry = await ensureSearchIndex(definition);
		const limit = definition.limit ?? 20;
		const readRule = readRuleFor(definition.table);
		// Re-rank: top-K from the (shared, live) index, scoped by the read rule,
		// each row tagged with its score so the client can sort by relevance.
		const rerun = (): unknown[] => {
			const candidates = entry.index.search(
				query,
				readRule ? limit * 5 : limit
			);
			const visible = readRule
				? candidates.filter((hit) => readRule(ctx, hit.row))
				: candidates;
			return visible.slice(0, limit).map((hit) => ({
				...(hit.row as Record<string, unknown>),
				[SEARCH_SCORE_FIELD]: hit.score
			}));
		};
		const initial = rerun();
		const current = new Map<RowKey, unknown>();
		for (const row of initial) {
			current.set(definition.key(row), row);
		}
		const atVersion = version;
		const subscription: Extract<ActiveSubscription, { kind: 'search' }> = {
			kind: 'search',
			collection,
			key: definition.key,
			rerun,
			current,
			onDiff
		};
		set.add(subscription);
		searchSubs.add(subscription);
		return {
			initial,
			cursor: currentCursor(),
			version: atVersion,
			unsubscribe: () => {
				set.delete(subscription);
				searchSubs.delete(subscription);
			}
		};
	};

	const engine: SyncEngine = {
		register: (collection) => {
			registry.set(collection.name, collection);
			for (const table of collection.tables ?? [collection.name]) {
				addTableIndex(table, collection.name);
			}
		},

		registerJoin: (collection) => {
			registry.set(collection.name, collection);
			addTableIndex(collection.left.table, collection.name);
			addTableIndex(collection.right.table, collection.name);
		},

		registerGraph: (collection) => {
			registry.set(collection.name, collection);
			for (const table of collection.query.tables()) {
				addTableIndex(table, collection.name);
			}
		},

		registerSearch: (collection) => {
			// Like reactive: not in tableIndex — its index is driven directly by
			// searchPairs off the change feed.
			registry.set(collection.name, collection);
		},

		subscribe: async ({ collection, params, ctx, onDiff, since, signal }) => {
			// 1.21.0: wrap subscribe setup in a span. The Subscription
			// lives past `subscribe()` returning — the span only covers
			// the setup cost (authorize / hydrate / view materialization),
			// not the ongoing reactive lifetime.
			const subscribeSpan = tracer.startSpan('sync.subscribe', {
				attributes: {
					[ABS_ATTRS.engineId]: instanceId,
					[ABS_ATTRS.collection]: collection
				}
			});
			try {
			// (1.15.0) Cheap up-front check — if the consumer already aborted
			// before we got here, throw before any side effect (no authorize,
			// no hydrate, no view materialization).
			checkAborted(signal);

			const registered = registry.get(collection);
			if (registered === undefined) {
				throw new Error(`Unknown collection "${collection}"`);
			}

			// (1.20.1) Per-tenant cap. Acquired BEFORE authorize/hydrate/any
			// state allocation. If subscribe throws between here and the
			// successful return (auth rejection, abort, schema error, etc.)
			// we release in the `catch` below — otherwise the wrapped
			// `unsubscribe` is the release path.
			const tenantSlot = acquireSubscriptionSlot(ctx, { collection });
			let slotHandedOff = false;
			try {

			const typedOnDiff = onDiff as OnDiff;
			const subscribeSet = subsFor(collection);

			// Wrap the eventual return so we (a) re-check signal after the
			// async setup (catches mid-flight aborts), (b) auto-call
			// unsubscribe when signal fires after the subscription is live,
			// and (c) decrement the tenant's active-sub count idempotently
			// when unsubscribe runs.
			const wrapReturn = <T>(sub: Subscription<T>): Subscription<T> => {
				checkAborted(signal);
				const innerUnsubscribe = sub.unsubscribe;
				let released = false;
				const wrappedUnsubscribe = (): void => {
					if (released) return;
					released = true;
					releaseSubscriptionSlot(tenantSlot);
					innerUnsubscribe();
				};
				const wrapped = { ...sub, unsubscribe: wrappedUnsubscribe };
				linkAbortToUnsubscribe(signal, wrappedUnsubscribe);
				slotHandedOff = true;
				return wrapped;
			};

			const registeredKind = (registered as { kind?: string }).kind;
			if (registeredKind === 'join') {
				const joined = await subscribeJoin(
					collection,
					registered as JoinCollectionDefinition<
						unknown,
						unknown,
						unknown,
						unknown,
						unknown
					>,
					params,
					ctx,
					typedOnDiff,
					subscribeSet
				);
				return wrapReturn(joined) as Subscription<never>;
			}
			if (registeredKind === 'graph') {
				const graphed = await subscribeGraph(
					collection,
					registered as GraphCollectionDefinition<
						unknown,
						unknown,
						unknown
					>,
					params,
					ctx,
					typedOnDiff,
					subscribeSet
				);
				return wrapReturn(graphed) as Subscription<never>;
			}
			if (registeredKind === 'reactive') {
				const reactived = await subscribeReactive(
					collection,
					registered as ReactiveQueryDefinition<
						unknown,
						unknown,
						unknown
					>,
					params,
					ctx,
					typedOnDiff,
					subscribeSet
				);
				return wrapReturn(reactived) as Subscription<never>;
			}
			if (registeredKind === 'search') {
				const searched = await subscribeSearch(
					collection,
					registered as SearchCollectionDefinition<
						unknown,
						unknown,
						unknown
					>,
					params,
					ctx,
					typedOnDiff,
					subscribeSet
				);
				return wrapReturn(searched) as Subscription<never>;
			}
			const definition = registered as CollectionDefinition<
				unknown,
				unknown,
				unknown
			>;

			if (definition.authorize !== undefined) {
				const allowed = await definition.authorize(params, ctx);
				if (!allowed) {
					throw new UnauthorizedError(
						`subscribe to collection "${collection}"`
					);
				}
			}

			const key = definition.key ?? defaultKey;
			const match = definition.match;
			const tables = definition.tables ?? [collection];
			// Declarative read rule + schema migration apply to single-table
			// collections (their rows are that table's rows); join/aggregate
			// collections scope via match.
			const scopedTable = tables.length === 1 ? tables[0]! : undefined;
			const readRule =
				scopedTable !== undefined
					? readRuleFor(scopedTable)
					: undefined;
			// Migrate the DB result to the current shape, then filter it through the
			// read rule — so the initial snapshot and the refetch fallback are
			// always current-shape and never include a row the caller can't see.
			const rehydrate = async () => {
				const raw = [...(await definition.hydrate(params, ctx))];
				const rows =
					scopedTable !== undefined
						? raw.map((row) => migrateRow(scopedTable, row))
						: raw;
				return readRule
					? rows.filter((row) => readRule(ctx, row))
					: rows;
			};
			// Incremental matching only applies to single-table collections; a
			// join/aggregate spanning tables can't match a single row, so it uses
			// the refetch fallback.
			const incremental = match !== undefined && tables.length === 1;
			// Fold the read rule into the incremental predicate (also used by the
			// catch-up builder), so an unreadable row never enters the view.
			const boundMatch = incremental
				? (row: unknown) =>
						match(row, params, ctx) &&
						(readRule ? readRule(ctx, row) : true)
				: () => true;
			const view = createMaterializedView<unknown>({
				key,
				match: boundMatch
			});

			// Resume from the log when possible (catch-up diff); else send a
			// snapshot. The view is hydrated either way so future changes match.
			const resuming =
				since !== undefined && canResume(since, incremental);
			view.hydrate([...(await rehydrate())]);
			const atVersion = version;

			const subscription: ActiveSubscription = {
				kind: 'view',
				collection,
				view,
				incremental,
				rehydrate,
				key,
				onDiff: typedOnDiff
			};
			subscribeSet.add(subscription);

			const unsubscribe = () => {
				subscribeSet.delete(subscription);
			};

			if (resuming) {
				return wrapReturn({
					initial: [],
					catchup: buildCatchup(
						since,
						tables,
						key,
						boundMatch
					) as ViewDiff<never>,
					cursor: currentCursor(),
					version: atVersion,
					unsubscribe
				}) as Subscription<never>;
			}
			return wrapReturn({
				initial: view.rows() as never[],
				cursor: currentCursor(),
				version: atVersion,
				unsubscribe
			}) as Subscription<never>;
			} catch (error) {
				// (1.20.1) If anything between acquire and the successful
				// return throws (authorize, abort, schema error, etc.),
				// release the tenant slot so the cap doesn't leak by one
				// per failed call.
				if (!slotHandedOff) releaseSubscriptionSlot(tenantSlot);
				throw error;
			}
			} catch (spanError) {
				// 1.21.0: outer span wrap — re-throw, recording any
				// failure (subscribe-time errors are common and worth
				// surfacing).
				subscribeSpan.recordException(spanError);
				subscribeSpan.setStatus({
					code: 2 /* SpanStatusCode.ERROR */,
					message:
						spanError instanceof Error
							? spanError.message
							: String(spanError)
				});
				throw spanError;
			} finally {
				subscribeSpan.end();
			}
		},

		hydrate: async (collection, params, ctx, options) => {
			const signal = options?.signal;
			checkAborted(signal);
			const definition = registry.get(collection) as
				| CollectionDefinition<unknown, unknown, unknown>
				| undefined;
			if (definition === undefined) {
				throw new Error(`Unknown collection "${collection}"`);
			}
			if (definition.authorize !== undefined) {
				const allowed = await definition.authorize(params, ctx);
				checkAborted(signal);
				if (!allowed) {
					throw new UnauthorizedError(
						`hydrate collection "${collection}"`
					);
				}
			}
			const raw = [...(await definition.hydrate(params, ctx))];
			checkAborted(signal);
			const tables = definition.tables ?? [collection];
			const scopedTable = tables.length === 1 ? tables[0]! : undefined;
			const rows =
				scopedTable !== undefined
					? raw.map((row) => migrateRow(scopedTable, row))
					: raw;
			const readRule =
				scopedTable !== undefined
					? readRuleFor(scopedTable)
					: undefined;
			return readRule ? rows.filter((row) => readRule(ctx, row)) : rows;
		},

		applyChange: (table, change) =>
			applyChange(table, change as RowChange<unknown>),

		connectSource: async (source) => {
			await source.start((table, change) => applyChange(table, change));
			return async () => {
				await source.stop();
			};
		},

		connectCluster: async (bus) => {
			const unsubscribe = await bus.subscribe((message) => {
				// Ignore our own broadcasts; apply peers' changes locally without
				// re-broadcasting (that would loop).
				if (message.origin === instanceId) {
					return;
				}
				// 1.17.0 — log peer changes with their origin + originVersion
				// so a client carrying a cross-instance cursor can resume
				// against them. Pre-1.17 buses that don't carry originVersion
				// default to 0 (any cross-instance resume falls back to a
				// snapshot — matches pre-1.17 behavior exactly).
				void applyChangeBatch(message.changes, false, {
					origin: message.origin,
					originVersion: message.originVersion ?? 0
				});
			});
			clusterBus = bus;

			return async () => {
				clusterBus = undefined;
				await unsubscribe();
			};
		},

		subscriptionCount: (collection) => {
			if (collection !== undefined) {
				return active.get(collection)?.size ?? 0;
			}
			let total = 0;
			for (const set of active.values()) {
				total += set.size;
			}
			return total;
		},

		registerMutation: (mutation) => {
			if (
				mutation.handler === undefined &&
				mutation.sandboxedHandler === undefined
			) {
				throw new Error(
					`Mutation "${mutation.name}" must define either \`handler\` or \`sandboxedHandler\``
				);
			}
			if (
				mutation.handler !== undefined &&
				mutation.sandboxedHandler !== undefined
			) {
				throw new Error(
					`Mutation "${mutation.name}" defines both \`handler\` and \`sandboxedHandler\` — pick one`
				);
			}
			mutations.set(mutation.name, mutation);
			// Build the sandbox runner eagerly only if we know the source —
			// the actual isolate spawn is still lazy (inside makeSandboxedHandler).
			if (mutation.sandboxedHandler !== undefined) {
				sandboxRunners.set(
					mutation.name,
					makeSandboxedHandler(
						mutation.sandboxedHandler,
						mutation.sandbox,
						{
							bridgeFetch: options.bridgeFetch,
							metricsHook:
								options.handlerMetrics === undefined
									? undefined
									: {
											mutationName: mutation.name,
											onMetrics: options.handlerMetrics
										}
						}
					)
				);
			}
		},

		registerWriter: (table, writer) => {
			writers.set(table, writer as TableWriter);
		},

		registerReactive: (query) => {
			registry.set(query.name, query);
		},

		registerReader: (table, reader) => {
			readers.set(table, reader as TableReader);
		},

		registerPermissions: (table, rules) => {
			permissions.set(table, rules as TablePermissions<unknown, unknown>);
		},

		registerSchema: (table, schema) => {
			schemas.set(table, schema as TableSchema<unknown>);
		},

		registerCrdt: (table, fields) => {
			crdtFields.set(
				table,
				fields as Record<string, CrdtMergeable<unknown>>
			);
			// A ready-made merge mutation so a client needs no custom server code:
			// upsert the row patch — the CRDT auto-merge in makeActions folds the
			// declared fields into the stored row. Named "<table>:merge".
			const name = `${table}:merge`;
			mutations.set(name, {
				handler: async (args, ctx, actions) => {
					const existing = await readExisting(table, args, ctx);
					return existing === undefined
						? actions.insert(table, args)
						: actions.update(table, args);
				},
				name
			} as MutationDefinition<unknown, unknown, unknown>);
		},

		migrate: (table, row) => migrateRow(table, row) as typeof row,

		runMutation: async (name, args, ctx) => {
			// 1.21.0: wrap the entire mutation lifecycle in a span. Noop
			// when no tracerProvider was supplied.
			const span = tracer.startSpan('sync.runMutation', {
				attributes: {
					[ABS_ATTRS.engineId]: instanceId,
					[ABS_ATTRS.mutation]: name
				}
			});
			try {
				// 1.24.0: reject early when fenced — before authorize, before
				// slot acquisition. Operators expect a fenced engine to be a
				// hard wall, not a queue.
				if (activeFences.size > 0) {
					const oldest = activeFences.values().next()
						.value as FenceHandle;
					throw new EngineFencedError(oldest.reason);
				}
				const mutation = mutations.get(name);
				if (mutation === undefined) {
					throw new Error(`Unknown mutation "${name}"`);
				}
			if (mutation.authorize !== undefined) {
				const allowed = await mutation.authorize(args, ctx);
				if (!allowed) {
					throw new UnauthorizedError(`run mutation "${name}"`);
				}
			}
			// 1.20.0: gate at the entry. Wait if `mutationConcurrency`
			// is saturated; throw `MutationQueueOverflowError` if the
			// queue is also capped and full. Authorization fails before
			// the gate so a denied call never burns a slot.
			await acquireMutationSlot();

			// Pick the handler shape: in-process function or sandboxed string
			// source (runs inside @absolutejs/isolated-jsc). Sandbox runner is
			// built lazily and pre-cached in registerMutation.
			const sandboxRunner = sandboxRunners.get(name);
			const invokeHandler =
				sandboxRunner !== undefined
					? sandboxRunner
					: (
							a: unknown,
							c: unknown,
							actions: MutationActions
						): Promise<unknown> =>
							Promise.resolve(
								// Non-null assertion: registerMutation guarantees one of
								// handler/sandboxedHandler is defined.
								mutation.handler!(a, c, actions)
							);

			// Run the handler (optionally inside the DB transaction), collecting its
			// changes into a fresh buffer per attempt — so a transaction that retries
			// or rolls back never double-emits or leaks a half-applied batch.
			const runHandler = async (tx: unknown) => {
				const { actions, buffered } = makeActions(tx, ctx, true);
				const result = await invokeHandler(args, ctx, actions);
				return { buffered, result };
			};

			// Resolve the retry policy once per call. When `mutation.retry` is
			// undefined we still go through the loop, but bounded to one
			// attempt with no backoff (cheaper than a separate code path).
			const retry = mutation.retry;
			const maxAttempts =
				retry === undefined ? 1 : (retry.maxAttempts ?? 5);
			const isRetryable = retry?.isRetryable ?? isSerializationFailure;
			const computeDelay = retry?.backoff ?? exponentialBackoff();
			const maxElapsedMs = retry?.maxElapsedMs ?? 30_000;
			const startedAt = Date.now();

			// Each attempt builds fresh `actions`/`buffered` via the makeActions
			// call inside runHandler, so a retry never inherits half-applied
			// buffered changes from a failed attempt. Transactions reopen too:
			// runInTransaction wraps each individual attempt.
			let lastError: unknown;
			let attemptsMade = 0;
			try {
			for (let attempt = 1; attempt <= maxAttempts; attempt++) {
				attemptsMade = attempt;
				try {
					const { buffered, result } =
						runInTransaction !== undefined
							? await runInTransaction((tx) => runHandler(tx))
							: await runHandler(undefined);
					await applyChangeBatch(buffered);
					mutationsCompleted += 1;
					emitActivity({
						type: 'mutation',
						at: Date.now(),
						name,
						status: 'ok'
					});
					return result;
				} catch (error) {
					lastError = error;
					const elapsedMs = Date.now() - startedAt;
					const canRetry =
						attempt < maxAttempts &&
						isRetryable(error) &&
						elapsedMs < maxElapsedMs;
					if (!canRetry) break;
					mutationsRetried += 1;

					const rawDelay = computeDelay(attempt);
					// Cap the delay so we don't blow past maxElapsedMs while
					// sleeping. If the cap would be negative we're already past
					// the budget; treat as exhausted.
					const remaining = maxElapsedMs - elapsedMs;
					if (remaining <= 0) break;
					const delayMs = Math.max(0, Math.min(rawDelay, remaining));

					emitActivity({
						type: 'mutationRetry',
						at: Date.now(),
						name,
						attempt,
						delayMs,
						errorName:
							error instanceof Error ? error.name : 'Error',
						errorMessage:
							error instanceof Error
								? error.message
								: String(error)
					});
					if (delayMs > 0) {
						await new Promise((resolve) =>
							setTimeout(resolve, delayMs)
						);
					}
				}
			}

			mutationsFailed += 1;
			emitActivity({
				type: 'mutation',
				at: Date.now(),
				name,
				status: 'error'
			});
			// Wrap only when we actually burned through more than one attempt
			// — a non-retryable first-attempt failure passes through with its
			// original error preserved, even if `retry` is configured.
			if (attemptsMade > 1) {
				throw new RetriesExhaustedError(
					attemptsMade,
					Date.now() - startedAt,
					lastError
				);
			}
			throw lastError;
			} finally {
				releaseMutationSlot();
			}
			} catch (spanError) {
				// 1.21.0: outer span wrap — record any throw, rethrow.
				span.recordException(spanError);
				span.setStatus({
					code: 2 /* SpanStatusCode.ERROR */,
					message:
						spanError instanceof Error
							? spanError.message
							: String(spanError)
				});
				throw spanError;
			} finally {
				span.end();
			}
		},

		runMutations: async (specs, ctx) => {
			// Empty batch: short-circuit. Don't open a DB tx for nothing —
			// some adapters (PG with auto-commit, MySQL with implicit
			// commit, etc.) count even an empty BEGIN/COMMIT as a real
			// transaction, which is wasteful and noisy in observability.
			if (specs.length === 0) return [];
			// Snapshot the requested mutation names up front so the
			// authorization + handler resolution happens BEFORE we open
			// the DB transaction. A typo'd name aborts cleanly without
			// burning a tx.
			const resolved = specs.map((spec) => {
				const mutation = mutations.get(spec.name);
				if (mutation === undefined) {
					throw new Error(`Unknown mutation "${spec.name}"`);
				}
				return { args: spec.args, mutation, name: spec.name };
			});
			// 1.20.0: the whole batch is one slot. Resolve names BEFORE
			// the gate so an unknown-mutation typo never queues.
			await acquireMutationSlot();

			const runBatch = async (tx: unknown) => {
				const results: unknown[] = [];
				const accumulated: {
					table: string;
					change: RowChange<unknown>;
				}[] = [];
				for (const { args, mutation, name } of resolved) {
					if (mutation.authorize !== undefined) {
						const allowed = await mutation.authorize(args, ctx);
						if (!allowed) {
							throw new UnauthorizedError(
								`run mutation "${name}"`
							);
						}
					}
					const sandboxRunner = sandboxRunners.get(name);
					const invokeHandler =
						sandboxRunner !== undefined
							? sandboxRunner
							: (
									a: unknown,
									c: unknown,
									actions: MutationActions
								): Promise<unknown> =>
									Promise.resolve(
										mutation.handler!(a, c, actions)
									);
					// Each handler gets its own `actions`/`buffered` so per-
					// call validation + crdt merges still work — we collect
					// the buffered tail into `accumulated` after each
					// handler returns. If the next handler throws, the
					// surrounding `runInTransaction` rolls everything back;
					// applyChangeBatch never runs.
					const { actions, buffered } = makeActions(tx, ctx, true);
					const result = await invokeHandler(args, ctx, actions);
					results.push(result);
					accumulated.push(...buffered);
				}
				return { accumulated, results };
			};

			try {
				const { accumulated, results } =
					runInTransaction !== undefined
						? await runInTransaction((tx) => runBatch(tx))
						: await runBatch(undefined);
				await applyChangeBatch(accumulated);
				emitActivity({
					type: 'mutationBatch',
					at: Date.now(),
					names: resolved.map((entry) => entry.name),
					status: 'ok'
				});
				return results;
			} catch (error) {
				emitActivity({
					type: 'mutationBatch',
					at: Date.now(),
					names: resolved.map((entry) => entry.name),
					status: 'error'
				});
				throw error;
			} finally {
				releaseMutationSlot();
			}
		},

		registerSchedule: (schedule) => {
			schedules.set(schedule.name, schedule);
		},

		listSchedules: () => [...schedules.values()],

		runSchedule: async (name) => {
			const schedule = schedules.get(name);
			if (schedule === undefined) {
				throw new Error(`Unknown schedule "${name}"`);
			}
			// A schedule reads unscoped and writes without permission checks (it's
			// trusted server code); its writes emit as one live batch like a mutation.
			const runHandler = async (tx: unknown) => {
				const { actions, buffered } = makeActions(tx, {}, false);
				const db = makeReadHandle({}, new Set(), new Set(), [], false);
				await schedule.run({ actions, db });
				return buffered;
			};

			const retry = schedule.retry;
			const maxAttempts =
				retry === undefined ? 1 : (retry.maxAttempts ?? 5);
			const isRetryable = retry?.isRetryable ?? isSerializationFailure;
			const computeDelay = retry?.backoff ?? exponentialBackoff();
			const maxElapsedMs = retry?.maxElapsedMs ?? 30_000;
			const startedAt = Date.now();

			let lastError: unknown;
			let attemptsMade = 0;
			for (let attempt = 1; attempt <= maxAttempts; attempt++) {
				attemptsMade = attempt;
				try {
					const buffered =
						runInTransaction !== undefined
							? await runInTransaction((tx) => runHandler(tx))
							: await runHandler(undefined);
					await applyChangeBatch(buffered);
					emitActivity({
						type: 'schedule',
						at: Date.now(),
						name,
						status: 'ok'
					});
					return;
				} catch (error) {
					lastError = error;
					const elapsedMs = Date.now() - startedAt;
					const canRetry =
						attempt < maxAttempts &&
						isRetryable(error) &&
						elapsedMs < maxElapsedMs;
					if (!canRetry) break;

					const rawDelay = computeDelay(attempt);
					const remaining = maxElapsedMs - elapsedMs;
					if (remaining <= 0) break;
					const delayMs = Math.max(0, Math.min(rawDelay, remaining));

					emitActivity({
						type: 'scheduleRetry',
						at: Date.now(),
						name,
						attempt,
						delayMs,
						errorName:
							error instanceof Error ? error.name : 'Error',
						errorMessage:
							error instanceof Error
								? error.message
								: String(error)
					});
					if (delayMs > 0) {
						await new Promise((resolve) =>
							setTimeout(resolve, delayMs)
						);
					}
				}
			}

			emitActivity({
				type: 'schedule',
				at: Date.now(),
				name,
				status: 'error'
			});
			if (attemptsMade > 1) {
				throw new RetriesExhaustedError(
					attemptsMade,
					Date.now() - startedAt,
					lastError
				);
			}
			throw lastError;
		},

		registerPack: (pack) => {
			for (const table of pack.ownsTables) {
				const existing = packTableOwners.get(table);
				if (existing !== undefined) {
					throw new PackTableConflictError(
						table,
						existing,
						pack.name
					);
				}
			}
			if (pack.requireDependencies === true) {
				for (const table of pack.readsTables ?? []) {
					if (!readers.has(table)) {
						throw new PackMissingDependencyError(pack.name, table);
					}
				}
			}
			if (pack.schemas !== undefined) {
				for (const [table, schema] of Object.entries(pack.schemas)) {
					engine.registerSchema(table, schema);
				}
			}
			if (pack.permissions !== undefined) {
				for (const [table, rules] of Object.entries(pack.permissions)) {
					engine.registerPermissions(table, rules);
				}
			}
			if (pack.readers !== undefined) {
				for (const [table, reader] of Object.entries(pack.readers)) {
					engine.registerReader(table, reader);
				}
			}
			if (pack.writers !== undefined) {
				for (const [table, writer] of Object.entries(pack.writers)) {
					engine.registerWriter(table, writer);
				}
			}
			if (pack.crdt !== undefined) {
				for (const [table, fields] of Object.entries(pack.crdt)) {
					engine.registerCrdt(table, fields);
				}
			}
			for (const collection of pack.collections ?? []) {
				engine.register(collection);
			}
			for (const collection of pack.joinCollections ?? []) {
				engine.registerJoin(collection);
			}
			for (const collection of pack.graphCollections ?? []) {
				engine.registerGraph(collection);
			}
			for (const collection of pack.searchCollections ?? []) {
				engine.registerSearch(collection);
			}
			for (const query of pack.reactiveQueries ?? []) {
				engine.registerReactive(query);
			}
			for (const mutation of pack.mutations ?? []) {
				engine.registerMutation(mutation);
			}
			for (const schedule of pack.schedules ?? []) {
				engine.registerSchedule(schedule);
			}
			for (const table of pack.ownsTables) {
				packTableOwners.set(table, pack.name);
			}
			registeredPacks.push({
				name: pack.name,
				version: pack.version,
				ownsTables: [...pack.ownsTables],
				readsTables: [...(pack.readsTables ?? [])]
			});
		},

		inspect: () => {
			const collections = [...registry.entries()].map(([name, def]) => {
				const kind = ((def as { kind?: CollectionKind }).kind ??
					'view') as CollectionKind;
				let tables: string[] = [];
				if (kind === 'join') {
					const join = def as JoinCollectionDefinition<
						unknown,
						unknown,
						unknown,
						unknown,
						unknown
					>;
					tables = [join.left.table, join.right.table];
				} else if (kind === 'graph') {
					tables = (
						def as GraphCollectionDefinition<
							unknown,
							unknown,
							unknown
						>
					).query.tables();
				} else if (kind === 'search') {
					tables = [
						(
							def as SearchCollectionDefinition<
								unknown,
								unknown,
								unknown
							>
						).table
					];
				} else if (kind === 'view') {
					tables = (
						def as CollectionDefinition<unknown, unknown, unknown>
					).tables ?? [name];
				}
				return {
					name,
					kind,
					tables,
					subscriptions: active.get(name)?.size ?? 0
				};
			});
			const DEVTOOLS_RECENT = 50;
			return {
				version,
				collections,
				mutations: [...mutations.keys()],
				schedules: [...schedules.values()].map((schedule) => ({
					name: schedule.name,
					pattern: schedule.pattern
				})),
				readers: [...readers.keys()],
				writers: [...writers.keys()],
				recentChanges: changeLog
					.slice(-DEVTOOLS_RECENT)
					.map((entry) => ({
						version: entry.version,
						table: entry.table,
						op: entry.change.op
					})),
				packs: registeredPacks.map((pack) => ({
					name: pack.name,
					version: pack.version,
					ownsTables: [...pack.ownsTables],
					readsTables: [...pack.readsTables]
				}))
			};
		},

		exportChangeLog: () => ({
			entries: changeLog.slice(),
			exportedAt: Date.now(),
			instanceId,
			version
		}),

		importChangeLog,

		replayTo: async ({ at, tables }) => {
			// 1.22.0: walk the bounded change log forward to targetAt,
			// folding each op into a per-table keyed view. Last write
			// wins per key. Delete removes the key.
			const filterTables =
				tables !== undefined ? new Set(tables) : undefined;
			const state = new Map<string, Map<RowKey, unknown>>();
			let asOfVersion = 0;
			let asOfAt = 0;
			// Truncation: the log doesn't extend back to `at`. We've
			// trimmed entries that may have mattered for the
			// reconstruction, so the result is "state walked forward
			// from the OLDEST retained entry" rather than the actual
			// state at `at`. Distinguishable from "no history at all"
			// (`changeLog[0]?.version === 1`).
			const oldest = changeLog[0];
			const truncated =
				oldest !== undefined &&
				oldest.version > 1 &&
				oldest.at > at;
			for (const entry of changeLog) {
				if (entry.at > at) break;
				if (
					filterTables !== undefined &&
					!filterTables.has(entry.table)
				) {
					continue;
				}
				let tableState = state.get(entry.table);
				if (tableState === undefined) {
					tableState = new Map();
					state.set(entry.table, tableState);
				}
				const reader = readers.get(entry.table);
				const key =
					reader?.key?.(entry.change.row) ??
					((entry.change.row as { id?: RowKey })?.id as RowKey);
				if (key === undefined) {
					// Without a stable key we can't apply ops idempotently
					// — skip silently. In practice every table the
					// engine has emitted changes for has a reader (the
					// engine's own change-emit path doesn't run without
					// one), so this branch is defensive.
					continue;
				}
				if (entry.change.op === 'delete') {
					tableState.delete(key);
				} else {
					tableState.set(key, entry.change.row);
				}
				asOfVersion = entry.version;
				asOfAt = entry.at;
			}
			const rows: Record<string, ReadonlyArray<unknown>> = {};
			for (const [table, map] of state) {
				rows[table] = [...map.values()];
			}
			return { asOfAt, asOfVersion, rows, truncated };
		},

		fence: ({ reason }) => {
			const handle: FenceHandle = {
				fencedAt: Date.now(),
				reason,
				lift: () => {
					activeFences.delete(handle);
				}
			};
			activeFences.add(handle);
			return handle;
		},

		exportSnapshot: async ({ tables, ctx = {} }: ExportSnapshotOptions = {}) => {
			const tableFilter = tables !== undefined ? new Set(tables) : undefined;
			const rows: Record<string, ReadonlyArray<unknown>> = {};
			for (const [table, reader] of readers) {
				if (tableFilter !== undefined && !tableFilter.has(table)) {
					continue;
				}
				const iterable = await reader.all(ctx);
				rows[table] = [...iterable];
			}
			return {
				exportedAt: Date.now(),
				sourceInstanceId: instanceId,
				tables: rows,
				version
			};
		},

		importSnapshot: async (
			snapshot,
			{ tables, onProgress, ctx = {} }: ImportSnapshotOptions = {}
		) => {
			const tableFilter = tables !== undefined ? new Set(tables) : undefined;
			const perTable: Record<string, number> = {};
			const skipped: string[] = [];
			let tablesImported = 0;
			let rowsImported = 0;
			for (const [table, snapshotRows] of Object.entries(
				snapshot.tables
			)) {
				if (tableFilter !== undefined && !tableFilter.has(table)) {
					continue;
				}
				const writer = writers.get(table);
				if (writer === undefined) {
					skipped.push(table);
					continue;
				}
				const total = snapshotRows.length;
				let done = 0;
				for (const row of snapshotRows) {
					await writer.insert(row, ctx, undefined);
					done += 1;
					rowsImported += 1;
					if (onProgress !== undefined) {
						onProgress(table, done, total);
					}
				}
				perTable[table] = done;
				if (done > 0) tablesImported += 1;
			}
			return {
				perTable,
				rowsImported,
				skipped,
				tablesImported
			};
		},

		metrics: () => {
			const now = Date.now();
			const byCollection: Record<string, number> = {};
			let totalSubscriptions = 0;
			for (const [name, subs] of active) {
				byCollection[name] = subs.size;
				totalSubscriptions += subs.size;
			}
			const oldest = changeLog[0];
			return {
				at: now,
				changeLog: {
					capacity: changeLogSize,
					entries: changeLog.length,
					oldestAgeMs: oldest ? now - oldest.at : null,
					oldestVersion: oldest ? oldest.version : null,
					retainMs: changeLogRetainMs
				},
				mutations: {
					completed: mutationsCompleted,
					failed: mutationsFailed,
					inFlight: mutationsInFlight,
					queued: mutationsQueued,
					retried: mutationsRetried
				},
				reactiveCache: {
					capacity: reactiveCacheMax,
					entries: cachedReruns.size
				},
				schedules: {
					registered: schedules.size
				},
				subscriptions: {
					byCollection,
					byTenant: Object.fromEntries(subscriptionsByTenant),
					total: totalSubscriptions
				},
				uptimeMs: now - engineStartedAt,
				version
			};
		},

		onActivity: (listener) => {
			activityListeners.add(listener);
			return () => {
				activityListeners.delete(listener);
			};
		},

		streamChanges: ({
			since = 0,
			signal,
			maxBuffer = 10_000
		}: StreamChangesOptions = {}) => {
			// Detect a gap up front so the consumer's `for await` sees the
			// throw immediately rather than after the first historical entry.
			// (We tolerate `since === 0`, which means "give me everything in
			// the log"; the gap check only kicks in for a non-zero cursor.)
			const oldest = changeLog[0];
			if (
				since > 0 &&
				oldest !== undefined &&
				oldest.version > since + 1
			) {
				const err = new MissedChangesError(since, oldest.version);
				return {
					[Symbol.asyncIterator]() {
						return {
							next: () => Promise.reject(err)
						};
					}
				};
			}

			// Register the subscriber BEFORE snapshotting history so a commit
			// landing between the snapshot and the live tail can't be missed.
			// Phase 2 dedupes against `cursor`.
			const buffer: LoggedChange[] = [];
			let waiter: (() => void) | null = null;
			let overflow = false;
			const wake = () => {
				if (waiter !== null) {
					const resume = waiter;
					waiter = null;
					resume();
				}
			};
			const subscriber = (entry: LoggedChange) => {
				if (buffer.length >= maxBuffer) {
					overflow = true;
					wake();
					return;
				}
				buffer.push(entry);
				wake();
			};
			streamSubscribers.add(subscriber);

			const onAbort = () => wake();
			signal?.addEventListener('abort', onAbort, { once: true });

			let lastDelivered = since;

			return {
				async *[Symbol.asyncIterator]() {
					try {
						// Phase 1: historical entries. Copy the array so a
						// concurrent log.shift() (when the ring buffer rotates)
						// can't surprise us mid-iteration.
						//
						// A single batched mutation writes N rows that all
						// share one version, so we filter on `entry.version >
						// since` directly (no per-yield cursor bump — that
						// would deliver only the first row of every batch).
						const history = [...changeLog];
						const headVersion =
							history.length > 0
								? history[history.length - 1]!.version
								: since;
						for (const entry of history) {
							if (signal?.aborted) return;
							if (entry.version > since) {
								lastDelivered = entry.version;
								yield entry;
							}
						}
						// Phase 2: live tail. Dedupe against `headVersion`
						// (the head of the log when phase 1 finished): any
						// buffered entry with `version <= headVersion` was
						// already yielded from history (a commit between
						// subscriber registration and the snapshot lands in
						// both the buffer and the snapshot).
						while (!signal?.aborted) {
							while (buffer.length > 0) {
								const entry = buffer.shift()!;
								if (entry.version > headVersion) {
									lastDelivered = entry.version;
									yield entry;
								}
							}
							if (overflow) {
								throw new CdcConsumerSlowError(
									maxBuffer,
									lastDelivered
								);
							}
							if (signal?.aborted) return;
							await new Promise<void>((resolve) => {
								waiter = resolve;
							});
						}
					} finally {
						streamSubscribers.delete(subscriber);
						signal?.removeEventListener('abort', onAbort);
					}
				}
			};
		}
	};

	return engine;
};
