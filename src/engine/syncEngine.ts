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
import { makeSandboxedHandler } from './sandbox';
import type {
	PermissionsDefinition,
	ReadRule,
	TablePermissions,
	WriteRule
} from './permissions';
import type { SearchCollectionDefinition, SearchIndex } from './search';
import { SEARCH_SCORE_FIELD } from './search';
import type { ScheduleDefinition } from './schedule';
import type {
	CollectionKind,
	EngineActivity,
	EngineInspection
} from './devtools';
import type { SchemaDefinition, TableSchema } from './schema';
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
	/** Receives every non-empty diff (with its version) after the initial reply. */
	onDiff: (diff: ViewDiff<T>, version: number) => void;
	/**
	 * Resume from a version the client already applied. When the change log still
	 * covers `(since, now]` for a single-table collection, the engine replies with
	 * a catch-up diff instead of a full snapshot; otherwise it falls back to a
	 * snapshot.
	 */
	since?: number;
};

export type Subscription<T> = {
	/** The result set at subscribe time — a snapshot (empty when resuming). */
	initial: T[];
	/** Catch-up diff when resuming via `since` (instead of `initial`). */
	catchup?: ViewDiff<T>;
	/** The engine version this reply brings the client up to. */
	version: number;
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
	 */
	hydrate: (
		collection: string,
		params: unknown,
		ctx: unknown
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
	 * A point-in-time snapshot of the engine for devtools: registered collections
	 * (+ kind, tables, live subscription counts), mutations, schedules, readers,
	 * writers, the change-feed version, and recent changes. See `syncDevtools`.
	 */
	inspect: () => EngineInspection;
	/**
	 * Subscribe to the live engine activity stream (changes, mutation outcomes,
	 * subscribe/unsubscribe). Returns an unsubscribe. Powers the devtools feed.
	 */
	onActivity: (listener: (event: EngineActivity) => void) => () => void;
};

type OnDiff = (diff: ViewDiff<unknown>, version: number) => void;

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

type LoggedChange = {
	version: number;
	table: string;
	change: RowChange<unknown>;
};

export type SyncEngineOptions = {
	/**
	 * How many recent changes to retain for resumable reconnects. A client that
	 * reconnects within this window gets a catch-up diff; beyond it, a fresh
	 * snapshot. Defaults to 1024.
	 */
	changeLogSize?: number;
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
	const changeLog: LoggedChange[] = [];
	let version = 0;

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
	const runInTransaction = options.transaction;
	// Cluster fan-out: a unique id so we ignore our own broadcasts, and the bus
	// (set by connectCluster) we publish locally-committed changes to.
	const instanceId = globalThis.crypto?.randomUUID?.() ?? `i${Math.random()}`;
	let clusterBus: ClusterBus | undefined;

	const broadcast = (
		changes: { table: string; change: RowChange<unknown> }[]
	) => {
		if (clusterBus !== undefined && changes.length > 0) {
			void clusterBus.publish({ changes, origin: instanceId });
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
			}
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
		if (changeLog.length > changeLogSize) {
			changeLog.shift();
		}
	};

	/** Apply a single committed change at its own version (CDC / direct writes). */
	const applyChange = async (
		table: string,
		change: RowChange<unknown>,
		shouldBroadcast = true
	) => {
		version += 1;
		const changeVersion = version;
		logChange(changeVersion, { version: changeVersion, table, change });
		emitActivity({
			type: 'change',
			at: Date.now(),
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
		for (const [subscription, diff] of emissions) {
			subscription.onDiff(diff, changeVersion);
		}
		if (shouldBroadcast) {
			broadcast([{ table, change }]);
		}
	};

	/**
	 * Apply a set of changes atomically: one version bump for the whole batch and
	 * a single net-merged diff per affected subscription. Used by mutations so a
	 * client never renders a torn intermediate state mid-mutation.
	 */
	const applyChangeBatch = async (
		changes: { table: string; change: RowChange<unknown> }[],
		shouldBroadcast = true
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
		for (const { table, change } of changes) {
			logChange(batchVersion, { version: batchVersion, table, change });
			emitActivity({
				type: 'change',
				at: Date.now(),
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
		for (const [subscription, diff] of emissions) {
			subscription.onDiff(diff, batchVersion);
		}
		if (shouldBroadcast) {
			broadcast(changes);
		}
	};

	/**
	 * Can we replay `(since, now]` from the log for `tables`? Only when the log
	 * hasn't been trimmed past `since` (no gap).
	 */
	const canResume = (since: number, incremental: boolean): boolean => {
		if (!incremental) {
			return false; // refetch/join subs can't be replayed precisely
		}
		if (since >= version) {
			return true; // nothing newer to replay
		}
		const oldest = changeLog[0];
		return oldest !== undefined && oldest.version <= since + 1;
	};

	/** Build a catch-up diff from the log for one subscription (last op per key wins). */
	const buildCatchup = (
		since: number,
		tables: string[],
		key: (row: unknown) => RowKey,
		match: (row: unknown) => boolean
	): ViewDiff<unknown> => {
		const latest = new Map<
			RowKey,
			{ op: 'upsert' | 'remove'; row: unknown }
		>();
		for (const entry of changeLog) {
			if (entry.version <= since || !tables.includes(entry.table)) {
				continue;
			}
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
			version: atVersion,
			unsubscribe: () => {
				set.delete(subscription);
				searchSubs.delete(subscription);
			}
		};
	};

	return {
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

		subscribe: async ({ collection, params, ctx, onDiff, since }) => {
			const registered = registry.get(collection);
			if (registered === undefined) {
				throw new Error(`Unknown collection "${collection}"`);
			}

			const typedOnDiff = onDiff as OnDiff;
			const subscribeSet = subsFor(collection);

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
				return joined as Subscription<never>;
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
				return graphed as Subscription<never>;
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
				return reactived as Subscription<never>;
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
				return searched as Subscription<never>;
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
				return {
					initial: [],
					catchup: buildCatchup(
						since,
						tables,
						key,
						boundMatch
					) as ViewDiff<never>,
					version: atVersion,
					unsubscribe
				};
			}
			return {
				initial: view.rows() as never[],
				version: atVersion,
				unsubscribe
			};
		},

		hydrate: async (collection, params, ctx) => {
			const definition = registry.get(collection) as
				| CollectionDefinition<unknown, unknown, unknown>
				| undefined;
			if (definition === undefined) {
				throw new Error(`Unknown collection "${collection}"`);
			}
			if (definition.authorize !== undefined) {
				const allowed = await definition.authorize(params, ctx);
				if (!allowed) {
					throw new UnauthorizedError(
						`hydrate collection "${collection}"`
					);
				}
			}
			const raw = [...(await definition.hydrate(params, ctx))];
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
				void applyChangeBatch(message.changes, false);
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
						mutation.sandbox
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
			for (let attempt = 1; attempt <= maxAttempts; attempt++) {
				attemptsMade = attempt;
				try {
					const { buffered, result } =
						runInTransaction !== undefined
							? await runInTransaction((tx) => runHandler(tx))
							: await runHandler(undefined);
					await applyChangeBatch(buffered);
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
			const buffered =
				runInTransaction !== undefined
					? await runInTransaction((tx) => runHandler(tx))
					: await runHandler(undefined);
			await applyChangeBatch(buffered);
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
					}))
			};
		},

		onActivity: (listener) => {
			activityListeners.add(listener);
			return () => {
				activityListeners.delete(listener);
			};
		}
	};
};
