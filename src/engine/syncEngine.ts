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
import type { ChangeSource, RowChange, RowKey, ViewDiff } from './types';

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
			/** Current result set, keyed (diffed against the next re-run). */
			current: Map<RowKey, unknown>;
			/** Full-table dependencies (from `db.all`). */
			readTables: Set<string>;
			/** Row-level dependencies `table\0key` (from `db.get`). */
			readKeys: Set<string>;
			/** Range dependencies (from `db.where`) — predicate + matched keys. */
			rangeDeps: RangeDep[];
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
	>();
	const mutations = new Map<string, MutationDefinition<any, any, any>>();
	const writers = new Map<string, TableWriter>();
	const readers = new Map<string, TableReader>();
	// Reactive (read-set-tracked) subscriptions, scanned on each change since
	// their dependencies (the tables they read) are dynamic, not in tableIndex.
	const reactiveSubs = new Set<
		Extract<ActiveSubscription, { kind: 'reactive' }>
	>();
	const active = new Map<string, Set<ActiveSubscription>>();
	// Which collections read each table — so a table change fans to all of them.
	const tableIndex = new Map<string, Set<string>>();

	// Monotonic change feed: every applyChange bumps `version` and appends to a
	// bounded log, so a client can resume from the version it last applied.
	const changeLogSize = options.changeLogSize ?? 1024;
	const changeLog: LoggedChange[] = [];
	let version = 0;
	const runInTransaction = options.transaction;

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
		rangeDeps: RangeDep[]
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

		return {
			all: async (table) => {
				readTables.add(table);
				return [...(await readerFor(table).all(ctx))] as never[];
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
				return (await reader.get(key, ctx)) as never;
			},
			where: async (table, predicate) => {
				const reader = readerFor(table);
				const matched = [...(await reader.all(ctx))].filter(
					predicate as (row: unknown) => boolean
				);
				if (reader.key !== undefined) {
					// Remember which rows matched, so an update/delete that pulls a
					// row out of the range still re-runs (it's in this key set).
					const key = reader.key;
					rangeDeps.push({
						table,
						predicate: predicate as (row: unknown) => boolean,
						keys: new Set(matched.map(key))
					});
				} else {
					readTables.add(table);
				}
				return matched as never[];
			}
		};
	};

	/** Diff a reactive query's re-run against its current set; updates `current`. */
	const diffRerun = (
		sub: ReactiveSub,
		rows: unknown[]
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
			} else if (!shallowEqual(previous, row)) {
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

	/** Did this batch touch a table, row key, or range the sub read? */
	const isReactiveAffected = (
		sub: ReactiveSub,
		changes: ReactiveChange[]
	): boolean =>
		changes.some(
			(change) =>
				sub.readTables.has(change.table) ||
				(change.key !== undefined &&
					sub.readKeys.has(depKey(change.table, change.key))) ||
				sub.rangeDeps.some((dep) => inRange(dep, change))
		);

	const reactivePairs = async (
		changes: ReactiveChange[]
	): Promise<[ActiveSubscription, ViewDiff<unknown>][]> => {
		const pairs: [ActiveSubscription, ViewDiff<unknown>][] = [];
		for (const sub of reactiveSubs) {
			if (!isReactiveAffected(sub, changes)) {
				continue;
			}
			const { rows, readTables, readKeys, rangeDeps } = await sub.rerun();
			sub.readTables = readTables;
			sub.readKeys = readKeys;
			sub.rangeDeps = rangeDeps;
			const diff = diffRerun(sub, rows);
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
	const applyChange = async (table: string, change: RowChange<unknown>) => {
		version += 1;
		const changeVersion = version;
		logChange(changeVersion, { version: changeVersion, table, change });
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
		for (const [subscription, diff] of emissions) {
			subscription.onDiff(diff, changeVersion);
		}
	};

	/**
	 * Apply a set of changes atomically: one version bump for the whole batch and
	 * a single net-merged diff per affected subscription. Used by mutations so a
	 * client never renders a torn intermediate state mid-mutation.
	 */
	const applyChangeBatch = async (
		changes: { table: string; change: RowChange<unknown> }[]
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
		for (const [subscription, diff] of emissions) {
			subscription.onDiff(diff, batchVersion);
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
		const first = await rerun();
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
			const rehydrate = async () => definition.hydrate(params, ctx);
			const match = definition.match;
			const tables = definition.tables ?? [collection];
			// Incremental matching only applies to single-table collections; a
			// join/aggregate spanning tables can't match a single row, so it uses
			// the refetch fallback.
			const incremental = match !== undefined && tables.length === 1;
			const boundMatch = incremental
				? (row: unknown) => match(row, params, ctx)
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
			return [...(await definition.hydrate(params, ctx))];
		},

		applyChange: (table, change) =>
			applyChange(table, change as RowChange<unknown>),

		connectSource: async (source) => {
			await source.start((table, change) => applyChange(table, change));
			return async () => {
				await source.stop();
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
			mutations.set(mutation.name, mutation);
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
			const writerFor = (table: string): TableWriter => {
				const writer = writers.get(table);
				if (writer === undefined) {
					throw new Error(
						`No writer registered for table "${table}" — register one with engine.registerWriter, or use actions.change`
					);
				}
				return writer;
			};

			// Run the handler (optionally inside the DB transaction), collecting its
			// changes into a fresh buffer per attempt — so a transaction that retries
			// or rolls back never double-emits or leaks a half-applied batch. The
			// `tx` handle threads through to each writer.
			const runHandler = async (tx: unknown) => {
				const buffered: {
					table: string;
					change: RowChange<unknown>;
				}[] = [];
				const actions: MutationActions = {
					change: (collection, change) => {
						buffered.push({
							table: collection,
							change: change as RowChange<unknown>
						});
						return Promise.resolve();
					},
					insert: async (table, data) => {
						const row = await writerFor(table).insert(
							data,
							ctx,
							tx
						);
						buffered.push({ table, change: { op: 'insert', row } });
						return row;
					},
					update: async (table, data) => {
						const row = await writerFor(table).update(
							data,
							ctx,
							tx
						);
						buffered.push({ table, change: { op: 'update', row } });
						return row;
					},
					delete: async (table, row) => {
						await writerFor(table).delete(row, ctx, tx);
						buffered.push({ table, change: { op: 'delete', row } });
					}
				};
				const handlerResult = await mutation.handler(
					args,
					ctx,
					actions
				);
				return { buffered, result: handlerResult };
			};

			// Emit only after the transaction commits, so subscribers never see a
			// change that later rolls back.
			const { buffered, result } =
				runInTransaction !== undefined
					? await runInTransaction((tx) => runHandler(tx))
					: await runHandler(undefined);
			await applyChangeBatch(buffered);
			return result;
		}
	};
};
