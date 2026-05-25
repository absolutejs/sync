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
	>();
	const mutations = new Map<string, MutationDefinition<any, any, any>>();
	const writers = new Map<string, TableWriter>();
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
		for (const subscription of subscriptionsForTable(table)) {
			const diff = await subscriptionDiff(subscription, table, change);
			if (!isEmptyViewDiff(diff)) {
				subscription.onDiff(diff, changeVersion);
			}
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
		for (const { table, change } of changes) {
			logChange(batchVersion, { version: batchVersion, table, change });
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
		for (const [subscription, diffs] of perSubscription) {
			const merged =
				diffs.length === 1
					? diffs[0]!
					: mergeViewDiffs(diffs, subscription.key);
			if (!isEmptyViewDiff(merged)) {
				subscription.onDiff(merged, batchVersion);
			}
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
