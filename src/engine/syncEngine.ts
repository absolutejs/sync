import type {
	CollectionContext,
	CollectionDefinition,
	JoinCollectionDefinition
} from './collection';
import { createEquiJoin } from './equiJoin';
import type { EquiJoin } from './equiJoin';
import { createMaterializedView, isEmptyViewDiff } from './materializedView';
import type { MaterializedView } from './materializedView';
import type { MutationDefinition } from './mutation';
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
			onDiff: OnDiff;
	  }
	| {
			kind: 'join';
			collection: string;
			join: JoinState;
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
	>();
	const mutations = new Map<string, MutationDefinition<any, any, any>>();
	const active = new Map<string, Set<ActiveSubscription>>();
	// Which collections read each table — so a table change fans to all of them.
	const tableIndex = new Map<string, Set<string>>();

	// Monotonic change feed: every applyChange bumps `version` and appends to a
	// bounded log, so a client can resume from the version it last applied.
	const changeLogSize = options.changeLogSize ?? 1024;
	const changeLog: LoggedChange[] = [];
	let version = 0;

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

	const applyToSubscription = async (
		subscription: ActiveSubscription,
		table: string,
		change: RowChange<unknown>,
		changeVersion: number
	) => {
		let diff: ViewDiff<unknown>;
		if (subscription.kind === 'join') {
			const js = subscription.join;
			if (table === js.leftTable) {
				diff = js.op.applyLeft(sideChange(change, js.leftMatch));
			} else if (table === js.rightTable) {
				diff = js.op.applyRight(sideChange(change, js.rightMatch));
			} else {
				return;
			}
		} else if (subscription.incremental) {
			try {
				diff = subscription.view.apply(change);
			} catch {
				// The predicate couldn't decide this change (e.g. an operator the
				// inferred matcher doesn't support) — degrade to a correct refetch
				// rather than a wrong diff.
				diff = subscription.view.reset(await subscription.rehydrate());
			}
		} else {
			diff = subscription.view.reset(await subscription.rehydrate());
		}
		if (!isEmptyViewDiff(diff)) {
			subscription.onDiff(diff, changeVersion);
		}
	};

	const applyChange = async (table: string, change: RowChange<unknown>) => {
		version += 1;
		changeLog.push({ version, table, change });
		if (changeLog.length > changeLogSize) {
			changeLog.shift();
		}
		const changeVersion = version;
		const collectionNames = tableIndex.get(table);
		if (collectionNames === undefined) {
			return;
		}
		for (const name of collectionNames) {
			const set = active.get(name);
			if (set === undefined || set.size === 0) {
				continue;
			}
			for (const subscription of set) {
				await applyToSubscription(
					subscription,
					table,
					change,
					changeVersion
				);
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

		subscribe: async ({ collection, params, ctx, onDiff, since }) => {
			const registered = registry.get(collection);
			if (registered === undefined) {
				throw new Error(`Unknown collection "${collection}"`);
			}

			const typedOnDiff = onDiff as OnDiff;
			const subscribeSet = subsFor(collection);

			if ((registered as { kind?: string }).kind === 'join') {
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
			return mutation.handler(args, ctx, {
				change: (collection, change) =>
					applyChange(collection, change as RowChange<unknown>)
			});
		}
	};
};
