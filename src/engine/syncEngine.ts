import type { CollectionContext, CollectionDefinition } from './collection';
import { createMaterializedView, isEmptyViewDiff } from './materializedView';
import type { MaterializedView } from './materializedView';
import type { MutationDefinition } from './mutation';
import type { RowChange, RowKey, ViewDiff } from './types';

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
	/** Receives every non-empty diff after the initial snapshot. */
	onDiff: (diff: ViewDiff<T>) => void;
};

export type Subscription<T> = {
	/** The result set at subscribe time (the initial snapshot). */
	initial: T[];
	/** Stop receiving diffs and release the view. */
	unsubscribe: () => void;
};

export type SyncEngine = {
	/** Register a collection definition (see {@link defineCollection}). */
	register: <T, P = void, Ctx = CollectionContext>(
		collection: CollectionDefinition<T, P, Ctx>
	) => void;
	/**
	 * Open a live subscription: authorize, hydrate the initial set, and stream
	 * diffs as changes arrive. Rejects with {@link UnauthorizedError} on deny.
	 */
	subscribe: <T, P = void, Ctx = CollectionContext>(
		args: SubscribeArgs<T, P, Ctx>
	) => Promise<Subscription<T>>;
	/**
	 * Feed a committed change into the engine, fanning the resulting diff to every
	 * live subscription of that collection. Call after a mutation (or from a CDC
	 * source). Incremental subscriptions diff the single row; refetch-fallback
	 * subscriptions re-hydrate.
	 */
	applyChange: <T>(collection: string, change: RowChange<T>) => Promise<void>;
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

type ActiveSubscription = {
	collection: string;
	view: MaterializedView<unknown>;
	/** Incremental (has a predicate) vs refetch fallback. */
	incremental: boolean;
	/** Re-run the bound hydrate for the refetch fallback. */
	rehydrate: () => Promise<Iterable<unknown>>;
	onDiff: (diff: ViewDiff<unknown>) => void;
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
export const createSyncEngine = (): SyncEngine => {
	// Heterogeneous registry: `any` here is what lets collections of different
	// row/param/context types share one map (the public `register`/`subscribe`
	// surface stays fully typed).
	const registry = new Map<string, CollectionDefinition<any, any, any>>();
	const mutations = new Map<string, MutationDefinition<any, any, any>>();
	const active = new Map<string, Set<ActiveSubscription>>();

	const subsFor = (collection: string) => {
		let set = active.get(collection);
		if (set === undefined) {
			set = new Set();
			active.set(collection, set);
		}
		return set;
	};

	const applyChange = async (
		collection: string,
		change: RowChange<unknown>
	) => {
		const set = active.get(collection);
		if (set === undefined || set.size === 0) {
			return;
		}
		for (const subscription of set) {
			let diff;
			if (subscription.incremental) {
				try {
					diff = subscription.view.apply(change);
				} catch {
					// The predicate couldn't decide this change (e.g. an operator
					// the inferred matcher doesn't support) — degrade to a correct
					// refetch rather than a wrong diff.
					diff = subscription.view.reset(
						await subscription.rehydrate()
					);
				}
			} else {
				diff = subscription.view.reset(await subscription.rehydrate());
			}
			if (!isEmptyViewDiff(diff)) {
				subscription.onDiff(diff);
			}
		}
	};

	return {
		register: (collection) => {
			registry.set(collection.name, collection);
		},

		subscribe: async ({ collection, params, ctx, onDiff }) => {
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
						`subscribe to collection "${collection}"`
					);
				}
			}

			const key = definition.key ?? defaultKey;
			const rehydrate = async () => definition.hydrate(params, ctx);
			const match = definition.match;
			const incremental = match !== undefined;
			const view = createMaterializedView<unknown>({
				key,
				match: incremental
					? (row) => match(row, params, ctx)
					: () => true
			});

			// Hydrate, then register. A change arriving in this gap is missed —
			// the known hydrate-vs-live race; change-feed sequencing addresses it
			// in a later checkpoint.
			view.hydrate([...(await rehydrate())]);

			const subscription: ActiveSubscription = {
				collection,
				view,
				incremental,
				rehydrate,
				onDiff: onDiff as (diff: ViewDiff<unknown>) => void
			};
			const set = subsFor(collection);
			set.add(subscription);

			return {
				initial: view.rows() as never[],
				unsubscribe: () => {
					set.delete(subscription);
				}
			};
		},

		applyChange: (collection, change) =>
			applyChange(collection, change as RowChange<unknown>),

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
