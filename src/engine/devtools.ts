import type { RowOp } from './types';

/**
 * Devtools introspection — a live window into a running {@link SyncEngine}: what
 * collections are registered, how many clients subscribe to each, which
 * mutations/schedules/readers/writers exist, the change-feed version, and a tail
 * of recent changes. Paired with the activity stream ({@link EngineActivity}) it
 * powers the `syncDevtools` dashboard. Read-only — purely observational.
 */

export type CollectionKind = 'view' | 'join' | 'graph' | 'reactive' | 'search';

/** One registered collection's current state. */
export type CollectionInspection = {
	name: string;
	kind: CollectionKind;
	/** Source tables it reads (empty for a reactive query — its deps are dynamic). */
	tables: string[];
	/** Active client subscriptions to it right now. */
	subscriptions: number;
};

/** A point-in-time snapshot of the engine (see {@link SyncEngine.inspect}). */
export type EngineInspection = {
	/** Current change-feed version (monotonic). */
	version: number;
	collections: CollectionInspection[];
	mutations: string[];
	schedules: { name: string; pattern: string }[];
	/** Tables with a registered reader / writer. */
	readers: string[];
	writers: string[];
	/** Most recent changes from the change log (oldest first). */
	recentChanges: { version: number; table: string; op: RowOp }[];
};

/**
 * A live engine event (see {@link SyncEngine.onActivity}): a committed change or
 * a mutation outcome. `at` is `Date.now()`. (Live subscription counts come from
 * the {@link EngineInspection} snapshot.)
 */
export type EngineActivity =
	| { type: 'change'; at: number; table: string; op: RowOp; version: number }
	| { type: 'mutation'; at: number; name: string; status: 'ok' | 'error' }
	| {
			/** Emitted between attempts of a retried mutation. `attempt` is the
			 * attempt that just failed (1-indexed); `delayMs` is the wait before
			 * the next attempt. Surfaces OCC retries to observability. */
			type: 'mutationRetry';
			at: number;
			name: string;
			attempt: number;
			delayMs: number;
			errorName: string;
			errorMessage: string;
	  };
