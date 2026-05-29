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
	/**
	 * Registered sync packs (see {@link SyncEngine.registerPack}). Each
	 * entry reports the pack's name, version, the tables it owns, and the
	 * tables it reads but does not own. Surfaced for devtools and for
	 * conflict diagnostics.
	 */
	packs: {
		name: string;
		version: string;
		ownsTables: string[];
		readsTables: string[];
	}[];
};

/**
 * Operator-shaped point-in-time engine state (see {@link SyncEngine.metrics}) —
 * numeric counters + memory estimates + throughput totals since engine start.
 *
 * Distinct from {@link EngineInspection}, which is devtools-shaped (named
 * collections, recent-change tail, registered packs). `metrics()` is what a
 * PaaS host scrapes on an interval to answer "is this engine healthy" and
 * "what's its resource footprint" — feed it to `@absolutejs/metering` to
 * attribute cost per engine.
 */
export type EngineMetrics = {
	/** `Date.now()` when this snapshot was taken. */
	at: number;
	/** How long this engine has been running, in milliseconds. */
	uptimeMs: number;
	/** Current change-feed version (monotonic). */
	version: number;
	changeLog: {
		/** Number of entries currently retained. */
		entries: number;
		/** Hard cap on entries (from `SyncEngineOptions.changeLogSize`). */
		capacity: number;
		/** Time-based retention window, when set (`SyncEngineOptions.changeLogRetainMs`). */
		retainMs: number | null;
		/** Version of the oldest retained entry, or `null` when empty. */
		oldestVersion: number | null;
		/** Wall-clock age of the oldest retained entry in ms, or `null` when empty. */
		oldestAgeMs: number | null;
	};
	subscriptions: {
		/** Active subscriptions across every collection. */
		total: number;
		/** Per-collection breakdown — the values sum to `total`. */
		byCollection: Record<string, number>;
	};
	reactiveCache: {
		entries: number;
		capacity: number;
	};
	mutations: {
		/** Mutations completed successfully since engine start. */
		completed: number;
		/** Mutations that exhausted their retry budget and failed. */
		failed: number;
		/** Per-attempt retries fired since start (a single mutation may bump this multiple times). */
		retried: number;
		/** Currently running, not yet committed or failed. */
		inFlight: number;
	};
	schedules: {
		registered: number;
	};
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
			/** Emitted by `engine.runMutations(...)` — a batch of mutations
			 * run in a single transaction. `names` is the list in batch
			 * order; on error the entire batch rolls back, so the status
			 * applies to the whole list, not any individual mutation. */
			type: 'mutationBatch';
			at: number;
			names: string[];
			status: 'ok' | 'error';
	  }
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
	  }
	| { type: 'schedule'; at: number; name: string; status: 'ok' | 'error' }
	| {
			/** Emitted between attempts of a retried schedule. Mirrors
			 * {@link mutationRetry}. */
			type: 'scheduleRetry';
			at: number;
			name: string;
			attempt: number;
			delayMs: number;
			errorName: string;
			errorMessage: string;
	  };
