import type { ServerFrame } from '../engine/connection';
import type { RowKey } from '../engine/types';

export type { ServerFrame } from '../engine/connection';

export type SyncCollectionStatus = 'connecting' | 'ready' | 'closed';

export type SyncCollectionState<T> = {
	/** Visible rows: the server state with pending optimistic mutations applied. */
	data: T[];
	/** Connection/sync status. */
	status: SyncCollectionStatus;
	/** Last error message from the server, or `undefined`. */
	error: unknown;
};

/** A working set a mutation's optimistic effect edits in place. */
export type OptimisticDraft<T> = {
	/** Insert or replace a row by key. */
	set: (row: T) => void;
	/** Remove a row by key. */
	delete: (key: RowKey) => void;
};

export type MutateOptions<T> = {
	/** Registered server mutation name. */
	name: string;
	/** Arguments forwarded to the mutation handler. */
	args?: unknown;
	/**
	 * Apply this mutation's effect to the local set immediately for instant UI.
	 * Reverted automatically if the server rejects it. Omit for a non-optimistic
	 * mutation (UI updates only once the authoritative diff arrives).
	 */
	optimistic?: (draft: OptimisticDraft<T>) => void;
};

/** A pending mutation persisted for replay across reloads. */
export type PendingMutationRecord = {
	mutationId: number;
	name: string;
	args: unknown;
};

/**
 * Durable storage for the pending-mutation queue, so unconfirmed mutations
 * survive a page reload (offline). The queue is replayed when the socket
 * connects; records are dropped as they're acked.
 */
export type MutationStorage = {
	load: () => PendingMutationRecord[] | Promise<PendingMutationRecord[]>;
	save: (records: PendingMutationRecord[]) => void | Promise<void>;
};

/**
 * A {@link MutationStorage} backed by `localStorage` under `key`. No-ops where
 * `localStorage` is unavailable (e.g. SSR).
 */
export const localStorageMutationStorage = (key: string): MutationStorage => ({
	load: () => {
		const raw = globalThis.localStorage?.getItem(key);
		return raw ? (JSON.parse(raw) as PendingMutationRecord[]) : [];
	},
	save: (records) => {
		globalThis.localStorage?.setItem(key, JSON.stringify(records));
	}
});

/**
 * A persisted snapshot of a collection's server-authoritative rows plus the
 * change-feed `version` they were current as of — the cursor used to resume on
 * the next connect (catch-up diff if the server's changelog still covers it, a
 * fresh snapshot otherwise).
 */
export type CollectionCacheSnapshot<T> = {
	rows: T[];
	version: number;
};

/**
 * Durable local cache of a collection's confirmed rows, so reads are instant on
 * reload and available offline (local-first). Distinct from {@link
 * MutationStorage}, which persists *unconfirmed writes*: the cache is the
 * read side, the queue is the write side. On startup the cache hydrates the
 * collection before the socket connects; the engine then resumes from the
 * cached `version`.
 */
export type CollectionCache<T> = {
	load: () =>
		| CollectionCacheSnapshot<T>
		| undefined
		| Promise<CollectionCacheSnapshot<T> | undefined>;
	save: (snapshot: CollectionCacheSnapshot<T>) => void | Promise<void>;
	/** Drop the cached snapshot (optional). */
	clear?: () => void | Promise<void>;
};

/**
 * A {@link CollectionCache} backed by `localStorage` under `key`. Synchronous
 * and capped (~5MB); fine for small collections. No-ops where `localStorage`
 * is unavailable (e.g. SSR). For larger sets use {@link indexedDbCollectionCache}.
 */
export const localStorageCollectionCache = <T>(
	key: string
): CollectionCache<T> => ({
	load: () => {
		const raw = globalThis.localStorage?.getItem(key);
		return raw
			? (JSON.parse(raw) as CollectionCacheSnapshot<T>)
			: undefined;
	},
	save: (snapshot) => {
		globalThis.localStorage?.setItem(key, JSON.stringify(snapshot));
	},
	clear: () => {
		globalThis.localStorage?.removeItem(key);
	}
});

const openIndexedDb = (
	databaseName: string,
	storeName: string
): Promise<IDBDatabase> =>
	new Promise((resolve, reject) => {
		const request = globalThis.indexedDB.open(databaseName, 1);
		request.onupgradeneeded = () => {
			request.result.createObjectStore(storeName);
		};
		request.onsuccess = () => resolve(request.result);
		request.onerror = () => reject(request.error);
	});

/**
 * A {@link CollectionCache} backed by IndexedDB — the durable, large-capacity
 * local-first store. Asynchronous; one row per collection `key` in a shared
 * object store. No-ops (resolving to `undefined`) where `indexedDB` is
 * unavailable (e.g. SSR), so the collection falls back to the server snapshot.
 */
export const indexedDbCollectionCache = <T>({
	key,
	databaseName = 'absolutejs-sync',
	storeName = 'collections'
}: {
	/** Distinct entry name within the store (e.g. the collection + params). */
	key: string;
	/** IndexedDB database name. Defaults to `absolutejs-sync`. */
	databaseName?: string;
	/** Object-store name. Defaults to `collections`. */
	storeName?: string;
}): CollectionCache<T> => {
	let handle: Promise<IDBDatabase> | undefined;
	const database = () => {
		handle ??= openIndexedDb(databaseName, storeName);
		return handle;
	};
	const withStore = async <R>(
		mode: IDBTransactionMode,
		run: (store: IDBObjectStore) => IDBRequest
	): Promise<R | undefined> => {
		if (globalThis.indexedDB === undefined) {
			return undefined;
		}
		const db = await database();
		return new Promise<R>((resolve, reject) => {
			const request = run(
				db.transaction(storeName, mode).objectStore(storeName)
			);
			request.onsuccess = () => resolve(request.result as R);
			request.onerror = () => reject(request.error);
		});
	};

	return {
		load: () =>
			withStore<CollectionCacheSnapshot<T>>('readonly', (store) =>
				store.get(key)
			),
		save: async (snapshot) => {
			await withStore('readwrite', (store) => store.put(snapshot, key));
		},
		clear: async () => {
			await withStore('readwrite', (store) => store.delete(key));
		}
	};
};

export type SyncCollectionOptions<T> = {
	/** WebSocket URL of the {@link syncSocket} endpoint (e.g. `ws://host/sync/ws`). */
	url: string;
	/** Registered collection name to subscribe to. */
	collection: string;
	/** Query params forwarded to the server collection's hydrate/match/authorize. */
	params?: unknown;
	/** Row identity, used to apply diffs and optimistic edits. Defaults to `row.id`. */
	key?: (row: T) => RowKey;
	/** WebSocket implementation; defaults to the global one (pass for tests/SSR). */
	webSocketImpl?: typeof WebSocket;
	/**
	 * Base reconnect delay (ms), doubled each attempt up to `maxReconnectMs`.
	 * Set 0 to disable auto-reconnect. Defaults to 500.
	 */
	reconnectMs?: number;
	/** Maximum reconnect backoff (ms). Defaults to 10000. */
	maxReconnectMs?: number;
	/**
	 * Persist the pending-mutation queue so it survives a reload (offline) and
	 * replays on connect. See {@link localStorageMutationStorage}.
	 */
	storage?: MutationStorage;
	/**
	 * Persist confirmed rows locally for instant reads on reload and offline
	 * (local-first). Hydrated before the socket connects; the engine then
	 * resumes from the cached version (catch-up diff, or a fresh snapshot if the
	 * server's changelog no longer covers it). See {@link
	 * localStorageCollectionCache} / {@link indexedDbCollectionCache}.
	 */
	cache?: CollectionCache<T>;
	/** Called with each server error message. */
	onError?: (error: unknown) => void;
};

export type SyncCollection<T> = {
	/** Current state snapshot (stable reference until the next change). */
	get: () => SyncCollectionState<T>;
	/** Subscribe to state changes; returns an unsubscribe. */
	subscribe: (
		listener: (state: SyncCollectionState<T>) => void
	) => () => void;
	/**
	 * Run a server mutation, optionally applying it optimistically. Resolves with
	 * the server's result on ack, rejects (and rolls back) on reject. Pending
	 * mutations are replayed when the socket reconnects, so they survive a drop.
	 */
	mutate: <R = unknown>(options: MutateOptions<T>) => Promise<R>;
	/**
	 * Force-close the underlying WebSocket without tearing down state. The
	 * auto-reconnect loop fires after `reconnectMs`; the collection's
	 * `appliedVersion` is preserved so the resumed subscribe carries `since`
	 * and the engine replies with a catch-up diff (or a fresh snapshot if
	 * the change log no longer covers the gap).
	 *
	 * Useful for simulating an offline blip in tests and benches that need
	 * to measure resume cost specifically (vs cold-hydration on a fresh
	 * collection). No-op if the collection has been `close()`d.
	 */
	disconnect: () => void;
	/** Unsubscribe on the server, close the socket, and stop reconnecting. */
	close: () => void;
};

// One store subscribes to exactly one collection, so a fixed frame id suffices.
const SUBSCRIPTION_ID = 's';

type PendingMutation<T> = {
	mutationId: number;
	name: string;
	args: unknown;
	optimistic?: (draft: OptimisticDraft<T>) => void;
	resolve: (result: unknown) => void;
	reject: (error: unknown) => void;
};

/**
 * A live collection backed by the WebSocket sync engine. Reads: connect,
 * subscribe, apply the server's snapshot then row-level diffs, re-sync on
 * reconnect. Writes: {@link SyncCollection.mutate} applies an optimistic overlay
 * immediately, sends the mutation, and reconciles on ack (drop the overlay — the
 * authoritative diff already arrived) or reject (roll back). Framework-agnostic
 * (`get` + `subscribe`).
 *
 * Mutations are replayed on reconnect, so make server mutations idempotent —
 * delivery is at-least-once if an ack is lost across a drop.
 */
export const createSyncCollection = <T>(
	options: SyncCollectionOptions<T>
): SyncCollection<T> => {
	const key = options.key ?? ((row: T) => (row as { id: RowKey }).id);
	const reconnectMs = options.reconnectMs ?? 500;
	const maxReconnectMs = options.maxReconnectMs ?? 10_000;
	const Impl = options.webSocketImpl ?? globalThis.WebSocket;
	if (!Impl) {
		throw new Error(
			'createSyncCollection requires WebSocket. Run in a browser or pass webSocketImpl.'
		);
	}

	// Server-authoritative rows; `pending` is the optimistic overlay on top.
	const confirmed = new Map<RowKey, T>();
	const pending: PendingMutation<T>[] = [];
	let mutationSeq = 0;

	let state: SyncCollectionState<T> = {
		data: [],
		status: 'connecting',
		error: undefined
	};
	const listeners = new Set<(state: SyncCollectionState<T>) => void>();
	const setState = (patch: Partial<SyncCollectionState<T>>) => {
		state = { ...state, ...patch };
		for (const listener of listeners) {
			listener(state);
		}
	};

	/** Recompute visible rows = confirmed + pending optimistic effects. */
	const recompute = (patch: Partial<SyncCollectionState<T>> = {}) => {
		const working = new Map(confirmed);
		const draft: OptimisticDraft<T> = {
			set: (row) => working.set(key(row), row),
			delete: (rowKey) => working.delete(rowKey)
		};
		for (const mutation of pending) {
			mutation.optimistic?.(draft);
		}
		setState({ ...patch, data: [...working.values()] });
	};

	let socket: WebSocket | undefined;
	let connected = false;
	let closed = false;
	let attempt = 0;
	let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	// Highest change-feed version applied; sent as `since` to resume on reconnect.
	let appliedVersion = 0;

	const persist = () => {
		void options.storage?.save(
			pending.map((mutation) => ({
				mutationId: mutation.mutationId,
				name: mutation.name,
				args: mutation.args
			}))
		);
	};

	// Coalesce a burst of confirmed changes (a frame of diffs) into one cache
	// write per tick. Persists only the server-authoritative set — never the
	// optimistic overlay (those live in the mutation queue instead).
	let cacheScheduled = false;
	const persistCache = () => {
		if (options.cache === undefined || cacheScheduled) {
			return;
		}
		cacheScheduled = true;
		queueMicrotask(() => {
			cacheScheduled = false;
			void options.cache?.save({
				rows: [...confirmed.values()],
				version: appliedVersion
			});
		});
	};

	const settlePending = (mutationId: number) => {
		const index = pending.findIndex(
			(mutation) => mutation.mutationId === mutationId
		);
		if (index === -1) {
			return undefined;
		}
		const [mutation] = pending.splice(index, 1);
		persist();
		return mutation;
	};

	const applyFrame = (frame: ServerFrame<T>) => {
		if (frame.type === 'snapshot') {
			confirmed.clear();
			for (const row of frame.rows) {
				confirmed.set(key(row), row);
			}
			if (frame.version !== undefined) {
				appliedVersion = frame.version;
			}
			persistCache();
			recompute({ status: 'ready', error: undefined });
		} else if (frame.type === 'diff') {
			for (const row of frame.removed) {
				confirmed.delete(key(row));
			}
			for (const row of frame.added) {
				confirmed.set(key(row), row);
			}
			for (const row of frame.changed) {
				confirmed.set(key(row), row);
			}
			if (frame.version !== undefined) {
				appliedVersion = Math.max(appliedVersion, frame.version);
			}
			persistCache();
			// A diff only arrives once subscribed — including the catch-up diff a
			// resume replies with — so receiving one means we're live.
			recompute({ status: 'ready', error: undefined });
		} else if (frame.type === 'error') {
			setState({ error: frame.message });
			options.onError?.(frame.message);
		} else if (frame.type === 'ack') {
			// The authoritative diff already arrived (ordered before the ack), so
			// dropping the overlay leaves the confirmed row in place — no flicker.
			const mutation = settlePending(frame.mutationId);
			if (mutation !== undefined) {
				recompute();
				mutation.resolve(frame.result);
			}
		} else if (frame.type === 'reject') {
			// roll the optimistic overlay back.
			const mutation = settlePending(frame.mutationId);
			if (mutation !== undefined) {
				recompute();
				mutation.reject(new Error(String(frame.message)));
			}
		}
		// A `frame` (multi-collection batch) never reaches a single-collection
		// store — that's the multiplexed createSyncClient's job — so ignore it.
	};

	const sendMutate = (mutation: PendingMutation<T>) => {
		if (connected) {
			socket?.send(
				JSON.stringify({
					type: 'mutate',
					mutationId: mutation.mutationId,
					name: mutation.name,
					args: mutation.args
				})
			);
		}
	};

	const connect = () => {
		if (closed) {
			return;
		}
		setState({ status: 'connecting' });
		const ws = new Impl(options.url);
		socket = ws;
		ws.onopen = () => {
			attempt = 0;
			connected = true;
			ws.send(
				JSON.stringify({
					type: 'subscribe',
					id: SUBSCRIPTION_ID,
					collection: options.collection,
					params: options.params,
					// Resume from what we've applied (catch-up instead of snapshot).
					since: appliedVersion > 0 ? appliedVersion : undefined
				})
			);
			// Replay anything still pending across the (re)connect.
			for (const mutation of pending) {
				sendMutate(mutation);
			}
		};
		ws.onmessage = (event) => {
			try {
				applyFrame(JSON.parse(event.data as string) as ServerFrame<T>);
			} catch {
				// ignore non-JSON frames
			}
		};
		ws.onclose = () => {
			connected = false;
			if (closed || reconnectMs <= 0) {
				return;
			}
			const delay = Math.min(reconnectMs * 2 ** attempt, maxReconnectMs);
			attempt += 1;
			reconnectTimer = setTimeout(connect, delay);
		};
	};

	// Reload recovery: re-queue persisted unconfirmed mutations so they replay on
	// connect. They carry no optimistic effect or promise (the resumed/snapshot
	// state is authoritative); resending produces the diffs that bring them in.
	const hydratePersisted = async () => {
		if (options.storage === undefined) {
			return;
		}
		const records = await options.storage.load();
		for (const record of records) {
			if (pending.some((m) => m.mutationId === record.mutationId)) {
				continue;
			}
			pending.push({
				mutationId: record.mutationId,
				name: record.name,
				args: record.args,
				resolve: () => {},
				reject: () => {}
			});
			mutationSeq = Math.max(mutationSeq, record.mutationId);
		}
		if (connected) {
			for (const mutation of pending) {
				sendMutate(mutation);
			}
		}
	};

	// Local-first: load cached rows + version before connecting, so reads are
	// instant on reload and available offline. The subscribe then resumes from
	// the cached version — a catch-up diff if the server's changelog still
	// covers it, else a fresh snapshot that replaces the stale cache.
	const hydrateCache = async () => {
		if (options.cache === undefined) {
			return;
		}
		let snapshot: CollectionCacheSnapshot<T> | undefined;
		try {
			snapshot = await options.cache.load();
		} catch {
			return; // corrupt/unavailable cache: fall back to the server snapshot
		}
		// Don't clobber server data if a frame somehow already landed.
		if (snapshot === undefined || appliedVersion > 0) {
			return;
		}
		for (const row of snapshot.rows) {
			confirmed.set(key(row), row);
		}
		appliedVersion = snapshot.version;
		recompute(); // show cached rows immediately (status stays 'connecting')
	};

	if (options.cache === undefined) {
		// No cache: preserve the original connect-then-hydrate ordering/timing.
		connect();
		void hydratePersisted();
	} else {
		// Cache: hydrate reads + queued writes first, then connect so the
		// subscribe carries the cached resume version.
		void (async () => {
			await hydrateCache();
			await hydratePersisted();
			connect();
		})();
	}

	return {
		get: () => state,
		subscribe: (listener) => {
			listeners.add(listener);
			return () => {
				listeners.delete(listener);
			};
		},
		mutate: <R = unknown>(mutateOptions: MutateOptions<T>) =>
			new Promise<R>((resolve, reject) => {
				const mutation: PendingMutation<T> = {
					mutationId: (mutationSeq += 1),
					name: mutateOptions.name,
					args: mutateOptions.args,
					optimistic: mutateOptions.optimistic,
					resolve: (result) => resolve(result as R),
					reject
				};
				pending.push(mutation);
				persist();
				recompute(); // apply the optimistic overlay immediately
				sendMutate(mutation);
			}),
		disconnect: () => {
			// Force-close the WS without tearing down state. The existing
			// `ws.onclose` handler schedules a reconnect via the auto-
			// reconnect loop (unless the collection has been `close()`d).
			// `appliedVersion` is preserved, so the resumed subscribe carries
			// `since` and the engine sends a catch-up diff (or snapshot if
			// the change log can't cover the gap).
			if (closed || socket === undefined) {
				return;
			}
			try {
				socket.close();
			} catch {
				// already closing/closed
			}
		},
		close: () => {
			if (closed) {
				return;
			}
			closed = true;
			connected = false;
			if (reconnectTimer !== undefined) {
				clearTimeout(reconnectTimer);
			}
			try {
				socket?.send(
					JSON.stringify({ type: 'unsubscribe', id: SUBSCRIPTION_ID })
				);
				socket?.close();
			} catch {
				// socket already closing/closed
			}
			// Fail any still-pending mutations so their promises don't hang.
			for (const mutation of pending.splice(0)) {
				mutation.reject(new Error('sync collection closed'));
			}
			persist();
			setState({ status: 'closed' });
			listeners.clear();
		}
	};
};
