import type { ServerFrame } from '../engine/connection';
import type { RowKey } from '../engine/types';
import type {
	MutateOptions,
	OptimisticDraft,
	SyncCollectionState,
	SyncCollectionStatus
} from './syncCollection';

export type SyncClientOptions = {
	/** WebSocket URL of the {@link syncSocket} endpoint (e.g. `ws://host/sync/ws`). */
	url: string;
	/** WebSocket implementation; defaults to the global one (pass for tests/SSR). */
	webSocketImpl?: typeof WebSocket;
	/** Initial reconnect backoff (ms); doubles per attempt. Defaults to 500. */
	reconnectMs?: number;
	/** Max reconnect backoff (ms). Defaults to 10000. */
	maxReconnectMs?: number;
	/** Called with the message of any server `error` frame. */
	onError?: (message: unknown) => void;
};

export type SyncCollectionHandleOptions<T> = {
	/** Registered collection name to subscribe to. */
	collection: string;
	/** Query params forwarded to the server collection. */
	params?: unknown;
	/** Row identity. Defaults to `row.id`. */
	key?: (row: T) => RowKey;
};

export type SyncCollectionHandle<T> = {
	/** Current state snapshot (stable until the next change). */
	get: () => SyncCollectionState<T>;
	/** Subscribe to state changes; returns an unsubscribe. */
	subscribe: (
		listener: (state: SyncCollectionState<T>) => void
	) => () => void;
	/** Run a server mutation, optionally applying it optimistically. */
	mutate: <R = unknown>(options: MutateOptions<T>) => Promise<R>;
	/** Unsubscribe this collection (the socket stays open for others). */
	close: () => void;
};

export type SyncClient = {
	/** Subscribe to a collection over the shared socket. */
	collection: <T>(
		options: SyncCollectionHandleOptions<T>
	) => SyncCollectionHandle<T>;
	/**
	 * Force-close the underlying WebSocket without tearing down state. The
	 * auto-reconnect loop fires after `reconnectMs`; each entry's
	 * `appliedVersion` is preserved so the resumed connection's subscribe
	 * carries `since` and the engine replies with a catch-up diff (or a
	 * fresh snapshot if the change log no longer covers the gap).
	 *
	 * Useful for simulating an offline blip in tests and for benches that
	 * measure catch-up cost specifically (vs cold-hydration on a fresh
	 * client). No-op if the socket is already closed.
	 */
	disconnect: () => void;
	/** Close the socket and every handle. */
	close: () => void;
};

type PendingMutation = {
	mutationId: number;
	name: string;
	args: unknown;
	optimistic?: (draft: OptimisticDraft<unknown>) => void;
	resolve: (result: unknown) => void;
	reject: (error: unknown) => void;
};

type Entry = {
	id: string;
	collection: string;
	params: unknown;
	key: (row: unknown) => RowKey;
	confirmed: Map<RowKey, unknown>;
	pending: PendingMutation[];
	state: SyncCollectionState<unknown>;
	listeners: Set<(state: SyncCollectionState<unknown>) => void>;
	appliedVersion: number;
	closed: boolean;
};

/**
 * A multiplexed sync client: one WebSocket serving many live collections. Its
 * reason to exist over per-collection {@link createSyncCollection} is the
 * **consistent frame** — when one atomic mutation touches several collections,
 * the server bundles the diffs into a single `frame` and this client applies
 * them all (to every collection's confirmed state) before notifying any
 * listener, so a view reading multiple collections never paints a torn
 * intermediate where one moved and the other hasn't.
 *
 * Reads: subscribe, apply snapshot then diffs/frames, resume on reconnect.
 * Writes: per-collection optimistic overlay, reconciled on ack/reject and
 * replayed on reconnect (make server mutations idempotent).
 */
export const createSyncClient = (options: SyncClientOptions): SyncClient => {
	const reconnectMs = options.reconnectMs ?? 500;
	const maxReconnectMs = options.maxReconnectMs ?? 10_000;
	const Impl = options.webSocketImpl ?? globalThis.WebSocket;
	if (!Impl) {
		throw new Error(
			'createSyncClient requires WebSocket. Run in a browser or pass webSocketImpl.'
		);
	}

	const entries = new Map<string, Entry>();
	const mutationOwner = new Map<number, Entry>();
	let nextEntryId = 0;
	let mutationSeq = 0;

	let socket: WebSocket | undefined;
	let connected = false;
	let closed = false;
	let attempt = 0;
	let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

	const notify = (entry: Entry) => {
		for (const listener of entry.listeners) {
			listener(entry.state);
		}
	};

	/** Recompute one entry's visible state (confirmed + optimistic), no notify. */
	const rebuild = (
		entry: Entry,
		patch: Partial<SyncCollectionState<unknown>> = {}
	) => {
		const working = new Map(entry.confirmed);
		const draft: OptimisticDraft<unknown> = {
			set: (row) => working.set(entry.key(row), row),
			delete: (rowKey) => working.delete(rowKey)
		};
		for (const mutation of entry.pending) {
			mutation.optimistic?.(draft);
		}
		entry.state = {
			...entry.state,
			...patch,
			data: [...working.values()]
		};
	};

	const recompute = (
		entry: Entry,
		patch: Partial<SyncCollectionState<unknown>> = {}
	) => {
		rebuild(entry, patch);
		notify(entry);
	};

	const applyDiffToConfirmed = (
		entry: Entry,
		diff: { added: unknown[]; removed: unknown[]; changed: unknown[] }
	) => {
		for (const row of diff.removed) {
			entry.confirmed.delete(entry.key(row));
		}
		for (const row of diff.added) {
			entry.confirmed.set(entry.key(row), row);
		}
		for (const row of diff.changed) {
			entry.confirmed.set(entry.key(row), row);
		}
	};

	const settlePending = (mutationId: number) => {
		const entry = mutationOwner.get(mutationId);
		mutationOwner.delete(mutationId);
		if (entry === undefined) {
			return undefined;
		}
		const index = entry.pending.findIndex(
			(mutation) => mutation.mutationId === mutationId
		);
		if (index === -1) {
			return undefined;
		}
		const [mutation] = entry.pending.splice(index, 1);
		return { entry, mutation: mutation! };
	};

	const applyFrame = (frame: ServerFrame) => {
		if (frame.type === 'snapshot') {
			const entry = entries.get(frame.id);
			if (entry === undefined) {
				return;
			}
			entry.confirmed.clear();
			for (const row of frame.rows) {
				entry.confirmed.set(entry.key(row), row);
			}
			if (frame.version !== undefined) {
				entry.appliedVersion = frame.version;
			}
			recompute(entry, { status: 'ready', error: undefined });
		} else if (frame.type === 'diff') {
			const entry = entries.get(frame.id);
			if (entry === undefined) {
				return;
			}
			applyDiffToConfirmed(entry, frame);
			if (frame.version !== undefined) {
				entry.appliedVersion = Math.max(
					entry.appliedVersion,
					frame.version
				);
			}
			recompute(entry);
		} else if (frame.type === 'frame') {
			// The consistent frame: update every affected collection's confirmed
			// state first, then notify — so no listener observes a partial batch.
			const affected = new Set<Entry>();
			for (const diff of frame.diffs) {
				const entry = entries.get(diff.id);
				if (entry === undefined) {
					continue;
				}
				applyDiffToConfirmed(entry, diff);
				if (frame.version !== undefined) {
					entry.appliedVersion = Math.max(
						entry.appliedVersion,
						frame.version
					);
				}
				// Update state now, but defer notifying until every collection in
				// the frame is updated — so no listener observes a partial batch.
				rebuild(entry);
				affected.add(entry);
			}
			for (const entry of affected) {
				notify(entry);
			}
		} else if (frame.type === 'error') {
			if (frame.id !== undefined) {
				const entry = entries.get(frame.id);
				if (entry !== undefined) {
					recompute(entry, { error: frame.message });
				}
			}
			options.onError?.(frame.message);
		} else if (frame.type === 'ack') {
			const settled = settlePending(frame.mutationId);
			if (settled !== undefined) {
				recompute(settled.entry);
				settled.mutation.resolve(frame.result);
			}
		} else if (frame.type === 'reject') {
			const settled = settlePending(frame.mutationId);
			if (settled !== undefined) {
				recompute(settled.entry);
				settled.mutation.reject(new Error(String(frame.message)));
			}
		}
	};

	const sendSubscribe = (entry: Entry) => {
		socket?.send(
			JSON.stringify({
				type: 'subscribe',
				id: entry.id,
				collection: entry.collection,
				params: entry.params,
				since:
					entry.appliedVersion > 0 ? entry.appliedVersion : undefined
			})
		);
	};

	const sendMutate = (mutation: PendingMutation) => {
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
		const ws = new Impl(options.url);
		socket = ws;
		ws.onopen = () => {
			attempt = 0;
			connected = true;
			for (const entry of entries.values()) {
				sendSubscribe(entry);
			}
			for (const entry of entries.values()) {
				for (const mutation of entry.pending) {
					sendMutate(mutation);
				}
			}
		};
		ws.onmessage = (event) => {
			try {
				applyFrame(JSON.parse(event.data as string) as ServerFrame);
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

	connect();

	const collection = <T>(
		handleOptions: SyncCollectionHandleOptions<T>
	): SyncCollectionHandle<T> => {
		const entryId = `c${nextEntryId}`;
		nextEntryId += 1;
		const entry: Entry = {
			id: entryId,
			collection: handleOptions.collection,
			params: handleOptions.params,
			key:
				(handleOptions.key as ((row: unknown) => RowKey) | undefined) ??
				((row: unknown) => (row as { id: RowKey }).id),
			confirmed: new Map(),
			pending: [],
			state: { data: [], status: 'connecting', error: undefined },
			listeners: new Set(),
			appliedVersion: 0,
			closed: false
		};
		entries.set(entryId, entry);
		if (connected) {
			sendSubscribe(entry);
		}

		return {
			get: () => entry.state as SyncCollectionState<T>,
			subscribe: (listener) => {
				const typed = listener as (
					state: SyncCollectionState<unknown>
				) => void;
				entry.listeners.add(typed);
				listener(entry.state as SyncCollectionState<T>);
				return () => {
					entry.listeners.delete(typed);
				};
			},
			mutate: <R = unknown>(mutateOptions: MutateOptions<T>) =>
				new Promise<R>((resolve, reject) => {
					mutationSeq += 1;
					const mutation: PendingMutation = {
						mutationId: mutationSeq,
						name: mutateOptions.name,
						args: mutateOptions.args,
						optimistic: mutateOptions.optimistic as
							| ((draft: OptimisticDraft<unknown>) => void)
							| undefined,
						resolve: (result) => resolve(result as R),
						reject
					};
					entry.pending.push(mutation);
					mutationOwner.set(mutation.mutationId, entry);
					recompute(entry);
					sendMutate(mutation);
				}),
			close: () => {
				if (entry.closed) {
					return;
				}
				entry.closed = true;
				entries.delete(entryId);
				if (connected) {
					socket?.send(
						JSON.stringify({ type: 'unsubscribe', id: entryId })
					);
				}
			}
		};
	};

	const close = () => {
		closed = true;
		if (reconnectTimer !== undefined) {
			clearTimeout(reconnectTimer);
		}
		socket?.close();
		entries.clear();
		mutationOwner.clear();
	};

	const disconnect = () => {
		// Force-close the WS without tearing down state. The existing
		// `ws.onclose` handler schedules a reconnect via the auto-reconnect
		// loop (unless the whole client has been `close()`d). Each entry's
		// `appliedVersion` survives, so the resumed subscribe carries `since`
		// and the engine sends a catch-up diff (or a snapshot if the gap is
		// too large for the change log).
		if (closed || socket === undefined) {
			return;
		}
		socket.close();
	};

	return { collection, close, disconnect };
};

export type { SyncCollectionState, SyncCollectionStatus };
