import type { ServerFrame } from '../engine/connection';
import type { RowKey } from '../engine/types';

export type { ServerFrame } from '../engine/connection';

export type SyncCollectionStatus = 'connecting' | 'ready' | 'closed';

export type SyncCollectionState<T> = {
	/** Current rows of the collection (insertion order; not sorted). */
	data: T[];
	/** Connection/sync status. */
	status: SyncCollectionStatus;
	/** Last error message from the server, or `undefined`. */
	error: unknown;
};

export type SyncCollectionOptions<T> = {
	/** WebSocket URL of the {@link syncSocket} endpoint (e.g. `ws://host/sync/ws`). */
	url: string;
	/** Registered collection name to subscribe to. */
	collection: string;
	/** Query params forwarded to the server collection's hydrate/match/authorize. */
	params?: unknown;
	/** Row identity, used to apply diffs. Defaults to `row.id`. */
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
	/** Unsubscribe on the server, close the socket, and stop reconnecting. */
	close: () => void;
};

// One store subscribes to exactly one collection, so a fixed frame id suffices.
const SUBSCRIPTION_ID = 's';

/**
 * A live collection backed by the WebSocket sync engine: connect, subscribe,
 * apply the server's snapshot then its diffs into a local set, and re-sync on
 * reconnect. Framework-agnostic (`get` + `subscribe`, for `useSyncExternalStore`
 * or any equivalent).
 *
 * Unlike {@link createLiveQuery} (Tier 2 — refetch on a topic), this maintains
 * the result set from row-level diffs, so a change moves only the affected rows.
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

	const rows = new Map<RowKey, T>();
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
	const commitRows = () =>
		setState({
			data: [...rows.values()],
			status: 'ready',
			error: undefined
		});

	let socket: WebSocket | undefined;
	let closed = false;
	let attempt = 0;
	let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

	const applyFrame = (frame: ServerFrame<T>) => {
		if (frame.type === 'snapshot') {
			rows.clear();
			for (const row of frame.rows) {
				rows.set(key(row), row);
			}
			commitRows();
		} else if (frame.type === 'diff') {
			for (const row of frame.removed) {
				rows.delete(key(row));
			}
			for (const row of frame.added) {
				rows.set(key(row), row);
			}
			for (const row of frame.changed) {
				rows.set(key(row), row);
			}
			commitRows();
		} else if (frame.type === 'error') {
			setState({ error: frame.message });
			options.onError?.(frame.message);
		}
		// ack/reject are handled by the mutation layer.
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
			ws.send(
				JSON.stringify({
					type: 'subscribe',
					id: SUBSCRIPTION_ID,
					collection: options.collection,
					params: options.params
				})
			);
		};
		ws.onmessage = (event) => {
			try {
				applyFrame(JSON.parse(event.data as string) as ServerFrame<T>);
			} catch {
				// ignore non-JSON frames
			}
		};
		ws.onclose = () => {
			if (closed || reconnectMs <= 0) {
				return;
			}
			const delay = Math.min(reconnectMs * 2 ** attempt, maxReconnectMs);
			attempt += 1;
			reconnectTimer = setTimeout(connect, delay);
		};
	};

	connect();

	return {
		get: () => state,
		subscribe: (listener) => {
			listeners.add(listener);
			return () => {
				listeners.delete(listener);
			};
		},
		close: () => {
			if (closed) {
				return;
			}
			closed = true;
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
			setState({ status: 'closed' });
			listeners.clear();
		}
	};
};
