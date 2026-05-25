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

	const settlePending = (mutationId: number) => {
		const index = pending.findIndex(
			(mutation) => mutation.mutationId === mutationId
		);
		if (index === -1) {
			return undefined;
		}
		const [mutation] = pending.splice(index, 1);
		return mutation;
	};

	const applyFrame = (frame: ServerFrame<T>) => {
		if (frame.type === 'snapshot') {
			confirmed.clear();
			for (const row of frame.rows) {
				confirmed.set(key(row), row);
			}
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
			recompute();
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
		} else {
			// reject — roll the optimistic overlay back.
			const mutation = settlePending(frame.mutationId);
			if (mutation !== undefined) {
				recompute();
				mutation.reject(new Error(String(frame.message)));
			}
		}
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
					params: options.params
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

	connect();

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
				recompute(); // apply the optimistic overlay immediately
				sendMutate(mutation);
			}),
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
			setState({ status: 'closed' });
			listeners.clear();
		}
	};
};
