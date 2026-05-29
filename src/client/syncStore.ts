import type { ServerFrame } from '../engine/connection';
import type { RowKey } from '../engine/types';
import type { MutationStorage, PendingMutationRecord } from './syncCollection';
import { jsonSerializer, type FrameSerializer } from '../serializer';

export type SyncStoreStatus = 'connecting' | 'ready' | 'closed';

export type SyncStoreState<Row> = {
	/** Visible rows: confirmed server state with pending optimistic edits applied. */
	data: Row[];
	status: SyncStoreStatus;
	error: unknown;
};

/** A working set a mutation's optimistic effect edits in place. */
export type OptimisticDraft<Row> = {
	set: (row: Row) => void;
	delete: (key: RowKey) => void;
};

/** A map of named server mutations — typically Eden calls. */
export type MutationMap = Record<string, (args: never) => Promise<unknown>>;

export type MutateOptions<Row> = {
	/** Apply the mutation's effect locally for instant UI (rolled back on reject). */
	optimistic?: (draft: OptimisticDraft<Row>) => void;
};

export type SyncStoreOptions<Row, M extends MutationMap> = {
	/** WebSocket URL of the {@link syncSocket} endpoint. */
	url: string;
	/** Collection name to subscribe to for diffs. */
	collection: string;
	/** Query params forwarded to the server collection. */
	params?: unknown;
	/**
	 * Typed read — typically an Eden call (`() => unwrapEden(api.sync.orders.get(...))`).
	 * It is the source of the `Row` **type**, gives an eager first paint, and is
	 * reusable for SSR. Live confirmed state then comes from the WS snapshot.
	 */
	hydrate?: () => Promise<Row[]>;
	/** Seed rows (e.g. from SSR); shown immediately, refreshed by the WS snapshot. */
	initialData?: Row[];
	/** Typed server mutations (Eden calls). Enables `store.mutate(name, args, ...)`. */
	mutations?: M;
	/** Row identity. Defaults to `row.id`. */
	key?: (row: Row) => RowKey;
	webSocketImpl?: typeof WebSocket;
	reconnectMs?: number;
	maxReconnectMs?: number;
	/**
	 * After the server confirms a mutation, drop its optimistic overlay this long
	 * after if no diff has reflected it (covers mutations that don't touch this
	 * collection). Defaults to 3000.
	 */
	reconcileGraceMs?: number;
	/** Persist the pending-mutation queue across reloads (offline). */
	storage?: MutationStorage;
	onError?: (error: unknown) => void;
	/**
	 * Wire-format serializer (1.16.0). Defaults to `jsonSerializer`. MUST
	 * match the server's `syncSocket` serializer.
	 */
	serializer?: FrameSerializer;
};

export type SyncStore<Row, M extends MutationMap> = {
	get: () => SyncStoreState<Row>;
	subscribe: (listener: (state: SyncStoreState<Row>) => void) => () => void;
	/**
	 * Run a named server mutation, optionally applying it optimistically. Resolves
	 * with the server's result; rolls back and rejects if the server rejects it.
	 * While offline (socket down) it stays queued and retries on reconnect.
	 */
	mutate: <K extends keyof M>(
		name: K,
		args: Parameters<M[K]>[0],
		options?: MutateOptions<Row>
	) => Promise<Awaited<ReturnType<M[K]>>>;
	/** Re-run `hydrate` and refresh confirmed state. */
	refetch: () => Promise<void>;
	close: () => void;
};

const SUBSCRIPTION_ID = 's';

type Pending<Row> = {
	id: number;
	name: string;
	args: unknown;
	/** Keys this mutation's optimistic effect touched, and how. */
	touched: Map<RowKey, 'set' | 'delete'>;
	optimistic?: (draft: OptimisticDraft<Row>) => void;
	settled: boolean;
	inFlight: boolean;
	resolve: (value: unknown) => void;
	reject: (error: unknown) => void;
	graceTimer?: ReturnType<typeof setTimeout>;
};

/**
 * A generic, Eden-fed live collection store (the typed Tier 3 client). Confirmed
 * state is maintained from the WS snapshot + diffs; mutations run over your typed
 * transport (Eden), apply an optimistic overlay, and reconcile against the diffs.
 * Types come entirely from the `hydrate`/`mutations` you pass — no `<T>`.
 */
export const syncStore = <Row, M extends MutationMap = MutationMap>(
	options: SyncStoreOptions<Row, M>
): SyncStore<Row, M> => {
	const key = options.key ?? ((row: Row) => (row as { id: RowKey }).id);
	const reconnectMs = options.reconnectMs ?? 500;
	const maxReconnectMs = options.maxReconnectMs ?? 10_000;
	const reconcileGraceMs = options.reconcileGraceMs ?? 3000;
	const mutations = options.mutations ?? ({} as M);
	const serializer: FrameSerializer = options.serializer ?? jsonSerializer;
	const Impl = options.webSocketImpl ?? globalThis.WebSocket;
	if (!Impl) {
		throw new Error(
			'syncStore requires WebSocket. Run in a browser or pass webSocketImpl.'
		);
	}

	const confirmed = new Map<RowKey, Row>();
	const pending: Pending<Row>[] = [];
	let mutationSeq = 0;

	let state: SyncStoreState<Row> = {
		data: options.initialData ? [...options.initialData] : [],
		status: 'connecting',
		error: undefined
	};
	if (options.initialData) {
		for (const row of options.initialData) {
			confirmed.set(key(row), row);
		}
	}

	const listeners = new Set<(state: SyncStoreState<Row>) => void>();
	const setState = (patch: Partial<SyncStoreState<Row>>) => {
		state = { ...state, ...patch };
		for (const listener of listeners) {
			listener(state);
		}
	};
	const recompute = (patch: Partial<SyncStoreState<Row>> = {}) => {
		const working = new Map(confirmed);
		const draft: OptimisticDraft<Row> = {
			set: (row) => working.set(key(row), row),
			delete: (rowKey) => working.delete(rowKey)
		};
		for (const mutation of pending) {
			mutation.optimistic?.(draft);
		}
		setState({ ...patch, data: [...working.values()] });
	};

	const persist = () => {
		void options.storage?.save(
			pending.map((mutation) => ({
				mutationId: mutation.id,
				name: mutation.name,
				args: mutation.args
			}))
		);
	};

	const dropPending = (mutation: Pending<Row>) => {
		const index = pending.indexOf(mutation);
		if (index !== -1) {
			pending.splice(index, 1);
		}
		if (mutation.graceTimer !== undefined) {
			clearTimeout(mutation.graceTimer);
		}
	};

	/** Drop settled overlays whose touched keys are now reflected in confirmed. */
	const reconcileSettled = () => {
		let changed = false;
		for (const mutation of [...pending]) {
			if (!mutation.settled) {
				continue;
			}
			let reflected = true;
			for (const [rowKey, kind] of mutation.touched) {
				const present = confirmed.has(rowKey);
				if (kind === 'set' ? !present : present) {
					reflected = false;
					break;
				}
			}
			if (reflected) {
				dropPending(mutation);
				changed = true;
			}
		}
		if (changed) {
			recompute();
		}
	};

	let socket: WebSocket | undefined;
	let connected = false;
	let closed = false;
	let attempt = 0;
	let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	// Highest change-feed version applied; sent as `since` to resume on reconnect.
	let appliedVersion = 0;

	const applyFrame = (frame: ServerFrame<Row>) => {
		if (frame.type === 'snapshot') {
			confirmed.clear();
			for (const row of frame.rows) {
				confirmed.set(key(row), row);
			}
			if (frame.version !== undefined) {
				appliedVersion = frame.version;
			}
			recompute({ status: 'ready', error: undefined });
			reconcileSettled();
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
			recompute();
			reconcileSettled();
		} else if (frame.type === 'error') {
			setState({ error: frame.message });
			options.onError?.(frame.message);
		}
		// ack/reject frames are unused here — mutations run over the typed transport.
	};

	const runMutation = async (mutation: Pending<Row>) => {
		if (mutation.inFlight || mutation.settled) {
			return;
		}
		const run = mutations[mutation.name];
		if (run === undefined) {
			dropPending(mutation);
			recompute();
			mutation.reject(new Error(`Unknown mutation "${mutation.name}"`));
			return;
		}
		mutation.inFlight = true;
		try {
			const result = await (run as (args: unknown) => Promise<unknown>)(
				mutation.args
			);
			mutation.inFlight = false;
			mutation.settled = true;
			mutation.resolve(result);
			persist();
			reconcileSettled();
			// If the diff hasn't reflected it yet, drop the overlay after a grace.
			if (pending.includes(mutation)) {
				mutation.graceTimer = setTimeout(() => {
					dropPending(mutation);
					recompute();
				}, reconcileGraceMs);
			}
		} catch (error) {
			mutation.inFlight = false;
			if (connected) {
				// Server is reachable → a real rejection: roll back.
				dropPending(mutation);
				recompute();
				persist();
				mutation.reject(error);
			} else {
				// Offline → keep queued; retry on reconnect.
				options.onError?.(error);
			}
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
			ws.send(serializer.encodeClient({
				type: 'subscribe',
				id: SUBSCRIPTION_ID,
				collection: options.collection,
				params: options.params,
				// Resume from what we've applied (catch-up instead of snapshot).
				since: appliedVersion > 0 ? appliedVersion : undefined
			}) as string);
			// Retry mutations that failed/queued while offline.
			for (const mutation of pending) {
				if (!mutation.settled && !mutation.inFlight) {
					void runMutation(mutation);
				}
			}
		};
		ws.onmessage = (event) => {
			try {
				const decoded = serializer.decode(event.data);
				if (decoded !== null && typeof decoded === 'object') {
					applyFrame(decoded as ServerFrame<Row>);
				}
			} catch {
				// ignore unparseable frames
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

	const eagerHydrate = async () => {
		if (
			options.hydrate === undefined ||
			options.initialData !== undefined
		) {
			return;
		}
		try {
			const rows = await options.hydrate();
			// Don't clobber a WS snapshot that already arrived.
			if (state.status !== 'ready') {
				confirmed.clear();
				for (const row of rows) {
					confirmed.set(key(row), row);
				}
				recompute({ status: 'ready' });
			}
		} catch (error) {
			options.onError?.(error);
		}
	};

	const hydratePersisted = async () => {
		if (options.storage === undefined) {
			return;
		}
		const records = await options.storage.load();
		for (const record of records as PendingMutationRecord[]) {
			if (pending.some((m) => m.id === record.mutationId)) {
				continue;
			}
			pending.push({
				id: record.mutationId,
				name: record.name,
				args: record.args,
				touched: new Map(),
				settled: false,
				inFlight: false,
				resolve: () => {},
				reject: () => {}
			});
			mutationSeq = Math.max(mutationSeq, record.mutationId);
		}
		if (connected) {
			for (const mutation of pending) {
				void runMutation(mutation);
			}
		}
	};

	connect();
	void eagerHydrate();
	void hydratePersisted();

	const collectTouched = (
		optimistic?: (draft: OptimisticDraft<Row>) => void
	) => {
		const touched = new Map<RowKey, 'set' | 'delete'>();
		optimistic?.({
			set: (row) => touched.set(key(row), 'set'),
			delete: (rowKey) => touched.set(rowKey, 'delete')
		});
		return touched;
	};

	return {
		get: () => state,
		subscribe: (listener) => {
			listeners.add(listener);
			return () => {
				listeners.delete(listener);
			};
		},
		mutate: ((name, args, mutateOptions) =>
			new Promise((resolve, reject) => {
				const mutation: Pending<Row> = {
					id: (mutationSeq += 1),
					name: name as string,
					args,
					touched: collectTouched(mutateOptions?.optimistic),
					optimistic: mutateOptions?.optimistic,
					settled: false,
					inFlight: false,
					resolve: resolve as (value: unknown) => void,
					reject
				};
				pending.push(mutation);
				persist();
				recompute();
				void runMutation(mutation);
			})) as SyncStore<Row, M>['mutate'],
		refetch: async () => {
			if (options.hydrate === undefined) {
				return;
			}
			const rows = await options.hydrate();
			confirmed.clear();
			for (const row of rows) {
				confirmed.set(key(row), row);
			}
			recompute({ status: 'ready' });
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
					serializer.encodeClient({
						type: 'unsubscribe',
						id: SUBSCRIPTION_ID
					}) as string
				);
				socket?.close();
			} catch {
				// already closing
			}
			for (const mutation of pending.splice(0)) {
				if (mutation.graceTimer !== undefined) {
					clearTimeout(mutation.graceTimer);
				}
				mutation.reject(new Error('sync store closed'));
			}
			persist();
			setState({ status: 'closed' });
			listeners.clear();
		}
	};
};

/**
 * Unwrap an Eden treaty response (`{ data, error }`) to its data, throwing the
 * error. Use it to feed Eden calls to {@link syncStore}'s `hydrate`/`mutations`:
 * `hydrate: () => unwrapEden(api.sync.orders.get({ query }))`.
 */
export const unwrapEden = async <T>(
	response: Promise<{ data: T | null; error?: unknown }>
): Promise<T> => {
	const { data, error } = await response;
	if (error !== null && error !== undefined) {
		throw error;
	}
	return data as T;
};
