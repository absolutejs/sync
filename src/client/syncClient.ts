import type { ServerFrame } from '../engine/connection';
import type { RowKey } from '../engine/types';
import type {
	MutateOptions,
	OptimisticDraft,
	SerializableOptimisticOperation,
	SyncCollectionState,
	SyncCollectionStatus
} from './syncCollection';
import { createSyncOperationId, ensureSyncInstallationId } from './localStore';
import type {
	LocalMutationRecord,
	LocalOptimisticOperation,
	SyncLocalStore
} from './localStore';
import { jsonSerializer, type FrameSerializer } from '../serializer';
import {
	createReconnectBackoff,
	hasReconnectHealthyFrameType
} from './reconnectBackoff';
import { getSyncClientRuntimeTransport } from './runtimeTransport';

const serverFrameTypes = [
	'snapshot',
	'diff',
	'frame',
	'error',
	'ack',
	'reject'
] as const;

export type SyncClientOptions = {
	/** WebSocket URL of the {@link syncSocket} endpoint (e.g. `ws://host/sync/ws`). */
	url: string;
	/** WebSocket implementation; defaults to the global one (pass for tests/SSR). */
	webSocketImpl?: typeof WebSocket;
	/** Fetch a new short-lived, single-use ticket before every connection or
	 * reconnect. The ticket is sent in the first WebSocket frame, never a URL. */
	socketTicket?: () => Promise<string>;
	/** Initial reconnect backoff (ms); doubles per attempt. Defaults to 500. */
	reconnectMs?: number;
	/** Max reconnect backoff (ms). Defaults to 10000. */
	maxReconnectMs?: number;
	/** Called with the message of any server `error` frame. */
	onError?: (message: unknown) => void;
	/**
	 * Wire-format serializer (1.16.0). Defaults to `jsonSerializer` — the
	 * historical JSON-over-WS behavior. MUST match the server's `syncSocket`
	 * serializer; opt into a binary one (msgpack, cbor, custom) on both
	 * ends to cut bandwidth + parse CPU on large snapshots.
	 */
	serializer?: FrameSerializer;
	/**
	 * Opt into restart-safe local-first state. The AbsoluteJS integration layer
	 * can provision this from Auth and the active runtime; direct users provide
	 * one principal namespace.
	 */
	durable?: DurableSyncClientOptions;
};

export type DurableSyncClientOptions = {
	store: SyncLocalStore;
	/** Stable authenticated principal/tenant partition. Never reuse across users. */
	namespace: string;
	/** Injectable UUID source for deterministic tests. */
	createId?: () => string;
	/** Storage failures are reported here and through the client's `onError`. */
	onError?: (error: unknown) => void;
};

export type SyncCollectionHandleOptions<T> = {
	/** Registered collection name to subscribe to. */
	collection: string;
	/** Query params forwarded to the server collection. */
	params?: unknown;
	/** Row identity. Defaults to `row.id`. */
	key?: (row: T) => RowKey;
	/**
	 * Stable persistence key. Defaults to collection + canonicalized params.
	 * Set explicitly when params are not JSON-serializable.
	 */
	localKey?: string;
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
	/** Reconnect immediately, refreshing any runtime-provided socket ticket. */
	reconnect: () => void;
	/** Close the socket and every handle. */
	close: () => void;
};

type PendingMutation = {
	mutationId: number;
	operationId?: string;
	ownerKey: string;
	name: string;
	args: unknown;
	optimistic?: (draft: OptimisticDraft<unknown>) => void;
	optimisticOperations: LocalOptimisticOperation[];
	inverse: LocalOptimisticOperation[];
	resolve?: (result: unknown) => void;
	reject?: (error: unknown) => void;
};

type Entry = {
	id: string;
	localKey: string;
	collection: string;
	params: unknown;
	key: (row: unknown) => RowKey;
	confirmed: Map<RowKey, unknown>;
	state: SyncCollectionState<unknown>;
	listeners: Set<(state: SyncCollectionState<unknown>) => void>;
	appliedVersion: number;
	/**
	 * Most-recent cross-instance resume cursor for this entry (1.18.0+).
	 * Captured from every snapshot/diff/frame that carries a `cursor` field
	 * and round-tripped on reconnect as `since`. Falls back to
	 * `appliedVersion` when the server doesn't surface a cursor (pre-1.17
	 * server, or a single-instance setup before a cluster bus connects).
	 */
	cursor: string | undefined;
	closed: boolean;
	hydrated: Promise<void>;
};

const canonicalJson = (value: unknown): string => {
	const seen = new Set<object>();
	const normalize = (input: unknown): unknown => {
		if (input === undefined) return null;
		if (
			input === null ||
			typeof input === 'string' ||
			typeof input === 'boolean'
		) {
			return input;
		}
		if (typeof input === 'number') {
			if (!Number.isFinite(input)) {
				throw new Error(
					'Sync collection params must contain finite numbers; provide an explicit localKey'
				);
			}
			return input;
		}
		if (typeof input !== 'object') {
			throw new Error(
				'Sync collection params must be JSON-serializable; provide an explicit localKey'
			);
		}
		if (seen.has(input)) {
			throw new Error(
				'Sync collection params contain a cycle; provide an explicit localKey'
			);
		}
		seen.add(input);
		try {
			if (Array.isArray(input)) return input.map(normalize);
			const prototype = Object.getPrototypeOf(input);
			if (prototype !== Object.prototype && prototype !== null) {
				throw new Error(
					'Sync collection params must use plain objects; provide an explicit localKey'
				);
			}
			return Object.fromEntries(
				Object.entries(input as Record<string, unknown>)
					.sort(([a], [b]) => a.localeCompare(b))
					.map(([key, item]) => [key, normalize(item)])
			);
		} finally {
			seen.delete(input);
		}
	};
	return JSON.stringify(normalize(value));
};

const defaultLocalKey = (collection: string, params: unknown): string =>
	params === undefined
		? collection
		: `${collection}:${canonicalJson(params)}`;

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
	const runtime = getSyncClientRuntimeTransport();
	const socketTicket = options.socketTicket ?? runtime?.socketTicket;
	const durable = options.durable ?? runtime?.durable;
	const reconnectMs = options.reconnectMs ?? 500;
	const maxReconnectMs = options.maxReconnectMs ?? 10_000;
	const serializer: FrameSerializer = options.serializer ?? jsonSerializer;
	const Impl = options.webSocketImpl ?? globalThis.WebSocket;
	if (!Impl) {
		throw new Error(
			'createSyncClient requires WebSocket. Run in a browser or pass webSocketImpl.'
		);
	}

	const entries = new Map<string, Entry>();
	const pending: PendingMutation[] = [];
	const mutationOwner = new Map<number, PendingMutation>();
	let nextEntryId = 0;
	let mutationSeq = 0;
	let installationId: string | undefined;

	let socket: WebSocket | undefined;
	let connected = false;
	let closed = false;
	const reconnectBackoff = createReconnectBackoff(
		reconnectMs,
		maxReconnectMs
	);
	let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	let immediateReconnect = false;

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
		for (const mutation of pending) {
			if (mutation.ownerKey === entry.localKey) {
				mutation.optimistic?.(draft);
			}
			for (const operation of mutation.optimisticOperations) {
				if (operation.collection !== entry.localKey) continue;
				if (operation.type === 'delete') draft.delete(operation.key);
				else draft.set(operation.row);
			}
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
		const mutation = mutationOwner.get(mutationId);
		mutationOwner.delete(mutationId);
		if (mutation === undefined) {
			return undefined;
		}
		const index = pending.findIndex((item) => item === mutation);
		if (index === -1) {
			return undefined;
		}
		pending.splice(index, 1);
		return mutation;
	};

	const reportStorageError = (error: unknown) => {
		durable?.onError?.(error);
		options.onError?.(error);
	};
	let localWriteTail: Promise<void> = Promise.resolve();
	const queueLocalWrite = <R>(run: () => Promise<R>): Promise<R> => {
		const next = localWriteTail.then(run);
		localWriteTail = next.then(
			() => undefined,
			(error) => reportStorageError(error)
		);
		return next;
	};

	const persistEntries = (affected: Iterable<Entry>) => {
		if (durable === undefined) return;
		const snapshots = new Map<
			string,
			{ rows: unknown[]; version: number; cursor?: string }
		>();
		for (const entry of affected) {
			snapshots.set(entry.localKey, {
				rows: [...entry.confirmed.values()],
				version: entry.appliedVersion,
				cursor: entry.cursor
			});
		}
		void queueLocalWrite(() =>
			durable.store.transaction(
				durable.namespace,
				'readwrite',
				async (tx) => {
					for (const [key, record] of snapshots) {
						await tx.putCollection(key, record);
					}
				}
			)
		).catch(() => {});
	};

	const recomputeMutationEntries = (mutation: PendingMutation) => {
		const keys = new Set([
			mutation.ownerKey,
			...mutation.optimisticOperations.map(
				(operation) => operation.collection
			)
		]);
		for (const entry of entries.values()) {
			if (keys.has(entry.localKey)) recompute(entry);
		}
	};

	const finishPending = (
		mutationId: number,
		result: unknown,
		error?: Error
	) => {
		const mutation = settlePending(mutationId);
		if (mutation === undefined) return;
		recomputeMutationEntries(mutation);
		if (error === undefined) mutation.resolve?.(result);
		else mutation.reject?.(error);
	};

	const finishDurablePending = (
		mutationId: number,
		echoedOperationId: string | undefined,
		result: unknown,
		error?: Error
	) => {
		const mutation = mutationOwner.get(mutationId);
		if (mutation?.operationId === undefined || durable === undefined) {
			finishPending(mutationId, result, error);
			return;
		}
		if (echoedOperationId !== mutation.operationId) {
			reportStorageError(
				new Error(
					`Durable mutation acknowledgment identity mismatch for ${mutation.operationId}`
				)
			);
			return;
		}
		void queueLocalWrite(() =>
			durable.store.transaction(durable.namespace, 'readwrite', (tx) =>
				tx.deleteMutation(mutation.operationId!)
			)
		)
			.catch(() => {})
			.finally(() => finishPending(mutationId, result, error));
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
			if (frame.cursor !== undefined) {
				entry.cursor = frame.cursor;
			}
			persistEntries([entry]);
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
			if (frame.cursor !== undefined) {
				entry.cursor = frame.cursor;
			}
			persistEntries([entry]);
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
				if (frame.cursor !== undefined) {
					entry.cursor = frame.cursor;
				}
				// Update state now, but defer notifying until every collection in
				// the frame is updated — so no listener observes a partial batch.
				rebuild(entry);
				affected.add(entry);
			}
			persistEntries(affected);
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
			finishDurablePending(
				frame.mutationId,
				frame.operationId,
				frame.result
			);
		} else if (frame.type === 'reject') {
			finishDurablePending(
				frame.mutationId,
				frame.operationId,
				undefined,
				new Error(String(frame.message))
			);
		}
	};

	const wsSend = (payload: string | ArrayBufferLike | Uint8Array) => {
		// Native WebSocket.send accepts string | ArrayBufferLike | Blob | ArrayBufferView.
		socket?.send(payload as string);
	};

	const sendSubscribe = (entry: Entry) => {
		// 1.18.0: prefer the opaque cursor (cross-instance resume) when
		// the server has surfaced one; fall back to the numeric appliedVersion
		// for pre-1.17 servers or before any cursor has arrived.
		const since: number | string | undefined =
			entry.cursor ??
			(entry.appliedVersion > 0 ? entry.appliedVersion : undefined);
		wsSend(
			serializer.encodeClient({
				type: 'subscribe',
				id: entry.id,
				collection: entry.collection,
				params: entry.params,
				since
			})
		);
	};

	const sendMutate = (mutation: PendingMutation) => {
		if (!connected) return;
		const payload = serializer.encodeClient({
			type: 'mutate',
			mutationId: mutation.mutationId,
			operationId: mutation.operationId,
			name: mutation.name,
			args: mutation.args
		});
		if (durable === undefined || mutation.operationId === undefined) {
			wsSend(payload);
			return;
		}
		const targetSocket = socket;
		void queueLocalWrite(() =>
			durable.store.transaction(
				durable.namespace,
				'readwrite',
				async (tx) => {
					const record = await tx.getMutation(mutation.operationId!);
					if (record !== undefined) {
						await tx.putMutation({
							...record,
							attempts: record.attempts + 1
						});
					}
				}
			)
		)
			.catch(() => {})
			.finally(() => {
				if (
					connected &&
					socket === targetSocket &&
					pending.includes(mutation)
				) {
					targetSocket?.send(payload as string);
				}
			});
	};

	const initializeDurable = async () => {
		if (durable === undefined) return;
		installationId = await ensureSyncInstallationId(
			durable.store,
			durable.namespace,
			durable.createId
		);
		const records = await durable.store.transaction(
			durable.namespace,
			'readonly',
			(tx) => tx.listMutations()
		);
		for (const record of records) {
			mutationSeq += 1;
			const mutation: PendingMutation = {
				mutationId: mutationSeq,
				operationId: record.operationId,
				ownerKey:
					record.owner ?? record.optimistic[0]?.collection ?? '',
				name: record.name,
				args: record.args,
				optimisticOperations: record.optimistic,
				inverse: record.inverse
			};
			pending.push(mutation);
			mutationOwner.set(mutation.mutationId, mutation);
		}
	};

	const durableReady =
		durable === undefined
			? Promise.resolve()
			: initializeDurable().catch((error) => {
					reportStorageError(error);
					throw error;
				});

	const hydrateEntry = async (entry: Entry) => {
		if (durable === undefined) return;
		await durableReady;
		const record = await durable.store.transaction(
			durable.namespace,
			'readonly',
			(tx) => tx.getCollection(entry.localKey)
		);
		if (entry.closed || record === undefined) {
			if (!entry.closed) recompute(entry);
			return;
		}
		entry.confirmed.clear();
		for (const row of record.rows) {
			entry.confirmed.set(entry.key(row), row);
		}
		entry.appliedVersion = record.version;
		entry.cursor = record.cursor;
		recompute(entry);
	};

	const connect = () => {
		if (closed) {
			return;
		}
		reconnectTimer = undefined;
		const ws = new Impl(options.url);
		socket = ws;
		const sendInitialFrames = () => {
			if (socket !== ws || closed) return;
			if (socketTicket) {
				void socketTicket()
					.then((ticket) => {
						if (socket !== ws || closed) return;
						wsSend(
							serializer.encodeClient({
								type: 'authenticate',
								ticket
							})
						);
						connected = true;
						for (const entry of entries.values())
							sendSubscribe(entry);
						for (const mutation of pending) sendMutate(mutation);
					})
					.catch((error) => {
						options.onError?.(error);
						ws.close();
					});
				return;
			}
			connected = true;
			for (const entry of entries.values()) {
				sendSubscribe(entry);
			}
			for (const mutation of pending) sendMutate(mutation);
		};
		ws.onopen = () => {
			if (durable === undefined) {
				sendInitialFrames();
				return;
			}
			void Promise.all(
				[...entries.values()].map((entry) => entry.hydrated)
			)
				.then(sendInitialFrames)
				.catch((error) => {
					reportStorageError(error);
					ws.close();
				});
		};
		ws.onmessage = (event) => {
			try {
				const decoded = serializer.decode(event.data);
				if (decoded !== null && typeof decoded === 'object') {
					applyFrame(decoded as ServerFrame);
					if (
						hasReconnectHealthyFrameType(decoded, serverFrameTypes)
					) {
						reconnectBackoff.markHealthy();
					}
				}
			} catch {
				// ignore unparseable frames
			}
		};
		ws.onclose = () => {
			if (socket === ws) socket = undefined;
			connected = false;
			if (closed) {
				return;
			}
			if (immediateReconnect) {
				immediateReconnect = false;
				connect();
				return;
			}
			if (reconnectMs <= 0) return;
			const delay = reconnectBackoff.nextDelay();
			reconnectTimer = setTimeout(connect, delay);
		};
	};

	if (durable === undefined) connect();
	else void durableReady.then(connect).catch(() => {});

	const normalizeOptimisticOperations = <T>(
		ownerKey: string,
		operations: SerializableOptimisticOperation<T>[] | undefined
	): LocalOptimisticOperation[] =>
		(operations ?? []).map((operation) =>
			operation.type === 'delete'
				? {
						type: 'delete',
						collection: operation.collection ?? ownerKey,
						key: operation.key
					}
				: {
						type: operation.type,
						collection: operation.collection ?? ownerKey,
						row: operation.row
					}
		);

	const captureInverse = (
		operations: LocalOptimisticOperation[]
	): LocalOptimisticOperation[] => {
		const working = new Map<string, Map<RowKey, unknown>>();
		const entryFor = (localKey: string) => {
			const entry = [...entries.values()].find(
				(item) => item.localKey === localKey
			);
			if (entry === undefined) {
				throw new Error(
					`Durable optimistic operation targets unopened collection "${localKey}"`
				);
			}
			let rows = working.get(localKey);
			if (rows === undefined) {
				rows = new Map(
					entry.state.data.map((row) => [entry.key(row), row])
				);
				working.set(localKey, rows);
			}
			return { entry, rows };
		};
		const inverse: LocalOptimisticOperation[] = [];
		for (const operation of operations) {
			const { entry, rows } = entryFor(operation.collection);
			if (operation.type === 'delete') {
				const existing = rows.get(operation.key);
				if (existing !== undefined) {
					inverse.unshift({
						type: 'insert',
						collection: operation.collection,
						row: existing
					});
				}
				rows.delete(operation.key);
				continue;
			}
			const rowKey = entry.key(operation.row);
			const existing = rows.get(rowKey);
			inverse.unshift(
				existing === undefined
					? {
							type: 'delete',
							collection: operation.collection,
							key: rowKey
						}
					: {
							type: 'update',
							collection: operation.collection,
							row: existing
						}
			);
			rows.set(rowKey, operation.row);
		}
		return inverse;
	};

	const collection = <T>(
		handleOptions: SyncCollectionHandleOptions<T>
	): SyncCollectionHandle<T> => {
		const entryId = `c${nextEntryId}`;
		nextEntryId += 1;
		const localKey =
			handleOptions.localKey ??
			defaultLocalKey(handleOptions.collection, handleOptions.params);
		if (localKey.length === 0) {
			throw new Error('Sync collection localKey must not be empty');
		}
		const entry: Entry = {
			id: entryId,
			localKey,
			collection: handleOptions.collection,
			params: handleOptions.params,
			key:
				(handleOptions.key as ((row: unknown) => RowKey) | undefined) ??
				((row: unknown) => (row as { id: RowKey }).id),
			confirmed: new Map(),
			state: { data: [], status: 'connecting', error: undefined },
			listeners: new Set(),
			appliedVersion: 0,
			cursor: undefined,
			closed: false,
			hydrated: Promise.resolve()
		};
		entries.set(entryId, entry);
		entry.hydrated = hydrateEntry(entry);
		if (connected)
			void entry.hydrated
				.then(() => sendSubscribe(entry))
				.catch(reportStorageError);

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
					const addPending = (
						operationId: string | undefined,
						optimisticOperations: LocalOptimisticOperation[],
						inverse: LocalOptimisticOperation[]
					) => {
						mutationSeq += 1;
						const mutation: PendingMutation = {
							mutationId: mutationSeq,
							operationId,
							ownerKey: entry.localKey,
							name: mutateOptions.name,
							args: mutateOptions.args,
							optimistic: mutateOptions.optimistic as
								| ((draft: OptimisticDraft<unknown>) => void)
								| undefined,
							optimisticOperations,
							inverse,
							resolve: (result) => resolve(result as R),
							reject
						};
						pending.push(mutation);
						mutationOwner.set(mutation.mutationId, mutation);
						recomputeMutationEntries(mutation);
						sendMutate(mutation);
					};

					if (durable === undefined) {
						const optimisticOperations =
							normalizeOptimisticOperations(
								entry.localKey,
								mutateOptions.optimisticOperations
							);
						const inverse = captureInverse(optimisticOperations);
						addPending(undefined, optimisticOperations, inverse);
						return;
					}

					void (async () => {
						await entry.hydrated;
						const optimisticOperations =
							normalizeOptimisticOperations(
								entry.localKey,
								mutateOptions.optimisticOperations
							);
						const inverse = captureInverse(optimisticOperations);
						await durableReady;
						const operationId = createSyncOperationId(
							installationId!,
							durable.createId
						);
						const record: LocalMutationRecord = {
							operationId,
							owner: entry.localKey,
							name: mutateOptions.name,
							args: mutateOptions.args,
							optimistic: optimisticOperations,
							inverse,
							createdAt: Date.now(),
							attempts: 0
						};
						await queueLocalWrite(() =>
							durable.store.transaction(
								durable.namespace,
								'readwrite',
								(tx) => tx.putMutation(record)
							)
						);
						addPending(operationId, optimisticOperations, inverse);
					})().catch(reject);
				}),
			close: () => {
				if (entry.closed) {
					return;
				}
				entry.closed = true;
				entries.delete(entryId);
				if (connected) {
					wsSend(
						serializer.encodeClient({
							type: 'unsubscribe',
							id: entryId
						})
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
		pending.length = 0;
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

	const reconnect = () => {
		if (closed) return;
		if (reconnectTimer !== undefined) {
			clearTimeout(reconnectTimer);
			reconnectTimer = undefined;
		}
		const current = socket;
		if (current && current.readyState !== current.CLOSED) {
			immediateReconnect = true;
			current.close();
			return;
		}
		socket = undefined;
		connect();
	};

	return { collection, close, disconnect, reconnect };
};

export type { SyncCollectionState, SyncCollectionStatus };
