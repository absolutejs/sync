import type { RowKey } from '../engine/types';
import type { SyncMutationRejection } from '../reconciliation';

/** Serializable optimistic effect that can be replayed after process death. */
export type LocalOptimisticOperation =
	| { type: 'insert' | 'update'; collection: string; row: unknown }
	| { type: 'delete'; collection: string; key: RowKey };

/** Durable outbound operation. Functions are deliberately excluded. */
export type LocalMutationRecord = {
	/** Stable for the lifetime of this operation, including every retry. */
	operationId: string;
	/** Local collection key whose handle initiated the mutation. */
	owner?: string;
	name: string;
	args: unknown;
	optimistic: LocalOptimisticOperation[];
	/** Inverse effects captured before optimism, used for deterministic rollback. */
	inverse: LocalOptimisticOperation[];
	createdAt: number;
	attempts: number;
	lastError?: string;
	/** Missing means pending for compatibility with records written before 2.22. */
	state?: 'dead-letter' | 'pending';
	/** Earliest wall-clock time at which an automatic retry may be sent. */
	nextAttemptAt?: number;
	/** Typed server outcome retained for remediation and diagnostics. */
	rejection?: SyncMutationRejection;
	deadLetteredAt?: number;
};

/** Server-authoritative collection state saved for offline reads and resume. */
export type LocalCollectionRecord<T = unknown> = {
	rows: T[];
	version: number;
	/** Opaque cross-instance cursor; round-trip without inspecting it. */
	cursor?: string;
};

export type SyncLocalStoreMode = 'readonly' | 'readwrite';

/**
 * One atomic view of an account/tenant namespace.
 *
 * Adapters must make every method participate in the enclosing transaction.
 * A callback throw aborts all writes, including collection state and queue
 * changes, so a crash cannot leave the two halves out of sync.
 */
export type SyncLocalTransaction = {
	getInstallationId: () => Promise<string | undefined>;
	setInstallationId: (installationId: string) => Promise<void>;
	getCollection: <T = unknown>(
		key: string
	) => Promise<LocalCollectionRecord<T> | undefined>;
	putCollection: <T = unknown>(
		key: string,
		record: LocalCollectionRecord<T>
	) => Promise<void>;
	deleteCollection: (key: string) => Promise<void>;
	listMutations: () => Promise<LocalMutationRecord[]>;
	getMutation: (
		operationId: string
	) => Promise<LocalMutationRecord | undefined>;
	putMutation: (record: LocalMutationRecord) => Promise<void>;
	deleteMutation: (operationId: string) => Promise<void>;
};

/**
 * Durable, transactional local state partitioned by authenticated principal.
 * Implementations back this with IndexedDB on web/PWA and SQLite on native.
 */
export type SyncLocalStore = {
	transaction: <R>(
		namespace: string,
		mode: SyncLocalStoreMode,
		run: (tx: SyncLocalTransaction) => Promise<R>
	) => Promise<R>;
	/** Delete one signed-out principal without affecting any other account. */
	deleteNamespace: (namespace: string) => Promise<void>;
};

export type IndexedDbSyncLocalStoreOptions = {
	/** Defaults to `absolutejs-sync-local-v1`. */
	databaseName?: string;
	/** Override for tests or non-window runtimes. Defaults to global IndexedDB. */
	indexedDB?: IDBFactory;
};

const clone = <T>(value: T): T => structuredClone(value);

type MemoryNamespace = {
	installationId?: string;
	collections: Map<string, LocalCollectionRecord>;
	mutations: Map<string, LocalMutationRecord>;
};

const emptyNamespace = (): MemoryNamespace => ({
	collections: new Map(),
	mutations: new Map()
});

const cloneNamespace = (source: MemoryNamespace): MemoryNamespace => ({
	installationId: source.installationId,
	collections: new Map(
		[...source.collections].map(([key, value]) => [key, clone(value)])
	),
	mutations: new Map(
		[...source.mutations].map(([key, value]) => [key, clone(value)])
	)
});

/**
 * In-memory reference adapter. Useful for SSR, tests, and as the executable
 * conformance model for durable adapters. Transactions are serialized and
 * roll back on throw.
 */
export const createMemorySyncLocalStore = (): SyncLocalStore => {
	const namespaces = new Map<string, MemoryNamespace>();
	let tail = Promise.resolve();
	const withLock = async <R>(run: () => Promise<R>): Promise<R> => {
		let release!: () => void;
		const previous = tail;
		tail = new Promise<void>((resolve) => {
			release = resolve;
		});
		await previous;
		try {
			return await run();
		} finally {
			release();
		}
	};

	const transaction: SyncLocalStore['transaction'] = async (
		namespace,
		mode,
		run
	) => {
		if (namespace.length === 0) {
			throw new Error('Sync local-store namespace must not be empty');
		}
		return withLock(async () => {
			const current = namespaces.get(namespace) ?? emptyNamespace();
			const working = cloneNamespace(current);
			const writable = () => {
				if (mode !== 'readwrite') {
					throw new Error(
						'Cannot write in a readonly Sync local transaction'
					);
				}
			};
			const tx: SyncLocalTransaction = {
				getInstallationId: async () => working.installationId,
				setInstallationId: async (installationId) => {
					writable();
					if (installationId.length === 0) {
						throw new Error(
							'Sync installation id must not be empty'
						);
					}
					working.installationId = installationId;
				},
				getCollection: async <T>(key: string) => {
					const record = working.collections.get(key);
					return record === undefined
						? undefined
						: clone(record as LocalCollectionRecord<T>);
				},
				putCollection: async (key, record) => {
					writable();
					working.collections.set(key, clone(record));
				},
				deleteCollection: async (key) => {
					writable();
					working.collections.delete(key);
				},
				listMutations: async () =>
					[...working.mutations.values()]
						.sort((a, b) => a.createdAt - b.createdAt)
						.map(clone),
				getMutation: async (operationId) => {
					const record = working.mutations.get(operationId);
					return record === undefined ? undefined : clone(record);
				},
				putMutation: async (record) => {
					writable();
					working.mutations.set(record.operationId, clone(record));
				},
				deleteMutation: async (operationId) => {
					writable();
					working.mutations.delete(operationId);
				}
			};
			const result = await run(tx);
			if (mode === 'readwrite') namespaces.set(namespace, working);
			return result;
		});
	};

	return {
		transaction,
		deleteNamespace: async (namespace) => {
			await withLock(async () => {
				namespaces.delete(namespace);
				return undefined;
			});
		}
	};
};

type IndexedCollectionRow = LocalCollectionRecord & {
	namespace: string;
	key: string;
};

type IndexedMutationRow = LocalMutationRecord & {
	namespace: string;
};

const requestResult = <T>(request: IDBRequest<T>): Promise<T> =>
	new Promise((resolve, reject) => {
		request.onsuccess = () => resolve(request.result);
		request.onerror = () => reject(request.error);
	});

const transactionComplete = (transaction: IDBTransaction): Promise<void> =>
	new Promise((resolve, reject) => {
		transaction.oncomplete = () => resolve();
		transaction.onabort = () =>
			reject(
				transaction.error ?? new Error('IndexedDB transaction aborted')
			);
		transaction.onerror = () =>
			reject(
				transaction.error ?? new Error('IndexedDB transaction failed')
			);
	});

const deleteIndexRows = (index: IDBIndex, namespace: string): Promise<void> =>
	new Promise((resolve, reject) => {
		const request = index.openCursor(namespace);
		request.onerror = () => reject(request.error);
		request.onsuccess = () => {
			const cursor = request.result;
			if (cursor === null) {
				resolve();
				return;
			}
			cursor.delete();
			cursor.continue();
		};
	});

/**
 * Browser/PWA implementation of {@link SyncLocalStore}. Confirmed collection
 * state, cursors, installation identity, and the mutation outbox share one
 * IndexedDB transaction, including multi-collection frame commits.
 */
export const createIndexedDbSyncLocalStore = ({
	databaseName = 'absolutejs-sync-local-v1',
	indexedDB: factory = globalThis.indexedDB
}: IndexedDbSyncLocalStoreOptions = {}): SyncLocalStore => {
	if (factory === undefined) {
		throw new Error(
			'createIndexedDbSyncLocalStore requires IndexedDB in this runtime'
		);
	}

	let databasePromise: Promise<IDBDatabase> | undefined;
	const database = () => {
		databasePromise ??= new Promise<IDBDatabase>((resolve, reject) => {
			const request = factory.open(databaseName, 1);
			request.onupgradeneeded = () => {
				const db = request.result;
				db.createObjectStore('metadata');
				const collections = db.createObjectStore('collections', {
					keyPath: ['namespace', 'key']
				});
				collections.createIndex('namespace', 'namespace');
				const mutations = db.createObjectStore('mutations', {
					keyPath: ['namespace', 'operationId']
				});
				mutations.createIndex('namespace', 'namespace');
			};
			request.onsuccess = () => {
				request.result.onversionchange = () => request.result.close();
				resolve(request.result);
			};
			request.onerror = () => reject(request.error);
			request.onblocked = () =>
				reject(
					new Error(`IndexedDB upgrade blocked for "${databaseName}"`)
				);
		});
		return databasePromise;
	};

	const transaction: SyncLocalStore['transaction'] = async (
		namespace,
		mode,
		run
	) => {
		if (namespace.length === 0) {
			throw new Error('Sync local-store namespace must not be empty');
		}
		const db = await database();
		const native = db.transaction(
			['metadata', 'collections', 'mutations'],
			mode
		);
		const completed = transactionComplete(native);
		const writable = () => {
			if (mode !== 'readwrite') {
				throw new Error(
					'Cannot write in a readonly Sync local transaction'
				);
			}
		};
		const metadata = native.objectStore('metadata');
		const collections = native.objectStore('collections');
		const mutations = native.objectStore('mutations');
		const tx: SyncLocalTransaction = {
			getInstallationId: () =>
				requestResult<string | undefined>(metadata.get(namespace)),
			setInstallationId: async (installationId) => {
				writable();
				if (installationId.length === 0) {
					throw new Error('Sync installation id must not be empty');
				}
				await requestResult(metadata.put(installationId, namespace));
			},
			getCollection: async <T>(key: string) => {
				const row = await requestResult<
					IndexedCollectionRow | undefined
				>(collections.get([namespace, key]));
				if (row === undefined) return undefined;
				const { namespace: _namespace, key: _key, ...record } = row;
				return record as LocalCollectionRecord<T>;
			},
			putCollection: async (key, record) => {
				writable();
				await requestResult(
					collections.put({ ...record, key, namespace })
				);
			},
			deleteCollection: async (key) => {
				writable();
				await requestResult(collections.delete([namespace, key]));
			},
			listMutations: async () => {
				const rows = await requestResult<IndexedMutationRow[]>(
					mutations.index('namespace').getAll(namespace)
				);
				return rows
					.map(({ namespace: _namespace, ...record }) => record)
					.sort((a, b) => a.createdAt - b.createdAt);
			},
			getMutation: async (operationId) => {
				const row = await requestResult<IndexedMutationRow | undefined>(
					mutations.get([namespace, operationId])
				);
				if (row === undefined) return undefined;
				const { namespace: _namespace, ...record } = row;
				return record;
			},
			putMutation: async (record) => {
				writable();
				await requestResult(mutations.put({ ...record, namespace }));
			},
			deleteMutation: async (operationId) => {
				writable();
				await requestResult(mutations.delete([namespace, operationId]));
			}
		};

		try {
			const result = await run(tx);
			await completed;
			return result;
		} catch (error) {
			try {
				native.abort();
			} catch {
				// The transaction may already have aborted because a request failed.
			}
			await completed.catch(() => {});
			throw error;
		}
	};

	return {
		transaction,
		deleteNamespace: async (namespace) => {
			const db = await database();
			const native = db.transaction(
				['metadata', 'collections', 'mutations'],
				'readwrite'
			);
			const completed = transactionComplete(native);
			await requestResult(
				native.objectStore('metadata').delete(namespace)
			);
			await deleteIndexRows(
				native.objectStore('collections').index('namespace'),
				namespace
			);
			await deleteIndexRows(
				native.objectStore('mutations').index('namespace'),
				namespace
			);
			await completed;
		}
	};
};

const randomId = (): string => {
	const id = globalThis.crypto?.randomUUID?.();
	if (id === undefined) {
		throw new Error(
			'Sync operation ids require crypto.randomUUID or a createId callback'
		);
	}
	return id;
};

/** Get or create the stable installation id used to prefix operation ids. */
export const ensureSyncInstallationId = (
	store: SyncLocalStore,
	namespace: string,
	createId: () => string = randomId
): Promise<string> =>
	store.transaction(namespace, 'readwrite', async (tx) => {
		const existing = await tx.getInstallationId();
		if (existing !== undefined) return existing;
		const installationId = createId();
		await tx.setInstallationId(installationId);
		return installationId;
	});

/** Create a globally unique id grouped under a stable installation identity. */
export const createSyncOperationId = (
	installationId: string,
	createId: () => string = randomId
): string => `${installationId}:${createId()}`;
