import type { RowKey } from '../engine/types';

/** Serializable optimistic effect that can be replayed after process death. */
export type LocalOptimisticOperation =
	| { type: 'insert' | 'update'; collection: string; row: unknown }
	| { type: 'delete'; collection: string; key: RowKey };

/** Durable outbound operation. Functions are deliberately excluded. */
export type LocalMutationRecord = {
	/** Stable for the lifetime of this operation, including every retry. */
	operationId: string;
	name: string;
	args: unknown;
	optimistic: LocalOptimisticOperation[];
	/** Inverse effects captured before optimism, used for deterministic rollback. */
	inverse: LocalOptimisticOperation[];
	createdAt: number;
	attempts: number;
	lastError?: string;
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
