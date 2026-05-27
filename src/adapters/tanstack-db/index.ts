import type {
	CollectionConfig,
	DeleteMutationFn,
	InsertMutationFn,
	PendingMutation,
	UpdateMutationFn
} from '@tanstack/db';
import {
	createSyncCollection,
	type CollectionCache,
	type MutationStorage,
	type SyncCollection,
	type SyncCollectionOptions
} from '../../client/syncCollection';
import type { RowKey } from '../../engine/types';

export type TanStackRowKey = Extract<RowKey, string | number>;

export type TanStackMutationCall = {
	name: string;
	args?: unknown;
};

export type TanStackMutationMapper<
	T extends object,
	TOperation extends 'insert' | 'update' | 'delete'
> =
	| string
	| ((
			mutation: PendingMutation<T, TOperation>
	  ) => TanStackMutationCall | undefined);

export type SyncTanStackMutations<T extends object> = {
	insert?: TanStackMutationMapper<T, 'insert'>;
	update?: TanStackMutationMapper<T, 'update'>;
	delete?: TanStackMutationMapper<T, 'delete'>;
};

export type SyncTanStackCollectionOptions<
	T extends object,
	TKey extends TanStackRowKey = TanStackRowKey
> = Omit<
	CollectionConfig<T, TKey>,
	'sync' | 'getKey' | 'onInsert' | 'onUpdate' | 'onDelete'
> & {
	/** WebSocket URL of the Absolute Sync endpoint. */
	url: string;
	/** Registered Absolute Sync collection name. */
	collection: string;
	/** Query params forwarded to the server collection hydrate/match/authorize hooks. */
	params?: unknown;
	/** Row identity shared by TanStack DB and Absolute Sync. */
	getKey: (row: T) => TKey;
	webSocketImpl?: typeof WebSocket;
	reconnectMs?: number;
	maxReconnectMs?: number;
	storage?: MutationStorage;
	cache?: CollectionCache<T>;
	onError?: (error: unknown) => void;
	/**
	 * Optional mapping from TanStack mutations to registered Absolute Sync
	 * mutation names. TanStack already applies optimistic writes, so forwarded
	 * sync mutations intentionally do not add another optimistic overlay.
	 */
	mutations?: SyncTanStackMutations<T>;
	/** Optional prebuilt Absolute Sync collection, useful when sharing lifecycle externally. */
	syncCollection?: SyncCollection<T>;
};

const toMutationCall = <
	T extends object,
	TOperation extends 'insert' | 'update' | 'delete'
>(
	mapper: TanStackMutationMapper<T, TOperation>,
	mutation: PendingMutation<T, TOperation>
): TanStackMutationCall | undefined => {
	if (typeof mapper === 'function') {
		return mapper(mutation);
	}
	if (mutation.type === 'insert') {
		return {
			name: mapper,
			args: { row: mutation.modified, metadata: mutation.metadata }
		};
	}
	if (mutation.type === 'update') {
		return {
			name: mapper,
			args: {
				key: mutation.key,
				row: mutation.modified,
				changes: mutation.changes,
				metadata: mutation.metadata
			}
		};
	}
	return {
		name: mapper,
		args: {
			key: mutation.key,
			row: mutation.original,
			metadata: mutation.metadata
		}
	};
};

const createMutationHandler =
	<T extends object, TOperation extends 'insert' | 'update' | 'delete'>(
		sync: SyncCollection<T>,
		mapper: TanStackMutationMapper<T, TOperation> | undefined
	) =>
	async ({
		transaction
	}: {
		transaction: {
			mutations: [
				PendingMutation<T, TOperation>,
				...PendingMutation<T, TOperation>[]
			];
		};
	}) => {
		if (mapper === undefined) {
			return;
		}
		await Promise.all(
			transaction.mutations.map((mutation) => {
				const call = toMutationCall(mapper, mutation);
				return call === undefined
					? Promise.resolve()
					: sync.mutate({ name: call.name, args: call.args });
			})
		);
	};

const createSyncConfig = <T extends object, TKey extends TanStackRowKey>(
	sync: SyncCollection<T>,
	getKey: (row: T) => TKey
): CollectionConfig<T, TKey>['sync'] => ({
	rowUpdateMode: 'full',
	sync: ({ begin, write, commit, markReady }) => {
		let previous = new Map<TKey, T>();
		let markedReady = false;

		const flush = () => {
			const state = sync.get();
			const next = new Map<TKey, T>();
			for (const row of state.data) {
				next.set(getKey(row), row);
			}

			begin();
			for (const [key, row] of next) {
				const old = previous.get(key);
				if (old === undefined) {
					write({ type: 'insert', value: row });
				} else if (!Object.is(old, row)) {
					write({ type: 'update', value: row, previousValue: old });
				}
			}
			for (const key of previous.keys()) {
				if (!next.has(key)) {
					write({ type: 'delete', key });
				}
			}
			previous = next;
			commit();

			if (state.status === 'ready' && !markedReady) {
				markedReady = true;
				markReady();
			}
		};

		flush();
		const unsubscribe = sync.subscribe(flush);
		return () => {
			unsubscribe();
			sync.close();
		};
	}
});

export const createSyncTanStackCollectionOptions = <
	T extends object,
	TKey extends TanStackRowKey = TanStackRowKey
>(
	options: SyncTanStackCollectionOptions<T, TKey>
): CollectionConfig<T, TKey> => {
	const {
		url,
		collection,
		params,
		getKey,
		webSocketImpl,
		reconnectMs,
		maxReconnectMs,
		storage,
		cache,
		onError,
		mutations,
		syncCollection,
		...collectionOptions
	} = options;

	const sync =
		syncCollection ??
		createSyncCollection<T>({
			url,
			collection,
			params,
			key: getKey as SyncCollectionOptions<T>['key'],
			webSocketImpl,
			reconnectMs,
			maxReconnectMs,
			storage,
			cache,
			onError
		});

	return {
		...collectionOptions,
		getKey,
		sync: createSyncConfig(sync, getKey),
		onInsert: createMutationHandler(
			sync,
			mutations?.insert
		) as InsertMutationFn<T, TKey>,
		onUpdate: createMutationHandler(
			sync,
			mutations?.update
		) as UpdateMutationFn<T, TKey>,
		onDelete: createMutationHandler(
			sync,
			mutations?.delete
		) as DeleteMutationFn<T, TKey>
	};
};

export { createSyncTanStackCollectionOptions as syncTanStackCollectionOptions };
