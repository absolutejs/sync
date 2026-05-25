export type { ReactiveEvent } from '../reactiveHub';

export { createSyncSubscriber } from './subscriber';
export type { SyncSubscriber, SyncSubscriberOptions } from './subscriber';

export { createLiveQuery, jsonFetcher } from './liveQuery';
export type { LiveQuery, LiveQueryOptions, LiveQueryState } from './liveQuery';

export {
	createSyncCollection,
	indexedDbCollectionCache,
	localStorageCollectionCache,
	localStorageMutationStorage
} from './syncCollection';
export type {
	CollectionCache,
	CollectionCacheSnapshot,
	MutateOptions,
	MutationStorage,
	OptimisticDraft,
	PendingMutationRecord,
	SyncCollection,
	SyncCollectionOptions,
	SyncCollectionState,
	SyncCollectionStatus
} from './syncCollection';

export { createCollaborativeText } from './collaborativeText';
export type {
	CollaborativeText,
	CollaborativeTextOptions,
	CollaborativeTextState
} from './collaborativeText';

export { createPresence } from './presence';
export type {
	PresenceClient,
	PresenceClientOptions,
	PresenceMember
} from './presence';

export { createSyncClient } from './syncClient';
export type {
	SyncClient,
	SyncClientOptions,
	SyncCollectionHandle,
	SyncCollectionHandleOptions
} from './syncClient';

export { syncStore, unwrapEden } from './syncStore';
export type {
	MutationMap,
	SyncStore,
	SyncStoreOptions,
	SyncStoreState,
	SyncStoreStatus
} from './syncStore';
