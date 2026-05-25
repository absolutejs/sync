import { createSyncCollection } from '../client/syncCollection';
import type {
	MutateOptions,
	SyncCollection,
	SyncCollectionOptions,
	SyncCollectionState
} from '../client/syncCollection';

/**
 * Svelte binding for a live sync-engine collection (the Tier 3 store). A proper
 * readable store — `$store` gives the current `{ data, status, error }`,
 * maintained from the WebSocket diff stream — with `mutate` (optimistic) and
 * `destroy` attached.
 *
 * SSR-safe: the socket opens lazily on the first browser subscription, so it is
 * inert during server rendering. Call `destroy()` (e.g. in `onDestroy`) to close.
 */
export const createSyncCollectionStore = <T>(
	options: SyncCollectionOptions<T>
) => {
	let collection: SyncCollection<T> | null = null;
	let current: SyncCollectionState<T> = {
		data: [],
		error: undefined,
		status: 'connecting'
	};
	const subscribers = new Set<(state: SyncCollectionState<T>) => void>();

	const ensureConnected = () => {
		if (collection !== null || typeof window === 'undefined') {
			return;
		}
		collection = createSyncCollection<T>(options);
		current = collection.get();
		collection.subscribe((state) => {
			current = state;
			subscribers.forEach((run) => run(current));
		});
	};

	return {
		subscribe(run: (state: SyncCollectionState<T>) => void) {
			subscribers.add(run);
			ensureConnected();
			run(current);

			return () => {
				subscribers.delete(run);
			};
		},
		mutate: <R = unknown>(mutateOptions: MutateOptions<T>): Promise<R> =>
			collection
				? collection.mutate<R>(mutateOptions)
				: Promise.reject(new Error('sync collection is not ready')),
		destroy() {
			collection?.close();
			collection = null;
			subscribers.clear();
		}
	};
};
