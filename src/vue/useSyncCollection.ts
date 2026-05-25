import { onMounted, onUnmounted, ref, shallowRef } from 'vue';
import type { Ref } from 'vue';
import { createSyncCollection } from '../client/syncCollection';
import type {
	MutateOptions,
	SyncCollection,
	SyncCollectionOptions,
	SyncCollectionStatus
} from '../client/syncCollection';

/**
 * Vue composable for a live sync-engine collection (the Tier 3 store). Returns
 * reactive `data`/`status`/`error` refs maintained from the WebSocket diff
 * stream, plus an optimistic `mutate`.
 *
 * SSR-safe: the socket opens in `onMounted` (client only) and closes in
 * `onUnmounted` (or via the returned `destroy`).
 */
export const useSyncCollection = <T>(options: SyncCollectionOptions<T>) => {
	const data = shallowRef<T[]>([]) as Ref<T[]>;
	const status = ref<SyncCollectionStatus>('connecting');
	const error = ref<unknown>(undefined);

	let collection: SyncCollection<T> | null = null;
	let unsubscribe: (() => void) | null = null;

	onMounted(() => {
		collection = createSyncCollection<T>(options);
		const apply = (state: {
			data: T[];
			status: SyncCollectionStatus;
			error: unknown;
		}) => {
			data.value = state.data;
			status.value = state.status;
			error.value = state.error;
		};
		apply(collection.get());
		unsubscribe = collection.subscribe(apply);
	});

	const destroy = () => {
		unsubscribe?.();
		collection?.close();
		unsubscribe = null;
		collection = null;
	};

	onUnmounted(destroy);

	const mutate = <R = unknown>(
		mutateOptions: MutateOptions<T>
	): Promise<R> =>
		collection
			? collection.mutate<R>(mutateOptions)
			: Promise.reject(new Error('sync collection is not ready'));

	return { data, destroy, error, mutate, status };
};
