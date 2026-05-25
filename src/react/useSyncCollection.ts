import { useCallback, useEffect, useRef, useState } from 'react';
import { createSyncCollection } from '../client/syncCollection';
import type {
	MutateOptions,
	SyncCollection,
	SyncCollectionOptions,
	SyncCollectionState
} from '../client/syncCollection';

/**
 * React binding for a live sync-engine collection (the Tier 3 store). Subscribes
 * to `{ added, removed, changed }` diffs over the WebSocket and re-renders on
 * change; returns the current `data`/`status`/`error` plus an optimistic
 * `mutate`.
 *
 * SSR-safe: the socket opens in an effect (client only), and re-opens if `url`,
 * `collection`, or `params` change. The collection closes on unmount.
 */
export const useSyncCollection = <T>(options: SyncCollectionOptions<T>) => {
	const [state, setState] = useState<SyncCollectionState<T>>({
		data: [],
		error: undefined,
		status: 'connecting'
	});
	const collectionRef = useRef<SyncCollection<T> | null>(null);
	const paramsKey = JSON.stringify(options.params ?? null);

	useEffect(() => {
		const collection = createSyncCollection<T>(options);
		collectionRef.current = collection;
		setState(collection.get());
		const unsubscribe = collection.subscribe(setState);

		return () => {
			unsubscribe();
			collection.close();
			collectionRef.current = null;
		};
		// Re-open only when the subscription identity changes.
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [options.url, options.collection, paramsKey]);

	const mutate = useCallback(
		<R = unknown>(mutateOptions: MutateOptions<T>): Promise<R> =>
			collectionRef.current
				? collectionRef.current.mutate<R>(mutateOptions)
				: Promise.reject(new Error('sync collection is not ready')),
		[]
	);

	return {
		data: state.data,
		error: state.error,
		mutate,
		status: state.status
	};
};
