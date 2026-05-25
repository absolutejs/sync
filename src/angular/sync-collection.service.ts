import { computed, Injectable, OnDestroy, signal } from '@angular/core';
import { createSyncCollection } from '../client/syncCollection';
import type {
	MutateOptions,
	SyncCollection,
	SyncCollectionOptions,
	SyncCollectionStatus
} from '../client/syncCollection';

/**
 * Angular binding for live sync-engine collections (the Tier 3 store). Inject
 * the service and call `connect(options)` to get `data`/`status`/`error` signals
 * maintained from the WebSocket diff stream, plus an optimistic `mutate`. All
 * opened collections close on the service's destroy.
 *
 * SSR-safe: the socket only opens in a browser, so server rendering is inert.
 */
@Injectable({ providedIn: 'root' })
export class SyncCollectionService implements OnDestroy {
	private readonly collections = new Set<SyncCollection<unknown>>();

	connect<T>(options: SyncCollectionOptions<T>) {
		const data = signal<T[]>([]);
		const status = signal<SyncCollectionStatus>('connecting');
		const error = signal<unknown>(undefined);

		let collection: SyncCollection<T> | null = null;

		if (typeof window !== 'undefined') {
			collection = createSyncCollection<T>(options);
			this.collections.add(collection as SyncCollection<unknown>);
			const apply = (state: {
				data: T[];
				status: SyncCollectionStatus;
				error: unknown;
			}) => {
				data.set(state.data);
				status.set(state.status);
				error.set(state.error);
			};
			apply(collection.get());
			collection.subscribe(apply);
		}

		const mutate = <R = unknown>(
			mutateOptions: MutateOptions<T>
		): Promise<R> =>
			collection
				? collection.mutate<R>(mutateOptions)
				: Promise.reject(new Error('sync collection is not ready'));

		return {
			data: computed(() => data()),
			error: computed(() => error()),
			mutate,
			status: computed(() => status())
		};
	}

	ngOnDestroy() {
		for (const collection of this.collections) {
			collection.close();
		}
		this.collections.clear();
	}
}
