import { SYNC_OPEN_TOPIC } from '../reactiveHub';
import type { ReactiveEvent } from '../reactiveHub';
import { createSyncSubscriber } from './subscriber';

export type LiveQueryState<T> = {
	/** Latest query result, or `undefined` before the first successful fetch. */
	data: T | undefined;
	/** Error from the most recent fetch, or `undefined` if it succeeded. */
	error: unknown;
	/** `true` until the first result arrives (no data yet). */
	loading: boolean;
	/** `true` while a (re)fetch is in flight — data may still be present. */
	fetching: boolean;
};

export type LiveQueryOptions<T> = {
	/**
	 * Topics this query depends on — typically the server's
	 * `deriveReadTopics(...).topics`. Any event on one of them triggers a
	 * refetch. A trailing `*` matches by prefix server-side.
	 */
	topics: string[];
	/**
	 * Runs the read and resolves the query result. Receives an `AbortSignal`
	 * that fires when a newer fetch supersedes this one or the query is closed.
	 */
	fetcher: (signal: AbortSignal) => Promise<T>;
	/** SSE endpoint mounted by the {@link sync} plugin. Defaults to `/sync`. */
	url?: string;
	/** Send cookies with the SSE request (cross-origin auth). */
	withCredentials?: boolean;
	/** EventSource implementation; defaults to the global one. */
	eventSourceImpl?: typeof EventSource;
	/**
	 * Seed data (e.g. from SSR). When provided, the initial fetch is skipped —
	 * the query trusts this until an event or a manual {@link LiveQuery.refetch}.
	 */
	initialData?: T;
	/**
	 * Skip the initial fetch and stay idle until the first event or a manual
	 * refetch. Reconnects still re-hydrate.
	 */
	manual?: boolean;
	/**
	 * Coalesce a burst of events into one refetch within this window (ms).
	 * Defaults to 0 — refetch once per event.
	 */
	debounceMs?: number;
	/** Called when a fetch rejects (stale data is retained). */
	onError?: (error: unknown) => void;
};

export type LiveQuery<T> = {
	/** Current state snapshot (stable reference until the next change). */
	get: () => LiveQueryState<T>;
	/** Subscribe to state changes; returns an unsubscribe. */
	subscribe: (listener: (state: LiveQueryState<T>) => void) => () => void;
	/** Force a refetch now. Resolves when this fetch settles. */
	refetch: () => Promise<void>;
	/** Stop the SSE subscription, cancel any in-flight fetch, drop listeners. */
	close: () => void;
};

/**
 * A live, self-refreshing query: hydrate once via `fetcher`, then refetch
 * whenever the server publishes one of `topics` — the read half of Tier 2,
 * built on {@link createSyncSubscriber}. Framework-agnostic: `get` + `subscribe`
 * plug straight into React's `useSyncExternalStore` or any equivalent.
 *
 * Pair it with the Drizzle adapter's `deriveReadTopics` (server) and
 * `publishChange`/`publishWhere` (mutations) so a write invalidates exactly the
 * queries that read the changed rows.
 */
export const createLiveQuery = <T>(
	options: LiveQueryOptions<T>
): LiveQuery<T> => {
	const hasSeed = options.initialData !== undefined;
	let state: LiveQueryState<T> = {
		data: options.initialData,
		error: undefined,
		loading: !options.manual && !hasSeed,
		fetching: false
	};

	const listeners = new Set<(state: LiveQueryState<T>) => void>();
	const setState = (patch: Partial<LiveQueryState<T>>) => {
		state = { ...state, ...patch };
		for (const listener of listeners) {
			listener(state);
		}
	};

	let requestSeq = 0;
	let inFlight: AbortController | undefined;
	let closed = false;

	const refetch = async () => {
		if (closed) {
			return;
		}
		const seq = (requestSeq += 1);
		inFlight?.abort();
		const controller = new AbortController();
		inFlight = controller;
		setState({ fetching: true });

		try {
			const data = await options.fetcher(controller.signal);
			if (seq !== requestSeq) {
				return; // superseded by a newer fetch
			}
			setState({
				data,
				error: undefined,
				loading: false,
				fetching: false
			});
		} catch (error) {
			if (controller.signal.aborted || seq !== requestSeq) {
				return; // aborted or superseded — leave state to the winner
			}
			setState({ error, loading: false, fetching: false });
			options.onError?.(error);
		} finally {
			if (inFlight === controller) {
				inFlight = undefined;
			}
		}
	};

	let debounceTimer: ReturnType<typeof setTimeout> | undefined;
	const scheduleRefetch = () => {
		if (closed) {
			return;
		}
		if (!options.debounceMs) {
			void refetch();
			return;
		}
		if (debounceTimer !== undefined) {
			return; // a refetch is already queued for this window
		}
		debounceTimer = setTimeout(() => {
			debounceTimer = undefined;
			void refetch();
		}, options.debounceMs);
	};

	let opened = false;
	const onEvent = (event: ReactiveEvent) => {
		if (event.topic === SYNC_OPEN_TOPIC) {
			// First open is the initial connect (already hydrated); a later open
			// is a reconnect, so re-hydrate to catch events missed while down.
			if (opened) {
				scheduleRefetch();
			}
			opened = true;
			return;
		}
		scheduleRefetch();
	};

	const subscriber = createSyncSubscriber({
		topics: options.topics,
		onEvent,
		url: options.url,
		withCredentials: options.withCredentials,
		eventSourceImpl: options.eventSourceImpl
	});

	if (!options.manual && !hasSeed) {
		void refetch();
	}

	const close = () => {
		if (closed) {
			return;
		}
		closed = true;
		subscriber.close();
		inFlight?.abort();
		if (debounceTimer !== undefined) {
			clearTimeout(debounceTimer);
			debounceTimer = undefined;
		}
		listeners.clear();
	};

	return {
		get: () => state,
		subscribe: (listener) => {
			listeners.add(listener);
			return () => {
				listeners.delete(listener);
			};
		},
		refetch,
		close
	};
};

/**
 * A small default `fetcher`: GET `url` and parse JSON. Forwards the live query's
 * abort signal and throws on a non-2xx response.
 *
 * @example
 * createLiveQuery({ topics, fetcher: jsonFetcher<User[]>('/api/users') })
 */
export const jsonFetcher =
	<T>(url: string, init?: RequestInit) =>
	async (signal: AbortSignal): Promise<T> => {
		const response = await fetch(url, { ...init, signal });
		if (!response.ok) {
			throw new Error(`${response.status} ${response.statusText}`);
		}
		return (await response.json()) as T;
	};
