import type { ReactiveEvent } from '../reactiveHub';

export type { ReactiveEvent } from '../reactiveHub';

export type SyncSubscriberOptions = {
	/** Topics to subscribe to. A trailing `*` matches by prefix server-side. */
	topics: string[];
	/** Called for every reactive event pushed from the server. */
	onEvent: (event: ReactiveEvent) => void;
	/** SSE endpoint mounted by the {@link sync} plugin. Defaults to `/sync`. */
	url?: string;
	onOpen?: () => void;
	onError?: (event: Event) => void;
	/** Send cookies with the SSE request (cross-origin auth). */
	withCredentials?: boolean;
	/**
	 * EventSource implementation to use. Defaults to the global one; pass a polyfill
	 * for non-browser runtimes.
	 */
	eventSourceImpl?: typeof EventSource;
};

export type SyncSubscriber = {
	close: () => void;
	/** The underlying EventSource, for advanced listeners. */
	source: EventSource;
};

/**
 * Subscribe a browser to the server's {@link ReactiveHub} over SSE. `onEvent` fires
 * whenever a subscribed topic is published — the cue to refetch (or read the pushed
 * payload) instead of polling. EventSource reconnects automatically on transient
 * network drops.
 */
export const createSyncSubscriber = ({
	topics,
	onEvent,
	url = '/sync',
	onOpen,
	onError,
	withCredentials,
	eventSourceImpl
}: SyncSubscriberOptions): SyncSubscriber => {
	const Impl = eventSourceImpl ?? globalThis.EventSource;
	if (!Impl) {
		throw new Error(
			'createSyncSubscriber requires EventSource. Run in a browser or pass eventSourceImpl.'
		);
	}

	const params = new URLSearchParams({ topics: topics.join(',') });
	const separator = url.includes('?') ? '&' : '?';
	const source = new Impl(`${url}${separator}${params.toString()}`, {
		withCredentials: withCredentials ?? false
	});

	source.onmessage = (event) => {
		try {
			onEvent(JSON.parse(event.data) as ReactiveEvent);
		} catch {
			// ignore heartbeats / non-JSON frames
		}
	};
	if (onOpen) {
		source.onopen = () => onOpen();
	}
	if (onError) {
		source.onerror = (event) => onError(event);
	}

	return {
		close: () => source.close(),
		source
	};
};
