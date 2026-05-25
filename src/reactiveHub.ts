/**
 * Topic of the synthetic frame the SSE plugin emits when a stream opens (and
 * re-opens after a reconnect). Clients use it to tell "the stream connected"
 * apart from a real data-change event.
 */
export const SYNC_OPEN_TOPIC = '@absolutejs/sync:open';

export type ReactiveEvent<TPayload = unknown> = {
	topic: string;
	at: number;
	payload?: TPayload;
};

export type ReactiveListener<TPayload = unknown> = (
	event: ReactiveEvent<TPayload>
) => void;

export type ReactiveHub = {
	/**
	 * Notify every subscriber of `topic` (and any prefix-wildcard subscriber that
	 * matches it). Call this from a mutation after the durable write commits.
	 */
	publish: (topic: string, payload?: unknown) => void;
	/**
	 * Listen on one or more topics. A topic ending in `*` matches every topic that
	 * starts with the prefix before it (e.g. `voice:session:*`). Returns an
	 * unsubscribe function.
	 */
	subscribe: (topics: string[], listener: ReactiveListener) => () => void;
	/** Number of active subscribers, optionally for a single exact topic. */
	subscriberCount: (topic?: string) => number;
};

type Subscription = {
	exact: Set<string>;
	prefixes: string[];
	listener: ReactiveListener;
};

/**
 * An in-memory topic pub/sub for reactive, push-on-change updates.
 *
 * The pattern that replaces polling: a query/widget subscribes to the topics its
 * data depends on; a mutation `publish`es those topics after it writes; subscribers
 * are notified immediately and refetch (or receive the pushed payload) — instead of
 * every client hammering the server on a timer.
 *
 * Dependencies are explicit (you name the topics) rather than auto-tracked from a
 * query's read set — deliberately small, with no sandbox or query interception.
 * Pair it with the {@link sync} Elysia plugin to stream events to browsers over SSE.
 */
export const createReactiveHub = (): ReactiveHub => {
	const subscriptions = new Set<Subscription>();

	const matches = (subscription: Subscription, topic: string) => {
		if (subscription.exact.has(topic)) {
			return true;
		}
		return subscription.prefixes.some((prefix) => topic.startsWith(prefix));
	};

	return {
		publish: (topic, payload) => {
			const event: ReactiveEvent = { topic, at: Date.now(), payload };
			for (const subscription of subscriptions) {
				if (matches(subscription, topic)) {
					subscription.listener(event);
				}
			}
		},
		subscribe: (topics, listener) => {
			const exact = new Set<string>();
			const prefixes: string[] = [];
			for (const topic of topics) {
				if (topic.endsWith('*')) {
					prefixes.push(topic.slice(0, -1));
				} else {
					exact.add(topic);
				}
			}
			const subscription: Subscription = { exact, prefixes, listener };
			subscriptions.add(subscription);
			return () => {
				subscriptions.delete(subscription);
			};
		},
		subscriberCount: (topic) => {
			if (topic === undefined) {
				return subscriptions.size;
			}
			let count = 0;
			for (const subscription of subscriptions) {
				if (matches(subscription, topic)) {
					count += 1;
				}
			}
			return count;
		}
	};
};
