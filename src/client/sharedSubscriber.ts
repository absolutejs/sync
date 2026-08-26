import { SYNC_OPEN_TOPIC } from '../reactiveHub';
import type { ReactiveEvent } from '../reactiveHub';
import type { SyncSubscriber, SyncSubscriberOptions } from './subscriber';

type Member = {
	topics: string[];
	onEvent: (event: ReactiveEvent) => void;
	onOpen?: () => void;
	onError?: (event: Event) => void;
	/** Whether this member has received its first open frame. */
	opened: boolean;
};

type Pool = {
	Impl: typeof EventSource;
	url: string;
	withCredentials: boolean;
	members: Set<Member>;
	source: EventSource | null;
	/** Comma-joined sorted topics the live `source` was opened with. */
	connectedTopics: string;
	/** Whether the live `source` has delivered its first open frame yet. */
	sourceOpened: boolean;
	reconcileTimer: ReturnType<typeof setTimeout> | undefined;
};

const pools = new Map<string, Pool>();

const topicMatches = (pattern: string, topic: string) =>
	pattern.endsWith('*')
		? topic.startsWith(pattern.slice(0, -1))
		: topic === pattern;

const unionTopics = (pool: Pool) => {
	const topics = new Set<string>();
	for (const member of pool.members) {
		for (const topic of member.topics) {
			topics.add(topic);
		}
	}
	return [...topics].sort();
};

const deliverOpen = (pool: Pool, event: ReactiveEvent, toAll: boolean) => {
	for (const member of pool.members) {
		if (!toAll && member.opened) {
			continue;
		}
		member.opened = true;
		member.onOpen?.();
		member.onEvent(event);
	}
};

const connect = (pool: Pool, topics: string[]) => {
	pool.source?.close();
	const params = new URLSearchParams({ topics: topics.join(',') });
	const separator = pool.url.includes('?') ? '&' : '?';
	const source = new pool.Impl(
		`${pool.url}${separator}${params.toString()}`,
		{
			withCredentials: pool.withCredentials
		}
	);
	pool.source = source;
	pool.connectedTopics = topics.join(',');
	pool.sourceOpened = false;

	source.onmessage = (message) => {
		let event: ReactiveEvent;
		try {
			event = JSON.parse(message.data) as ReactiveEvent;
		} catch {
			return; // heartbeat / non-JSON frame
		}
		if (pool.source !== source) {
			return; // a newer connection superseded this one
		}
		if (event.topic === SYNC_OPEN_TOPIC) {
			// The first open frame on a connection re-opened for a topic-set
			// change only concerns members that have never been opened —
			// everyone else is still hydrated. A later open frame on the same
			// connection is a real reconnect: everyone re-hydrates.
			deliverOpen(pool, event, pool.sourceOpened);
			pool.sourceOpened = true;
			return;
		}
		for (const member of pool.members) {
			if (
				member.topics.some((pattern) =>
					topicMatches(pattern, event.topic)
				)
			) {
				member.onEvent(event);
			}
		}
	};
	source.onerror = (event) => {
		if (pool.source !== source) {
			return;
		}
		for (const member of pool.members) {
			member.onError?.(event);
		}
	};
};

const reconcile = (pool: Pool) => {
	pool.reconcileTimer = undefined;
	if (pool.members.size === 0) {
		pool.source?.close();
		pool.source = null;
		pool.connectedTopics = '';
		pools.delete(poolKey(pool.url, pool.withCredentials));
		return;
	}
	const topics = unionTopics(pool);
	if (pool.source && topics.join(',') === pool.connectedTopics) {
		return;
	}
	connect(pool, topics);
};

const scheduleReconcile = (pool: Pool) => {
	if (pool.reconcileTimer !== undefined) {
		return;
	}
	// Coalesce the burst of subscriptions a page mounts in one tick into a
	// single connection instead of one reconnect per member.
	pool.reconcileTimer = setTimeout(() => reconcile(pool), 0);
};

const poolKey = (url: string, withCredentials: boolean) =>
	`${url}|${withCredentials ? 1 : 0}`;

/**
 * Like {@link createSyncSubscriber}, but every subscriber on the same page
 * shares ONE EventSource per endpoint. The connection carries the union of all
 * members' topics and is only re-opened when that set changes; events are
 * routed to the members whose topics match.
 *
 * Why: browsers cap concurrent connections per host (six over HTTP/1.1), and an
 * SSE stream holds one for its whole life. A view with a handful of live
 * queries would otherwise exhaust the pool and stall every other request —
 * the "loads forever" failure. Sharing keeps it at one.
 */
export const createSharedSyncSubscriber = ({
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
			'createSharedSyncSubscriber requires EventSource. Run in a browser or pass eventSourceImpl.'
		);
	}
	const key = poolKey(url, withCredentials ?? false);
	let pool = pools.get(key);
	if (!pool || pool.Impl !== Impl) {
		pool = {
			Impl,
			url,
			withCredentials: withCredentials ?? false,
			members: new Set(),
			source: null,
			connectedTopics: '',
			sourceOpened: false,
			reconcileTimer: undefined
		};
		pools.set(key, pool);
	}
	const member: Member = {
		topics: [...topics],
		onEvent,
		onOpen,
		onError,
		opened: false
	};
	pool.members.add(member);
	scheduleReconcile(pool);
	const owner = pool;

	return {
		close: () => {
			owner.members.delete(member);
			scheduleReconcile(owner);
		},
		get source() {
			// The shared connection may be (re)opened after this member joins;
			// resolve it lazily so callers always see the live one.
			return owner.source as EventSource;
		}
	};
};
