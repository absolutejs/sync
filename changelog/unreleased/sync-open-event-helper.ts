import type { Change } from '@absolutejs/changelog';

export const change: Change = {
	detail: "`@absolutejs/sync/client` now exports `SYNC_OPEN_TOPIC` and `isSyncOpenEvent(event)`. A subscriber's `onEvent` receives the open frame the server sends when a connection opens or reopens; it says the stream is live, not that anything changed. A subscriber that refetches on every event refetched each page's data once per load for nothing. Check `isSyncOpenEvent` first, and catch up on a reopen (the second open frame, or `onOpen` after the first). `createLiveQuery` and the shared subscriber already handle it.",
	kind: 'added',
	summary: 'isSyncOpenEvent tells the open frame from a change',
	symbols: ['isSyncOpenEvent', 'SYNC_OPEN_TOPIC', 'SyncSubscriberOptions']
};
