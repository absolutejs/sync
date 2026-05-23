# @absolutejs/sync

Two small, composable primitives for live data in [Elysia](https://elysiajs.com)
and the AbsoluteJS ecosystem:

- **`createReactiveHub` + `sync` plugin** — push-on-change over SSE so clients stop
  polling. A widget subscribes to the topics its data depends on; a mutation
  publishes those topics; subscribers refetch (or read the pushed payload) the
  instant data changes.
- **`createWriteBehindCache`** — an in-memory hot cache with write-behind
  persistence, so a latency-sensitive hot path doesn't pay a round-trip to a remote
  store on every read/write.

It is **not a sync engine.** Convex, ElectricSQL, and Zero are whole backends —
read-set tracking, OCC/MVCC, a transaction log, client SQLite replicas. This package
does **not** rebuild any of that. It's a thin reactive layer over the store and
transport you already have: pair it with **Drizzle _or_ Prisma** (or any store) and
your existing SSE/WebSocket. Dependencies are explicit (you name topics), not
auto-tracked from query read sets. If you want a full local-first sync engine, reach
for one of the above; if you just want to delete your polling loop and keep a remote
DB off your hot path, reach for this.

> Status: early (`0.0.1`). In-memory hub, write-behind cache, Elysia SSE plugin, and
> a browser subscriber. Durable/transport adapters land in a companion
> `-adapters` package as the API settles.

## Install

```bash
bun add @absolutejs/sync
```

`elysia` is an optional peer (only needed for the `sync` plugin).

## Reactive push — kill the polling loop

```ts
// server
import { Elysia } from 'elysia';
import { createReactiveHub, sync } from '@absolutejs/sync';

const hub = createReactiveHub();

new Elysia()
	.use(sync({ hub })) // serves SSE at GET /sync?topics=a,b,c
	.post('/orders', async ({ body }) => {
		const order = await db.orders.insert(body); // your Drizzle/Prisma write
		hub.publish('orders'); // notify everyone watching "orders"
		hub.publish(`orders:${order.id}`); // …and this one specifically
		return order;
	})
	.listen(3000);
```

```ts
// browser
import { createSyncSubscriber } from '@absolutejs/sync/client';

const sub = createSyncSubscriber({
	topics: ['orders', 'orders:*'], // trailing * matches by prefix
	onEvent: (event) => {
		// data changed — refetch instead of polling on a timer
		if (event.topic.startsWith('orders')) refetchOrders();
	}
});
// sub.close() when the view unmounts
```

## Write-behind cache — keep a remote store off your hot path

```ts
import { createWriteBehindCache } from '@absolutejs/sync';

const sessions = createWriteBehindCache({
	load: (id) => db.sessions.get(id), // read-through on a miss
	persist: (id, value) => db.sessions.set(id, value), // coalesced background write
	remove: (id) => db.sessions.delete(id),
	debounceMs: 250,
	evict: (value) => value.status === 'closed' // drop terminal entries
});

sessions.set('s1', next); // synchronous; persists ~250ms later
const current = await sessions.get('s1'); // from memory
await sessions.flush(); // on shutdown
```

This is what `@absolutejs/voice` uses to keep its per-audio-frame session state in
memory while the Drizzle/Postgres store stays the durable source of truth — without
it, ~3 store round-trips every 20ms ran the voice pipeline far slower than real time.

## API

| Export | What it is |
| --- | --- |
| `createReactiveHub()` | In-memory topic pub/sub (`publish`, `subscribe`, `subscriberCount`). |
| `sync({ hub, path?, resolveTopics?, heartbeatMs? })` | Elysia plugin: SSE stream of hub events. |
| `createSyncSubscriber({ topics, onEvent, url? })` | Browser SSE client (from `@absolutejs/sync/client`). |
| `createWriteBehindCache({ load, persist, remove?, debounceMs?, evict?, onPersistError? })` | In-memory cache + write-behind persistence. |

## License

MIT
