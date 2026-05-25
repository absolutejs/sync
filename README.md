# @absolutejs/sync

Reactive data primitives for [Elysia](https://elysiajs.com) and the AbsoluteJS
ecosystem — kill polling and keep a remote store off your hot path, **on your own
database and ORM** (Drizzle _or_ Prisma, any DB they support).

- **`createReactiveHub` + `sync` plugin** — push-on-change over SSE. A view
  subscribes to the topics its data depends on; a mutation publishes those topics;
  subscribers refetch (or read the pushed payload) the instant data changes.
- **ORM adapters (`/drizzle`, `/prisma`)** — _derive_ those topics automatically
  from a query, so you stop hand-naming them. A read maps to a `table` topic (or a
  `table:key` row topic for a primary-key lookup); a mutation publishes the matching
  topics.
- **`createLiveQuery`** — a client query that hydrates once, then refetches whenever
  one of its topics fires. Framework-agnostic (`get` + `subscribe`).
- **`createWriteBehindCache`** — an in-memory hot cache with write-behind
  persistence, so a latency-sensitive hot path doesn't pay a round-trip to a remote
  store on every read/write.

It is **not (yet) a full sync engine.** Convex, ElectricSQL, and Zero own or
replicate the database (read-set tracking, IVM, a transaction log, client SQLite
replicas). This package stays a _library_ over the store and transport you already
have. Topic granularity is deliberately coarse (table/row) — it over-invalidates a
little rather than tracking exact read sets, which is fine for the large majority of
dashboards and stays DB-agnostic. Row-level reactive query results, optimistic
mutations, and offline are the [roadmap](./ROADMAP.md) (Tier 3).

> Status: early (`0.0.1`). Tier 1 (hub, SSE plugin, browser subscriber,
> write-behind cache) and Tier 2 (Drizzle + Prisma topic adapters, `createLiveQuery`)
> are in place. The ORM adapters ship as subpaths of this package, not a separate
> one.

## Install

```bash
bun add @absolutejs/sync
```

`elysia` is an optional peer (only needed for the `sync` plugin). The Drizzle adapter
expects `drizzle-orm` if you use it; the Prisma adapter needs no Prisma import at all.

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

`resolveTopics` on the plugin lets you derive a connection's topics from the session
or auth instead of trusting the client's `?topics=`.

## ORM auto-reactivity — stop hand-naming topics

The adapters turn a query into the topics it touches, so reads and writes line up
automatically. Same function names for both ORMs; pick the matching subpath.

```ts
// server — Drizzle
import { eq } from 'drizzle-orm';
import { deriveReadTopics, publishWhere } from '@absolutejs/sync/drizzle';

new Elysia()
	.use(sync({ hub }))
	.get('/api/orders', () => db.select().from(orders)) // list -> topic "orders"
	.patch('/api/orders/:id', async ({ params, body }) => {
		const id = Number(params.id);
		await db.update(orders).set(body).where(eq(orders.id, id));
		publishWhere(hub, orders, eq(orders.id, id), { op: 'update' });
		// publishes "orders" and "orders:<id>"
	});
```

```ts
// browser — createLiveQuery + Prisma topic derivation (just a model name, no deps)
import { createLiveQuery, jsonFetcher } from '@absolutejs/sync/client';
import { deriveReadTopics } from '@absolutejs/sync/prisma';

const orders = createLiveQuery({
	topics: deriveReadTopics('order').topics, // ['order']
	fetcher: jsonFetcher('/api/orders')
});

orders.subscribe((state) => render(state.data)); // refetches on every order change
// orders.close() when the view unmounts
```

`createLiveQuery` is a small observable store: `get()` for the current
`{ data, error, loading, fetching }`, `subscribe(listener)` for changes (plugs
straight into React's `useSyncExternalStore`), plus `refetch()` and `close()`. It
supersedes overlapping fetches (last write wins), re-hydrates on reconnect, and takes
`initialData` (SSR seed), `manual`, and `debounceMs`.

What the adapters derive:

- `deriveReadTopics(orders)` → `{ topics: ['orders'], rowLevel: false }`
- `deriveReadTopics(orders, eq(orders.id, 5))` → `{ topics: ['orders:5'], rowLevel: true }`
- anything more complex (joins, `and`/`or`, ranges, `in`, non-key columns) falls back
  to the table topic — over-invalidating rather than missing an update.
- Write side: `publishChange` (explicit keys), `publishRows` (keys from a mutation's
  returned/created records), `publishWhere` (keys from an update/delete filter).

The Prisma adapter parses Prisma's plain `where`/result objects, so it needs no
`@prisma/client` import; the Drizzle adapter reads the schema's table objects.

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

### `@absolutejs/sync`

| Export                                                                                     | What it is                                                           |
| ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------- |
| `createReactiveHub()`                                                                      | In-memory topic pub/sub (`publish`, `subscribe`, `subscriberCount`). |
| `sync({ hub, path?, resolveTopics?, heartbeatMs? })`                                       | Elysia plugin: SSE stream of hub events.                             |
| `createWriteBehindCache({ load, persist, remove?, debounceMs?, evict?, onPersistError? })` | In-memory cache + write-behind persistence.                          |

### `@absolutejs/sync/client`

| Export                                            | What it is                                                      |
| ------------------------------------------------- | --------------------------------------------------------------- |
| `createSyncSubscriber({ topics, onEvent, url? })` | Browser SSE client.                                             |
| `createLiveQuery({ topics, fetcher, ... })`       | Hydrate-once, refetch-on-event observable query store.          |
| `jsonFetcher(url, init?)`                         | Default `fetcher`: GET + JSON parse, forwards the abort signal. |

### `@absolutejs/sync/drizzle` and `@absolutejs/sync/prisma`

| Export                                                                | What it is                                         |
| --------------------------------------------------------------------- | -------------------------------------------------- |
| `deriveReadTopics(table\|model, where?, options?)`                    | Topics a read depends on (`{ topics, rowLevel }`). |
| `publishChange(hub, table\|model, { keys?, op? })`                    | Publish the table topic + a row topic per key.     |
| `publishRows(hub, table\|model, rows, { keyField?/keyColumn?, op? })` | Publish topics for returned/created records.       |
| `publishWhere(hub, table\|model, where, { ..., op? })`                | Publish topics for an update/delete filter.        |
| `tableTopic` / `keyTopic`                                             | The shared topic vocabulary both sides speak.      |

## License

MIT
