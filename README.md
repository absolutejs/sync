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
- **Sync engine (`/engine`, `/postgres`)** — row-level reactive query results:
  hydrate a collection once, then maintain it from `{ added, removed, changed }`
  diffs over a WebSocket, with optimistic mutations, an offline queue, and access
  control. CDC catches out-of-band writes; aggregations are incremental.
- **`createWriteBehindCache`** — an in-memory hot cache with write-behind
  persistence, so a latency-sensitive hot path doesn't pay a round-trip to a remote
  store on every read/write.

Unlike Convex, ElectricSQL, or Zero, it does **not** own or replicate your database
— it stays a _library_ over the store, ORM, and transport you already have. Tier 1/2
keep granularity deliberately coarse (table/row topics, refetch on change); the Tier
3 engine adds true row-level diffs and optimistic writes. Single-table filtered
queries are matched incrementally; joins (inner and left), aggregations, and
top-N ordering are maintained incrementally through a composable operator graph
(`query(...).filter().join().leftJoin().groupBy().orderBy()`).

> Status: early (`0.0.1`). Tier 1 (hub, SSE plugin, browser subscriber,
> write-behind cache), Tier 2 (Drizzle + Prisma topic adapters, `createLiveQuery`),
> and Tier 3 (sync engine: collections, WebSocket diff transport, optimistic
> mutations + offline queue, CDC for Postgres/MySQL/SQLite, incremental
> aggregations + joins, and a declarative operator graph) are in place.
> Everything ships as subpaths of this one package.

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

## Live collections — the sync engine (Tier 3)

Row-level reactive results: the client holds a collection and the server pushes
`{ added, removed, changed }` diffs over a WebSocket, instead of refetching. Define
a collection once (the filter powers both the DB hydrate and the incremental
matcher), expose it over `syncSocket`, and drive changes from mutations.

```ts
// server
import { Elysia } from 'elysia';
import { syncSocket } from '@absolutejs/sync';
import { createSyncEngine, defineMutation } from '@absolutejs/sync/engine';
import { prismaCollection } from '@absolutejs/sync/prisma';

// `transaction` runs every mutation in your DB's transaction (any ORM), so its
// writes are ACID and the diff is emitted only after the commit.
const engine = createSyncEngine({
	transaction: (run) => prisma.$transaction(run)
});

engine.register(
	prismaCollection({
		name: 'orders',
		where: (params) => ({ userId: params.userId, status: 'open' }), // written once
		find: (where) => prisma.order.findMany({ where }),
		authorize: (params, ctx) => params.userId === ctx.userId // never leak rows
	})
);

// Teach the engine how to persist the table once — now writes auto-emit. The
// third arg is the transaction handle, so the write joins the mutation's tx.
engine.registerWriter('orders', {
	insert: (data, ctx, tx) =>
		tx.order.create({ data: { ...data, userId: ctx.userId } }),
	update: (data, _ctx, tx) =>
		tx.order.update({ where: { id: data.id }, data }),
	delete: (row, _ctx, tx) => tx.order.delete({ where: { id: row.id } })
});

engine.registerMutation(
	defineMutation({
		name: 'createOrder',
		// Persists AND goes live in one step — you can't forget to emit, and the
		// diff carries the stored row (db-assigned id). Commits atomically.
		handler: (args, ctx, actions) => actions.insert('orders', args)
	})
);

new Elysia()
	.use(
		syncSocket({
			engine,
			resolveContext: (data) => ({ userId: data.userId })
		})
	)
	.listen(3000);
```

```ts
// browser
import { createSyncCollection } from '@absolutejs/sync/client';

const orders = createSyncCollection({
	url: 'ws://localhost:3000/sync/ws',
	collection: 'orders',
	params: { userId }
});

orders.subscribe((state) => render(state.data)); // live: diff-driven, auto-reconnect

// optimistic write — instant UI, reconciled (or rolled back) by the server
await orders.mutate({
	name: 'createOrder',
	args: { total: 42 },
	optimistic: (draft) => draft.set({ id: tempId, total: 42, status: 'open' })
});
```

- **Incremental vs refetch.** A single-table filtered collection is matched
  incrementally (only the changed rows move). Joins/aggregations and filters the
  matcher can't evaluate fall back to a correct re-hydrate. `createAggregate`
  (`/engine`) maintains `count`/`sum`/`avg`/`min`/`max` + `groupBy` incrementally.
- **Out-of-band writes.** Writes that bypass mutations are caught by a
  `ChangeSource` — e.g. `postgresChangeSource` (`/postgres`) over `LISTEN/NOTIFY`,
  wired with `engine.connectSource(...)` and the trigger SQL from
  `postgresNotifyTrigger`.
- **Offline.** Pending mutations replay on reconnect; pass `storage`
  (e.g. `localStorageMutationStorage`) to also survive a reload.
- **Access control is mandatory.** Each collection's `authorize` gates subscribe and
  its filter scopes rows, so a change to a row a caller can't see never reaches them.

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
| `syncSocket({ engine, path?, resolveContext? })`                                           | Elysia WebSocket plugin for the sync engine.                         |
| `createWriteBehindCache({ load, persist, remove?, debounceMs?, evict?, onPersistError? })` | In-memory cache + write-behind persistence.                          |

### `@absolutejs/sync/client`

| Export                                            | What it is                                                                                                                                                         |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `createSyncSubscriber({ topics, onEvent, url? })` | Browser SSE client.                                                                                                                                                |
| `createLiveQuery({ topics, fetcher, ... })`       | Hydrate-once, refetch-on-event observable query store.                                                                                                             |
| `jsonFetcher(url, init?)`                         | Default `fetcher`: GET + JSON parse, forwards the abort signal.                                                                                                    |
| `createSyncCollection({ url, collection, ... })`  | Live diff-driven collection store with optimistic `mutate`.                                                                                                        |
| `createSyncClient({ url })`                       | One socket, many collections (`client.collection(...)`). Applies a multi-collection mutation's diffs as one **consistent frame** — no torn cross-collection paint. |
| `createPresence({ url, room, state })`            | Join a presence room: see who's online / typing (`get` + `subscribe`) and publish your own state (`set`).                                                          |
| `localStorageMutationStorage(key)`                | `localStorage`-backed offline queue for `createSyncCollection`.                                                                                                    |

### Framework bindings — `@absolutejs/sync/{react,vue,svelte,angular}`

Idiomatic wrappers over `createSyncCollection`, one per framework, so a live
collection is one call. Each returns the same `{ data, status, error, mutate }`
and is SSR-safe (the socket opens on the client only).

| Subpath    | Export                                   | What it is                                           |
| ---------- | ---------------------------------------- | ---------------------------------------------------- |
| `/react`   | `useSyncCollection(options)`             | React hook (re-renders on diffs).                    |
| `/vue`     | `useSyncCollection(options)`             | Vue composable (reactive refs).                      |
| `/svelte`  | `createSyncCollectionStore(options)`     | Svelte readable store (`$store` → state) + `mutate`. |
| `/angular` | `SyncCollectionService.connect(options)` | Angular service returning signals.                   |

```tsx
// React
import { useSyncCollection } from '@absolutejs/sync/react';

const { data, status, mutate } = useSyncCollection<Order>({
	url: 'ws://localhost:3000/sync/ws',
	collection: 'orders',
	params: { userId }
});

mutate({
	name: 'createOrder',
	args: { total: 42 },
	optimistic: (draft) => draft.set({ id: tempId, total: 42 } as Order)
});
```

### `@absolutejs/sync/engine`

| Export                                                                            | What it is                                                                                                                                                                                                             |
| --------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `createSyncEngine()`                                                              | Registry + view syncer: `register`, `subscribe`, `applyChange`, `connectSource`, `registerMutation`, `registerWriter`, `runMutation`.                                                                                  |
| `defineCollection({ name, hydrate, key?, match?, authorize?, tables? })`          | Define a syncable collection.                                                                                                                                                                                          |
| `defineMutation({ name, handler, authorize? })`                                   | Define a server mutation. Its `handler` gets `actions.insert/update/delete` (write through a registered `TableWriter` → persists + emits in one step) plus `actions.change` (escape hatch). Changes commit atomically. |
| `registerWriter(table, { insert, update, delete })`                               | Teach the engine how to persist a table (any ORM), so writes auto-emit — you can't write without going live.                                                                                                           |
| `createAggregate({ key, groupBy?, value? })`                                      | Incremental count/sum/avg/min/max by group.                                                                                                                                                                            |
| `createMaterializedView({ key, match, equals? })`                                 | The predicate-matching IVM primitive (`apply`/`reset` → diffs).                                                                                                                                                        |
| `createPollingChangeSource({ poll, intervalMs?, startSeq?, onProcessed? })`       | DB-agnostic CDC `ChangeSource` that tails a changelog (outbox) table.                                                                                                                                                  |
| `engine.connectCluster(bus)` + `createInMemoryClusterBus()`                       | Horizontal scale: fan changes across server instances over a `ClusterBus` (BYO Redis/Postgres; in-memory bus for dev).                                                                                                 |
| `createPresenceHub()` + `syncSocket({ engine, presence })`                        | Ephemeral room-scoped presence (online / typing / cursors) over the same socket — not persisted, auto-cleaned on disconnect.                                                                                           |
| `query(source).filter().map().join().leftJoin().groupBy().orderBy()`              | Declarative incremental query builder (the operator graph).                                                                                                                                                            |
| `defineGraphCollection({ name, query, key, authorize? })`                         | Run a `query` as a live collection.                                                                                                                                                                                    |
| `defineReactiveQuery({ name, run, key })` + `registerReactive` / `registerReader` | Read-set-tracked query: `run(ctx)` reads via `ctx.db` (`all`/`get`/`where`) and re-runs only when the rows/ranges it read change — no `match`, no manual emit.                                                         |

### `@absolutejs/sync/postgres`

| Export                                                       | What it is                                                       |
| ------------------------------------------------------------ | ---------------------------------------------------------------- |
| `postgresChangeSource({ listen, channel?, parse? })`         | CDC `ChangeSource` over `LISTEN/NOTIFY` (bring your own client). |
| `postgresNotifyTrigger({ tables, channel?, functionName? })` | SQL to install the notify triggers (run once).                   |

### `@absolutejs/sync/mysql`

| Export                                                       | What it is                                                                  |
| ------------------------------------------------------------ | --------------------------------------------------------------------------- |
| `mysqlChangelogSchema({ tables, changelogTable?, prefix? })` | SQL to install the changelog table + triggers (run once).                   |
| `createPollingChangeSource({ poll, ... })`                   | Tail the changelog (re-exported from the engine).                           |
| `mysqlBinlogChangeSource({ subscribe, normalize? })`         | Higher-throughput CDC over the binlog (bring your own reader, e.g. zongji). |
| `normalizeBinlogEvent(event)`                                | Pure: a binlog row event → engine changes.                                  |

### `@absolutejs/sync/sqlite`

| Export                                                        | What it is                                                |
| ------------------------------------------------------------- | --------------------------------------------------------- |
| `sqliteChangelogSchema({ tables, changelogTable?, prefix? })` | SQL to install the changelog table + triggers (run once). |
| `createPollingChangeSource({ poll, ... })`                    | Tail the changelog (re-exported from the engine).         |

### `@absolutejs/sync/drizzle` and `@absolutejs/sync/prisma`

| Export                                                                | What it is                                                  |
| --------------------------------------------------------------------- | ----------------------------------------------------------- |
| `deriveReadTopics(table\|model, where?, options?)`                    | Topics a read depends on (`{ topics, rowLevel }`).          |
| `publishChange(hub, table\|model, { keys?, op? })`                    | Publish the table topic + a row topic per key.              |
| `publishRows(hub, table\|model, rows, { keyField?/keyColumn?, op? })` | Publish topics for returned/created records.                |
| `publishWhere(hub, table\|model, where, { ..., op? })`                | Publish topics for an update/delete filter.                 |
| `tableTopic` / `keyTopic`                                             | The shared topic vocabulary both sides speak.               |
| `prismaCollection({ name, where, find, ... })` (prisma)               | A sync-engine collection; one `where` → hydrate + matcher.  |
| `matchesWhere(where, row)` (prisma)                                   | Evaluate a Prisma `where` against a row (the matcher).      |
| `drizzleCollection({ name, table, where, find, ... })` (drizzle)      | Same one-`where`→hydrate+matcher, for Drizzle.              |
| `matchesDrizzleWhere(table, where, row)` (drizzle)                    | Evaluate a Drizzle SQL `where` against a row (the matcher). |

## License

MIT
