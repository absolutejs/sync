# @absolutejs/sync

Reactive data primitives for [Elysia](https://elysiajs.com) and the AbsoluteJS
ecosystem — kill polling and keep a remote store off your hot path, **on your own
database and ORM** (Drizzle _or_ Prisma, any DB they support).

## Platform-managed runtime

Platforms can supply bounded Sync lifecycle settings without inventing their
own environment parser or plugin assembly. Declare one
`ABSOLUTE_SYNC_RUNTIME` JSON value, then let the application register its own
collections, permissions, writers, and authentication on the returned runtime:

```ts
import { Elysia } from 'elysia';
import { createPlatformSyncRuntime } from '@absolutejs/sync/platform';
import { createCommentsPack } from '@absolutejs/sync-pack-comments';
import { createPostgresClusterBus } from '@absolutejs/sync-bus-pg';

const managedSync = createPlatformSyncRuntime({
	clusterBus: createPostgresClusterBus({ sql }),
	packs: [createCommentsPack({ store: commentsStore })],
	resolveContext: (request) => authenticate(request)
});

await managedSync.ready;
new Elysia().use(managedSync.app).listen(3000);

// During graceful shutdown:
await managedSync.dispose();
```

The platform contract controls paths, reconnect history, mutation pressure,
slow-client behavior, and stable instance identity. A fixed
`/.well-known/absolute/sync` endpoint lets the runtime host fail readiness when
an opted-in release forgot to mount the returned app, or while its declared
cluster bus is disconnected. Its response reports the connected cluster posture
and registered packs, so operators can verify the application assembled the
capabilities it declared. The platform never owns
application schemas or authorization rules, so the project remains portable
and can construct `@absolutejs/sync` explicitly outside managed hosting.

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
`orderBy({ limit, offset })` accepts either fixed numbers or functions of the
subscription params/context, so each subscriber can hold a different live page.

> Status: **1.0** — public API frozen across all subpaths. See
> [`CHANGELOG.md`](./CHANGELOG.md). Tier 1 (hub, SSE plugin, browser subscriber,
> write-behind cache), Tier 2 (Drizzle + Prisma topic adapters, `createLiveQuery`),
> and Tier 3 (sync engine: collections, WebSocket diff transport, optimistic
> mutations + offline queue, a local-first client cache, declarative row-level
> permissions, schema validation + lazy migrations, live full-text + vector
> search, scheduled functions, a live devtools dashboard, conflict-free
> collaborative editing (CRDTs), CDC for Postgres/MySQL/SQLite, incremental
> aggregations + joins, and a declarative operator graph) are in place.
> Operator-grade primitives also shipped — **point-in-time replay**
> (`engine.replayTo`, 1.22; clickable Replay panel in `syncDevtools`, 1.23) and
> **tenant migration** (`engine.fence` / `exportSnapshot` / `importSnapshot`,
> 1.24). Everything ships as subpaths of this one package. See
> [Substrate complete (G1–G7)](https://absolutejs.com/documentation/substrate-audit)
> for the consolidated record of the seven cross-cutting substrate gaps and
> what closed each.

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
import { createSyncSocketController, syncSocket } from '@absolutejs/sync';
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

const socketController = createSyncSocketController();

new Elysia()
	.use(
		syncSocket({
			controller: socketController,
			engine,
			resolveContext: (data) => ({ userId: data.userId })
		})
	)
	.listen(3000);

// After a blue-green load-balancer switch, permanently drain the old slot.
// Current and late sockets receive RFC 6455 code 1012 and reconnect cleanly.
socketController.drain();
```

Browser and installed-app clients that cannot set an `Authorization` header on
the WebSocket upgrade can authenticate with a short-lived one-time ticket. The
ticket is fetched over authenticated HTTPS, sent as the first socket frame, and
never appears in a URL. `@absolutejs/auth` supplies the issuer/store/consumer;
Sync owns only the transport handshake.

```ts
const client = createSyncClient({
	url: 'wss://app.example/sync/ws',
	socketTicket: () => mobileAuth.socketTicket('https://app.example/sync')
});

syncSocket({
	authenticate: async (ticket) => {
		const principal = await consumeSocketTicket({
			audience: 'https://app.example/sync',
			getUser,
			store: socketTicketStore,
			ticket
		});
		if (!principal) throw new Error('Invalid socket ticket');
		return principal;
	},
	engine
});
```

Each reconnect invokes `socketTicket` again. The server rejects malformed,
expired, replayed, or missing tickets with close code `4401`; unauthenticated
sockets also have a bounded first-frame timeout.

AbsoluteJS native shells install the ticket supplier through Sync's scoped
client runtime registry. Existing `createSyncClient({ url })` calls therefore
send a fresh one-time ticket on every native connection without putting a token
in the URL or requiring application changes. An explicit `socketTicket` option
always takes precedence, and browser/server runtimes remain unchanged.

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

TanStack DB can own the client-side collection graph while Absolute Sync handles
the live transport:

```ts
import { createCollection } from '@tanstack/db';
import { createSyncTanStackCollectionOptions } from '@absolutejs/sync/tanstack-db';

type Order = { id: string; total: number; status: string };

const orders = createCollection(
	createSyncTanStackCollectionOptions<Order>({
		id: 'orders',
		url: 'ws://localhost:3000/sync/ws',
		collection: 'orders',
		getKey: (order) => order.id,
		mutations: {
			insert: 'createOrder',
			update: 'updateOrder',
			delete: 'deleteOrder'
		}
	})
);
```

- **Incremental vs refetch.** A single-table filtered collection is matched
  incrementally (only the changed rows move). Joins/aggregations and filters the
  matcher can't evaluate fall back to a correct re-hydrate. `createAggregate`
  (`/engine`) maintains `count`/`sum`/`avg`/`min`/`max` + `groupBy` incrementally.
- **Out-of-band writes.** Writes that bypass mutations are caught by a
  `ChangeSource` — e.g. `postgresChangeSource` (`/postgres`) over `LISTEN/NOTIFY`,
  wired with `engine.connectSource(...)` and the trigger SQL from
  `postgresNotifyTrigger`.
- **Offline & local-first.** Pending mutations replay on reconnect; pass `storage`
  (e.g. `localStorageMutationStorage`) to let unconfirmed writes survive a reload.
  Pass `cache` (`localStorageCollectionCache` or `indexedDbCollectionCache`) to
  persist the confirmed rows too — reads are then instant on reload and available
  offline, and the socket resumes from the cached version (a catch-up diff if the
  server's changelog still covers it, a fresh snapshot otherwise).
- **Access control is mandatory.** Each collection's `authorize` gates subscribe and
  its filter scopes rows, so a change to a row a caller can't see never reaches them.
- **Declarative permissions.** Instead of restating a row filter across `authorize`,
  `hydrate`, and `match`, register row-level rules once with `definePermissions` and
  the engine enforces them: `read` rules filter every row emitted (initial snapshot,
  incremental diff, catch-up, one-shot hydrate, and a reactive query's `ctx.db`
  reads); `insert`/`update`/`delete`/`write` rules gate the mutation actions. For
  `update`/`delete` the rule is checked against the _existing_ row (loaded via the
  table's reader), so it can't be spoofed by the client payload.

    ```ts
    const engine = createSyncEngine({
    	permissions: definePermissions<{ userId: number }>({
    		tasks: {
    			read: (ctx, row) => row.userId === ctx.userId, // see only your rows
    			write: (ctx, row) => row.userId === ctx.userId // touch only your rows
    		}
    	})
    });
    ```

- **Live search.** A `defineSearchCollection` is a full-text or vector index kept
  live from a table's change feed. The subscription's `params` are the query (a
  string for keyword search, an embedding for similarity); the ranked top-K stream
  back as an ordinary collection and re-rank as rows change. Read permissions on
  the source table still scope a caller's hits. Standalone, `createTextIndex` and
  `createVectorIndex` are reusable (e.g. RAG retrieval with `@absolutejs/rag`).

    ```ts
    // server
    engine.registerSearch(
    	defineSearchCollection<Doc>({
    		name: 'docSearch',
    		table: 'docs',
    		index: () =>
    			createTextIndex({
    				key: (d) => d.id,
    				fields: ['title', 'body']
    			}),
    		source: () => db.select().from(docs), // the corpus to index
    		key: (d) => d.id
    	})
    );

    // client — params are the query; each result row carries `_score`
    const results = createSyncCollection<Doc>({
    	url,
    	collection: 'docSearch',
    	params: 'quick brown fox' // a vector for createVectorIndex
    });
    ```

- **Scheduled functions.** Register server-side work that runs on a cron pattern;
  whatever it writes via `ctx.actions` goes live through the change feed (and it can
  read current state via `ctx.db`). Cron decides _when_ (via `@elysiajs/cron`, an
  optional peer); the engine makes the effect _live_. It doesn't reinvent jobs —
  for durable, retryable work a schedule can `enqueue` into
  [`@absolutejs/queue`](https://github.com/absolutejs/queue).

    ```ts
    import { scheduled } from '@absolutejs/sync/scheduled';

    engine.registerSchedule({
    	name: 'digest',
    	pattern: '0 8 * * 1', // Mondays 08:00 (6-field for seconds: '*/5 * * * * *')
    	run: async ({ db, actions }) => {
    		const stale = await db.all('reports');
    		await actions.insert('digests', {
    			id: crypto.randomUUID(),
    			at: Date.now()
    		});
    		// or: queue.enqueue('email.send', { … }) for durable delivery
    	}
    });

    new Elysia().use(syncSocket({ engine })).use(scheduled({ engine })); // wires cron
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

## Connection broker — one upstream pool, many tenants

BYO-Postgres multi-tenancy has a sharp edge: one shard hosting 50 customers, each
spawning its own PG pool, instantly exceeds a managed provider's connection limit
(Supabase, Neon, RDS all cap you long before 50 × pool-size). `createConnectionBroker`
multiplexes ONE upstream connection budget across every tenant on the shard — a
global in-use cap, optional per-tenant budgets, FIFO queueing with timeouts when at
cap, and idle harvesting so a burst doesn't pin connections forever. No pgbouncer
sidecar to run.

The broker never imports a DB driver — `create`/`destroy` are yours, so it wraps
postgres-js, `Bun.sql`, or anything that opens a connection:

```ts
import postgres from 'postgres';
import { createConnectionBroker } from '@absolutejs/sync';

const broker = createConnectionBroker({
	create: () => postgres(process.env.DATABASE_URL!, { max: 1 }),
	destroy: (sql) => sql.end(),
	maxTotal: 20, // the whole shard shares 20 upstream connections
	maxPerTenant: 3, // one noisy tenant queues against its own budget
	idleReleaseMs: 30_000, // harvest idle connections after a burst
	acquireTimeoutMs: 5_000, // reject (LeaseTimeoutError) instead of hanging
	validate: (sql) => sql`select 1`.then(() => true).catch(() => false)
});

// Per-tenant work leases from the shared budget:
const rows = await broker.withLease(
	tenantId,
	(sql) => sql`select * from orders where tenant = ${tenantId}`
);

// Or hold a lease across several statements:
const { conn, release } = await broker.lease(tenantId);
try {
	/* ... */
} finally {
	release(); // idempotent; hands the connection to the next queued waiter
}

// Shutdown: stop new leases, wait for outstanding work, close idle connections.
await broker.drain();
await broker.dispose();
```

Released connections return to a LIFO idle pool (the warmest connection is reused
first) and are handed to queued waiters immediately. `broker.metrics()` returns an
operator-shaped snapshot (`inUse`, `idle`, `queued`, `byTenant`, cumulative
lease/timeout/create/destroy counters), and a `tracerProvider` traces every lease as
a `sync.broker_lease` span carrying `abs.tenant` and the queue wait in milliseconds.

This pairs with the tenant-per-engine hosting pattern: each tenant's writers and
readers call `broker.withLease(tenant, ...)` instead of owning a pool, so adding a
tenant to a shard adds zero standing connections.

## API

### `@absolutejs/sync`

| Export                                                                                                                                                 | What it is                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `createReactiveHub()`                                                                                                                                  | In-memory topic pub/sub (`publish`, `subscribe`, `subscriberCount`).                                                                                                                                                                                                                                                                                         |
| `sync({ hub, path?, resolveTopics?, heartbeatMs? })`                                                                                                   | Elysia plugin: SSE stream of hub events.                                                                                                                                                                                                                                                                                                                     |
| `syncSocket({ engine, path?, resolveContext?, authenticate? })`                                                                                        | Elysia WebSocket plugin for the sync engine. `authenticate` requires a first-frame one-time ticket and returns the per-connection authorization context.                                                                                                                                                                                                     |
| `createSyncSocketController()`                                                                                                                         | Host control plane for blue-green drains. Pass it as `syncSocket({ controller, engine })`, then call `controller.drain()` on the old slot to close current and late sockets with code 1012 while clients reconnect through the switched load balancer.                                                                                                       |
| `scheduled({ engine, prefix?, onError? })` _(`/scheduled` subpath)_                                                                                    | Elysia plugin: fires the engine's registered schedules on their cron patterns (via `@elysiajs/cron`). Kept off the main entry so `syncSocket` needs no cron dep.                                                                                                                                                                                             |
| `syncDevtools({ engine, path?, snapshotMs? })`                                                                                                         | Elysia plugin: a live devtools dashboard (collections, subscription counts, mutations, schedules, change feed) over SSE. Backed by `engine.inspect()` + `engine.onActivity()`. **1.23** also exposes a Point-in-time replay panel (datetime picker + tables filter) and a `GET <path>/replay?at=<ms>&tables=<csv>` JSON endpoint wrapping `engine.replayTo`. |
| `createWriteBehindCache({ load, persist, remove?, debounceMs?, evict?, onPersistError? })`                                                             | In-memory cache + write-behind persistence.                                                                                                                                                                                                                                                                                                                  |
| `createConnectionBroker({ create, maxTotal, maxPerTenant?, idleReleaseMs?, acquireTimeoutMs?, validate?, destroy?, onError?, now?, tracerProvider? })` | Multiplex one upstream connection budget across many tenants: `lease`/`withLease`/`metrics`/`drain`/`dispose`. Throws `LeaseTimeoutError` / `ConnectionBrokerDrainedError` (both exported).                                                                                                                                                                  |

### `@absolutejs/sync/client`

| Export                                                         | What it is                                                                                                                                                                                                                                                             |
| -------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `createSyncSubscriber({ topics, onEvent, url? })`              | Browser SSE client.                                                                                                                                                                                                                                                    |
| `createLiveQuery({ topics, fetcher, ... })`                    | Hydrate-once, refetch-on-event observable query store.                                                                                                                                                                                                                 |
| `jsonFetcher(url, init?)`                                      | Default `fetcher`: GET + JSON parse, forwards the abort signal.                                                                                                                                                                                                        |
| `createSyncCollection({ url, collection, ... })`               | Live diff-driven collection store with optimistic `mutate`.                                                                                                                                                                                                            |
| `createSyncClient({ url, socketTicket? })`                     | One socket, many collections (`client.collection(...)`). Fetches a fresh one-time ticket for every connection/reconnect when configured, before sending subscriptions. Applies a multi-collection mutation's diffs as one **consistent frame**.                        |
| `createPresence({ url, room, state })`                         | Join a presence room: see who's online / typing (`get` + `subscribe`) and publish your own state (`set`).                                                                                                                                                              |
| `createCollaborativeText({ url, collection, id, field, ... })` | Live CRDT collaborative-text controller (`get`/`subscribe`/`setText`/`close`): tracks a row's CRDT field, merges remote edits into a local replica, and broadcasts via the engine's `"<collection>:merge"` mutation. Backs the `useCollaborativeText` framework hooks. |
| `localStorageMutationStorage(key)`                             | `localStorage`-backed offline write queue for `createSyncCollection`.                                                                                                                                                                                                  |
| `localStorageCollectionCache(key)`                             | `localStorage`-backed local-first read cache: confirmed rows survive a reload, resume from the cached version.                                                                                                                                                         |
| `indexedDbCollectionCache({ key, ... })`                       | IndexedDB-backed local-first read cache — durable, large-capacity. Same resume semantics, async storage.                                                                                                                                                               |
| `createMemorySyncLocalStore()`                                 | Reference implementation of the next-generation transactional local store: principal namespaces, installation identity, confirmed rows/cursors, and a serializable operation outbox commit atomically.                                                                 |
| `createIndexedDbSyncLocalStore()`                              | Browser/PWA implementation of the same transaction contract. Multi-collection frames, confirmed cursors, installation identity, and the operation outbox commit atomically in IndexedDB.                                                                               |
| `ensureSyncInstallationId(store, namespace)`                   | Atomically creates or returns the stable installation identity used by `createSyncOperationId`.                                                                                                                                                                        |

The multiplexed client has an additive durable profile. The namespace must come
from trusted Auth state and must change when the active account or tenant changes.
Serializable optimistic operations survive reload/process death; the historical
callback form remains available but is intentionally process-local.

```ts
const client = createSyncClient({
	url: 'wss://app.example/sync/ws',
	durable: {
		store: createIndexedDbSyncLocalStore(),
		namespace: principal.cachePartition
	}
});

const orders = client.collection<Order>({ collection: 'orders' });
await orders.mutate({
	name: 'orders:create',
	args: { total: 42 },
	optimisticOperations: [
		{ type: 'insert', row: { id: temporaryId, total: 42 } }
	]
});
```

The server must configure `createSyncEngine({ durableMutations })`; durable
clients reject acknowledgments that do not echo the exact operation ID. Database
writes and receipt results share the adapter transaction. HTTP/email/payment
effects must still be handed to a transactional outbox rather than performed
directly in a replayable mutation.

### Framework bindings — `@absolutejs/sync/{react,vue,svelte,angular}`

Idiomatic wrappers over `createSyncCollection`, one per framework, so a live
collection is one call. Each returns the same `{ data, status, error, mutate }`
and is SSR-safe (the socket opens on the client only). Each also ships a
**collaborative-text** binding over `createCollaborativeText` — a CRDT shared text
field in one call (`text`/`setText`/`status`).

| Subpath    | Collection                               | Collaborative text                              |
| ---------- | ---------------------------------------- | ----------------------------------------------- |
| `/react`   | `useSyncCollection(options)`             | `useCollaborativeText(options)`                 |
| `/vue`     | `useSyncCollection(options)`             | `useCollaborativeText(options)`                 |
| `/svelte`  | `createSyncCollectionStore(options)`     | `createCollaborativeTextStore(options)`         |
| `/angular` | `SyncCollectionService.connect(options)` | `SyncCollectionService.collaborativeText(opts)` |

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

| Export                                                                                   | What it is                                                                                                                                                                                                                                                                                                                                      |
| ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `createSyncEngine()`                                                                     | Registry + view syncer: `register`, `subscribe`, `applyChange`, `connectSource`, `registerMutation`, `registerWriter`, `runMutation`.                                                                                                                                                                                                           |
| `createSyncEngine({ durableMutations })`                                                 | Server-owned replay safety: derive the receipt scope from authenticated context and atomically commit an operation receipt with table writes. A repeated `operationId` returns the stored result without re-running the handler. External effects still belong in a transactional outbox.                                                       |
| `defineCollection({ name, hydrate, key?, match?, authorize?, tables? })`                 | Define a syncable collection.                                                                                                                                                                                                                                                                                                                   |
| `defineMutation({ name, handler, authorize? })`                                          | Define a server mutation. Its `handler` gets `actions.insert/update/delete` (write through a registered `TableWriter` → persists + emits in one step) plus `actions.change` (escape hatch). Changes commit atomically.                                                                                                                          |
| `registerWriter(table, { insert, update, delete })`                                      | Teach the engine how to persist a table (any ORM), so writes auto-emit — you can't write without going live.                                                                                                                                                                                                                                    |
| `createAggregate({ key, groupBy?, value? })`                                             | Incremental count/sum/avg/min/max by group.                                                                                                                                                                                                                                                                                                     |
| `createMaterializedView({ key, match, equals? })`                                        | The predicate-matching IVM primitive (`apply`/`reset` → diffs).                                                                                                                                                                                                                                                                                 |
| `createPollingChangeSource({ poll, intervalMs?, startSeq?, onProcessed? })`              | DB-agnostic CDC `ChangeSource` that tails a changelog (outbox) table.                                                                                                                                                                                                                                                                           |
| `engine.connectCluster(bus)` + `createInMemoryClusterBus()`                              | Horizontal scale: fan changes across server instances over a `ClusterBus` (BYO Redis/Postgres; in-memory bus for dev).                                                                                                                                                                                                                          |
| `createPresenceHub()` + `syncSocket({ engine, presence })`                               | Ephemeral room-scoped presence (online / typing / cursors) over the same socket — not persisted, auto-cleaned on disconnect.                                                                                                                                                                                                                    |
| `query(source).filter().map().join().leftJoin().groupBy().orderBy()`                     | Declarative incremental query builder (the operator graph).                                                                                                                                                                                                                                                                                     |
| `defineGraphCollection({ name, query, key, authorize? })`                                | Run a `query` as a live collection.                                                                                                                                                                                                                                                                                                             |
| `defineReactiveQuery({ name, run, key })` + `registerReactive` / `registerReader`        | Read-set-tracked query: `run(ctx)` reads via `ctx.db` (`all`/`get`/`where`) and re-runs only when the rows/ranges it read change — no `match`, no manual emit.                                                                                                                                                                                  |
| `definePermissions({ [table]: { read?, insert?, update?, delete?, write? } })`           | Declarative row-level access control. Pass as `createSyncEngine({ permissions })` or `registerPermissions(table, rules)`. Read rules filter every row emitted; write rules gate `actions.insert/update/delete`.                                                                                                                                 |
| `defineSchema({ [table]: { fields, version?, migrate? } })` + `field` kit                | Declarative row schema. Pass as `createSyncEngine({ schemas })` or `registerSchema(table, schema)`. Writes are validated (bad write → `SchemaError`); `migrate` lazily upcasts rows on read (no DB migration needed).                                                                                                                           |
| `registerCrdt(table, { [field]: mergeable })`                                            | Declare CRDT fields (a `CrdtMergeable` like `rgaText`, or `yjsText` from `@absolutejs/sync-yjs`). The engine **merges** those fields on `actions.insert/update` instead of overwriting — conflict-free collaborative editing with no merge code — and auto-registers a `"<table>:merge"` mutation the `useCollaborativeText` hooks call.        |
| `defineSearchCollection({ name, table, index, source, key, limit? })` + `registerSearch` | Live search collection: the subscription's `params` are the query (string/vector), the ranked top-K stream back as a normal collection, re-ranked as rows change. Each row carries its score under `_score`.                                                                                                                                    |
| `createTextIndex({ key, fields, tokenize?, stopwords?, k1?, b? })`                       | Incremental BM25 full-text index (keyword search). Implements `SearchIndex`; usable standalone or inside a search collection.                                                                                                                                                                                                                   |
| `createVectorIndex({ key, embedding, metric? })`                                         | Incremental vector index (cosine/dot/euclidean exact k-NN) for semantic search — pairs with `@absolutejs/ai` / `@absolutejs/rag` for RAG retrieval on your own data.                                                                                                                                                                            |
| `defineSchedule({ name, pattern, run })` + `registerSchedule` / `runSchedule`            | Scheduled function: `run({ db, actions })` fires on a cron `pattern`; its writes go live through the change feed. Wire triggers with the `scheduled` plugin (or call `runSchedule(name)` on demand).                                                                                                                                            |
| `engine.replayTo({ at, tables? })` _(1.22)_                                              | Walk the bounded change log forward to a target timestamp and return `{ asOfVersion, asOfAt, rows, truncated }`. Forensic incident response ("what did the tenant see at 14:32?") + restore-from-time ("revert to 2 hours ago"). `truncated: true` when the log doesn't extend back to `at` — widen `changeLogRetainMs` for forensic use cases. |
| `engine.fence({ reason })` _(1.24)_                                                      | Pause new mutations on the engine — the source half of tenant migration. `runMutation` rejects with `EngineFencedError`; subscribe / hydrate / streamChanges stay open. Multiple fences compose; every handle must `lift()` before the engine unfences. `lift()` is idempotent.                                                                 |
| `engine.exportSnapshot({ tables?, ctx? })` _(1.24)_                                      | Walk every registered reader's `all(ctx)` and return a portable `EngineSnapshot { sourceInstanceId, version, exportedAt, tables }`. Detached from `ChangeLogSnapshot` — snapshots carry live state, not history.                                                                                                                                |
| `engine.importSnapshot(snapshot, { tables?, onProgress?, ctx? })` _(1.24)_               | Bulk-load an `EngineSnapshot` on the target via each table's registered writer. Returns `{ tablesImported, rowsImported, perTable, skipped }`. Tables in the snapshot without a writer on the target surface in `skipped` so misconfigured targets don't silently drop rows.                                                                    |

### `@absolutejs/sync/crdt`

Conflict-free replicated data types — pure, **zero-dependency**, and isomorphic (same code client and server). They merge concurrent edits from different tabs/devices without a server round-trip per keystroke and without clobbering: every `merge` is commutative, associative, and idempotent, so replicas converge no matter the order. They ride the existing engine with no engine changes — store the CRDT state as a row field. The declarative path is one line each end: server `engine.registerCrdt(table, { field: rgaText })` (auto-merges that field on write), client `useCollaborativeText({ collection, id, field, url })`. The primitives below are also usable directly.

| Export                                       | What it is                                                                                                                                                                                                                                                                                                                                                                                          |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `counter`                                    | PN-counter: `create/value/increment/decrement/merge`. Concurrent increments and decrements across replicas all survive.                                                                                                                                                                                                                                                                             |
| `lww`                                        | Last-write-wins register: `create/set/merge`. The latest timestamp wins (replica id breaks ties) — for "just take the newest value" fields.                                                                                                                                                                                                                                                         |
| `orSet`                                      | Observed-remove set: `create/add/remove/has/values/merge`. Concurrent add/remove resolves **add-wins** (each add gets a unique tag; remove retracts only observed tags) — for collaborative tags/labels/memberships.                                                                                                                                                                                |
| `lwwMap`                                     | Last-write-wins map: `create/set/get/delete/has/keys/entries/merge`. Each key is an independent LWW register; `delete` is a tombstone that can lose to a later concurrent `set` — for collaborative key→value records.                                                                                                                                                                              |
| `createList(replica, initial?)`              | Ordered list CRDT (the RGA over arbitrary items): `list/insert/delete/merge/state/takeDelta` (+ `listOf`/`mergeListState`). Concurrent inserts/deletes at any position merge and converge — for collaborative reorderable lists.                                                                                                                                                                    |
| `createTextCrdt(replica, initial?)`          | Collaborative text (an RGA sequence CRDT): `text/insert/delete/setText/merge/state` + `takeDelta` + `anchorAt`/`indexOfAnchor`. `takeDelta()` returns just this client's new ops (delta-state) so uploads are O(edit), not O(doc); `anchorAt`/`indexOfAnchor` give a caret a stable element-id anchor for collaborative cursors that survive concurrent edits. Concurrent edits merge and converge. |
| `textOf(state)` / `mergeTextState(a, b)`     | Pure helpers for the text state — use them server-side (e.g. a merge-on-write mutation) with no live instance.                                                                                                                                                                                                                                                                                      |
| `compact(state)` / `tombstoneCount(state)`   | Bound state growth: `compact` drops tombstones no live text anchors to (visible text unchanged); `tombstoneCount` is the metric to decide when. Run server-side on the stored state past a threshold; clients adopt the compacted state on the next broadcast.                                                                                                                                      |
| `CrdtText<State>` / `TextCrdtAdapter<State>` | The pluggable collaborative-text contract. `rgaText` is the first-party (zero-dep) backend; swap in an adapter from the `sync-adapters` repo (e.g. `@absolutejs/sync-yjs`, which wraps the Yjs staple) behind the same call sites.                                                                                                                                                                  |

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

## Benchmarks

Run `bun run bench/run.ts`. Highlights (Bun 1.3, full results + methodology in
[`docs/benchmarks.md`](./docs/benchmarks.md)):

- **Delta uploads scale flat.** One keystroke on a 10,000-char doc: a full-state
  upload is ~877 KB; the delta is ~105 bytes — an **8,350×** reduction (and ~84×
  even at 100 chars). The server keeps full state, so late joiners still hydrate
  in one shot.
- **~50,000 mutations/sec** (write + emit) locally; diff fan-out is linear in
  subscriber count.
- **Tombstone compaction** halves a delete-heavy document's stored state.

`docs/benchmarks.md` also has an architectural comparison with Convex and Zero —
the short version: live queries, optimistic writes, and conflict-free editing
**without adopting a new backend** (it rides your own DB/ORM/server).

## License

MIT
