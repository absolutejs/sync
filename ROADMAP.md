# @absolutejs/sync — roadmap

The bet: a **reactive data / sync layer that stays on your own database and ORM**
(Postgres, MySQL, SQLite, Turso, Neon… via Drizzle _or_ Prisma), running on
Bun + Elysia — no proprietary backend, no lock-in. The thing Convex/Zero/Electric
can't offer because they own or replicate the database; the thing TanStack DB proves
is achievable as a _library_ rather than a backend.

## Positioning

| Engine               | Owns/replicates storage?            | Reactivity                          | BYO database                           |
| -------------------- | ----------------------------------- | ----------------------------------- | -------------------------------------- |
| Convex               | Yes (proprietary backend)           | auto read-set, row-level            | No                                     |
| Zero                 | Yes (Postgres replica `zero-cache`) | IVM, row-level                      | Postgres only                          |
| ElectricSQL          | Yes (Postgres logical replication)  | shapes, CRDT                        | Postgres only                          |
| TanStack DB          | No (client lib)                     | differential-dataflow IVM           | Yes (BYO writes)                       |
| LiveStore            | Client SQLite + event log           | event-sourced/materializers         | event-log                              |
| **@absolutejs/sync** | **No — your DB via the ORM**        | **explicit → table → row (staged)** | **Yes, any DB Drizzle/Prisma support** |

We sit closest to TanStack DB (a library, BYO backend) but server-first on
Bun/Elysia, integrated with the AbsoluteJS multi-framework SSR story and the existing
`createVoiceConnection`/SSE transport.

## Tier ladder (status)

- **Tier 1 — explicit topics — DONE.** `createReactiveHub` (topic pub/sub, prefix
  wildcards) + `live` Elysia SSE plugin + `createSyncSubscriber` browser client +
  `createWriteBehindCache` (in-memory hot path, write-behind persistence). You name
  the topics; mutations publish; subscribers refetch. Kills polling today.
- **Tier 2 — ORM auto-reactivity — DONE (library); voice validation pending.**
  Drizzle and Prisma adapters that _derive_ topics automatically: a read maps to
  `table` (or `table:key` for a simple primary-key equality); `publishChange` /
  `publishRows` / `publishWhere` publish the matching topics from a mutation. Coarse
  (table/key) granularity — over-invalidates a little, fine for ~95% of dashboards,
  DB-agnostic. Shipped as **subpaths** (`@absolutejs/sync/drizzle`,
  `@absolutejs/sync/prisma`) rather than a separate `-adapters` package — one repo,
  one version, extractable later. Client `createLiveQuery` wraps fetch + subscription
  (hydrate-once, refetch-on-event, supersede/reconnect/SSR-seed). Remaining: prove on
  the voice example's dashboards.
- **Tier 3 — sync engine MVP — READ + WRITE PATHS DONE.** Row-level reactive
  query results (predicate-matching IVM + collections + view syncer), a WebSocket
  diff transport (`syncSocket`), a client live-collection store
  (`createSyncCollection`), write-once predicate inference (Prisma), and
  optimistic mutations with reconciliation + offline queue (replay-on-reconnect +
  durable cross-reload storage). Plus CDC for out-of-band writes across **every
  target database** (Postgres `LISTEN/NOTIFY`, MySQL binlog or changelog poll,
  SQLite changelog poll — all behind one `ChangeSource` seam),
  incremental aggregations (`createAggregate`) and **incremental equi-joins**
  (`createEquiJoin` + `defineJoinCollection` — a change to either side moves only
  the affected pairs, with per-side access scoping), an end-to-end typed client via
  Eden + TypeBox (`hydrateRoute`/`mutateRoute` + `syncStore`), and a **version
  cursor** for resumable reconnects (catch-up diffs from a bounded change log).
  Plus a **general operator graph**: composable incremental operators
  (`filter`/`map`/`join`/`aggregate`/`orderBy`+limit over a keyed change stream)
  and a declarative `query(...).filter().map().join().groupBy().orderBy()` builder,
  run as live engine collections via `defineGraphCollection` — including live
  top-N, **left joins** (`selectUnmatched` / `query(...).leftJoin(...)`, keeping
  unmatched left rows and reverting when their last match leaves), and joining
  derived subqueries (a join's right input can be another query). The Tier 3
  engine frontier is now closed end to end; remaining work is ecosystem
  validation (voice flagship + a standalone sync example) and horizontal scale
  (a shared change-feed for multi-instance).

## Tier 3 MVP architecture (Bun + Elysia, BYO DB)

The achievable MVP, learning from Zero ("hydrate once, then push diffs") and TanStack
DB (differential-dataflow IVM in a client store):

### Components

1. **View syncer (server).** Per subscribed query: `hydrate` once from the durable
   store (via Drizzle/Prisma — any DB), then maintain the result set incrementally as
   changes arrive. Hold the materialized view in memory or in a per-connection
   `bun:sqlite` table (Bun's native SQLite makes server-side materialization cheap).
2. **IVM core.** Start with **predicate matching** for single-table filtered queries:
   on each changed row, evaluate the query's WHERE to decide enter/leave/update, then
   push a diff (`{added, removed, changed}`) — covers the large majority of real app
   queries. Graduate to **differential dataflow** (à la TanStack DB) for joins/
   aggregations later; keep the operator set explicit and small.
3. **Change source — two pluggable strategies:**
    - **Route mutations through us (MVP).** All writes go through a mutation API that
      applies to the durable store and emits the change feed. Works on _any_ DB
      immediately; misses out-of-band writes.
    - **CDC adapters (shipped).** Postgres `LISTEN/NOTIFY`, MySQL binlog _or_
      changelog poll, SQLite changelog poll — catch external writes too. One
      subpath per DB, all behind the `ChangeSource` seam (the only DB-specific
      surface). Postgres logical replication can drop in behind the same seam.
4. **Client store.** Normalized in-memory collections + the IVM engine for
   next-frame local query results; optional persistence to IndexedDB / WASM-SQLite
   for offline reads.
5. **Optimistic mutations + reconciliation.** Client applies a mutation locally
   against its store immediately, queues it, and reconciles when the server confirms
   (roll back the local delta if the authoritative result diverges) — Convex's
   server-reconciliation model, our transport.
6. **Offline queue.** Persist the pending-mutation queue; replay on reconnect.
7. **Transport.** Reuse the Tier 1 hub + SSE/WS; add a binary/JSON diff frame format.

### Consistency & conflicts

Server-authoritative with per-mutation ordering (a monotonic change-feed sequence);
optimistic rollback on divergence. A mutation commits **atomically**: all of its
changes share one version and reach each subscriber as a single net-merged diff
(add-then-remove of a row cancels), so no client renders a torn intermediate state.

Cross-table transactional consistency is not something we concede to engines that
own the database — it's something we **inherit from yours**. Run a mutation's
writes inside your DB's transaction; the engine buffers the change batch and emits
it only on commit, so subscribers get an all-or-nothing update at the isolation
level your database already enforces (serializable if you ask for it). Convex gives
you this by owning the store; we give you the same guarantee on the
Postgres/MySQL/SQLite you already run. Multi-writer offline merge (CRDT /
event-ordering, LiveStore-style) is the next frontier to take — not a ceiling.

### Access control

Sync must respect authorization: each synced query/collection declares a server-side
projection + row filter (à la Convex's programmable sync tables) so we never leak
rows a user can't read. This is mandatory for Tier 3, not optional.

## Biggest-win sequencing

- **M1 (done):** Tier 1 primitives + voice flagship (replace voice's polling widgets
  with reactive push; the polling is what exhausted the first Neon project).
- **M2 (library done):** Drizzle + Prisma topic adapters (auto table/key topics) +
  client `createLiveQuery`, shipped as subpaths. Remaining: prove on the voice
  example's dashboards.
- **M3 (done):** Tier 3 read path — predicate-matching IVM + collections + view
  syncer + WebSocket diff transport + client live-collection store + write-once
  predicate inference (Prisma). End-to-end verified.
- **M4 (done):** Tier 3 write path — server mutations (`defineMutation`/
  `runMutation`) + optimistic mutations, reconciliation (ack/reject), and
  replay-on-reconnect. (Cross-reload persistence: small follow-up.)
- **M5 (done):** CDC change sources across all target databases via the
  `ChangeSource` seam + `connectSource` — Postgres `LISTEN/NOTIFY`
  (`@absolutejs/sync/postgres`), MySQL binlog _or_ changelog-poll
  (`@absolutejs/sync/mysql`), and SQLite changelog-poll (`@absolutejs/sync/sqlite`),
  sharing one dependency-free `createPollingChangeSource` over an outbox table;
  incremental aggregations; incremental equi-joins; and a **general operator
  graph** — composable `filter`/`map`/`join`/`aggregate`/`orderBy` operators + a
  declarative `query` builder run as live collections (`defineGraphCollection`),
  covering filter → multi-join → group-by pipelines with inner **and left** joins.

## Risks / open questions

- **Granularity vs cost:** how far to push predicate matching before it's cheaper to
  just refetch (Tier 2 table-level) — measure.
- **Joins/aggregations:** differential dataflow is the answer but is the complex part;
  keep it behind M5 and a small operator set.
- **Memory:** server-held materialized views per connection — bound them, evict cold
  subscriptions, consider `bun:sqlite` spill.
- **Fan-out:** many subscribers to a hot query — share one materialization, push the
  same diff (Zero dedupes object updates this way).
- **Multi-instance:** the hub is in-process; horizontal scale needs a shared
  change-feed (Redis stream / Postgres `LISTEN`) — an adapter, not core.

## Prior art

- Convex sync engine — read-set tracking, OCC/MVCC, transaction log.
  https://stack.convex.dev/how-convex-works
- Zero (Rocicorp) — IVM "hydrate once, push diffs", Postgres replica.
  https://zero.rocicorp.dev/
- TanStack DB — client differential-dataflow IVM, BYO backend.
  https://tanstack.com/blog/tanstack-db-0.5-query-driven-sync
- ElectricSQL — Postgres logical replication, shapes.
  https://electric-sql.com/
- LiveStore — event-sourced SQLite-on-client.
- Triplit — batteries-included sync + offline.
