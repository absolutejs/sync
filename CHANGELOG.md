# Changelog

All notable changes to `@absolutejs/sync` are recorded here. The format is loosely
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
follows [Semantic Versioning](https://semver.org) from 1.0 onward.

## [1.6.0] — 2026-05-27

### Added

- **`engine.streamChanges({ since, signal, maxBuffer })`** — outbound CDC
  stream. Returns an `AsyncIterable<LoggedChange>` that yields historical
  log entries (where `version > since`) first, then tails live commits as
  they happen. Notify-driven (no polling): the iterator parks on a Promise
  that resolves the instant a new commit lands. Use it to feed Kafka /
  NATS / search indexers / audit pipelines / analytics warehouses from
  the engine.

    ```ts
    let cursor = lastCursorFromStorage();
    for await (const entry of engine.streamChanges({ since: cursor, signal })) {
    	await kafka.send('sync.changes', JSON.stringify(entry));
    	cursor = entry.version;
    	await persist(cursor);
    }
    ```

    - If `since` is older than the oldest entry retained in the bounded
      change log, the iterator throws `MissedChangesError` so the consumer
      notices the gap (versus silently dropping commits). Re-bootstrap
      from a fresh hydrate and resume from `availableSince`.
    - If the consumer iterates slower than the engine commits and the
      in-flight buffer overflows (`maxBuffer`, default 10000), the iterator
      throws `CdcConsumerSlowError`. Resubscribe with the last cursor.
    - Multiple concurrent streams work independently; each gets every
      entry exactly once in version order.

- **`syncCdc({ engine, path })` Elysia plugin** — exposes
  `streamChanges` as a Server-Sent Events route (defaults to
  `/sync/cdc`). Each entry becomes one SSE event with `id`, `event:
change`, and the JSON-serialized `LoggedChange` as `data`. Consumers
  resume via `?since=<version>` query param or the `Last-Event-ID`
  header that browser `EventSource` sets on reconnect. Errors
  (`MissedChangesError`, `CdcConsumerSlowError`, or anything else) come
  through as `event: error` SSE events so the client can distinguish
  them from changes.

    ```ts
    import { syncCdc } from '@absolutejs/sync';
    new Elysia().use(syncSocket({ engine })).use(syncCdc({ engine }));
    ```

    New exports from `@absolutejs/sync` and `@absolutejs/sync/engine`:
    `syncCdc`, `SyncCdcOptions`, `LoggedChange`, `StreamChangesOptions`,
    `MissedChangesError`, `CdcConsumerSlowError`.

### Changed

- **CDC adapters: `onSkip` hook for silently-dropped events.** The
  existing CDC sources (`postgresChangeSource`,
  `mysqlBinlogChangeSource`, `createPollingChangeSource`) used to drop
  malformed payloads / unknown event types / parse-failed rows
  silently. They now accept an optional `onSkip` callback so you can
  log skips and detect oversized rows (PG `NOTIFY` truncates past
  8000 bytes), new MySQL event types, or malformed outbox rows
  before they become a "where are my changes?" mystery. Defaults to
  the previous silent behaviour.

## [1.5.0] — 2026-05-27

### Added

- **OCC retry for mutations.** `defineMutation` gains an optional
  `retry: RetryPolicy` (the type has been there since 1.3 but the loop
  is now wired). When a handler throws a classified-as-retryable error
  (default: PG `40001` / `40P01`), the engine discards the buffered
  changes, waits a backoff, and re-runs the handler in a fresh
  transaction. The number-of-attempts ceiling and the time-budget
  ceiling both apply.

    ```ts
    defineMutation({
    	name: 'transfer',
    	retry: {
    		maxAttempts: 5,
    		backoff: exponentialBackoff({ baseMs: 25, maxMs: 1_000 }),
    		isRetryable: isSerializationFailure, // default
    		maxElapsedMs: 30_000 // default
    	},
    	handler: async (args, ctx, actions) => { ... }
    });
    ```

    Each attempt builds fresh `actions` / `buffered` from `makeActions`,
    so a retry never inherits half-applied buffered changes from a
    failed attempt. Transactions reopen per attempt under
    `runInTransaction`. Handlers MUST be idempotent under retry —
    external side effects (HTTP, email) will fire more than once.

    On exhaustion the engine throws a `RetriesExhaustedError` whose
    `.cause` is the underlying error and whose `.attempts` /
    `.elapsedMs` describe the run. A non-retryable first-attempt
    failure passes through with its original error preserved, even if
    `retry` is configured.

    New exports from `@absolutejs/sync/engine`: `RetryPolicy`,
    `exponentialBackoff`, `isSerializationFailure`,
    `RetriesExhaustedError`, `ExponentialBackoffOptions`.

- **`mutationRetry` engine activity event.** Between attempts the
  engine emits a new event shape on `onActivity(...)` so dashboards
  and observability sinks can see retries happen:

    ```ts
    {
    	type: 'mutationRetry',
    	at: number,
    	name: string,
    	attempt: number, // the attempt that just failed (1-indexed)
    	delayMs: number,
    	errorName: string,
    	errorMessage: string,
    }
    ```

    The final `mutation` event (`status: 'ok'` or `'error'`) still fires
    exactly once per call.

## [1.4.0] — 2026-05-27

### Added

- **Sandboxed mutation handlers.** `defineMutation` now accepts a
  `sandboxedHandler: string` (mutually exclusive with `handler`) that runs
  inside an [`@absolutejs/isolated-jsc`](https://github.com/absolutejs/isolated-jsc)
  Isolate — a separate JavaScriptCore VM with its own heap. Per-mutation
  `sandbox: { memoryLimit, timeout }` caps CPU/memory. Use for
  multi-tenant PaaS handlers, plugin systems, AI-generated code, or as
  defense-in-depth on first-party logic.

    ```ts
    defineMutation({
    	name: 'transfer',
    	sandbox: { memoryLimit: 32, timeout: 1000 },
    	sandboxedHandler: `async (args, ctx, actions) => {
    		// Runs in a fresh JSC heap. Only args, ctx, and actions are reachable.
    		await actions.update('accounts', { id: args.from, balance: ... });
    		await actions.update('accounts', { id: args.to, balance: ... });
    	}`
    });
    ```

    First call per mutation pays a Worker spawn + compile (~30 ms); every
    subsequent call reuses the isolate (~0.5 ms cold-context spin-up). Timeout
    terminates the isolate; the next call transparently re-spawns.
    `@absolutejs/isolated-jsc` is an **optional** peer dependency — install
    only if you use `sandboxedHandler`.

- **Registration validation.** `registerMutation` now throws if a
  definition has neither `handler` nor `sandboxedHandler`, or if it has
  both. (Previously the missing-handler case crashed at first invocation
  with a less helpful error.)

## [1.3.0] — 2026-05-27

### Added

- **Cross-client reactive query cache.** Subscriptions sharing the same
  `(collection, params, ctx)` now reuse a cached snapshot on initial
  subscribe instead of each re-running the query body. 1.1 deduped reruns
  within a single write batch; 1.3 lifts that sharing _across_ batches.
  Behaviour, in one line: with N fresh subscribers to the same query, the
  query body runs **once** at the first subscribe; subscribers 2…N hit
  the cache. An overlapping write invalidates the entry (same
  `isReactiveAffected` check live subs already use), and the rerun fired
  by that write refreshes the cache so the next subscriber is a hit
  again.

    Configurable via the new `reactiveCache` option on `createSyncEngine`:

    ```ts
    createSyncEngine({
    	reactiveCache: {
    		max: 256, // LRU bound (default 256). 0 disables the cache.
    		ttlMs: 60_000 // TTL (default 60s). 0 disables the TTL.
    	}
    });
    ```

    Defaults are bounded by design — no engine should leak memory on a
    query that's never re-subscribed. Different `ctx` references stay
    isolated (per-user query bodies are unaffected).

    This is the same pattern Convex uses to coalesce queries across all
    online clients ("every specific combination of (query code, parameters,
    database read set) executes only once"). Sync's read-set tracking +
    stable sub-key were already there; this PR just lifts the existing
    per-batch `sharedRuns` map to a persistent one with invalidation +
    bounded eviction.

    4 new tests in `tests/reactiveQuery.test.ts`: cache hit on second
    subscribe, invalidation on overlapping write + refresh on the rerun,
    `max: 0` disables, different ctxs miss independently.

## [1.2.0] — 2026-05-27

### Added

- **`disconnect()` on client + collection.** Force-close the underlying
  WebSocket _without_ tearing down state. The auto-reconnect loop fires
  after `reconnectMs`, each entry's `appliedVersion` is preserved, and the
  resumed subscribe carries `since` so the engine replies with a catch-up
  diff (or a fresh snapshot if the change log no longer covers the gap).

    This exposes the existing resume-via-`since` path (already shipped, see
    `engine.subscribe`'s `since` parameter + the change-log + the catch-up
    diff builder) so tests, benches, and apps can simulate an offline blip
    cleanly. Pairs with the auto-reconnect loop that's been in place since
    the WebSocket client landed.

    ```ts
    const tasks = createSyncCollection<Task>({ collection: 'tasks', url });
    // …client has appliedVersion = N…
    tasks.disconnect(); // closes the WS without losing state
    // …server applies M more changes while we're "offline"…
    // auto-reconnect fires, subscribe carries since: N,
    // engine replies with a catch-up diff covering (N, N+M]
    ```

    No behavioural change for clients that don't call `disconnect()`. The
    existing `close()` semantics (tear down everything, stop reconnecting)
    are unchanged.

## [1.1.0] — 2026-05-27

### Changed

- **Reactive subscription fan-out is no longer O(N) in subscribers.** The
  engine now memoises each reactive query's rerun per change batch, keyed by
  `(collection, params, ctx)`. Subscribers sharing equivalent keys (e.g.
  many tabs of the same view, many clients of the same anonymous query)
  share a single rerun of the query body instead of each triggering their
  own. Each subscription still diffs the shared result against its own
  current state and receives its own per-sub frame.

    Measured against the existing reactive-read bench (one writer mutates,
    N subscribers receive — slowest-tail latency per write):

    | subscribers | tail p50 before (1.0) | tail p50 after (1.1) | speedup   |
    | ----------- | --------------------- | -------------------- | --------- |
    | 1           | 7.3 ms                | 11.3 ms              | ~same     |
    | 10          | 28.2 ms               | 12.5 ms              | 2.3×      |
    | 100         | 161.4 ms              | 27.6 ms              | **5.9×**  |
    | 1,000       | 1,645.3 ms            | **81.2 ms**          | **20.3×** |

    At 1,000 subscribers the tail dropped from ~1.6 s to ~80 ms; p99 from
    ~2,650 ms to ~93 ms (28×). The cost shape changed from linear O(N) to
    near-constant in the number of subscribers — what's left is per-WS frame
    write, the focus of the next pass.

    Correctness: subscribers with different `ctx` references still get
    independent reruns (a per-user query body can depend on `ctx.userId`).
    The dedup key uses stable JSON when possible and falls back to per-object
    identity for values JSON can't represent, so an exotic `ctx` never silently
    shares results with a different one.

## [1.0.0] — 2026-05-26

API freeze. The package has been in production-quality shape across the recent
minor releases; 1.0 declares the surface stable.

### Added (cumulative since 0.10)

- **`/crdt` — conflict-free collaborative editing.** Zero-dependency, isomorphic
  CRDT kit (PN-counter, LWW register, an RGA collaborative-text type), plus a
  pluggable `TextCrdtAdapter<State>` contract with `rgaText` as the first-party
  backend. Third-party backends (Yjs, Automerge, Loro) live in the
  [`sync-adapters`](https://github.com/absolutejs/sync-adapters) monorepo and
  drop in behind the same hook (0.10).
- **Declarative CRDT primitive.** `engine.registerCrdt(table, { field: rgaText })`
  auto-merges declared CRDT fields on `actions.insert/update` and auto-registers
  a `"<table>:merge"` upsert mutation. A `createCollaborativeText` controller and
  `useCollaborativeText` / `createCollaborativeTextStore` /
  `SyncCollectionService.collaborativeText` bindings (React, Vue, Svelte,
  Angular) wrap it (0.11).
- **Delta uploads.** `createTextCrdt` gains `takeDelta()` (returns just this
  client's new ops); the controller uploads deltas — O(edit), not O(doc) per
  keystroke. Server keeps full state for trivial late-joiner hydration. Benched
  at **84×–8,350×** smaller than full-state at 100–10,000 chars (0.12).
- **Tombstone compaction.** `compact(state)` drops tombstones no live element
  anchors to (visible text unchanged); `tombstoneCount(state)` is the metric.
  `linearize` re-roots orphans deterministically, so a stale client briefly
  referencing a compacted tombstone never loses content (0.13).
- **More CRDT types.** `orSet` (observed-remove, add-wins), `lwwMap` (per-key
  LWW with delete tombstones), `createList` (ordered-list RGA over arbitrary
  items, with delta support) (0.14).
- **Collaborative cursors.** `anchorAt(index)` / `indexOfAnchor(anchor)` on the
  text CRDT — caret survives concurrent edits; passthrough on the controller and
  every framework hook (0.15).

### Architectural baseline (from earlier releases, in shipped state)

- Reactive push (`createReactiveHub`, `sync` plugin), ORM-derived topics
  (Drizzle, Prisma), `createLiveQuery`, and a write-behind cache.
- Sync engine (Tier 3): row-level diffs over a WebSocket, optimistic mutations,
  offline queue, local-first IndexedDB cache, declarative permissions, schema
  validation + lazy migrations, live full-text + vector search, scheduled
  functions, a live devtools dashboard, CDC for Postgres/MySQL/SQLite,
  incremental aggregations + joins, and a declarative operator graph.
- Framework bindings as subpaths: `/react`, `/vue`, `/svelte`, `/angular`,
  `/client`, `/engine`, `/crdt`, `/scheduled`, `/drizzle`, `/prisma`,
  `/postgres`, `/mysql`, `/sqlite`.

### Compatibility

- Public APIs across `.`, `/client`, `/engine`, `/crdt`, `/scheduled`, the four
  framework bindings, and the DB/ORM adapter subpaths are stable as of 1.0.
- All listed peers (`elysia`, `drizzle-orm`, `@elysiajs/cron`, framework
  packages) remain optional.
- Adapters (`@absolutejs/sync-yjs`, `-automerge`, `-loro`) version
  independently and target `@absolutejs/sync >= 0.10`; they are unchanged by
  1.0.

## Earlier releases

The 0.x line evolved the engine, framework bindings, CDC adapters, search,
scheduled functions, devtools, permissions, schema/migrations, and the CRDT
story above. Git history is the source of truth; this changelog starts tracking
formally at 1.0.
