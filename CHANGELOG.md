# Changelog

All notable changes to `@absolutejs/sync` are recorded here. The format is loosely
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
follows [Semantic Versioning](https://semver.org) from 1.0 onward.

## [1.2.0] — 2026-05-27

### Added

- **`disconnect()` on client + collection.** Force-close the underlying
  WebSocket *without* tearing down state. The auto-reconnect loop fires
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
  tasks.disconnect();   // closes the WS without losing state
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

  | subscribers | tail p50 before (1.0) | tail p50 after (1.1) | speedup     |
  | ----------- | --------------------- | -------------------- | ----------- |
  | 1           | 7.3 ms                | 11.3 ms              | ~same       |
  | 10          | 28.2 ms               | 12.5 ms              | 2.3×        |
  | 100         | 161.4 ms              | 27.6 ms              | **5.9×**    |
  | 1,000       | 1,645.3 ms            | **81.2 ms**          | **20.3×**   |

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
