# Changelog

All notable changes to `@absolutejs/sync` are recorded here. The format is loosely
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
follows [Semantic Versioning](https://semver.org) from 1.0 onward.

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
