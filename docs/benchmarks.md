# Benchmarks

Run them yourself: `bun run bench/run.ts` (numbers below from Bun 1.3 on a WSL2
dev box — relative ratios matter more than absolute ms).

## CRDT upload payload — one keystroke on an N-char document

The collaborative-text controller uploads only each edit's ops (`takeDelta`)
instead of the whole document. The win grows with document size:

| doc size (chars) | full state (bytes) | delta (bytes) | full / delta |
| ---------------- | ------------------ | ------------- | ------------ |
| 100              | 8,272              | 99            | 84×          |
| 1,000            | 84,777             | 102           | 831×         |
| 10,000           | 876,782            | 105           | 8,350×       |

A delta stays ~100 bytes regardless of document size; full-state upload is O(doc)
per keystroke. (The server keeps full state, so late joiners still hydrate in one
shot.)

## CRDT merge throughput

Combining two concurrently-edited states (pure `mergeTextState`):

| doc size (chars) | merge time (ms) | merges / sec |
| ---------------- | --------------- | ------------ |
| 1,000            | 1.58            | 631          |
| 10,000           | 9.80            | 102          |

## Tombstone compaction

Type N chars, delete the trailing half, then `compact`:

| edits (chars) | tombstones | before (bytes) | after (bytes) | shrink |
| ------------- | ---------- | -------------- | ------------- | ------ |
| 1,000         | 500        | 84,189         | 42,187        | 2.0×   |
| 10,000        | 5,000      | 871,691        | 436,689       | 2.0×   |

## Engine throughput

- **50,000 mutations/sec** (write + emit, no subscribers)
- 2,000 mutations fanned to **10** live subscribers: ~12,000 mutations/sec
- 2,000 mutations fanned to **100** live subscribers: ~2,350 mutations/sec

Fan-out cost is linear in subscriber count, as expected for per-subscriber diffing.

## How it compares to Convex, Zero, and TanStack DB

**Measured head-to-head** lives in [`absolutejs/benchmarks`](https://github.com/absolutejs/benchmarks)
under `sync/` — same workload (a shared counter), same harness, same hardware.
Headline (Bun 1.3, WSL2 dev box):

| Backend          | Where                           | round-trip p50 | round-trip p95 | writes/sec |
| ---------------- | ------------------------------- | -------------- | -------------- | ---------- |
| @absolutejs/sync | local (WS, loopback)            | **0.82 ms**    | **3.7 ms**     | **853**    |
| TanStack DB      | local (REST + queryCollection)  | 2.53 ms        | 9.1 ms         | 271        |
| Convex           | cloud, US East (HTTPS)          | 52.8 ms        | 66.2 ms        | 18         |
| Zero             | local (zero-cache + PG) — scaffolded | _pending_  | —              | —          |

Read the table with the conditions in mind: `@absolutejs/sync` is a **library**
in your own server (writes never leave loopback); Convex is a **hosted cloud
backend** (every write is a public-internet round-trip — most of the ~55 ms is
network); TanStack DB is a **client store + sync coordinator** over your
backend; Zero is the closest architectural rival (its own zero-cache + your
Postgres). The point isn't "X is faster than Y" — it's the **deployment-model
gap**: you get the live-query, optimistic-write, CRDT story without adopting a
new backend.

| Dimension             | @absolutejs/sync                                                              | Convex                        | Zero (Rocicorp)                      |
| --------------------- | ----------------------------------------------------------------------------- | ----------------------------- | ------------------------------------ |
| Deployment            | Library in your Elysia server                                                 | Hosted backend (or self-host) | `zero-cache` service + your PG       |
| Your own DB?          | Yes — Postgres/MySQL/SQLite via Drizzle/Prisma                                | No — Convex's managed store   | Yes — Postgres (logical replication) |
| Reactivity            | Row-level diffs + incremental operator graph                                  | Reactive query re-run         | Client-side query cache (ZQL)        |
| Conflict handling     | Optional CRDTs (RGA text, OR-Set, LWW-map, list; Yjs/Automerge/Loro adapters) | Transactional (server LWW)    | Server-authoritative                 |
| Offline / local-first | Yes — local cache + offline queue + CRDT merge                                | Limited                       | Yes — local cache                    |
| Per-keystroke payload | Delta-state O(edit)                                                           | n/a (function call)           | Mutation + query refresh             |
| Lock-in               | None — it's a dependency                                                      | Adopt the Convex platform     | Adopt the zero-cache service         |

The thesis: you get Convex/Zero-style live queries, optimistic writes, and
conflict-free collaboration **without adopting a new backend** — it rides the
database, ORM, and server you already run.

> To add measured competitor numbers, stand up Zero (`zero-cache` + a logical-
> replication Postgres) and/or Convex (`npx convex dev` or the self-hosted image),
> port the `bench/run.ts` workload to each, and run on the same hardware.
