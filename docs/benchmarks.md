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
under `sync/` — same workload (a shared counter, incremented + acked), same
harness, **same Postgres backing every local backend**, same hardware (Bun 1.3,
WSL2 dev box):

| Backend          | Where                                      | p50 (ms) | p95 (ms) | writes/sec (seq) |
| ---------------- | ------------------------------------------ | -------- | -------- | ---------------- |
| @absolutejs/sync | local (WS) + Postgres                      | **9.5**  | **18.0** | **96**           |
| TanStack DB      | local (REST + queryCollection) + Postgres  | 17.5     | 30.3     | 53               |
| Convex           | **self-hosted, loopback Docker**           | 15.9     | 21.3     | 61               |
| Convex           | cloud, dev WSL → Convex                    | 52.8     | 66.2     | 18               |
| Convex           | cloud, GH Actions runner → Convex          | 76.6     | 83.2     | 13               |
| Zero             | local (zero-cache + push server + PG)      | 66.9     | 104.9    | 14               |

**Pipelined throughput** (same workload, K writes in flight):

| Backend     | seq | c=4 | c=16 | c=64 | scaling (1→64) |
| ----------- | --- | --- | ---- | ---- | -------------- |
| **sync**    | 54  | 99  | 188  | **305** | **5.6×**    |
| TanStack DB | 73  | 175 | 278  | 297  | 4.1×           |
| Convex      | 18  | 34  | 42   | 43   | 2.4× (saturates) |
| Zero        | 16  | 24  | 24   | 32   | 2.0× (saturates) |

Read the table with the conditions in mind. The **self-hosted Convex row** is
the honest engine-vs-engine comparison — both running on loopback, no network.
Sync wins by ~1.7× on write round-trip and ~5× on concurrent throughput. The
cloud Convex rows (53 / 77 ms) are deployment-model rows, not engine rows;
most of that delta is the public-internet hop, not engine craft. We say so.

`@absolutejs/sync` is a **library** in your own Elysia server (writes never
leave loopback); TanStack DB is a **client store + sync coordinator** that
POSTs each write over HTTP; Zero is the **closest architectural rival** but
its v1.5 mutation path goes through two hops (client → zero-cache → push
server → PG); Convex is a **hosted cloud backend** (or self-hosted Docker —
the loopback row removes the network so the engine cost is visible).

The honest thesis: sync's in-process write path (WS → engine → PG, no extra
hop) is **~1.7–3× faster on these specific workloads, ~5× more concurrent**
against Convex's engine — and gets you live queries + optimistic writes +
CRDTs **without adopting a new backend**. Not "5× faster" out of context
(that was the cloud number, which conflated engine cost with network cost).

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

## Propagation latency — write → remote-subscriber-receive

Same workload, but the metric is the qualitative thing live-query engines
exist for: *two* clients connect, one mutates, the other has a subscription
on `counter` — measure the time from issuing the mutation to the *subscriber*
observing the new value:

| Backend          | Where                                  | p50      | p95      | p99    |
| ---------------- | -------------------------------------- | -------- | -------- | ------ |
| @absolutejs/sync | single engine, local (WS + PG)         | **11.0** | **15.8** | 23.3   |
| @absolutejs/sync | 2-engine cluster, in-memory bus, local | **6.2**  | **11.1** | 14.0   |
| Convex           | self-hosted, loopback Docker           | 19.8     | 28.5     | 36.8   |
| Convex           | cloud (HTTPS)                          | 69.4     | 86.9     | 105.6  |

Sync's propagation adds only ~1.5 ms over its own write-ack — fan-out is
in-process; the subscriber's WS gets the diff frame on the same tick. Convex
self-hosted (loopback) adds ~4 ms over write-ack: their reactive-subscriber
notification is a second HTTP/WS hop, but it's local. Cloud Convex's ~17 ms
over write-ack is that same hop carrying across the internet.

**Cluster mode adds essentially zero overhead.** Sync ships a `ClusterBus`
seam (you bring Redis / PG-NOTIFY / NATS) for horizontal scale. The 2-engine
row above measures writer-on-A → subscriber-on-B over the bundled in-memory
bus: identical to single-engine because the fan-out happens in the same
tick. A real PG-NOTIFY/Redis bus would add the bus's own latency on top
(~1–3 ms LAN). Caveat: per-instance version cursors mean a client that
reconnects to a *different* instance falls back to a fresh snapshot, not
a catch-up diff — use sticky sessions for cross-instance resume.

Zero is unmeasured: v1.5 deprecates the old `definePermissions` model and is
mid-transition to cookie-based auth — against a `zero-cache` with deployed
permissions and `auth: undefined`, the materialized view stays at
`resultType: 'unknown'` and never receives row data (mutations still ack +
write to PG). Re-run is queued once the auth-transition lands. Script:
[`propagation-zero.ts`](https://github.com/absolutejs/benchmarks/blob/main/sync/scripts/propagation-zero.ts).

> To add measured competitor numbers, stand up Zero (`zero-cache` + a logical-
> replication Postgres) and/or Convex (`npx convex dev` or the self-hosted image),
> port the `bench/run.ts` workload to each, and run on the same hardware.
