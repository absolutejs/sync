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

Read the table with the conditions in mind. `@absolutejs/sync` is a **library**
in your own Elysia server (writes never leave loopback); TanStack DB is a
**client store + sync coordinator** that POSTs each write over HTTP; Zero is the
**closest architectural rival** but its v1.5 mutation path goes through two
hops (client → zero-cache → push server → PG); Convex is a **hosted cloud
backend** (every write is a public-internet round-trip — we re-ran the same
bench from a cloud VM via GitHub Actions to remove the consumer-ISP variable
and p50 settled at **76.6 ms**, very tight distribution, confirming the floor
is the network round-trip itself, not the engine — see
[`bench-convex-us-east.yml`](https://github.com/absolutejs/benchmarks/blob/main/.github/workflows/bench-convex-us-east.yml)).
The honest thesis isn't "X is N× faster" — it's that sync's single-process write
path (WS → engine → PG, no extra hop) gives you live queries + optimistic writes
+ CRDTs **without adopting a new backend**.

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

| Backend          | Where             | p50      | p95      | p99    |
| ---------------- | ----------------- | -------- | -------- | ------ |
| @absolutejs/sync | local (WS + PG)   | **11.0** | **15.8** | 23.3   |
| Convex           | cloud (HTTPS)     | 69.4     | 86.9     | 105.6  |

Sync's propagation adds only ~1.5 ms over its own write-ack — fan-out is
in-process; the subscriber's WS gets the diff frame on the same tick. Convex's
propagation adds ~17 ms over its write-ack — the recomputed result has to
make a second public-internet hop to push to the subscriber. That overhead is
structural to a hosted-backend deployment, not a Convex flaw.

Zero is unmeasured: v1.5 deprecates the old `definePermissions` model and is
mid-transition to cookie-based auth — against a `zero-cache` with deployed
permissions and `auth: undefined`, the materialized view stays at
`resultType: 'unknown'` and never receives row data (mutations still ack +
write to PG). Re-run is queued once the auth-transition lands. Script:
[`propagation-zero.ts`](https://github.com/absolutejs/benchmarks/blob/main/sync/scripts/propagation-zero.ts).

> To add measured competitor numbers, stand up Zero (`zero-cache` + a logical-
> replication Postgres) and/or Convex (`npx convex dev` or the self-hosted image),
> port the `bench/run.ts` workload to each, and run on the same hardware.
