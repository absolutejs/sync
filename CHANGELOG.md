# Changelog

All notable changes to `@absolutejs/sync` are recorded here. The format is loosely
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
follows [Semantic Versioning](https://semver.org) from 1.0 onward.

## [1.18.1] — 2026-05-29

### Fixed

- **Stale `dist/engine/cluster.d.ts`** missing the 1.17.0
  `ClusterMessage.originVersion` field. The source had it; the published
  type bundle didn't because the build that shipped 1.17.0 / 1.18.0
  carried a cached `.d.ts` from an earlier build. Downstream `ClusterBus`
  adapters (e.g. `@absolutejs/sync-bus-pg`) saw type errors when
  populating `originVersion`. Rebuilt + republished — no source-code
  changes vs 1.18.0.

## [1.18.0] — 2026-05-29

### Added — client-side cursor plumbing

The 1.17.0 cursor primitive now flows end-to-end through every client lib.
Cross-instance resume just works: client connects to engine A, captures
the cursor, reconnects to engine B (different cluster shard), engine B
serves catch-up via peer-broadcasted changes.

- **`OnDiff` callback signature widened** — now receives `(diff, version,
  cursor?)`. The cursor is the engine's current cross-instance resume
  cursor at the time of the batch. Pre-1.18 2-arg callbacks keep
  working — the 3rd arg is optional.
- **`ServerFrame.cursor?: string`** — every `snapshot` / `diff` / `frame`
  frame may now carry the cursor. Old servers that don't emit one cause
  the client to fall back to the numeric `version`.
- **`ClientFrame.subscribe.since`** widened to `number | string`. The
  server-side `parseFrame` accepts both; the client lib picks the cursor
  when one has been received, falling back to `appliedVersion`.
- **Client libs (`syncClient`, `syncCollection`, `syncStore`)** now
  capture the cursor from incoming frames and round-trip it on
  reconnect. The `appliedCursor` / `entry.cursor` field shadows
  `appliedVersion` whenever the server has surfaced one.

### Fixed

- **`currentCursor()` was reading `entry.version` instead of
  `entry.originVersion`** when summarizing peer-relayed entries. For
  local-only changes this was a no-op (the two are equal); for cluster
  bus traffic the cursor was emitting THIS engine's local version
  against the peer's id, so a follow-up resume would never match the
  peer's actual log entries. Caught by extending the multi-instance
  test to verify the cursor's contents instead of just its presence.

### Notes

Backwards-compatible — every old client that ignores `cursor` and
sends `since: number` keeps working. The cross-instance resume only
activates when both ends use cursors.

## [1.17.0] — 2026-05-29

### Added — cross-instance resume cursor

The big one: clients reconnecting to a DIFFERENT cluster instance can now
resume with a catch-up diff instead of falling back to a fresh snapshot.
The `cluster.ts` comment that said "use sticky sessions if you want
cross-instance resume" is now obsolete.

- **`SyncEngineOptions.instanceId`** — stable string id. Default: random
  UUID per engine. For a real cluster, pass a stable per-shard value
  (e.g. `${hostname}:${shardId}`) so resume cursors remain decodable
  across restarts.
- **`LoggedChange.origin` + `LoggedChange.originVersion`** — every entry
  now records which engine produced it and that engine's local version
  at commit. Locally-committed changes have `origin === instanceId`;
  cluster-received changes carry the peer's identity.
- **`Subscription.cursor: string`** — opaque resume cursor. JSON-encoded
  vector of `(instanceId, version)` per origin the client has caught up
  to. The client round-trips it on reconnect.
- **`SubscribeArgs.since: number | string`** — `number` is the legacy
  pre-1.17 form (this engine's version); `string` is the new cursor.
  Backwards-compatible.
- **`ClusterMessage.originVersion?: number`** — pre-1.17 buses that omit
  this default to 0 (cross-instance resume falls back to a snapshot —
  matches pre-1.17 behavior). New buses include the originating
  instance's local version, so each engine logs peer changes against
  `(origin, originVersion)` for cross-instance catch-up.

### Why this matters for the PaaS

Without 1.17.0: the @absolutejs/router rotates shards (or load shifts a
tenant between them), the client lands on a different engine, the engine
returns a fresh snapshot. For a tenant with a large dataset that's
expensive bandwidth + a visible UX stall. With 1.17.0: the engine the
client lands on builds a catch-up from peer-broadcasted changes —
exactly the same diff the original engine would have produced.

### Notes

- Single-instance setups (no `connectCluster`) behave identically to 1.16.
- The cursor is opaque. Clients must round-trip it unmodified; the format
  will shift in future versions (HLC, monotonic UUIDs, compressed binary)
  without bumping major.

## [1.16.0] — 2026-05-29

### Added — pluggable wire-format serializer

- **`FrameSerializer` interface + `jsonSerializer` default**, exported from
  `@absolutejs/sync`. The serializer owns the wire format only — frame-shape
  validation stays in the engine, so the SAME validation works for JSON,
  msgpack, cbor, or any binary layout. The default `jsonSerializer`
  preserves every existing call site's behavior.
- **Threaded through every layer:**
  - Server: `createSyncConnection({ serializer })` + `syncSocket({ serializer })`.
  - Client: `createSyncClient({ serializer })`,
    `createSyncCollection({ serializer })`, `syncStore({ serializer })`,
    `createPresence({ serializer })`.
- Both ends MUST use the same serializer; opt into a binary one on BOTH
  ends to cut the bandwidth + parse CPU on large snapshots — the
  customer-app side wins observable bytes-egress + faster cold reads.

### Why this matters for the PaaS

JSON is great until you're sending a 1 MB initial hydrate per reconnect.
At a million reconnect-events/day across tenants, the meter's
`bytesEgress` line item is the actual customer bill — cutting it 40-60%
with msgpack/cbor is real money. This release ships the seam without
prescribing a specific binary encoder; pick the one your client lib
already has and plug it in.

### Notes

- The `decode` step accepts strings, `Uint8Array`, and `ArrayBuffer` —
  Bun's WS gives bytes for binary frames; Browser WS gives ArrayBuffer.
  `jsonSerializer.decode` handles all three by UTF-8 decoding when
  needed.
- A custom serializer's `encodeServer` / `encodeClient` may return
  `string | ArrayBufferLike | Uint8Array`. The WS adapter passes
  through whatever shape the WS impl accepts.

## [1.15.0] — 2026-05-29

### Added — AbortSignal on subscribe + hydrate

- **`SubscribeArgs.signal: AbortSignal`** — first-class cancellation on
  `engine.subscribe`. Two effects:
  1. If the signal is already aborted when `subscribe` is called, the engine
     throws `AbortError` immediately — no authorize, no hydrate, no
     subscription is built.
  2. If the signal fires AFTER the subscription is live, the engine
     auto-calls `unsubscribe()`. The consumer never has to thread two
     handles for the same lifetime.
- **`engine.hydrate(collection, params, ctx, options?)`** — new optional
  4th arg. `options.signal` cancels mid-flight; the engine re-checks the
  signal after each major await (authorize, hydrate body) and throws
  `AbortError` when fired.
- **`AbortError` exported** — `name === 'AbortError'` to match the
  DOM-standard spelling so existing `catch (error) { if (error.name ===
  'AbortError') ... }` patterns work unchanged.

### Notes

Real production hazard the signal addresses: a slow hydrate (large search
index warm-up, expensive graph rerun, multi-table join) that started
because the client subscribed, then the client disconnects mid-flight,
and the engine keeps churning CPU on work nobody will ever read.

Backwards-compatible — omit the signal and the engine behaves exactly as
in 1.14.0. Both function signatures already accepted extra arguments
that were unused; nothing breaks at the call-site level.

## [1.14.0] — 2026-05-29

### Added — connection-layer + WS-layer slow-client signaling

- **`connection.stats()`** — point-in-time counters on a `SyncConnection`:
  `{ subscriptionCount, presenceRoomCount, framesSent, slowSendsRecent }`.
  Cheap; safe to call from a metering loop. `slowSendsRecent` counts
  consecutive `send()` calls that returned `-1` (the WS backpressure
  signal), resetting to `0` on the next successful send.
- **`SyncConnectionOptions.send` return type widened** — `send` may now
  return `void | number`. By convention `-1` signals backpressure
  (matches Bun's `ws.send()` return). Legacy void-returning sends keep
  working unchanged.
- **`syncSocket({ maxBufferedBytes, onSlow, closeOnSlow })`** — WS-layer
  slow-client detection. The plugin wraps every `ws.send()` to capture
  the return value AND read `ws.getBufferedAmount()`. If either the
  buffer exceeds `maxBufferedBytes` OR the send returns `-1`, `onSlow`
  fires once with `{ wsId, bufferedAmount, stats, reason }` where reason
  is `'buffer-threshold'` or `'send-backpressure'`. The signal re-arms
  on the WS `drain` event. `closeOnSlow: true` kicks the socket on the
  first slow signal; the client reconnects and rehydrates.

### Notes

The plugin's return type (`Elysia`) is unchanged; new behavior is opt-in
via the new options. Existing callers without `maxBufferedBytes` or
`onSlow` see identical semantics to 1.13.0. Pairs naturally with
`@absolutejs/metering` (wire `onSlow` to charge tenant extra) OR with
`@absolutejs/runtime`'s drain mode (kick slow tenants off a shard about
to reboot).

## [1.13.0] — 2026-05-29

### Added — engine introspection & retention pass

- **`engine.metrics()`** — operator-shaped point-in-time engine state.
  Distinct from `engine.inspect()` (which is devtools-shaped). Returns
  `{ at, uptimeMs, version, changeLog: { entries, capacity, retainMs,
  oldestVersion, oldestAgeMs }, subscriptions: { total, byCollection },
  reactiveCache: { entries, capacity }, mutations: { completed, failed,
  retried, inFlight }, schedules: { registered } }`. Designed for PaaS
  hosts to scrape on an interval and attribute cost per engine via
  `@absolutejs/metering`.
- **`changeLogRetainMs` option** — time-based change-log retention,
  layered on top of the existing count cap (`changeLogSize`). Drops
  entries older than the configured window. Lets a high-throughput
  engine keep a short log ("60s of changes") regardless of mutation
  rate — both bounds memory and bounds catch-up work on reconnect.
  Default `null` (no time-based eviction).
- **Per-mutation counters** — `engine.metrics().mutations.{completed,
  failed, retried, inFlight}` accumulate since engine start. The
  `inFlight` counter wraps every `runMutation` call in a try/finally
  so it decrements correctly on the exception path too. Retry attempts
  bump `retried` regardless of whether the eventual outcome is success
  or failure.
- **`LoggedChange.at`** — every entry in the change log now carries the
  wall-clock timestamp it was logged at. Drives the time-based
  retention sweep + the `oldestAgeMs` field on `engine.metrics()`.
  Surfaced on every entry yielded by `engine.streamChanges()`.
  Additive — pre-1.13.0 consumers that destructure `LoggedChange` are
  unaffected.

### Notes

This pass deliberately does NOT touch the connection layer or the WS
hotpath — those are larger surgical changes (slow-client backpressure,
binary frame encoding, cross-instance version cursors) and ship in
1.14.0+. Everything in 1.13.0 is engine-internal, additive, and
backwards-compatible.

## [1.12.0] — 2026-05-29

### Added

- **`SandboxConfig.unsafeHost` escape hatch** — opt-in map of host
  functions a `sandboxedHandler` may call as
  `unsafeHost.fnName(...args)`. Without this option the sandbox stays
  hermetic; with it, the engine routes declared host calls through the
  existing `__dispatch` Reference (no extra IPC primitive) so an
  isolated mutation can reach a payment gateway / queue / SDK / mailer
  when it has to. The name is deliberately loud — anyone reading the
  handler source sees the escape immediately. Convex actions are the
  same pattern; the trade-off is the same: retries WILL re-fire the
  host fn, so make it idempotent or pair with compensation. Undeclared
  calls throw a clear `unsafeHost.${fnName} ... was not declared`
  error; thrown host errors propagate into the sandbox as normal JS
  errors. The wrapped handler signature becomes
  `(args, ctx, actions, unsafeHost)` — fully backwards-compatible (the
  4th param is just ignored by existing handlers). SB-5.

## [1.11.0] — 2026-05-29

### Added

- **`engine.runMutations(specs, ctx)`** — new batch primitive that runs
  N mutations in a single DB transaction, fans them out as ONE live
  diff on success, and rolls every accumulated write back on any
  thrown error. No partial commits, no surprise per-mutation diffs.
  Per-mutation `authorize` still runs (inside the tx); per-mutation
  retry policies do NOT apply to batches. Empty `specs` short-circuits
  without opening a tx; unknown mutation names throw before any tx
  opens. Surfaces as a `mutationBatch` activity event.
- **`transactionalBatchAsHostTool` in `@absolutejs/sync/code-mode`** —
  pairs with `engine.runMutations`. Exposes one Code Mode host fn (by
  convention `run_transaction`) that takes an `Array<{ name, args }>`
  from the model and runs it atomically. Mutations are allow-listed at
  factory time so a hallucinated name fails fast with a clear error.
  Drop-in alongside `engineMutationsAsHostTools`; the model gets BOTH
  the individual host fns (for scripts that branch on intermediate
  results) and `run_transaction` (for atomic batches) and picks based
  on the prompt.

This closes the v0.1 cross-mutation atomicity gap the `/code-mode`
docs page called out, with no breaking changes to the v0.1 surface.

## [1.10.0] — 2026-05-28

### Added

- **New subpath `@absolutejs/sync/code-mode`** — exposes the engine's
  mutation surface as a host-tool map shape-compatible with
  `@absolutejs/ai`'s `codeModeTool({ tools })`. The factory
  `engineMutationsAsHostTools({ engine, ctx, mutations })` returns
  `Record<hostFnName, CodeModeHostTool>` with auto-derived JS-safe
  names (`'comments:create'` → `comments_create`), throws at build
  time on unregistered names or duplicate host-fn names, and threads
  a per-call `ctx()` factory through every mutation in the script.
  Tests cover the documented v0.1 partial-failure semantics: each
  `runMutation` runs in its own DB transaction, and a later throw does
  NOT roll back earlier commits. Cross-mutation atomicity is a
  deliberate v0.2 followup; this slice ships honest semantics rather
  than a transactional promise the engine can't keep without new
  primitives. SB-1 ("Code Mode as a sync primitive") groundwork.

## [1.9.2] — 2026-05-28

### Added

- **New subpath `@absolutejs/sync/testing`** — small helpers for testing
  sync engines and sync packs without each consumer redefining the same
  boilerplate. Exports:
    - `createTestEngine(options?)` — documented re-export of
      `createSyncEngine` that signals test scope at the call site. Future
      test-mode defaults (e.g. an inline transaction runner) can land
      here without churning existing tests.
    - `expectRejection(work)` — awaits `work()` and returns the thrown
      value, or throws if it resolves. Avoids Bun 1.3.x's flaky
      `expect(...).rejects.toThrow(...)` behavior
      (oven-sh/bun#31462). Equivalent to the `rejection()` helper sync's
      own tests already use locally.
    - `runAsActor(engine, actorId, mutation, args, extraCtx?)` — runs a
      mutation with `{ ...extraCtx, userId: actorId }` ctx, matching the
      standard pack convention (`getActorId: (ctx) => ctx.userId`).
      No new test framework; these are plain helpers usable with `bun:test`,
      vitest, etc.

## [1.9.1] — 2026-05-28

### Fixed

- **`@absolutejs/sync/engine` no longer eagerly evaluates Elysia at module
  load.** Previously the engine subpath barrel re-exported `syncCdc` from
  `engine/cdc.ts`, which had a top-level `import { Elysia } from 'elysia'`.
  Any consumer of `@absolutejs/sync/engine` — including sync packs that
  only use `defineCollection` / `defineSyncPack` and never touch CDC —
  had to install `elysia` in their dependency tree, or `bun test` would
  fail with `Cannot find package 'elysia'`. `engine/cdc.ts` now lazy-loads
  Elysia via `require('elysia')` on first call to `syncCdc(...)`. The
  public API is unchanged; only the load timing differs. Pack authors no
  longer need `elysia` as a devDep.

## [1.9.0] — 2026-05-28

### Added

- **Sync packs — `engine.registerPack(pack)`.** A `SyncPack` is a
  self-contained bundle of schemas, permissions, readers/writers,
  collections (view/join/graph/search/reactive), mutations, schedules,
  and CRDT field declarations. `engine.registerPack` walks the bundle and
  dispatches each entry to the matching `register*` method — no new
  persistence path, pure composition. Packs declare `ownsTables` and the
  engine rejects two packs claiming the same table with
  `PackTableConflictError`. Optional `requireDependencies: true` throws
  `PackMissingDependencyError` at register time if a `readsTables` entry
  has no reader yet. `engine.inspect().packs` surfaces registered packs
  for devtools and conflict diagnostics. See `src/engine/syncPacks.design.md`
  for the rationale, factory pattern (`create<Name>Pack(config)`), and
  composition rules ("subscribe layer, not call layer"). Pack repos live
  in the new `~/abs/sync-packs/` monorepo.
- **`ScheduleDefinition.retry: RetryPolicy`** — opt-in retry of the
  whole handler on classified-as-retryable errors. Mirrors
  `defineMutation.retry`: handler must be idempotent under retry,
  default is `isSerializationFailure`, defaults `maxAttempts: 5` /
  `maxElapsedMs: 30_000` / exponential backoff. New activity events
  `schedule` (`status: 'ok' | 'error'`) and `scheduleRetry` mirror the
  mutation activity stream.

### Exports (new on `@absolutejs/sync/engine`)

- `defineSyncPack`, `PackTableConflictError`,
  `PackMissingDependencyError`
- Types: `SyncPack`, `RegisteredPack`, `CrdtFieldsMap`

## [1.8.1] — 2026-05-28

### Changed

- **`sandboxedHandler` now uses isolated-jsc 0.8 runners.** The sync engine
  keeps the same `sandboxedHandler` API, but the implementation now builds a
  per-mutation `createIsolatedRunner()` with the `tenant-script` policy,
  precompiles the wrapped callable, and invokes it through `runner.call()`.
  Metrics records now include the resolved isolated-jsc backend when metrics
  are enabled.

### Bumped

- Optional peer dep `@absolutejs/isolated-jsc` `>= 0.6.0` → `>= 0.8.0`.
  Required for `createIsolatedRunner()`, policy presets, and runner metrics.

## [1.8.0] — 2026-05-27

### Added

- **New subpath `@absolutejs/sync/tanstack-db`** for creating TanStack DB
  collection options backed by Absolute Sync. The adapter forwards Absolute Sync
  snapshots/diffs into TanStack DB and can map TanStack insert/update/delete
  mutations back to registered Absolute Sync mutations. `@tanstack/db` is an
  optional, tightly pinned peer (`>= 0.6.7 <0.7`) because the API is still
  pre-1.0.

## [1.7.9] — 2026-05-27

### Added

- **New subpath `@absolutejs/sync/mcp` — Model Context Protocol server
  for the engine.** Surface a {@link SyncEngine}'s read + mutate
  surface to MCP-aware clients (Claude Code, Cursor, custom agents)
  through five tools:
    - `list_collections` — registered collection names + kinds + tables
    - `list_mutations` — registered mutation names
    - `inspect_engine` — full {@link EngineInspection} snapshot
    - `get_snapshot` — `{ collection, params?, ctx? }` → current rows
    - `run_mutation` — `{ mutation, args, ctx? }` → result

    ```ts
    // mcp-stdio-server.ts — point your MCP client at this file's path.
    import { createSyncMcpServer, serveStdio } from '@absolutejs/sync/mcp';
    const server = await createSyncMcpServer({
    	engine,
    	defaultCtx: { tenantId: 'demo' }
    });
    await serveStdio(server);
    ```

    Multi-tenant gating is built in: spawn one MCP server per tenant
    with `defaultCtx: { tenantId }`; the agent's per-call `ctx`
    overrides aren't required (and tools that take a `ctx` fall back
    to the server default).

    Closes the QW-5 strategy item from the May 2026 competitive dive
    (Val.town's MCP server with 36+ tools, Cloudflare's MCP exposing
    the entire Cloudflare API through "two tools in under 1,000 tokens"
    via Code Mode — sync needed to be in this conversation).

### Bumped

- New OPTIONAL peer dep `@modelcontextprotocol/sdk >= 1.29.0`.
  Install only if you import from `@absolutejs/sync/mcp`. The
  subpath loads the SDK lazily via dynamic `import()`.

## [1.7.8] — 2026-05-27

### Added

- **`bridgeFetch` — credential-brokered HTTP from inside a sandboxed
  handler.** Engine option pairs an allowlist with per-host auth
  injection. The sandbox calls `actions.fetch(url, init)`; the host
  validates the URL is in the allowlist, computes the auth header on
  the host side (so the secret never crosses into the JSC heap), runs
  `fetch`, and returns a structured-cloned response the sandbox can
  pick apart:

    ```ts
    createSyncEngine({
      bridgeFetch: {
        'api.stripe.com': {
          authorization: () => \`Bearer \${process.env.STRIPE_KEY}\`,
        },
        'api.openai.com': {
          authorization: () => \`Bearer \${process.env.OPENAI_KEY}\`,
          headers: { 'OpenAI-Beta': 'assistants=v2' },
        },
      },
    });

    // Inside any sandboxedHandler:
    const res = await actions.fetch('https://api.stripe.com/v1/customers');
    const customers = JSON.parse(res.body);
    ```

    Sandbox-supplied `Authorization` headers are stripped before the
    request is sent (so a malicious tenant can't probe whether the host
    injected an auth header by reflecting one). All other sandbox
    headers pass through. Non-allowlisted hostnames throw a clean
    "not allowlisted" error inside the sandbox before any network call.

    Closes E2B wishlist #1160's "per-sandbox credential brokering with
    selective injection" pattern, which Cloudflare Dynamic Workers
    shipped in April 2026. None of the peer sync engines have an
    equivalent (closest is Convex actions, which run with full host
    trust — no allowlist boundary).

## [1.7.7] — 2026-05-27

### Added

- **`actions.now()`** on `MutationActions`. Returns a wall-clock
  timestamp (ms since epoch) that the engine controls. Today it's
  `Date.now()`; in a future replay / rebase path the engine will pin
  it to the original call's timestamp so optimistic client and
  authoritative server runs don't silently diverge.

    Use `actions.now()` everywhere you'd reach for `Date.now()` inside
    a mutation handler. The plain `handler` form gets it as part of
    the `actions` parameter; the `sandboxedHandler` form gets it
    through the same in-VM shim as the other actions (one
    `__dispatch(callId, 'now')` host-fn call).

    Trivially additive — existing handlers using `Date.now()` keep
    working; the new primitive is opt-in.

## [1.7.6] — 2026-05-27

### Added

- **Per-call telemetry for `sandboxedHandler`.** New
  `createSyncEngine({ handlerMetrics: (record) => void })` option fires
  a `HandlerMetricsRecord` after every sandboxed-mutation invocation
  (success or failure) with:

    ```ts
    type HandlerMetricsRecord = {
    	id: string; // random per-call id
    	mutationName: string;
    	durationMs: number; // wall-clock host-side
    	cpuMs: number; // inside the JSC sandbox
    	heapBytes: number; // measured right after the script returned
    	ok: boolean;
    	errorName?: string; // present when ok === false
    	errorMessage?: string;
    	timestamp: number; // Date.now() at call end
    };
    ```

    Wire to anything: a sync collection (per-tenant dashboards), your
    observability backend, a Drizzle table for cost attribution, or
    stderr for spot-checks. The runner switches to
    `callable.callWithMetrics` when the hook is set (~0.05 ms per call
    cost — disable for hot-path mutations that don't need it). Without
    the hook, the per-call hot path is unchanged.

    Hook failures are swallowed by design: a misbehaving metrics sink
    must NOT crash the caller's mutation.

    Closes the "per-tenant observability is universally weak" gap
    surfaced by the competitive deep dive — none of the peer sync
    engines ship this primitive.

## [1.7.5] — 2026-05-27

### Changed (performance)

- **`sandboxedHandler` — install the dispatch Reference ONCE per
  isolate; route per-call via a callId argument.** 1.7.4 passed a
  fresh dispatch `Reference` as a parameter on every call, which on
  the FFI backend triggered a `JSObjectMakeFunctionWithCallback` +
  JSCallback alloc per call (~0.5 ms fixed cost). Bench regressed
  pure-handler FFI from 0.33 ms (1.7.3) → 0.96 ms (1.7.4).

    1.7.5 reverts that: install `__dispatch` ONCE per isolate as a
    global Reference closed over a per-mutation `callMap`. Each call
    generates a fresh `callId`, registers its `actions` in the map,
    invokes `callable.call([callId, args, ctx])`, and deletes the
    entry in finally. The in-VM `actions` shim closes the call id
    over `__dispatch`, so concurrent calls route to the right
    `actions` instance via their own callId — concurrent-safe by
    construction, no shared-mutable slot, no serialization queue.

    Per-call hot path: one `JSObjectCallAsFunction` + 3 cheap
    primitive packings (callId is a number; args/ctx structured-clone
    via JSON). No per-call Reference alloc.

    Expected on the bench: pure FFI back to <0.4 ms, actions FFI
    ~similar to 1.7.4 (~1.5 ms — the pump dominates there).

## [1.7.4] — 2026-05-27

### Changed (performance)

- **`sandboxedHandler` switched to `Context.compileCallable`.**
  isolated-jsc 0.6 added a `Context.compileCallable(source)` primitive:
  compile a function expression once, call it many times with different
  args. Per-call cost is one `JSObjectCallAsFunction` (FFI) or one
  postMessage (Worker) — no per-call eval, no per-call `setGlobal`.

            The previous 1.7.2/1.7.3 design used a shared "current actions" slot
            with a router Reference installed on a reused context, plus a
            promise queue to serialize calls into that slot. 1.7.4 throws all of
            that out:
            - Each mutation is compiled to a `Callable` once at registration.
              Source becomes `function(args, ctx, __dispatch) { ... return

    userFn(args, ctx, actions); }`where`actions`is an in-VM shim
  over`\_\_dispatch`.
    - Per call: build a fresh dispatch `Reference`closed over this
      call's`actions`, invoke `callable.call([args, ctx, dispatch])`.
    - No shared slot → no serialization queue. Concurrent same-mutation
      calls are safe by construction (each has its own dispatch
      Reference closed over its own `actions`). - No reused context recycling — the callable's underlying function
      is reused; per-call work doesn't create JSC metadata that needs
      GCing.

              Behavioural notes: handler errors still propagate as `Error` objects
              with `.message` and `.name`. Timeouts still terminate the isolate
              on Worker; on FFI they throw `TimeoutError` and the isolate stays
              alive (next call respawns the context). No public API changes.

### Bumped

- Peer dep `@absolutejs/isolated-jsc` `>= 0.5.0` → `>= 0.6.0`.
  Required for `Context.compileCallable`.

## [1.7.3] — 2026-05-27

### Changed (performance)

- **`sandboxedHandler` wraps user source in a sync IIFE instead of an
  async one.** The wrapper used to be `(async () => { ... })()`, which
  always returned a Promise — forcing isolated-jsc's FFI backend to
  run setup + read evals through `unwrapResultPromise` for every call,
  even when the user's handler was sync. Switched to `(() => { ... })()`:
    - Sync user handler (returns a primitive): the IIFE returns the
      primitive directly. FFI's `unwrapResultPromise` short-circuits on
      `!JSValueIsObject` — zero extra evals, fast path.
    - Async user handler (returns a Promise): the IIFE returns the
      Promise. Unwrap pump fires normally. Same behaviour as before.

    Combined with isolated-jsc 0.5 (read+cleanup eval folded together),
    pure-handler warm dispatch on FFI is now ~1.5 ms (down from 2.47 ms
    in 1.7.2, ~4.7 ms in 1.7.1). The sync IIFE also propagates
    synchronous throws through the eval boundary directly instead of
    wrapping them in a rejection — the caller still sees an `Error`.

### Bumped

- Peer dep `@absolutejs/isolated-jsc` `>= 0.4.0` → `>= 0.5.0`. 0.5
  collapses the unwrap path's 3-eval cycle to 2 by combining the
  state read with the state-global delete.

## [1.7.2] — 2026-05-27

### Changed (performance)

- **`sandboxedHandler` warm-dispatch redesign — router Reference, reused
  context, serialized queue.** Profiling the FFI backend hot path showed
  53% of per-call time was spent installing four `actions.*` References
  (one per `setGlobal`, repeated every call), plus 8% on creating a
  fresh context per call. Refactor:
    - Install **one** `__syncAction(op, ...args)` router Reference per
      isolate at compile time (instead of four per call). The in-VM
      `actions` object is now a thin in-JS shim that dispatches through
      the router.
    - Reuse a single per-mutation context across calls; recycle every
      256 calls to bound JSC per-call metadata accumulation.
    - Serialize calls via a promise queue so the shared "current actions"
      slot stays coherent under concurrent invocations.

    Per-call hot path drops from `createContext` + 6 `setGlobal` to
    2 `setGlobal` (only the volatile `args` + `ctx`). Empirical on the
    Worker vs FFI sandbox bench (see `benchmarks/sync/RESULTS.md`):

    | Backend | warm p50 (1.7.1) | warm p50 (1.7.2) | speedup |
    | ------- | ---------------- | ---------------- | ------- |
    | worker  | 0.94 ms          | (≈ same)         | —       |
    | ffi     | 4.69 ms          | ≈ 1.5 ms         | ~3×     |

    Behavioural notes: a single mutation's sandboxed calls are now
    serialized; concurrent same-mutation invocations queue behind each
    other (they already shared one isolate, so the practical impact is
    small). Per-call context recycle is hidden from the caller. No
    public-API changes.

## [1.7.1] — 2026-05-27

### Changed

- **`sandboxedHandler` backend default flipped from `'worker'` back to
  `'auto'`.** isolated-jsc 0.4 added an async host-fn pump on the FFI
  backend (alternates Bun event-loop yields with JSC microtask drains,
  bounded by `Script.run`'s `timeout`), so the engine's `actions.*`
  async References now settle on FFI just like on Worker. `'auto'`
  picks FFI when libJSC is reachable (~300 KB cold heap, interrupt-
  driven CPU timeouts) and falls back to Worker (~46 MB cold heap,
  postMessage round-trips) otherwise. Pin to `'worker'` explicitly if
  your handler needs Web APIs (`URL`, `TextEncoder`, `WebSocket`) —
  those live in the Bun-Worker environment, not the bare JSC C API.

### Bumped

- Peer dependency `@absolutejs/isolated-jsc` from `>= 0.3.0` to
  `>= 0.4.0`. Required for the FFI async host-fn pump.

## [1.7.0] — 2026-05-27

### Changed

- **`sandboxedHandler` backend pinned to `'worker'` by default.**
  `@absolutejs/isolated-jsc` 0.2 introduced an FFI backend and changed
  its `createIsolate` default from Worker-only to `'auto'` (FFI when
  reachable). The engine's `actions.insert/update/delete/change` cross
  the sandbox boundary as **async** References, and the FFI backend
  doesn't pump async host fns (per its 0.3 documented limit). Without
  this pin, bumping `@absolutejs/isolated-jsc` to 0.3 in a downstream
  app would surface "Promise that doesn't settle synchronously" errors
  on the first `actions.*` call from a sandboxed handler. We now pass
  `backend: 'worker'` explicitly. Behavioural no-op for existing
  installs.

### Added

- **`SandboxConfig.backend: 'auto' | 'ffi' | 'worker'`** — opt into the
  FFI backend for **read-only** sandboxed handlers (ones that compute a
  derived value from `args` + `ctx` and `return` it without calling
  `actions.*`). FFI gives those a ~300 KB cold heap (vs ~46 MB on
  Worker) and interrupt-driven timeouts (the isolate survives a
  TimeoutError instead of dying). Defaults to `'worker'`.

    ```ts
    defineMutation({
    	name: 'computeRiskScore',
    	sandboxedHandler: `(args, ctx) => args.amount > ctx.dailyLimit ? 'high' : 'low'`,
    	sandbox: { backend: 'ffi', memoryLimit: 128, timeout: 1000 }
    });
    ```

### Bumped

- Peer dependency `@absolutejs/isolated-jsc` from `>= 0.0.1` to
  `>= 0.3.0`. Required for the `backend` option to exist on
  `createIsolate`; `0.3` also closes the indirect-eval residuals our
  earlier docs flagged.

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
