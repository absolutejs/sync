# Sync packs — design doc

> **Status**: draft, not implemented. SB-2 in `STRATEGY.md`. This doc exists so
> the API gets locked down before anyone writes the engine glue. Once a pack is
> on npm, breaking it churns every consumer; the surface has to be right.
>
> **Goal of this doc**: define the smallest API that lets a pack ship a working
> feature as one `import` + one `register` call, without the lock-in that
> makes Convex Components a moat.

---

## 1. What a "pack" is

A **sync pack** is a self-contained, npm-distributed bundle of:

- `schemas` — `defineSchema` entries for the tables it owns
- `collections` — `defineCollection` / `defineJoin` / `defineGraph` /
  `defineSearch` / `defineReactiveQuery` entries it exposes
- `mutations` — `defineMutation` entries (including sandboxedHandler form)
- `scheduled` — `defineSchedule` entries for the cron-fired work it owns
- `permissions` — `definePermissions` entries for the tables it owns
- `writers` / `readers` — host-side adapters for the tables it owns
- `crdt` — `registerCrdt` entries for any CRDT fields it owns

A pack must NOT own anything the host application also owns. The host's
`users` table, the host's auth context — those are _injected_ into the pack
(see §4), not redefined.

A pack must register everything in **one call**:

```ts
engine.registerPack(pack);
```

Order-independent — `registerPack` walks the bundle and dispatches to the
existing `register*` surfaces. There is no new persistence path.

---

## 2. Surface a pack can register

A pack's bundle mirrors what `createSyncEngine({ … })` already accepts plus
what the per-table `register*` methods accept. The full list:

| Field                | Type                              | Engine call           |
| -------------------- | --------------------------------- | --------------------- |
| `schemas?`           | `Record<table, TableSchema>`      | `registerSchema`      |
| `permissions?`       | `Record<table, TablePermissions>` | `registerPermissions` |
| `readers?`           | `Record<table, TableReader>`      | `registerReader`      |
| `writers?`           | `Record<table, TableWriter>`      | `registerWriter`      |
| `crdt?`              | `Record<table, CrdtFields>`       | `registerCrdt`        |
| `collections?`       | `CollectionDefinition[]`          | `register`            |
| `joinCollections?`   | `JoinCollectionDefinition[]`      | `registerJoin`        |
| `graphCollections?`  | `GraphCollectionDefinition[]`     | `registerGraph`       |
| `searchCollections?` | `SearchCollectionDefinition[]`    | `registerSearch`      |
| `reactiveQueries?`   | `ReactiveQueryDefinition[]`       | `registerReactive`    |
| `mutations?`         | `MutationDefinition[]`            | `registerMutation`    |
| `schedules?`         | `ScheduleDefinition[]`            | `registerSchedule`    |

Anything more exotic (custom transports, cluster bus wiring, devtools hooks)
is **out of scope for packs**. Packs are about app-level features, not
engine-level extensions.

---

## 3. API: `SyncPack`, `defineSyncPack`, `engine.registerPack`

### `SyncPack` (the shape engines accept)

```ts
export type SyncPack = {
	/**
	 * Pack identifier. Used for devtools labelling, conflict diagnostics,
	 * and the future `engine.inspect().packs` list. Should match the npm
	 * package name (e.g. "@absolutejs/sync-pack-presence").
	 */
	name: string;
	/**
	 * Pack semver. Surfaced in devtools and in conflict diagnostics
	 * (e.g. "table 'comments' is owned by sync-pack-comments@2.1.0").
	 */
	version: string;
	/**
	 * Tables this pack owns. The engine enforces that no two packs claim
	 * the same table; the app can still register on top of these (the
	 * standard host-app wins-last rule keeps working).
	 */
	ownsTables: string[];
	/**
	 * Tables this pack reads but does NOT own. Recorded for devtools so
	 * the dependency graph is reviewable; not enforced. Used to detect
	 * a missing schema/reader the host must supply (see §4).
	 */
	readsTables?: string[];

	schemas?: Record<string, TableSchema<any>>;
	permissions?: Record<string, TablePermissions<any, any>>;
	readers?: Record<string, TableReader<any>>;
	writers?: Record<string, TableWriter<any, any, any>>;
	crdt?: Record<string, CrdtFields<any>>;

	collections?: CollectionDefinition<any, any, any>[];
	joinCollections?: JoinCollectionDefinition<any, any, any, any, any>[];
	graphCollections?: GraphCollectionDefinition<any, any, any>[];
	searchCollections?: SearchCollectionDefinition<any, any, any>[];
	reactiveQueries?: ReactiveQueryDefinition<any, any, any>[];

	mutations?: MutationDefinition<any, any, any>[];
	schedules?: ScheduleDefinition[];
};
```

### `defineSyncPack` (the helper packs use)

`defineSyncPack` is an identity helper (like `defineCollection`,
`defineMutation`). It exists for the IDE — TypeScript infers the pack's row
and ctx types from the embedded `defineCollection` calls.

```ts
export const defineSyncPack = (pack: SyncPack): SyncPack => pack;
```

A pack is **not** a class. It's a plain data record. This matters: it stays
JSON-inspectable for devtools and serializable for "what's installed in
this engine?" diagnostics.

### `engine.registerPack`

```ts
engine.registerPack: (pack: SyncPack) => void;
```

Behavior:

1. Reject if any table in `ownsTables` is already owned by another registered
   pack (`PackTableConflictError(table, existingPack, newPack)`). The host
   app's directly-registered tables are NOT counted as owners — host
   registrations always win.
2. Walk `schemas`, `permissions`, `readers`, `writers`, `crdt`,
   `collections`, `joinCollections`, `graphCollections`, `searchCollections`,
   `reactiveQueries`, `mutations`, `schedules` and dispatch each to the
   matching `engine.register*` method.
3. Record `{ name, version, ownsTables, readsTables }` in the engine's pack
   registry; surfaced via `engine.inspect().packs` (additive — `inspect()`
   is already extension-friendly).

The engine **does not** clone or rewrite pack contents. Names stay as the
pack defined them. Namespacing is the _pack's_ job (see §4).

---

## 4. Namespacing + config injection

Two failure modes have to be designed out before v1:

1. **Two packs both want a `users` table.** If they ship hard-coded names,
   they fight. The engine cannot fix this; the pack has to be a factory.
2. **A pack needs to reference the app's auth context** (e.g. `ctx.userId`)
   without the pack assuming a specific shape.

### Pattern: each pack ships a factory, not a static bundle

Every published pack exports a `create<Name>Pack(config)` function that
returns a `SyncPack`. The factory takes:

- **`tablePrefix?: string`** (or per-table override) — all owned table names
  are prefixed. Default `""` if the pack only owns one table; default
  `"<pack-short-name>_"` if it owns several.
- **`userTable?: string`** — name of the host's user table when the pack
  needs to read it (comments, presence). Default `"users"`.
- **`getActorId: (ctx) => string`** — host-defined function returning the
  current actor id from the app's context shape. Default: `(ctx) => ctx.userId`.
  This is the _only_ contract the pack assumes about the app's ctx.
- **`scope?: (ctx) => string | undefined`** — optional tenant/workspace scope
  used for permissions and per-channel sharding. Default: `() => undefined`.

A pack-specific config (e.g. presence's heartbeat ms, comments' max thread
depth) lives on the same config object.

```ts
// Inside @absolutejs/sync-pack-presence
export const createPresencePack = (config: PresencePackConfig): SyncPack => {
	const table = config.tablePrefix
		? `${config.tablePrefix}presence`
		: 'presence';
	const userTable = config.userTable ?? 'users';
	const getActorId = config.getActorId ?? ((ctx: any) => ctx.userId);

	return defineSyncPack({
		name: '@absolutejs/sync-pack-presence',
		ownsTables: [table],
		readsTables: [userTable],
		version: '0.1.0',
		schemas: { [table]: presenceSchema }
		// …collections, mutations, schedules…
	});
};
```

The app glue stays one line:

```ts
engine.registerPack(
	createPresencePack({
		getActorId: (ctx) => ctx.session.userId,
		scope: (ctx) => ctx.session.workspaceId
	})
);
```

### Why factory + injection beats "engine rewrites names"

A name-rewriting engine would also have to rewrite every `tables: ["foo"]`
inside the pack's `defineCollection` calls, every reference to `foo` in a
mutation handler's `actions.insert("foo", …)`, every `definePermissions`
key, and so on. Rewriting **inside the pack's code** is brittle — the
pack's mutation source is a JS string when sandboxed, the host can't
parse-and-rewrite it safely.

A factory keeps name construction inside the pack's own code, where it
already has the variables.

### What the engine still enforces

- `ownsTables` conflict between packs (see §3).
- `readsTables` referenced but not registered by anyone — surface a
  `PackMissingDependencyError` at register time _only if_ the pack opts in
  via `requireDependencies: true`. Default: a `console.warn` plus an entry
  in `inspect()`, since reads can be lazily wired.

---

## 5. Composition rules

Packs that read tables they don't own (e.g. comments-pack reads
`users.displayName` for the author byline) participate via the standard
engine surface — they `defineCollection({ tables: ["comments", users] … })`
and rely on the host having registered `users`. Nothing pack-specific.

Two packs can read each other. The engine's existing fan-out from
`applyChange` already serves cross-collection subscriptions; nothing new is
needed.

A pack must NOT call `engine.runMutation` from inside its own mutation
handlers to chain across packs. That couples consumer packs to producer
packs at runtime. Composition is at the **subscription** layer
(collections), not the **call** layer (mutations).

---

## 6. Versioning + breaking changes

Packs publish on semver. The engine's pack registry stores the version per
pack. Breaking-change rules:

- **Renaming an owned table** is a major version bump.
- **Renaming or removing a mutation** is a major version bump (the client
  has the name in its calls).
- **Adding a field with a default** is minor.
- **Schema-only migrations** (adding a column with a default, widening a
  type) ship in the pack's own schema's `migrate` — the engine already
  applies it lazily on read.

The pack registry exposes `engine.inspect().packs` so devtools can flag
"pack X is at v1, requires engine ≥ Y." Engines should not silently
downgrade behavior; mismatch is loud.

---

## 7. Worked packs

The three picked in STRATEGY.md, designed in concrete form.

### 7.1 `@absolutejs/sync-pack-presence`

**Feature**: per-channel live presence — who's here, what they're doing
(typing in the editor, scrolling at line 32, idle 4 min). Server keeps the
authoritative set, expires stale entries via a schedule, broadcasts diffs.

```ts
export type PresencePackConfig = {
  tablePrefix?: string;
  userTable?: string;
  getActorId?: (ctx: any) => string;
  /** scope→channel grouping (e.g. workspace, project). */
  scope?: (ctx: any) => string;
  /** Seconds a heartbeat keeps a presence row alive. Default 30. */
  heartbeatTtlSec?: number;
  /** How often the cleanup schedule runs. Default `*/15 * * * * *`. */
  cleanupCron?: string;
};

export const createPresencePack = (config: PresencePackConfig): SyncPack => {
  // Owns one table: presence (or `${prefix}presence`).
  // Exposes:
  //   • collection "presence" — params: { channel }, returns active members
  //   • mutation "presence:heartbeat" — args: { channel, state }, upserts row, refreshes TTL
  //   • mutation "presence:leave" — args: { channel }, deletes the actor's row
  //   • schedule "presence:cleanup" — cron-fires; deletes rows with expiresAt < now
  //   • permissions: read scoped by `scope(ctx)`, write requires getActorId(ctx)
  // …details elided in this doc; the file is the source of truth.
};
```

**Why this is a pack and not a static helper**: the host app picks the
channel grouping (workspace? document? room?), the actor identity, and the
TTL. None of those can ship hard-coded.

**Engine surface used**: `register`, `registerMutation` (×2),
`registerSchedule`, `registerPermissions`, `registerSchema`. No CRDT, no
search.

### 7.2 `@absolutejs/sync-pack-comments`

**Feature**: threaded comments on any host-side resource. Comment row has
`id, resourceId, parentCommentId, authorId, body, createdAt, editedAt`. A
collection returns the comment tree for a resource, scoped by resource
read permission via host injection.

```ts
export type CommentsPackConfig = {
	tablePrefix?: string;
	userTable?: string;
	getActorId?: (ctx: any) => string;
	/**
	 * Gate read access on a resource: the host knows which resources a
	 * given ctx can read. Pack does not duplicate the host's ACL.
	 */
	canReadResource: (
		resourceId: string,
		ctx: any
	) => Promise<boolean> | boolean;
	/** Maximum thread depth. Default 8. */
	maxDepth?: number;
	/**
	 * Use the body field as CRDT text so concurrent edits merge instead of
	 * clobbering. Default false (last-write-wins).
	 */
	collaborativeBody?: boolean;
};
```

**Notable design choices**:

- The pack does **not** own a `users` table. Author display info comes from
  joining the host's `users` table by `authorId`. The pack's exposed
  collection `comments-with-author` uses `defineJoinCollection({ … })`
  with the host's user table as the right side.
- When `collaborativeBody: true`, the pack ships a `registerCrdt` entry
  for `{body: yjsText}` (requires the host to have installed
  `@absolutejs/sync-yjs`). Without it, the body is just a string field.
- Edit permission is `authorId === getActorId(ctx)`; delete permission is
  the same OR `ctx.isModerator === true` if the host provides that flag.
  The pack lets the host inject a moderation predicate via
  `canModerate?: (ctx) => boolean`, default false.

**Engine surface used**: `register`, `registerJoin`, `registerMutation`
(create / edit / delete / react), `registerPermissions`, `registerSchema`,
optionally `registerCrdt`. No schedule, no search (yet — a future major
could add `search` for in-thread text search via `registerSearch`).

### 7.3 `@absolutejs/sync-pack-digest`

**Feature**: scheduled-digest-emails. A pack that watches one or more
collections and on a cron emits a per-actor digest email with a configured
rendering function. The pack does NOT ship an SMTP client; the host
injects a `send(email) → Promise<void>` adapter (Resend, SES, Postmark).

```ts
export type DigestPackConfig = {
	tablePrefix?: string;
	getActorId?: (ctx: any) => string;
	/** Cron pattern for the digest fire. Default "0 8 * * 1" (Mon 8am). */
	cron?: string;
	/** Host-supplied email sender. The pack does not own transport. */
	send: (msg: { to: string; subject: string; body: string }) => Promise<void>;
	/**
	 * Build the digest payload for an actor since their last digest
	 * timestamp. The pack provides cursor management; the host implements
	 * the actual content roll-up against its own data.
	 */
	buildDigest: (
		actorId: string,
		since: Date,
		ctx: any
	) => Promise<{ subject: string; body: string; to: string } | null>;
	/** Optional bound on actors per fire (back-pressure). Default 1000. */
	maxActorsPerFire?: number;
};
```

**Notable design choices**:

- Pack owns one table: `digest_cursors` (one row per actor, with
  `lastSentAt`). On fire, the schedule iterates actors with
  `lastSentAt + cron-interval < now`, calls `buildDigest`, calls `send`,
  updates the cursor. Each actor's send is independent; one failure
  doesn't block the rest.
- The pack ships a sandboxed retry-with-backoff for `send` via the
  existing `RetryPolicy` on `defineMutation`. (No, that doesn't apply
  to schedules. Open question — see §8.)
- `engine.runSchedule("@absolutejs/sync-pack-digest:fire")` is callable
  ad-hoc for test runs.

**Engine surface used**: `register` (for the cursors collection so the
host can subscribe to "your last digest was at X"), `registerSchedule`,
`registerSchema`, `registerPermissions`, `registerReader`/`registerWriter`
for the cursors table.

---

## 8. Open questions

1. ~~**Retry policy for schedules.**~~ **Locked**: extend
   `ScheduleDefinition` with `retry?: RetryPolicy`. Retry applies to the
   _whole handler_ — handler must be idempotent under retry, same
   contract as `defineMutation.retry`. Per-item retry (e.g. digest's
   per-actor `send`) is the pack's job, written as a loop inside the
   handler; the outer schedule retry covers transient infra failures
   only. Rationale: this is an engine gap, not a pack concern — any
   user-defined schedule with external I/O needs it. Reusing
   `RetryPolicy` keeps schedules and mutations conceptually symmetric.

2. **Pack-to-pack dependency declarations.** A docs-comments-pack might
   want to soft-depend on the presence-pack ("when comments-pack notices
   you typed, ask presence-pack to flag you as typing in this thread").
   The composition rule (§5) says it should subscribe to presence
   collections, not call presence mutations. But this still means the
   docs-comments-pack reads from a table the presence-pack owns. Do we
   surface this as a recommended discovery API (`engine.inspect().packs`
   is enough) or build a typed dependency-declaration field?
   **Tentative**: leave it to `inspect()` for v1. Typed inter-pack deps
   force a bigger commitment than packs need yet.

3. **Multiple instances of the same pack.** Two presence packs (one per
   product surface) on the same engine — supported by `tablePrefix`, but
   collection names also need to differ. Should the pack's collection
   names be prefixed too? Recommendation: yes, prefix collections with
   the same `tablePrefix`, so `presence` becomes `feature_a_presence` etc.

4. **Testing story.** Each pack ships with `bun test` that boots a
   `createSyncEngine` in-memory and walks every mutation + collection.
   The pack repo should not invent a new test harness — it uses the
   sync engine's existing one. We may want a tiny `createTestEngine()`
   helper exported from `@absolutejs/sync/testing` (currently the
   tests/ in sync use `createSyncEngine` directly).

5. ~~**Where do pack repos live?**~~ **Locked**: `~/abs/sync-packs/`
   monorepo — same pattern as `~/abs/sync-adapters` (the CRDT-backend
   monorepo). Coordinated releases when the `SyncPack` shape evolves.

6. **Pre-release vs first-class.** Should the first pack release ship
   under a `0.x` line so we can adjust the shape, or is v1 the
   contract? Recommendation: **0.x** through the third pack. If the
   third pack reveals a missing pack-API field, we still have room to
   add it before locking the shape.

7. **Migration story for non-pack consumers.** A host app that already
   wrote its own `presence` table — what happens? Recommendation:
   `tablePrefix` is the answer; the existing setup keeps working under
   its own name, the pack registers under `pack_presence`. No magic
   adoption.

---

## 9. What ships first

Strict order to avoid the "third-pack-reveals-a-bug-in-the-API" failure:

1. **`engine.registerPack` + `SyncPack`** on `@absolutejs/sync`. No new
   features — purely a dispatcher. Land with contract tests against the
   existing surface so devtools and the registry are right.
2. **`@absolutejs/sync-pack-presence` v0.1.0.** Smallest of the three.
   Forces the pack API to handle config injection, ownership, schedules,
   and permissions.
3. **`examples/sync` demo** uses the presence pack. Per the standing
   `examples-sync-is-canonical-flagship` memory: presence is added to
   `examples/sync`, not a sibling demo.
4. **`@absolutejs/sync-pack-comments` v0.1.0.** Forces joins,
   per-resource permission injection, optional CRDT.
5. **`@absolutejs/sync-pack-digest` v0.1.0.** Forces scheduled work
   with side effects and the open question about schedule retries.
6. **Promote `SyncPack` to a v1 commitment** in `@absolutejs/sync` only
   after all three packs ship and the open-question list above is
   resolved or explicitly punted.

If at step 4 or 5 we discover a missing field, we add it to the engine
side first (additive, no break), then bump pack versions. Packs stay on
0.x until step 6.
