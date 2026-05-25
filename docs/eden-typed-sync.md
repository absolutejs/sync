# End-to-end type safety, the Eden way

This is the spec for the typed sync surface. The guiding decision: **don't build a
parallel type system — lean all the way into Eden + TypeBox.** Eden already solves
typed transport + validation; the sync engine only owns what Eden can't: the
stateful client (local cache, diffs, optimistic writes, offline).

## The layering

| Concern                                   | Owner                          |
| ----------------------------------------- | ------------------------------ |
| Types over the wire + runtime validation  | **Eden + TypeBox** (`t`)       |
| One-shot read (hydrate) + mutate          | **Elysia routes** (Eden-typed) |
| Live `{added,removed,changed}` diffs      | **`syncSocket`** (WebSocket)   |
| Local reactive cache / optimism / offline | **`syncStore`** (client)       |

Eden is a typed transport; it holds no state. The reactive store is the
irreducible "our own" piece — and it's generic, so its types come _from_ Eden.

## Server — ordinary Elysia routes

Hydrate and mutate are normal Elysia routes. The `hydrateRoute` / `mutateRoute`
helpers turn a typed collection / mutation definition into a route handler whose
**return type carries the row / result type**, so `treaty<typeof app>()` infers it.
TypeBox (`t`, re-exported by Elysia) validates and types the `query` / `body`.

```ts
import { Elysia, t } from 'elysia';
import {
	createSyncEngine,
	hydrateRoute,
	mutateRoute
} from '@absolutejs/sync/engine';
import { syncSocket } from '@absolutejs/sync';
import { prismaCollection } from '@absolutejs/sync/prisma';
import { defineMutation } from '@absolutejs/sync/engine';

const engine = createSyncEngine();

const orders = prismaCollection({
	name: 'orders',
	where: (p: { userId: number }) => ({ userId: p.userId, status: 'open' }),
	find: (where) => prisma.order.findMany({ where }),
	authorize: (p, ctx: { userId: number }) => p.userId === ctx.userId
});
engine.register(orders);

const createOrder = defineMutation({
	name: 'createOrder',
	handler: async (
		args: { total: number },
		ctx: { userId: number },
		actions
	) => {
		const order = await prisma.order.create({
			data: { ...args, userId: ctx.userId }
		});
		await actions.change('orders', { op: 'insert', row: order });
		return order;
	}
});
engine.registerMutation(createOrder);

const auth = (c: { userId?: number }) => ({ userId: c.userId ?? 0 }); // from your derive/session

const app = new Elysia()
	.use(syncSocket({ engine, resolveContext: auth })) // live diffs (WS)
	.get('/sync/orders', hydrateRoute(engine, orders, auth), {
		query: t.Object({ userId: t.Numeric() })
	})
	.post('/sync/createOrder', mutateRoute(engine, createOrder, auth), {
		body: t.Object({ total: t.Number() })
	});

export type App = typeof app; // ← the Eden export (type-only)
```

Why explicit routes, not a builder: TypeScript can't infer route types from a
runtime loop over definitions, so the only way to get Eden types without
_reimplementing Elysia's route-chaining generics_ (fragile, version-coupled) is
real chained routes. They're also a feature — per-route guards, rate limits,
`derive` all work because they _are_ Elysia routes. If the two-lines-per-collection
ever bites at scale, the robust fix is **codegen** (emit the chained routes from
definitions, like Convex's generated dir) — never a runtime builder.

## Client — literally Eden + a generic store

```ts
import { treaty } from '@elysiajs/eden';
import type { App } from '../server'; // type-only — no server code shipped
import { syncStore } from '@absolutejs/sync/client';

const api = treaty<App>('localhost:3000');

const orders = syncStore({
	hydrate: () => api.sync.orders.get({ query: { userId } }), // Eden-typed → infers Order[]
	mutate: (a) => api.sync.createOrder.post(a), // Eden-typed args + result
	diffs: { collection: 'orders', params: { userId } } // live WS diffs
});

orders.subscribe((s) => render(s.data)); // s.data: Order[] — type entirely from Eden
await orders.mutate(
	{ total: 42 },
	{ optimistic: (d) => d.set({ id: tmp, status: 'open' }) }
);
```

`syncStore` is generic: row type inferred from `hydrate`'s return, mutate args/result
from `mutate`'s signature. No `<T>`, no parallel schema, no custom inference.

### Runtime model

- **Confirmed state** comes from the WS (`syncSocket`): a snapshot on subscribe,
  then ordered diffs — race-free, already built and tested. `hydrate` is used for
  the row **type** and for SSR seeding (`initialData`).
- **Mutations** go over Eden HTTP (typed). The store applies an optimistic overlay,
  awaits the call, and reconciles: roll back on reject; on success drop the overlay
  once the WS diff has reflected the touched keys (with a short grace fallback for
  mutations that don't touch this collection). The precise long-term mechanism is a
  monotonic **version cursor** (hydrate/diff/mutate carry a version; drop the overlay
  at `appliedVersion >= mutationVersion`) — the same change-feed sequencing tracked
  as Tier C hardening.
- **Offline**: pending mutate calls are queued and replayed on reconnect; optional
  durable `storage` survives reload.

## What ships

- `@absolutejs/sync/engine`: `engine.hydrate(...)`, `hydrateRoute`, `mutateRoute`
  (done); `syncStore` is added on the client next.
- `@absolutejs/sync/client`: `syncStore` (generic, Eden-fed). `createSyncCollection`
  stays as the batteries-included path for non-Eden use.
- No new type machinery. Eden + TypeBox do 100% of the typing.

## Status

Server pieces (`engine.hydrate`, `hydrateRoute`, `mutateRoute`) are implemented and
verified with a real `treaty<typeof app>` round trip. `syncStore` is the next
checkpoint.
