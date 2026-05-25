import type { CollectionDefinition } from './collection';
import type { MutationDefinition } from './mutation';
import type { SyncEngine } from './syncEngine';

/**
 * Eden-native HTTP route helpers (Tier 3). These turn a typed collection /
 * mutation definition into a plain Elysia route handler — so hydrate and mutate
 * are ordinary Elysia routes that Eden types end to end, with TypeBox validating
 * the params/body. The live diff stream stays on the WebSocket (`syncSocket`).
 *
 * They import no Elysia: each returns `(context) => Promise<...>`, which you pass
 * to `.get` / `.post` with a TypeBox `query` / `body` schema. The handler's
 * return type carries the row / result type, so `treaty<typeof app>()` infers it.
 */

/** The slice of an Elysia route context these helpers read. */
export type SyncRouteContext = {
	query: unknown;
	body: unknown;
	[key: string]: unknown;
};

const emptyContext = () => ({});

/**
 * Build a GET handler that hydrates `collection` — authorize, then return its
 * current rows. The handler returns `Promise<Row[]>`, so Eden infers the row
 * type from the collection definition; the route's TypeBox `query` schema
 * validates and types the params.
 *
 * @example
 * .get('/sync/orders', hydrateRoute(engine, ordersCollection, (c) => ({ userId: c.userId })),
 *      { query: t.Object({ userId: t.Numeric() }) })
 */
export const hydrateRoute = <Row, Params, Ctx>(
	engine: SyncEngine,
	collection: CollectionDefinition<Row, Params, Ctx>,
	resolveContext: (
		context: SyncRouteContext
	) => Ctx = emptyContext as () => Ctx
) => {
	return async (context: SyncRouteContext): Promise<Row[]> => {
		const rows = await engine.hydrate(
			collection.name,
			context.query as Params,
			resolveContext(context)
		);
		return rows as Row[];
	};
};

/**
 * Build a POST handler that runs `mutation`. The handler returns
 * `Promise<Result>`, so Eden infers the result type; the route's TypeBox `body`
 * schema validates and types the args.
 *
 * @example
 * .post('/sync/createOrder', mutateRoute(engine, createOrder, (c) => ({ userId: c.userId })),
 *       { body: t.Object({ total: t.Number() }) })
 */
export const mutateRoute = <Args, Ctx, Result>(
	engine: SyncEngine,
	mutation: MutationDefinition<Args, Ctx, Result>,
	resolveContext: (
		context: SyncRouteContext
	) => Ctx = emptyContext as () => Ctx
) => {
	return async (context: SyncRouteContext): Promise<Result> => {
		const result = await engine.runMutation(
			mutation.name,
			context.body as Args,
			resolveContext(context)
		);
		return result as Result;
	};
};
