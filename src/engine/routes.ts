import type { CollectionDefinition } from './collection';
import type { MutationDefinition } from './mutation';
import type { SyncEngine } from './syncEngine';
import {
	type HeadlessSyncMutation,
	type HeadlessSyncPull,
	type HeadlessSyncRequest,
	type HeadlessSyncResponse
} from '../headlessProtocol';
import { toSyncMutationRejection } from '../reconciliation';

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

export type HeadlessSyncRouteOptions<Ctx> = {
	resolveContext: (context: SyncRouteContext) => Ctx | Promise<Ctx>;
	/** Maximum mutations accepted in one finite exchange. Defaults to 50. */
	maxMutations?: number;
	/** Maximum collection pulls accepted in one finite exchange. Defaults to 50. */
	maxPulls?: number;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
	typeof value === 'object' && value !== null && !Array.isArray(value);

const positiveLimit = (value: number | undefined, fallback: number) => {
	const limit = value ?? fallback;
	if (!Number.isSafeInteger(limit) || limit < 0)
		throw new TypeError(
			'Headless Sync limits must be non-negative integers.'
		);
	return limit;
};

const parseHeadlessMutation = (value: unknown): HeadlessSyncMutation => {
	if (
		!isRecord(value) ||
		typeof value.operationId !== 'string' ||
		value.operationId.length === 0 ||
		typeof value.name !== 'string' ||
		value.name.length === 0
	)
		throw new TypeError('Invalid Headless Sync mutation.');
	return {
		operationId: value.operationId,
		name: value.name,
		...(value.args === undefined ? {} : { args: value.args })
	};
};

const parseHeadlessPull = (value: unknown): HeadlessSyncPull => {
	if (
		!isRecord(value) ||
		typeof value.id !== 'string' ||
		value.id.length === 0 ||
		typeof value.collection !== 'string' ||
		value.collection.length === 0 ||
		(value.since !== undefined &&
			typeof value.since !== 'string' &&
			typeof value.since !== 'number')
	)
		throw new TypeError('Invalid Headless Sync pull.');
	return {
		id: value.id,
		collection: value.collection,
		...(value.params === undefined ? {} : { params: value.params }),
		...(value.since === undefined ? {} : { since: value.since })
	};
};

/**
 * Build one authenticated, finite HTTP exchange for background runtimes. The
 * host owns authentication through `resolveContext`; bearer/cookie policy is
 * never embedded in Sync. Mutations run in order through durable receipts,
 * then pulls observe their committed effects and immediately unsubscribe.
 */
export const headlessSyncRoute = <Ctx>(
	engine: SyncEngine,
	{
		resolveContext,
		maxMutations: configuredMaxMutations,
		maxPulls: configuredMaxPulls
	}: HeadlessSyncRouteOptions<Ctx>
) => {
	const maxMutations = positiveLimit(configuredMaxMutations, 50);
	const maxPulls = positiveLimit(configuredMaxPulls, 50);
	return async (context: SyncRouteContext): Promise<HeadlessSyncResponse> => {
		// Authenticate before inspecting attacker-controlled Sync payloads. The
		// auto-mounted route also requires a syntactically valid Bearer header.
		const ctx = await resolveContext(context);
		if (!isRecord(context.body) || context.body.version !== 1)
			throw new TypeError('Invalid Headless Sync request version.');
		const rawMutations = context.body.mutations ?? [];
		const rawPulls = context.body.pulls ?? [];
		if (!Array.isArray(rawMutations) || rawMutations.length > maxMutations)
			throw new TypeError('Headless Sync mutation limit exceeded.');
		if (!Array.isArray(rawPulls) || rawPulls.length > maxPulls)
			throw new TypeError('Headless Sync pull limit exceeded.');
		const request: HeadlessSyncRequest = {
			version: 1,
			mutations: rawMutations.map(parseHeadlessMutation),
			pulls: rawPulls.map(parseHeadlessPull)
		};
		const mutations: HeadlessSyncResponse['mutations'] = [];
		for (const mutation of request.mutations ?? []) {
			try {
				const result = await engine.runMutation(
					mutation.name,
					mutation.args,
					ctx,
					{ operationId: mutation.operationId }
				);
				mutations.push({
					operationId: mutation.operationId,
					status: 'ack',
					...(result === undefined ? {} : { result })
				});
			} catch (error) {
				mutations.push({
					operationId: mutation.operationId,
					status: 'reject',
					rejection: toSyncMutationRejection(error)
				});
			}
		}

		const pulls: HeadlessSyncResponse['pulls'] = [];
		for (const pull of request.pulls ?? []) {
			try {
				const subscription = await engine.subscribe({
					collection: pull.collection,
					params: pull.params,
					ctx,
					since: pull.since,
					onDiff: () => undefined
				});
				try {
					pulls.push(
						subscription.catchup === undefined
							? {
									id: pull.id,
									type: 'snapshot',
									rows: subscription.initial,
									version: subscription.version,
									cursor: subscription.cursor
								}
							: {
									id: pull.id,
									type: 'diff',
									...subscription.catchup,
									version: subscription.version,
									cursor: subscription.cursor
								}
					);
				} finally {
					subscription.unsubscribe();
				}
			} catch (error) {
				pulls.push({
					id: pull.id,
					type: 'error',
					message:
						error instanceof Error ? error.message : String(error)
				});
			}
		}
		return { version: 1, mutations, pulls };
	};
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
