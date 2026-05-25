import type {
	CollectionContext,
	CollectionDefinition
} from '../../engine/collection';
import type { RowKey } from '../../engine/types';
import { matchesWhere } from './predicate';
import type { PrismaWhere } from './topics';

export type PrismaCollectionOptions<T, P, Ctx> = {
	/** Collection name (change-feed key and topic root). */
	name: string;
	/**
	 * The query filter, written once. Used both to hydrate from the database
	 * (passed to `find`) and, mirrored in JS, to match changed rows
	 * incrementally. Receives the subscription's params and context.
	 */
	where: (params: P, ctx: Ctx) => PrismaWhere;
	/**
	 * Run the database read for a given `where` — your Prisma call, e.g.
	 * `(where) => prisma.order.findMany({ where })`.
	 */
	find: (
		where: PrismaWhere,
		params: P,
		ctx: Ctx
	) => Promise<Iterable<T>> | Iterable<T>;
	/** Row identity. Defaults to `row.id`. */
	key?: (row: T) => RowKey;
	/** Access control; return false (or throw) to deny. */
	authorize?: (params: P, ctx: Ctx) => boolean | Promise<boolean>;
};

/**
 * Build a syncable {@link CollectionDefinition} for Prisma from a single filter:
 * `where` is written once and powers both `hydrate` (via your `find`) and the
 * incremental `match` (via {@link matchesWhere}) — no restating the WHERE.
 *
 * If the filter uses an operator the JS matcher can't evaluate, that change
 * degrades to a refetch (handled by the engine), so the result stays correct.
 *
 * @example
 * prismaCollection({
 *   name: 'orders',
 *   where: (p) => ({ userId: p.userId, status: 'open' }),
 *   find: (where) => prisma.order.findMany({ where }),
 *   authorize: (p, ctx) => p.userId === ctx.userId
 * });
 */
export const prismaCollection = <T, P = void, Ctx = CollectionContext>(
	options: PrismaCollectionOptions<T, P, Ctx>
): CollectionDefinition<T, P, Ctx> => ({
	name: options.name,
	hydrate: (params, ctx) =>
		options.find(options.where(params, ctx), params, ctx),
	match: (row, params, ctx) =>
		matchesWhere(
			options.where(params, ctx),
			row as Record<string, unknown>
		),
	key: options.key,
	authorize: options.authorize
});
