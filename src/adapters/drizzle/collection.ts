import { getTableName } from 'drizzle-orm';
import type { SQL, Table } from 'drizzle-orm';
import type {
	CollectionContext,
	CollectionDefinition
} from '../../engine/collection';
import type { RowKey } from '../../engine/types';
import { matchesDrizzleWhere } from './predicate';
import { resolveKeyColumn } from './topics';

export type DrizzleCollectionOptions<T, P = void, Ctx = CollectionContext> = {
	/** Collection name clients subscribe to. */
	name: string;
	/** The Drizzle table this collection reads (drives change routing + key). */
	table: Table;
	/** The query filter, written once — powers both hydrate and the matcher. */
	where: (params: P, ctx: Ctx) => SQL;
	/** Run the read for `where` (your `db.select()...`), returning the rows. */
	find: (
		where: SQL,
		params: P,
		ctx: Ctx
	) => Promise<Iterable<T>> | Iterable<T>;
	/** Row identity. Defaults to the table's single primary key (else `row.id`). */
	key?: (row: T) => RowKey;
	/** Key column JS property, if not the table's primary key. */
	keyColumn?: string;
	/** Access control; return false (or throw) to deny the subscription. */
	authorize?: (params: P, ctx: Ctx) => boolean | Promise<boolean>;
};

/**
 * A sync-engine collection from one Drizzle query — the Drizzle counterpart to
 * `prismaCollection`. You write the `where` once: it drives the DB read
 * (`hydrate`) AND the incremental `match` (via {@link matchesDrizzleWhere}), so
 * the two can't drift and you never hand-maintain a separate predicate. A filter
 * the matcher can't evaluate falls back to a refetch, never a wrong result.
 */
export const drizzleCollection = <T, P = void, Ctx = CollectionContext>(
	options: DrizzleCollectionOptions<T, P, Ctx>
): CollectionDefinition<T, P, Ctx> => {
	const keyProp = resolveKeyColumn(
		options.table,
		options.keyColumn
	)?.property;
	const key =
		options.key ??
		((row: T) =>
			keyProp !== undefined
				? (row as Record<string, RowKey>)[keyProp]!
				: (row as { id: RowKey }).id);

	return {
		name: options.name,
		tables: [getTableName(options.table)],
		hydrate: (params, ctx) =>
			options.find(options.where(params, ctx), params, ctx),
		match: (row, params, ctx) =>
			matchesDrizzleWhere(
				options.table,
				options.where(params, ctx),
				row as Record<string, unknown>
			),
		key,
		authorize: options.authorize
	};
};
