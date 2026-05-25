import type { SQL, Table } from 'drizzle-orm';
import type { ReactiveHub } from '../../reactiveHub';
import {
	extractKeyFromWhere,
	extractRowKeys,
	keyTopic,
	tableTopic
} from './topics';

/**
 * Drizzle write-side topic publishing (Tier 2).
 *
 * The mirror of {@link deriveReadTopics}: after a mutation commits, publish the
 * topics it invalidates so subscribed reads refetch. Every change publishes the
 * **table** topic (so list/table queries refresh) plus a **row** topic per
 * affected key (so row-level queries refresh) — the exact topics the read side
 * subscribes to.
 *
 * These are the "route mutations through us" change source from the roadmap:
 * call them right after your durable write. They work on any DB Drizzle supports
 * and never touch DB-specific machinery; out-of-band writes (caught later by CDC
 * adapters) are the only thing they miss.
 */

/** The kind of mutation, forwarded in the change-event payload. */
export type ChangeOp = 'insert' | 'update' | 'delete';

/** Payload carried by every change event the write side publishes. */
export type ChangePayload = {
	/** Name of the table that changed. */
	table: string;
	/** Mutation kind, when the caller provided it. */
	op?: ChangeOp;
	/** Affected row keys (empty for a table-wide change). */
	keys: (string | number)[];
};

export type PublishChangeOptions = {
	/** Row keys that changed; each emits a `table:key` topic. */
	keys?: ReadonlyArray<string | number>;
	op?: ChangeOp;
};

/**
 * Publish the reactive topics a change to `table` invalidates: the whole-table
 * topic (always) plus a `table:key` topic per affected row. Call after the
 * durable write commits. Returns the (de-duplicated) topics published.
 */
export const publishChange = (
	hub: Pick<ReactiveHub, 'publish'>,
	table: Table,
	options: PublishChangeOptions = {}
): string[] => {
	const name = tableTopic(table);
	const keys = options.keys === undefined ? [] : [...new Set(options.keys)];
	const payload: ChangePayload = { table: name, op: options.op, keys };
	const topics = [
		...new Set([name, ...keys.map((key) => keyTopic(table, key))])
	];
	for (const topic of topics) {
		hub.publish(topic, payload);
	}
	return topics;
};

export type PublishRowsOptions = {
	/** Key column (JS property name); defaults to the table's primary key. */
	keyColumn?: string;
	op?: ChangeOp;
};

/**
 * Publish change topics for a set of rows — typically the output of a mutation's
 * `.returning()`, which yields real keys including auto-generated ones. Reads
 * each row's primary-key column (or `keyColumn`) to emit `table:key` topics.
 *
 * @example
 * const rows = await db.insert(users).values(input).returning();
 * publishRows(hub, users, rows, { op: 'insert' });
 */
export const publishRows = (
	hub: Pick<ReactiveHub, 'publish'>,
	table: Table,
	rows: ReadonlyArray<Record<string, unknown>>,
	options: PublishRowsOptions = {}
): string[] =>
	publishChange(hub, table, {
		keys: extractRowKeys(table, rows, options.keyColumn),
		op: options.op
	});

export type PublishWhereOptions = {
	/** Key column (JS property name); defaults to the table's primary key. */
	keyColumn?: string;
	op?: ChangeOp;
};

/**
 * Publish change topics for an `update`/`delete` identified by a `where` filter.
 * A simple primary-key equality narrows to that row's topic; any other filter
 * publishes just the table topic, so every affected subscriber refetches and
 * re-evaluates.
 *
 * @example
 * await db.update(users).set(patch).where(eq(users.id, id));
 * publishWhere(hub, users, eq(users.id, id), { op: 'update' });
 */
export const publishWhere = (
	hub: Pick<ReactiveHub, 'publish'>,
	table: Table,
	where: SQL,
	options: PublishWhereOptions = {}
): string[] => {
	const key = extractKeyFromWhere(table, where, options.keyColumn);
	return publishChange(hub, table, {
		keys: key === undefined ? [] : [key],
		op: options.op
	});
};
