import type { ReactiveHub } from '../../reactiveHub';
import {
	extractKeyFromWhere,
	extractRowKeys,
	keyTopic,
	tableTopic
} from './topics';
import type { PrismaWhere, RowKey } from './topics';

/**
 * Prisma write-side topic publishing (Tier 2).
 *
 * The mirror of {@link deriveReadTopics}: after a mutation commits, publish the
 * topics it invalidates so subscribed reads refetch. Every change publishes the
 * **model** topic (so list queries refresh) plus a **row** topic per affected
 * key (so row-level queries refresh) — the exact topics the read side derives.
 *
 * Use the SAME model identifier you pass on the read side; the topic is the
 * model name verbatim.
 */

/** The kind of mutation, forwarded in the change-event payload. */
export type ChangeOp = 'insert' | 'update' | 'delete';

/** Payload carried by every change event the write side publishes. */
export type ChangePayload = {
	/** Name of the model that changed. */
	table: string;
	/** Mutation kind, when the caller provided it. */
	op?: ChangeOp;
	/** Affected row keys (empty for a model-wide change). */
	keys: RowKey[];
};

export type PublishChangeOptions = {
	/** Row keys that changed; each emits a `model:key` topic. */
	keys?: ReadonlyArray<RowKey>;
	op?: ChangeOp;
};

/**
 * Publish the reactive topics a change to `model` invalidates: the whole-model
 * topic (always) plus a `model:key` topic per affected row. Call after the
 * mutation resolves. Returns the (de-duplicated) topics published.
 */
export const publishChange = (
	hub: Pick<ReactiveHub, 'publish'>,
	model: string,
	options: PublishChangeOptions = {}
): string[] => {
	const name = tableTopic(model);
	const keys = options.keys === undefined ? [] : [...new Set(options.keys)];
	const payload: ChangePayload = { table: name, op: options.op, keys };
	const topics = [
		...new Set([name, ...keys.map((key) => keyTopic(model, key))])
	];
	for (const topic of topics) {
		hub.publish(topic, payload);
	}
	return topics;
};

export type PublishRowsOptions = {
	/** Key field (defaults to `id`). */
	keyField?: string;
	op?: ChangeOp;
};

/**
 * Publish change topics for the record(s) a mutation returned. Accepts a single
 * record (Prisma `create`/`update`/`delete`) or an array (`findMany` results),
 * reading each record's `keyField` to emit `model:key` topics.
 *
 * @example
 * const user = await prisma.user.create({ data });
 * publishRows(hub, 'user', user, { op: 'insert' });
 */
export const publishRows = (
	hub: Pick<ReactiveHub, 'publish'>,
	model: string,
	rows: Record<string, unknown> | ReadonlyArray<Record<string, unknown>>,
	options: PublishRowsOptions = {}
): string[] => {
	const list = (Array.isArray(rows) ? rows : [rows]) as ReadonlyArray<
		Record<string, unknown>
	>;
	return publishChange(hub, model, {
		keys: extractRowKeys(list, options.keyField ?? 'id'),
		op: options.op
	});
};

export type PublishWhereOptions = {
	/** Key field (defaults to `id`). */
	keyField?: string;
	op?: ChangeOp;
};

/**
 * Publish change topics for an `update`/`delete` identified by a `where` filter.
 * A simple key-field equality narrows to that row's topic; any other filter
 * publishes just the model topic, so every affected subscriber refetches.
 *
 * @example
 * await prisma.user.update({ where: { id }, data });
 * publishWhere(hub, 'user', { id }, { op: 'update' });
 */
export const publishWhere = (
	hub: Pick<ReactiveHub, 'publish'>,
	model: string,
	where: PrismaWhere,
	options: PublishWhereOptions = {}
): string[] => {
	const key = extractKeyFromWhere(where, options.keyField ?? 'id');
	return publishChange(hub, model, {
		keys: key === undefined ? [] : [key],
		op: options.op
	});
};
