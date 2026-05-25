/**
 * Shared topic vocabulary + key extraction for the Prisma adapter. Operates on
 * Prisma's plain `where` objects and result records — there is no
 * `@prisma/client` import, so it stays generic across every database Prisma
 * supports. Both the read side (derive a query's topics) and the write side
 * (publish a mutation's topics) build on these so the two always agree.
 */

/** A Prisma `where` filter object, e.g. `{ id: 5 }` or `{ id: { gt: 5 } }`. */
export type PrismaWhere = Record<string, unknown>;

/** A scalar usable as a row key. */
export type RowKey = string | number | bigint;

/** The coarse topic every read/write of `model` touches, e.g. `user`. */
export const tableTopic = (model: string): string => model;

/** The row-level topic for one key of `model`, e.g. `user:5`. */
export const keyTopic = (model: string, key: RowKey): string =>
	`${model}:${key}`;

const isRowKey = (value: unknown): value is RowKey =>
	typeof value === 'string' ||
	typeof value === 'number' ||
	typeof value === 'bigint';

/**
 * Best-effort: pull a single key-field equality value out of a Prisma `where`.
 * Recognises `{ [keyField]: scalar }` and `{ [keyField]: { equals: scalar } }`
 * only — extra fields, other operators (`gt`/`in`/`not`/…), `AND`/`OR`, or a
 * compound-key object yield `undefined`, so the caller falls back to the table
 * topic (over-invalidating a little rather than missing an update).
 */
export const extractKeyFromWhere = (
	where: PrismaWhere,
	keyField: string
): RowKey | undefined => {
	const fields = Object.keys(where);
	if (fields.length !== 1 || fields[0] !== keyField) {
		return undefined;
	}
	const condition = where[keyField];
	if (isRowKey(condition)) {
		return condition;
	}
	if (
		condition !== null &&
		typeof condition === 'object' &&
		!Array.isArray(condition)
	) {
		const operators = Object.keys(condition as Record<string, unknown>);
		if (operators.length === 1 && operators[0] === 'equals') {
			const value = (condition as Record<string, unknown>).equals;
			if (isRowKey(value)) {
				return value;
			}
		}
	}
	return undefined;
};

/**
 * Read the key value from each result record (e.g. the output of a Prisma
 * `create`/`update`/`delete`), using `keyField`. Records without a scalar key
 * are skipped.
 */
export const extractRowKeys = (
	rows: ReadonlyArray<Record<string, unknown>>,
	keyField: string
): RowKey[] => {
	const keys: RowKey[] = [];
	for (const row of rows) {
		const value = row[keyField];
		if (isRowKey(value)) {
			keys.push(value);
		}
	}
	return keys;
};
