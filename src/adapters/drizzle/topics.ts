import {
	Column,
	getTableColumns,
	getTableName,
	is,
	Param,
	SQL
} from 'drizzle-orm';
import type { Table } from 'drizzle-orm';

/**
 * Shared topic vocabulary + key resolution for the Drizzle adapter. Both the
 * read side (derive the topics a query depends on) and the write side (publish
 * the topics a mutation invalidates) build on these so the two always agree.
 */

/** The coarse topic every read/write of `table` touches, e.g. `users`. */
export const tableTopic = (table: Table): string => getTableName(table);

/** The row-level topic for one key of `table`, e.g. `users:5`. */
export const keyTopic = (table: Table, key: string | number): string =>
	`${getTableName(table)}:${key}`;

type ResolvedKey = {
	/** JS property name of the key column on the table / result rows. */
	property: string;
	/** Underlying DB column name, as it appears in a SQL expression. */
	column: string;
};

/**
 * Resolve the column to use as the row key: an explicitly requested column (by
 * JS property name), otherwise the table's sole primary key. Returns `undefined`
 * when no single key column applies (composite or missing primary key).
 */
export const resolveKeyColumn = (
	table: Table,
	keyColumn?: string
): ResolvedKey | undefined => {
	const columns = getTableColumns(table);
	if (keyColumn !== undefined) {
		const column = columns[keyColumn];
		return column === undefined
			? undefined
			: { property: keyColumn, column: column.name };
	}
	const primaries = Object.entries(columns).filter(
		([, column]) => column.primary
	);
	const primary = primaries.length === 1 ? primaries[0] : undefined;
	return primary === undefined
		? undefined
		: { property: primary[0], column: primary[1].name };
};

/**
 * Best-effort: pull a single key-column equality value out of a Drizzle `where`
 * expression. Recognises only the simple `eq(keyColumn, scalar)` shape — any
 * nesting (`and`/`or`), extra columns/params, a non-`=` operator, or a
 * non-key/cross-table column yields `undefined`.
 *
 * Reads Drizzle's internal `queryChunks`, which is not a stable public API;
 * every branch degrades to `undefined` (coarser topic) rather than throwing, so
 * a Drizzle version bump can only cost precision, never correctness.
 */
export const extractKeyFromWhere = (
	table: Table,
	where: SQL,
	keyColumn?: string
): string | number | undefined => {
	const resolved = resolveKeyColumn(table, keyColumn);
	if (resolved === undefined) {
		return undefined;
	}

	const chunks: unknown = (where as { queryChunks?: unknown }).queryChunks;
	if (!Array.isArray(chunks)) {
		return undefined;
	}

	let column: Column | undefined;
	let param: Param | undefined;
	let tooComplex = false;
	let operator = '';

	for (const chunk of chunks) {
		if (is(chunk, SQL)) {
			// Nested expression (e.g. and/or) — not a simple equality.
			return undefined;
		}
		if (is(chunk, Column)) {
			if (column !== undefined) {
				tooComplex = true;
			}
			column = chunk;
		} else if (is(chunk, Param)) {
			if (param !== undefined) {
				tooComplex = true;
			}
			param = chunk;
		} else {
			const value: unknown = (chunk as { value?: unknown }).value;
			if (Array.isArray(value)) {
				operator += value.join('');
			}
		}
	}

	if (tooComplex || column === undefined || param === undefined) {
		return undefined;
	}
	if (operator.trim() !== '=') {
		return undefined;
	}
	if (column.name !== resolved.column) {
		return undefined;
	}
	if (getTableName(column.table) !== getTableName(table)) {
		return undefined;
	}

	const value: unknown = param.value;
	return typeof value === 'string' || typeof value === 'number'
		? value
		: undefined;
};

/**
 * Read the key value from each row (e.g. the output of a mutation's
 * `.returning()`), using the table's primary-key column or an explicit
 * `keyColumn`. Rows without a string/number key are skipped.
 */
export const extractRowKeys = (
	table: Table,
	rows: ReadonlyArray<Record<string, unknown>>,
	keyColumn?: string
): (string | number)[] => {
	const resolved = resolveKeyColumn(table, keyColumn);
	if (resolved === undefined) {
		return [];
	}
	const keys: (string | number)[] = [];
	for (const row of rows) {
		const value = row[resolved.property];
		if (typeof value === 'string' || typeof value === 'number') {
			keys.push(value);
		}
	}
	return keys;
};
