import { Column, getTableColumns, is, Param, SQL } from 'drizzle-orm';
import type { Table } from 'drizzle-orm';

/**
 * Thrown when a Drizzle `where` uses something the incremental matcher can't
 * evaluate in JS (an unsupported operator, a function, a cross-table column…).
 * The sync engine catches it and degrades that subscription to a refetch, so the
 * result is never wrong — only less efficient. Mirrors the Prisma adapter.
 */
export class UnsupportedDrizzleFilterError extends Error {
	constructor(detail: string) {
		super(`Cannot evaluate Drizzle filter "${detail}" incrementally`);
		this.name = 'UnsupportedDrizzleFilterError';
	}
}

const isDate = (value: unknown): value is Date => value instanceof Date;

const equals = (value: unknown, operand: unknown): boolean => {
	if (operand === null) {
		return value === null || value === undefined;
	}
	if (isDate(value) && isDate(operand)) {
		return value.getTime() === operand.getTime();
	}
	return value === operand;
};

const order = (value: unknown): number | string =>
	isDate(value) ? value.getTime() : (value as number | string);

const compare = (value: unknown, operand: unknown): number => {
	const a = order(value);
	const b = order(operand);
	if (a < b) {
		return -1;
	}
	if (a > b) {
		return 1;
	}
	return 0;
};

const comparable = (value: unknown): boolean =>
	value !== null && value !== undefined;

type Classified = {
	cols: Column[];
	params: unknown[];
	arrays: unknown[][];
	sqls: SQL[];
	ops: string[];
};

/**
 * Split a condition's `queryChunks` into its parts. Drizzle interleaves columns,
 * bound `Param`s, value arrays (for `in`), nested `SQL` (connectives), and string
 * chunks that carry the operators/parens. Reading `queryChunks` is Drizzle's
 * internal shape, not a stable API — but every unrecognized form throws below
 * (→ refetch), so a version bump can only cost efficiency, never correctness.
 */
const classify = (chunks: unknown[]): Classified => {
	const cols: Column[] = [];
	const params: unknown[] = [];
	const arrays: unknown[][] = [];
	const sqls: SQL[] = [];
	const ops: string[] = [];
	for (const chunk of chunks) {
		if (is(chunk, SQL)) {
			sqls.push(chunk);
		} else if (is(chunk, Column)) {
			cols.push(chunk);
		} else if (is(chunk, Param)) {
			params.push(chunk.value);
		} else if (Array.isArray(chunk)) {
			arrays.push(
				chunk.map((element) =>
					is(element, Param) ? element.value : element
				)
			);
		} else {
			const raw = (chunk as { value?: unknown }).value;
			const text = (Array.isArray(raw) ? raw.join('') : String(raw ?? ''))
				.trim()
				.replace(/^[()]+\s*/, '')
				.replace(/\s*[()]+$/, '')
				.trim();
			if (text !== '' && text !== '(' && text !== ')') {
				ops.push(text);
			}
		}
	}

	return { arrays, cols, ops, params, sqls };
};

const evaluateLeaf = (
	column: Column,
	op: string,
	params: unknown[],
	arrays: unknown[][],
	row: Record<string, unknown>,
	propFor: (column: Column) => string | undefined
): boolean => {
	const prop = propFor(column);
	if (prop === undefined) {
		throw new UnsupportedDrizzleFilterError(`column ${column.name}`);
	}
	const value = row[prop];
	const operand = params[0];
	switch (op) {
		case '=':
			return equals(value, operand);
		case '<>':
			return !equals(value, operand);
		case '>':
			return comparable(value) && compare(value, operand) > 0;
		case '>=':
			return comparable(value) && compare(value, operand) >= 0;
		case '<':
			return comparable(value) && compare(value, operand) < 0;
		case '<=':
			return comparable(value) && compare(value, operand) <= 0;
		case 'in':
			return (arrays[0] ?? []).some((item) => equals(value, item));
		case 'not in':
			return !(arrays[0] ?? []).some((item) => equals(value, item));
		case 'is null':
			return value === null || value === undefined;
		case 'is not null':
			return value !== null && value !== undefined;
		default:
			throw new UnsupportedDrizzleFilterError(op);
	}
};

const evaluateCondition = (
	node: SQL,
	row: Record<string, unknown>,
	propFor: (column: Column) => string | undefined
): boolean => {
	const { cols, params, arrays, sqls, ops } = classify(
		(node as unknown as { queryChunks: unknown[] }).queryChunks
	);

	// not (cond)
	if (
		ops.length === 1 &&
		ops[0] === 'not' &&
		sqls.length === 1 &&
		cols.length === 0
	) {
		return !evaluateCondition(sqls[0]!, row, propFor);
	}
	// A lone nested condition wrapped in parens — unwrap and recurse.
	if (cols.length === 0 && sqls.length === 1 && ops.length === 0) {
		return evaluateCondition(sqls[0]!, row, propFor);
	}
	// and / or over sub-conditions.
	if (cols.length === 0 && sqls.length >= 2 && ops.length > 0) {
		const connective = ops[0];
		if (
			(connective === 'and' || connective === 'or') &&
			ops.every((op) => op === connective)
		) {
			const results = sqls.map((sql) =>
				evaluateCondition(sql, row, propFor)
			);
			return connective === 'and'
				? results.every(Boolean)
				: results.some(Boolean);
		}
		throw new UnsupportedDrizzleFilterError(ops.join(' '));
	}
	// Leaf comparison: one column, one operator.
	if (cols.length === 1 && sqls.length === 0 && ops.length === 1) {
		return evaluateLeaf(cols[0]!, ops[0]!, params, arrays, row, propFor);
	}
	throw new UnsupportedDrizzleFilterError(
		ops.join(' ') || 'unrecognized condition'
	);
};

/**
 * Evaluate a Drizzle `where` condition against a plain row in JS — the
 * incremental matcher for {@link drizzleCollection}. Supports
 * `eq`/`ne`/`gt`/`gte`/`lt`/`lte`, `isNull`/`isNotNull`,
 * `inArray`/`notInArray`, and nested `and`/`or`/`not`; anything else throws
 * {@link UnsupportedDrizzleFilterError} (the engine then refetches). Rows are
 * read by JS property name, as Drizzle returns them.
 */
export const matchesDrizzleWhere = (
	table: Table,
	where: SQL,
	row: Record<string, unknown>
): boolean => {
	const nameToProp = new Map<string, string>();
	for (const [prop, column] of Object.entries(getTableColumns(table))) {
		nameToProp.set(column.name, prop);
	}

	return evaluateCondition(where, row, (column) =>
		nameToProp.get(column.name)
	);
};
