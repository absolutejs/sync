import type { PrismaWhere } from './topics';

/**
 * Thrown when a Prisma `where` uses an operator the incremental matcher can't
 * evaluate in JS. The sync engine catches it and degrades that subscription to
 * a refetch, so it never produces a wrong result — only a less efficient one.
 */
export class UnsupportedFilterError extends Error {
	constructor(operator: string) {
		super(
			`Cannot evaluate Prisma filter operator "${operator}" incrementally`
		);
		this.name = 'UnsupportedFilterError';
	}
}

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
	typeof value === 'object' &&
	value !== null &&
	!Array.isArray(value) &&
	!(value instanceof Date);

/** Prisma equality semantics (a `null` operand means IS NULL). */
const equals = (value: unknown, operand: unknown): boolean => {
	if (operand === null) {
		return value === null || value === undefined;
	}
	if (value instanceof Date && operand instanceof Date) {
		return value.getTime() === operand.getTime();
	}
	return value === operand;
};

const order = (value: unknown): number | string =>
	value instanceof Date ? value.getTime() : (value as number | string);

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

const FIELD_OPERATORS = new Set([
	'equals',
	'not',
	'in',
	'notIn',
	'lt',
	'lte',
	'gt',
	'gte',
	'contains',
	'startsWith',
	'endsWith'
]);

/** Evaluate a single field's condition (scalar equality or an operator object). */
const matchesField = (value: unknown, condition: unknown): boolean => {
	if (!isPlainObject(condition)) {
		return equals(value, condition);
	}
	// Validate support up front: an unsupported operator must force a refetch
	// even when an earlier operator would short-circuit to false.
	for (const operator of Object.keys(condition)) {
		if (!FIELD_OPERATORS.has(operator)) {
			throw new UnsupportedFilterError(operator);
		}
	}
	for (const [operator, operand] of Object.entries(condition)) {
		switch (operator) {
			case 'equals':
				if (!equals(value, operand)) return false;
				break;
			case 'not':
				if (isPlainObject(operand)) {
					if (matchesField(value, operand)) return false;
				} else if (equals(value, operand)) {
					return false;
				}
				break;
			case 'in':
				if (
					!Array.isArray(operand) ||
					!operand.some((item) => equals(value, item))
				)
					return false;
				break;
			case 'notIn':
				if (
					Array.isArray(operand) &&
					operand.some((item) => equals(value, item))
				)
					return false;
				break;
			case 'lt':
				if (!comparable(value) || compare(value, operand) >= 0)
					return false;
				break;
			case 'lte':
				if (!comparable(value) || compare(value, operand) > 0)
					return false;
				break;
			case 'gt':
				if (!comparable(value) || compare(value, operand) <= 0)
					return false;
				break;
			case 'gte':
				if (!comparable(value) || compare(value, operand) < 0)
					return false;
				break;
			case 'contains':
				if (
					typeof value !== 'string' ||
					!value.includes(String(operand))
				)
					return false;
				break;
			case 'startsWith':
				if (
					typeof value !== 'string' ||
					!value.startsWith(String(operand))
				)
					return false;
				break;
			case 'endsWith':
				if (
					typeof value !== 'string' ||
					!value.endsWith(String(operand))
				)
					return false;
				break;
			default:
				// `mode`, relation filters, etc. — bail so the engine refetches.
				throw new UnsupportedFilterError(operator);
		}
	}
	return true;
};

const toConditions = (value: unknown): PrismaWhere[] => {
	if (Array.isArray(value)) {
		return value.filter(isPlainObject);
	}
	return isPlainObject(value) ? [value] : [];
};

/**
 * Evaluate a Prisma `where` object against an in-memory row — the JS mirror of
 * the SQL filter, used for incremental matching. Supports field equality and the
 * `equals/not/in/notIn/lt/lte/gt/gte/contains/startsWith/endsWith` operators
 * plus `AND`/`OR`/`NOT`. Anything else throws {@link UnsupportedFilterError}, so
 * the engine falls back to a refetch.
 *
 * @example
 * matchesWhere({ userId: 5, status: { not: 'archived' } }, row)
 */
export const matchesWhere = (
	where: PrismaWhere,
	row: Record<string, unknown>
): boolean => {
	for (const [field, condition] of Object.entries(where)) {
		if (field === 'AND') {
			if (
				!toConditions(condition).every((part) =>
					matchesWhere(part, row)
				)
			)
				return false;
			continue;
		}
		if (field === 'OR') {
			const parts = toConditions(condition);
			if (
				parts.length > 0 &&
				!parts.some((part) => matchesWhere(part, row))
			)
				return false;
			continue;
		}
		if (field === 'NOT') {
			if (toConditions(condition).some((part) => matchesWhere(part, row)))
				return false;
			continue;
		}
		if (!matchesField(row[field], condition)) {
			return false;
		}
	}
	return true;
};
