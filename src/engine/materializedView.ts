import type { RowChange, RowKey, ViewDiff } from './types';

export type MaterializedViewOptions<T> = {
	/** Row identity within the result set. */
	key: (row: T) => RowKey;
	/**
	 * The query's WHERE as a JS predicate: does this row belong in the result
	 * set? Evaluated on every changed row to decide enter/leave/update. (The
	 * Drizzle/Prisma adapters can derive this from a query filter.)
	 */
	match: (row: T) => boolean;
	/**
	 * Equality used by {@link MaterializedView.reset} to detect changed rows.
	 * Defaults to a shallow compare of own enumerable properties.
	 */
	equals?: (a: T, b: T) => boolean;
};

export type MaterializedView<T> = {
	/**
	 * Replace the result set with `rows` (the initial DB query result). Rows are
	 * trusted to already satisfy the predicate — the database applied the filter.
	 */
	hydrate: (rows: Iterable<T>) => void;
	/**
	 * Apply one row change and return the resulting diff. Empty `added`/`removed`/
	 * `changed` arrays mean the change did not affect this view.
	 */
	apply: (change: RowChange<T>) => ViewDiff<T>;
	/**
	 * Replace the result set with a fresh query result and return the diff versus
	 * what the view previously held. Powers the refetch fallback for queries that
	 * can't be matched incrementally.
	 */
	reset: (rows: Iterable<T>) => ViewDiff<T>;
	/** Current result set, as an array. */
	rows: () => T[];
	/** Current result-set size. */
	size: () => number;
};

const emptyDiff = <T>(): ViewDiff<T> => ({
	added: [],
	removed: [],
	changed: []
});

const shallowEqual = (a: unknown, b: unknown): boolean => {
	if (a === b) {
		return true;
	}
	if (
		typeof a !== 'object' ||
		typeof b !== 'object' ||
		a === null ||
		b === null
	) {
		return false;
	}
	const aKeys = Object.keys(a);
	const bKeys = Object.keys(b);
	if (aKeys.length !== bKeys.length) {
		return false;
	}
	return aKeys.every(
		(key) =>
			(a as Record<string, unknown>)[key] ===
			(b as Record<string, unknown>)[key]
	);
};

/** True when a diff carries no changes. */
export const isEmptyViewDiff = <T>(diff: ViewDiff<T>): boolean =>
	diff.added.length === 0 &&
	diff.removed.length === 0 &&
	diff.changed.length === 0;

/**
 * A single query's materialized result set, maintained incrementally by
 * predicate matching (the Tier 3 IVM core). Hydrate once from the database, then
 * feed each changed row through {@link MaterializedView.apply}: the view decides
 * whether the row entered, left, stayed-and-changed, or is irrelevant, and
 * returns just that delta — so the server pushes diffs instead of refetching.
 *
 * Scope: single-table filtered queries (no ORDER BY / LIMIT windows — a top-N
 * query should hydrate-and-refetch rather than rely on this view, since a row
 * entering can silently evict another). Joins/aggregations are a later,
 * differential-dataflow engine.
 */
export const createMaterializedView = <T>(
	options: MaterializedViewOptions<T>
): MaterializedView<T> => {
	const { key, match } = options;
	const equals = options.equals ?? shallowEqual;
	const set = new Map<RowKey, T>();

	return {
		hydrate: (rows) => {
			set.clear();
			for (const row of rows) {
				set.set(key(row), row);
			}
		},
		reset: (rows) => {
			const next = new Map<RowKey, T>();
			const added: T[] = [];
			const changed: T[] = [];
			for (const row of rows) {
				const rowKey = key(row);
				next.set(rowKey, row);
				const previous = set.get(rowKey);
				if (previous === undefined) {
					added.push(row);
				} else if (!equals(previous, row)) {
					changed.push(row);
				}
			}
			const removed: T[] = [];
			for (const [rowKey, previous] of set) {
				if (!next.has(rowKey)) {
					removed.push(previous);
				}
			}
			set.clear();
			for (const [rowKey, row] of next) {
				set.set(rowKey, row);
			}
			return { added, removed, changed };
		},
		apply: ({ op, row }) => {
			const rowKey = key(row);
			const existing = set.get(rowKey);

			if (op === 'delete') {
				if (existing === undefined) {
					return emptyDiff();
				}
				set.delete(rowKey);
				return { added: [], removed: [existing], changed: [] };
			}

			// insert | update — let the predicate decide membership.
			if (match(row)) {
				set.set(rowKey, row);
				return existing === undefined
					? { added: [row], removed: [], changed: [] }
					: { added: [], removed: [], changed: [row] };
			}

			// No longer matches: it leaves the set if it was in it.
			if (existing !== undefined) {
				set.delete(rowKey);
				return { added: [], removed: [existing], changed: [] };
			}
			return emptyDiff();
		},
		rows: () => [...set.values()],
		size: () => set.size
	};
};
