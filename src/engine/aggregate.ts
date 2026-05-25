import type { RowChange, RowKey } from './types';

export type AggregateOptions<T> = {
	/** Row identity — used to track each row's contribution across updates. */
	key: (row: T) => RowKey;
	/** Group rows by this key. Omit to aggregate everything into one group (`''`). */
	groupBy?: (row: T) => RowKey;
	/**
	 * Numeric value to aggregate for `sum`/`avg`/`min`/`max`. Omit for a
	 * count-only aggregate (sum stays 0, min/max stay undefined).
	 */
	value?: (row: T) => number;
};

/** Maintained summary for one group. */
export type AggregateGroup = {
	group: RowKey;
	count: number;
	sum: number;
	/** `sum / count`, or 0 when the group is empty. */
	avg: number;
	min: number | undefined;
	max: number | undefined;
};

export type Aggregate<T> = {
	/** Bulk-load the initial rows (replaces current state). */
	hydrate: (rows: Iterable<T>) => void;
	/** Fold one row change into the running aggregates. */
	apply: (change: RowChange<T>) => void;
	/** Current summary for every non-empty group. */
	groups: () => AggregateGroup[];
	/** Current summary for one group, or `undefined` if empty. */
	group: (group: RowKey) => AggregateGroup | undefined;
};

type GroupState = {
	count: number;
	sum: number;
	/** Multiset of values, so removing the current min/max stays correct. */
	valueCounts: Map<number, number>;
	min: number | undefined;
	max: number | undefined;
};

const newGroupState = (): GroupState => ({
	count: 0,
	sum: 0,
	valueCounts: new Map(),
	min: undefined,
	max: undefined
});

const recomputeExtremes = (state: GroupState) => {
	if (state.valueCounts.size === 0) {
		state.min = undefined;
		state.max = undefined;
		return;
	}
	let min = Infinity;
	let max = -Infinity;
	for (const value of state.valueCounts.keys()) {
		if (value < min) {
			min = value;
		}
		if (value > max) {
			max = value;
		}
	}
	state.min = min;
	state.max = max;
};

const summarize = (group: RowKey, state: GroupState): AggregateGroup => ({
	group,
	count: state.count,
	sum: state.sum,
	avg: state.count > 0 ? state.sum / state.count : 0,
	min: state.min,
	max: state.max
});

/**
 * An incrementally-maintained aggregation — the DD-lite for `count`/`sum`/`avg`/
 * `min`/`max`, optionally grouped. Feed it the change feed (insert/update/delete)
 * and it updates each group's summary in place: count/sum/avg are O(1); min/max
 * use a value multiset so removing the current extremum recomputes correctly
 * (O(distinct values) only when the extremum leaves).
 *
 * Per-row contributions are tracked by `key`, so updates (including a row moving
 * between groups) and deletes adjust the right group without re-scanning. Use it
 * server-side over the engine's change feed, or client-side over collection
 * diffs.
 */
export const createAggregate = <T>(
	options: AggregateOptions<T>
): Aggregate<T> => {
	const { key, groupBy, value } = options;
	const groups = new Map<RowKey, GroupState>();
	// Each row's last (group, value), so updates/deletes adjust the right group.
	// `value` is undefined for a count-only aggregate (no value extractor).
	const contributions = new Map<
		RowKey,
		{ group: RowKey; value: number | undefined }
	>();

	const add = (group: RowKey, contribution: number | undefined) => {
		let state = groups.get(group);
		if (state === undefined) {
			state = newGroupState();
			groups.set(group, state);
		}
		state.count += 1;
		if (contribution === undefined) {
			return;
		}
		state.sum += contribution;
		state.valueCounts.set(
			contribution,
			(state.valueCounts.get(contribution) ?? 0) + 1
		);
		state.min =
			state.min === undefined
				? contribution
				: Math.min(state.min, contribution);
		state.max =
			state.max === undefined
				? contribution
				: Math.max(state.max, contribution);
	};

	const remove = (group: RowKey, contribution: number | undefined) => {
		const state = groups.get(group);
		if (state === undefined) {
			return;
		}
		state.count -= 1;
		if (contribution !== undefined) {
			state.sum -= contribution;
			const remaining = (state.valueCounts.get(contribution) ?? 0) - 1;
			if (remaining <= 0) {
				state.valueCounts.delete(contribution);
				if (contribution === state.min || contribution === state.max) {
					recomputeExtremes(state);
				}
			} else {
				state.valueCounts.set(contribution, remaining);
			}
		}
		if (state.count <= 0) {
			groups.delete(group);
		}
	};

	const apply = (change: RowChange<T>) => {
		const rowKey = key(change.row);
		const previous = contributions.get(rowKey);

		if (change.op === 'delete') {
			if (previous !== undefined) {
				remove(previous.group, previous.value);
				contributions.delete(rowKey);
			}
			return;
		}

		const group = groupBy ? groupBy(change.row) : '';
		const contribution = value ? value(change.row) : undefined;
		if (previous !== undefined) {
			remove(previous.group, previous.value);
		}
		add(group, contribution);
		contributions.set(rowKey, { group, value: contribution });
	};

	return {
		hydrate: (rows) => {
			groups.clear();
			contributions.clear();
			for (const row of rows) {
				apply({ op: 'insert', row });
			}
		},
		apply,
		groups: () =>
			[...groups.entries()].map(([group, state]) =>
				summarize(group, state)
			),
		group: (group) => {
			const state = groups.get(group);
			return state === undefined ? undefined : summarize(group, state);
		}
	};
};
