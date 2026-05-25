import { createAggregate } from './aggregate';
import type { AggregateGroup } from './aggregate';
import { createEquiJoin } from './equiJoin';
import type { RowChange, RowKey, ViewDiff } from './types';

/**
 * Composable incremental dataflow (the general operator graph, in progress).
 *
 * Every edge in the graph is a stream of keyed **changes** — an `upsert` (insert
 * or update, last-write-wins per key) or a `delete`. Collapsing added/changed
 * into one `upsert` is what makes operators compose: `filter`/`map` become
 * stateless stream transforms, `join` reuses the equi-join operator, and a
 * `materialize` sink turns the final stream back into a `{ added, removed,
 * changed }` diff for the transport.
 */
export type Change<T> = { op: 'upsert' | 'delete'; key: RowKey; row: T };

/** A single-input incremental operator: a batch of input changes → output changes. */
export type Operator<In, Out> = {
	push: (changes: Change<In>[]) => Change<Out>[];
};

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
	return (
		aKeys.length === bKeys.length &&
		aKeys.every(
			(key) =>
				(a as Record<string, unknown>)[key] ===
				(b as Record<string, unknown>)[key]
		)
	);
};

/** Lift a table row change into a dataflow change (a graph source). */
export const fromRowChange = <T>(
	change: RowChange<T>,
	key: (row: T) => RowKey
): Change<T> => ({
	op: change.op === 'delete' ? 'delete' : 'upsert',
	key: key(change.row),
	row: change.row
});

/**
 * Keep only rows matching `predicate`. Stateless: a row that fails the predicate
 * is emitted as a `delete` (downstream removes it if present, else no-op), so a
 * row that stops matching leaves correctly.
 */
export const filterOp = <T>(
	predicate: (row: T) => boolean
): Operator<T, T> => ({
	push: (changes) =>
		changes.map((change) =>
			change.op === 'upsert' && !predicate(change.row)
				? { op: 'delete', key: change.key, row: change.row }
				: change
		)
});

/**
 * Transform each row. Stateless; preserves identity unless `rekey` is given (to
 * derive the output key from the mapped row).
 */
export const mapOp = <In, Out>(
	transform: (row: In) => Out,
	rekey?: (row: Out) => RowKey
): Operator<In, Out> => ({
	push: (changes) =>
		changes.map((change) => {
			const row = transform(change.row);
			return {
				op: change.op,
				key: rekey ? rekey(row) : change.key,
				row
			};
		})
});

/** Compose two operators into one (`a` then `b`). Nest for longer chains. */
export const chain = <A, B, C>(
	a: Operator<A, B>,
	b: Operator<B, C>
): Operator<A, C> => ({
	push: (changes) => b.push(a.push(changes))
});

export type AggregateOpOptions<In> = {
	/** Input row identity (to track each row's contribution across updates). */
	key: (row: In) => RowKey;
	/** Group rows by this key (omit for one `''` group). */
	groupBy?: (row: In) => RowKey;
	/** Numeric value for sum/avg/min/max (omit for a count-only aggregate). */
	value?: (row: In) => number;
};

/**
 * Aggregate a change stream into a stream of group summaries — `count`/`sum`/
 * `avg`/`min`/`max` per group, maintained incrementally (wraps
 * {@link createAggregate}). Emits an `upsert` of the group summary for each group
 * a batch touched, or a `delete` when a group empties. Output is keyed by group,
 * so it composes downstream like any other operator (e.g. after a join).
 */
export const aggregateOp = <In>(
	options: AggregateOpOptions<In>
): Operator<In, AggregateGroup> => {
	const aggregate = createAggregate<In>(options);
	const groupOf = new Map<RowKey, RowKey>();

	return {
		push: (changes) => {
			const affected = new Set<RowKey>();
			for (const change of changes) {
				const inputKey = options.key(change.row);
				const previousGroup = groupOf.get(inputKey);
				if (previousGroup !== undefined) {
					affected.add(previousGroup);
				}
				if (change.op === 'delete') {
					aggregate.apply({ op: 'delete', row: change.row });
					groupOf.delete(inputKey);
				} else {
					const group = options.groupBy
						? options.groupBy(change.row)
						: '';
					affected.add(group);
					aggregate.apply({ op: 'update', row: change.row });
					groupOf.set(inputKey, group);
				}
			}
			const out: Change<AggregateGroup>[] = [];
			for (const group of affected) {
				const summary = aggregate.group(group);
				if (summary === undefined) {
					out.push({
						op: 'delete',
						key: group,
						row: {
							group,
							count: 0,
							sum: 0,
							avg: 0,
							min: undefined,
							max: undefined
						}
					});
				} else {
					out.push({ op: 'upsert', key: group, row: summary });
				}
			}
			return out;
		}
	};
};

export type OrderByOptions<T> = {
	/** Row identity. */
	key: (row: T) => RowKey;
	/** Sort comparator (ascending: negative = a before b). */
	compare: (a: T, b: T) => number;
	/** Keep at most this many rows (the top-N window). */
	limit?: number;
	/** Skip this many rows from the front (pagination). Defaults to 0. */
	offset?: number;
};

/**
 * Maintain a sorted top-N window: keep only the `[offset, offset + limit)` slice
 * by `compare`, emitting which rows entered/left the window as input changes.
 * A single insert can both add a row and evict the one it displaced — both are
 * emitted. Bounded output (≤ limit upserts per batch); it re-sorts its input, so
 * place it where the input is already narrowed (e.g. after a filter/join).
 *
 * The window is the right *set* of rows; sort them by the same comparator on the
 * client for display order (cheap for N rows — the diff protocol is unordered).
 */
export const orderByOp = <T>(options: OrderByOptions<T>): Operator<T, T> => {
	const { key, compare } = options;
	const offset = options.offset ?? 0;
	const limit = options.limit ?? Number.POSITIVE_INFINITY;
	const all = new Map<RowKey, T>();
	let window = new Map<RowKey, T>();

	return {
		push: (changes) => {
			for (const change of changes) {
				if (change.op === 'delete') {
					all.delete(change.key);
				} else {
					all.set(change.key, change.row);
				}
			}
			const windowed = [...all.values()]
				.sort(compare)
				.slice(offset, offset + limit);
			const next = new Map<RowKey, T>();
			for (const row of windowed) {
				next.set(key(row), row);
			}
			const out: Change<T>[] = [];
			for (const [rowKey, row] of window) {
				if (!next.has(rowKey)) {
					out.push({ op: 'delete', key: rowKey, row });
				}
			}
			for (const [rowKey, row] of next) {
				out.push({ op: 'upsert', key: rowKey, row });
			}
			window = next;
			return out;
		}
	};
};

/** A two-input incremental equi-join node. */
export type JoinNode<L, R, Out> = {
	hydrate: (left: Iterable<L>, right: Iterable<R>) => void;
	pushLeft: (changes: Change<L>[]) => Change<Out>[];
	pushRight: (changes: Change<R>[]) => Change<Out>[];
	rows: () => Out[];
};

export type JoinNodeOptions<L, R, Out> = {
	leftKey: (left: L) => RowKey;
	rightKey: (right: R) => RowKey;
	leftOn: (left: L) => RowKey;
	rightOn: (right: R) => RowKey;
	select: (left: L, right: R) => Out;
	/** Provide for a LEFT join: output for a left row with no match. */
	selectUnmatched?: (left: L) => Out;
	/** Output row identity (unique per emitted pair). */
	key: (out: Out) => RowKey;
};

/**
 * A join as a dataflow node — reuses {@link createEquiJoin} and converts its
 * `{ added, removed, changed }` deltas into the upsert/delete change stream.
 */
export const joinNode = <L, R, Out>(
	options: JoinNodeOptions<L, R, Out>
): JoinNode<L, R, Out> => {
	const join = createEquiJoin<L, R, Out>(options);
	const key = options.key;

	const toChanges = (diff: ViewDiff<Out>): Change<Out>[] => {
		const changes: Change<Out>[] = [];
		for (const row of diff.removed) {
			changes.push({ op: 'delete', key: key(row), row });
		}
		for (const row of diff.added) {
			changes.push({ op: 'upsert', key: key(row), row });
		}
		for (const row of diff.changed) {
			changes.push({ op: 'upsert', key: key(row), row });
		}
		return changes;
	};
	const asRowChange = <T>(change: Change<T>): RowChange<T> => ({
		op: change.op === 'delete' ? 'delete' : 'update',
		row: change.row
	});

	return {
		hydrate: (left, right) => join.hydrate(left, right),
		pushLeft: (changes) =>
			changes.flatMap((change) =>
				toChanges(join.applyLeft(asRowChange(change)))
			),
		pushRight: (changes) =>
			changes.flatMap((change) =>
				toChanges(join.applyRight(asRowChange(change)))
			),
		rows: () => join.rows()
	};
};

export type Materializer<T> = {
	/** Replace the set with initial rows (no diff emitted). */
	hydrate: (rows: Iterable<T>) => void;
	/** Apply a change stream and return the resulting result-set diff. */
	apply: (changes: Change<T>[]) => ViewDiff<T>;
	rows: () => T[];
};

/**
 * The graph sink: maintain a keyed result set from a change stream and emit the
 * `{ added, removed, changed }` diff each batch produces — the boundary back to
 * the transport / client.
 */
export const materialize = <T>(
	key: (row: T) => RowKey,
	equals: (a: T, b: T) => boolean = shallowEqual
): Materializer<T> => {
	const set = new Map<RowKey, T>();
	return {
		hydrate: (rows) => {
			set.clear();
			for (const row of rows) {
				set.set(key(row), row);
			}
		},
		apply: (changes) => {
			const added: T[] = [];
			const removed: T[] = [];
			const changed: T[] = [];
			for (const change of changes) {
				if (change.op === 'delete') {
					const previous = set.get(change.key);
					if (previous !== undefined) {
						removed.push(previous);
						set.delete(change.key);
					}
					continue;
				}
				const previous = set.get(change.key);
				set.set(change.key, change.row);
				if (previous === undefined) {
					added.push(change.row);
				} else if (!equals(previous, change.row)) {
					changed.push(change.row);
				}
			}
			return { added, removed, changed };
		},
		rows: () => [...set.values()]
	};
};
