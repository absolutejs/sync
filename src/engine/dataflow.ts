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
