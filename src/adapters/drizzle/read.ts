import type { SQL, Table } from 'drizzle-orm';
import { extractKeyFromWhere, keyTopic, tableTopic } from './topics';

export type DeriveReadTopicsOptions = {
	/**
	 * Column (its JS property name on the table) to treat as the row key when
	 * narrowing to a `table:key` topic. Defaults to the table's single
	 * primary-key column; composite or absent primary keys disable row-level
	 * narrowing.
	 */
	keyColumn?: string;
};

export type DerivedReadTopics = {
	/** Topics this read depends on — subscribe to all of them. */
	topics: string[];
	/**
	 * `true` when derivation narrowed to a specific row (`table:key`); `false`
	 * when it fell back to the whole-table topic.
	 */
	rowLevel: boolean;
};

/**
 * Derive the reactive topics a read of `table` (optionally filtered by `where`)
 * depends on. A recognised primary-key equality narrows to a single `table:key`
 * topic; everything else subscribes to the whole-table topic, over-invalidating
 * a little rather than missing an update.
 *
 * @example
 * deriveReadTopics(users);                  // { topics: ['users'], rowLevel: false }
 * deriveReadTopics(users, eq(users.id, 5)); // { topics: ['users:5'], rowLevel: true }
 * deriveReadTopics(users, gt(users.id, 5)); // { topics: ['users'], rowLevel: false }
 */
export const deriveReadTopics = (
	table: Table,
	where?: SQL,
	options: DeriveReadTopicsOptions = {}
): DerivedReadTopics => {
	const key =
		where === undefined
			? undefined
			: extractKeyFromWhere(table, where, options.keyColumn);

	if (key === undefined) {
		return { topics: [tableTopic(table)], rowLevel: false };
	}
	return { topics: [keyTopic(table, key)], rowLevel: true };
};
