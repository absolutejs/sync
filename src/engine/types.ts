/**
 * Core types shared across the Tier 3 sync engine (server-side). Deliberately
 * ORM-agnostic: the engine operates on plain row objects, a key function, and a
 * predicate — the Drizzle/Prisma adapters and the transport layer build on top.
 */

/** A scalar that identifies a row within a collection. */
export type RowKey = string | number | bigint;

/** The kind of row-level change flowing through the engine's change feed. */
export type RowOp = 'insert' | 'update' | 'delete';

/** One row-level change: a row that was inserted, updated, or deleted. */
export type RowChange<T> = {
	op: RowOp;
	/**
	 * The affected row. For `insert`/`update` it is the new row value; for
	 * `delete` only the key field(s) need be present.
	 */
	row: T;
};

/**
 * The delta a change makes to a query's result set: rows that entered (`added`),
 * left (`removed`), or stayed in the set but changed value (`changed`). The
 * `removed` rows are the values the view last held for those keys.
 */
export type ViewDiff<T> = {
	added: T[];
	removed: T[];
	changed: T[];
};

/** Report a committed row change on `table` into the engine. */
export type EmitChange = (
	table: string,
	change: RowChange<unknown>
) => void | Promise<void>;

/**
 * A pluggable source of committed row changes — the seam for catching writes the
 * mutation API didn't make (CDC: Postgres LISTEN/NOTIFY or logical replication,
 * MySQL binlog, SQLite update hooks). `start` begins emitting via the supplied
 * callback; `stop` tears it down. Wire one with {@link SyncEngine.connectSource}.
 */
export type ChangeSource = {
	start: (emit: EmitChange) => void | Promise<void>;
	stop: () => void | Promise<void>;
};
