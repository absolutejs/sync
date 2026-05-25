import type { ChangeSource, ParsedChange, RowOp } from '../../engine/types';

/**
 * MySQL CDC adapter for @absolutejs/sync (Tier 3, M5).
 *
 * MySQL has no `LISTEN/NOTIFY`, so two pluggable strategies catch out-of-band
 * writes — both behind the engine's {@link ChangeSource} seam:
 *
 * - **Changelog + poll (portable).** Install triggers with
 *   {@link mysqlChangelogSchema} and tail the changelog with
 *   {@link createPollingChangeSource}. Works anywhere, no extra privileges.
 * - **Binlog (higher throughput).** Wire a binlog reader (e.g. zongji) into
 *   {@link mysqlBinlogChangeSource}; it normalizes row events into the change
 *   feed. Catches writes without the per-write changelog overhead.
 *
 * Dependency-free — you bring the MySQL client / binlog reader.
 */

export {
	createPollingChangeSource,
	parseOutboxRow
} from '../../engine/pollingSource';
export type {
	OutboxRow,
	PollingChangeSourceOptions
} from '../../engine/pollingSource';

const DEFAULT_CHANGELOG = 'absolute_sync_changelog';
const DEFAULT_PREFIX = 'absolute_sync';
const OPS = ['insert', 'update', 'delete'] as const;

export type MysqlChangelogOptions = {
	/** Table name → the column names to capture in the change payload. */
	tables: Record<string, string[]>;
	/** Changelog table name. Defaults to `absolute_sync_changelog`. */
	changelogTable?: string;
	/** Trigger name prefix. Defaults to `absolute_sync`. */
	prefix?: string;
};

/**
 * Generate the SQL that installs the changelog table and per-table
 * insert/update/delete triggers — run it once (e.g. in a migration). Each
 * trigger appends `{ tbl, op, payload }` (payload built with `JSON_OBJECT` from
 * the listed columns) to the changelog for {@link createPollingChangeSource}.
 *
 * Each `CREATE TRIGGER` body is a single statement (no `DELIMITER` needed). The
 * statements are `;`-separated; run them as a script, or split on `;` if your
 * driver executes one statement per call.
 */
export const mysqlChangelogSchema = (
	options: MysqlChangelogOptions
): string => {
	const changelog = options.changelogTable ?? DEFAULT_CHANGELOG;
	const prefix = options.prefix ?? DEFAULT_PREFIX;

	const createTable = [
		`CREATE TABLE IF NOT EXISTS \`${changelog}\` (`,
		'\tseq BIGINT AUTO_INCREMENT PRIMARY KEY,',
		'\ttbl VARCHAR(255) NOT NULL,',
		'\top VARCHAR(16) NOT NULL,',
		'\tpayload JSON NOT NULL,',
		'\tcreated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP',
		');'
	].join('\n');

	const jsonObject = (columns: string[], ref: 'NEW' | 'OLD'): string =>
		`JSON_OBJECT(${columns
			.map((column) => `'${column}', ${ref}.\`${column}\``)
			.join(', ')})`;

	const triggers = Object.entries(options.tables).flatMap(
		([table, columns]) =>
			OPS.map((op) => {
				const ref = op === 'delete' ? 'OLD' : 'NEW';
				const name = `${prefix}_${table}_${op}`;
				return [
					`DROP TRIGGER IF EXISTS \`${name}\`;`,
					`CREATE TRIGGER \`${name}\` AFTER ${op.toUpperCase()} ON \`${table}\` FOR EACH ROW`,
					`INSERT INTO \`${changelog}\` (tbl, op, payload)`,
					`VALUES ('${table}', '${op}', ${jsonObject(columns, ref)});`
				].join('\n');
			})
	);

	return [createTable, ...triggers].join('\n\n');
};

const BINLOG_OP: Record<string, RowOp> = {
	writerows: 'insert',
	updaterows: 'update',
	deleterows: 'delete'
};

/** A row-level binlog event (the subset {@link normalizeBinlogEvent} needs). */
export type BinlogRowEvent = {
	/**
	 * Event kind — zongji's `writerows` / `updaterows` / `deleterows`
	 * (case-insensitive).
	 */
	type: string;
	/** The affected table. */
	table: string;
	/**
	 * Affected rows. Insert/delete: each entry is the row object. Update: each
	 * entry is `{ before, after }` (zongji's shape).
	 */
	rows: unknown[];
};

/**
 * Normalize a binlog row event into engine changes — one per affected row. For
 * updates it takes the `after` image; for deletes the row (or its `before`
 * image). Rows that aren't objects are skipped. Pure, so it's easy to test and
 * to swap for a different reader's shape.
 */
export const normalizeBinlogEvent = (event: BinlogRowEvent): ParsedChange[] => {
	const op = BINLOG_OP[event.type.toLowerCase()];
	if (op === undefined || typeof event.table !== 'string') {
		return [];
	}
	const changes: ParsedChange[] = [];
	for (const entry of event.rows) {
		let row: unknown = entry;
		if (entry !== null && typeof entry === 'object') {
			if (op === 'update' && 'after' in entry) {
				row = (entry as { after: unknown }).after;
			} else if (op === 'delete' && 'before' in entry) {
				row = (entry as { before: unknown }).before;
			}
		}
		if (typeof row === 'object' && row !== null) {
			changes.push({ table: event.table, change: { op, row } });
		}
	}
	return changes;
};

export type MysqlBinlogChangeSourceOptions = {
	/**
	 * Subscribe to row events from a binlog reader; return a function that stops
	 * it. e.g. with zongji:
	 * `(onEvent) => { zongji.on('binlog', e => onEvent({ type: e.getEventName(), table: e.tableMap[e.tableId].tableName, rows: e.rows })); zongji.start(...); return () => zongji.stop(); }`.
	 */
	subscribe: (
		onEvent: (event: BinlogRowEvent) => void
	) => Promise<() => void | Promise<void>> | (() => void | Promise<void>);
	/** Override the event normalizer (defaults to {@link normalizeBinlogEvent}). */
	normalize?: (event: BinlogRowEvent) => ParsedChange[];
};

/**
 * A {@link ChangeSource} backed by the MySQL binlog. Each row event is
 * normalized and emitted into the engine. Connect with
 * `engine.connectSource(...)`.
 */
export const mysqlBinlogChangeSource = (
	options: MysqlBinlogChangeSourceOptions
): ChangeSource => {
	const normalize = options.normalize ?? normalizeBinlogEvent;
	let unsubscribe: (() => void | Promise<void>) | undefined;

	return {
		start: async (emit) => {
			unsubscribe = await options.subscribe((event) => {
				for (const parsed of normalize(event)) {
					void emit(parsed.table, parsed.change);
				}
			});
		},
		stop: async () => {
			await unsubscribe?.();
			unsubscribe = undefined;
		}
	};
};
