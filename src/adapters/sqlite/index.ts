/**
 * SQLite CDC adapter for @absolutejs/sync (Tier 3, M5).
 *
 * SQLite has no `LISTEN/NOTIFY`, and its `update_hook` isn't reachable from the
 * JS runtimes we target, so out-of-band writes are caught with the portable
 * changelog (outbox) pattern: install triggers that append every row change to a
 * changelog table, then tail it with {@link createPollingChangeSource}. Polling a
 * local SQLite table is cheap (same process, no network).
 *
 * Dependency-free — it only generates SQL; bring your own client (`bun:sqlite`,
 * better-sqlite3, …) to run it and to back the poll query.
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

export type SqliteChangelogOptions = {
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
 * trigger appends `{ tbl, op, payload }` (payload built with `json_object` from
 * the listed columns) to the changelog for {@link createPollingChangeSource}.
 *
 * The statements are `;`-separated; run them as a script, or split on `;` if your
 * driver executes one statement per call.
 */
export const sqliteChangelogSchema = (
	options: SqliteChangelogOptions
): string => {
	const changelog = options.changelogTable ?? DEFAULT_CHANGELOG;
	const prefix = options.prefix ?? DEFAULT_PREFIX;

	const createTable = [
		`CREATE TABLE IF NOT EXISTS ${changelog} (`,
		'\tseq INTEGER PRIMARY KEY AUTOINCREMENT,',
		'\ttbl TEXT NOT NULL,',
		'\top TEXT NOT NULL,',
		'\tpayload TEXT NOT NULL,',
		"\tcreated_at TEXT NOT NULL DEFAULT (datetime('now'))",
		');'
	].join('\n');

	const jsonObject = (columns: string[], ref: 'NEW' | 'OLD'): string =>
		`json_object(${columns
			.map((column) => `'${column}', ${ref}.${column}`)
			.join(', ')})`;

	const triggers = Object.entries(options.tables).flatMap(
		([table, columns]) =>
			OPS.map((op) => {
				const ref = op === 'delete' ? 'OLD' : 'NEW';
				const name = `${prefix}_${table}_${op}`;
				return [
					`DROP TRIGGER IF EXISTS ${name};`,
					`CREATE TRIGGER ${name} AFTER ${op.toUpperCase()} ON ${table}`,
					'BEGIN',
					`\tINSERT INTO ${changelog} (tbl, op, payload)`,
					`\tVALUES ('${table}', '${op}', ${jsonObject(columns, ref)});`,
					'END;'
				].join('\n');
			})
	);

	return [createTable, ...triggers].join('\n\n');
};
