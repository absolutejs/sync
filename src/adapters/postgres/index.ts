import type { ChangeSource, ParsedChange, RowOp } from '../../engine/types';

/**
 * Postgres CDC adapter for @absolutejs/sync (Tier 3, M5).
 *
 * Catches writes that didn't go through the mutation API by turning Postgres
 * `LISTEN/NOTIFY` into the engine's change feed. Install the triggers once with
 * {@link postgresNotifyTrigger}, then connect {@link postgresChangeSource} via
 * `engine.connectSource(...)`.
 *
 * Client-agnostic: you supply how to `listen` on a channel, so it works with
 * porsager/postgres (`sql.listen`), node-postgres, or `Bun.sql` — and the
 * adapter itself has no database dependency.
 */

const DEFAULT_CHANNEL = 'absolute_sync';
const DEFAULT_FUNCTION = 'absolute_sync_notify';

const OP_BY_TG: Record<string, RowOp> = {
	INSERT: 'insert',
	UPDATE: 'update',
	DELETE: 'delete'
};

/** A parsed change ready to feed the engine. */
export type ParsedNotification = ParsedChange;

/**
 * Default NOTIFY-payload parser: expects the JSON the trigger from
 * {@link postgresNotifyTrigger} sends — `{ table, op, row }` where `op` is
 * `INSERT`/`UPDATE`/`DELETE`. Returns `undefined` for anything malformed so a
 * bad payload is skipped rather than throwing.
 */
export const parseNotification = (
	payload: string
): ParsedNotification | undefined => {
	let data: unknown;
	try {
		data = JSON.parse(payload);
	} catch {
		return undefined;
	}
	if (typeof data !== 'object' || data === null) {
		return undefined;
	}
	const { table, op, row } = data as {
		table?: unknown;
		op?: unknown;
		row?: unknown;
	};
	if (typeof table !== 'string') {
		return undefined;
	}
	const rowOp = typeof op === 'string' ? OP_BY_TG[op] : undefined;
	if (rowOp === undefined) {
		return undefined;
	}
	if (typeof row !== 'object' || row === null) {
		return undefined;
	}
	return { table, change: { op: rowOp, row } };
};

export type PostgresChangeSourceOptions = {
	/**
	 * Subscribe to a Postgres NOTIFY channel; return a function that stops
	 * listening. The wiring is yours, e.g. with porsager/postgres:
	 * `(channel, onNotify) => { const s = await sql.listen(channel, onNotify); return s.unlisten; }`.
	 */
	listen: (
		channel: string,
		onNotify: (payload: string) => void
	) => Promise<() => void | Promise<void>> | (() => void | Promise<void>);
	/** NOTIFY channel; must match the trigger's. Defaults to `absolute_sync`. */
	channel?: string;
	/** Override the payload parser (defaults to {@link parseNotification}). */
	parse?: (payload: string) => ParsedNotification | undefined;
	/**
	 * Called when a NOTIFY payload fails to parse (malformed JSON, unknown
	 * `op`, or — most often — payload was truncated past Postgres's 8000-byte
	 * cap and lost a closing brace). Default: silently dropped. Provide this
	 * to log the skip so you can detect oversized rows in production.
	 */
	onSkip?: (payload: string, reason: 'parse-failed') => void;
};

/**
 * A {@link ChangeSource} backed by Postgres `LISTEN/NOTIFY`. Each notification
 * is parsed to `(table, change)` and emitted into the engine.
 *
 * @example
 * const disconnect = await engine.connectSource(
 *   postgresChangeSource({
 *     listen: async (channel, onNotify) =>
 *       (await sql.listen(channel, onNotify)).unlisten
 *   })
 * );
 */
export const postgresChangeSource = (
	options: PostgresChangeSourceOptions
): ChangeSource => {
	const channel = options.channel ?? DEFAULT_CHANNEL;
	const parse = options.parse ?? parseNotification;
	const onSkip = options.onSkip;
	let unlisten: (() => void | Promise<void>) | undefined;

	return {
		start: async (emit) => {
			unlisten = await options.listen(channel, (payload) => {
				const parsed = parse(payload);
				if (parsed === undefined) {
					onSkip?.(payload, 'parse-failed');
					return;
				}
				void emit(parsed.table, parsed.change);
			});
		},
		stop: async () => {
			await unlisten?.();
			unlisten = undefined;
		}
	};
};

export type PostgresNotifyTriggerOptions = {
	/** Tables to emit changes for. */
	tables: string[];
	/** NOTIFY channel; must match the change source's. Defaults to `absolute_sync`. */
	channel?: string;
	/** Trigger function name. Defaults to `absolute_sync_notify`. */
	functionName?: string;
};

/**
 * Generate the SQL that installs a NOTIFY trigger on each table — run it once
 * (e.g. in a migration). On every insert/update/delete it sends
 * `{ table, op, row }` JSON on the channel for {@link postgresChangeSource}.
 *
 * Note: `pg_notify` payloads are capped at 8000 bytes, so very wide rows can be
 * truncated (the parser then skips them). For large rows or high throughput,
 * prefer a logical-replication source behind the same {@link ChangeSource} seam.
 */
export const postgresNotifyTrigger = (
	options: PostgresNotifyTriggerOptions
): string => {
	const channel = options.channel ?? DEFAULT_CHANNEL;
	const fn = options.functionName ?? DEFAULT_FUNCTION;

	const functionSql = [
		`CREATE OR REPLACE FUNCTION ${fn}() RETURNS trigger AS $$`,
		'BEGIN',
		`  PERFORM pg_notify('${channel}', json_build_object(`,
		`    'table', TG_TABLE_NAME,`,
		`    'op', TG_OP,`,
		`    'row', row_to_json(COALESCE(NEW, OLD))`,
		'  )::text);',
		'  RETURN COALESCE(NEW, OLD);',
		'END;',
		'$$ LANGUAGE plpgsql;'
	].join('\n');

	const triggerSql = options.tables.map((table) =>
		[
			`DROP TRIGGER IF EXISTS ${fn}_${table} ON ${table};`,
			`CREATE TRIGGER ${fn}_${table}`,
			`AFTER INSERT OR UPDATE OR DELETE ON ${table}`,
			`FOR EACH ROW EXECUTE FUNCTION ${fn}();`
		].join('\n')
	);

	return [functionSql, ...triggerSql].join('\n\n');
};
