import type { ChangeSource, EmitChange } from '../../engine/types';

const identifier = (value: string) => {
	if (!/^[a-z_][a-z0-9_]*$/i.test(value))
		throw new TypeError('Invalid PostgreSQL identifier');
	return '"' + value + '"';
};
/** Bounded, payload-free change feed for collections using hydrate/refetch.
 * Revisions commit with the source writes. Each table's counter is row-locked,
 * so a later commit cannot hide an earlier uncommitted revision. Does not expose
 * source row values (particularly important for auth/credential tables).
 */
export function postgresTableRevisionsMigration(tables: string[]) {
	return `CREATE TABLE IF NOT EXISTS absolute_sync_table_revisions (table_name text PRIMARY KEY, revision bigint NOT NULL DEFAULT 0);
CREATE OR REPLACE FUNCTION absolute_sync_bump_table_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO absolute_sync_table_revisions(table_name, revision) VALUES (TG_TABLE_NAME, 1)
 ON CONFLICT (table_name) DO UPDATE SET revision = absolute_sync_table_revisions.revision + 1;
 PERFORM pg_notify('absolute_sync_revisions', TG_TABLE_NAME);
 RETURN NULL;
END; $$;
${[...new Set(tables)]
	.map(
		(
			table
		) => `DROP TRIGGER IF EXISTS absolute_sync_table_revision ON ${identifier(table)};
CREATE TRIGGER absolute_sync_table_revision AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON ${identifier(table)} FOR EACH STATEMENT EXECUTE FUNCTION absolute_sync_bump_table_revision();`
	)
	.join('\n')}`;
}

export type PostgresTableRevisionSourceOptions = {
	/** Return ALL tracked table revisions; never page/limit this small metadata set. */
	read: () => Promise<{ table: string; revision: string }[]>;
	/** Optional low-latency wakeup; periodic reconciliation repairs missed signals. */
	listen?: (
		channel: string,
		wake: () => void
	) => Promise<() => void | Promise<void>>;
	reconcileMs?: number;
	onError?: (error: unknown) => void;
};
/** Only use with refetch collections: emitted rows are revision markers, not
 * domain rows. Consumers declare source table dependencies; the engine computes
 * authorized row diffs from their hydrate results. No client polling required.
 */
export function createPostgresTableRevisionSource(
	options: PostgresTableRevisionSourceOptions
): ChangeSource {
	let stopped = true,
		running = false,
		pending = false;
	let timer: ReturnType<typeof setTimeout> | undefined;
	let unlisten: (() => void | Promise<void>) | undefined;
	let emit: EmitChange;
	const seen = new Map<string, string>();
	const interval = options.reconcileMs ?? 5000;
	if (!Number.isFinite(interval) || interval < 100)
		throw new TypeError('reconcileMs must be at least 100');
	const report = (error: unknown) => options.onError?.(error);
	const drain = async () => {
		if (stopped) return;
		if (running) {
			pending = true;
			return;
		}
		running = true;
		clearTimeout(timer);
		try {
			do {
				pending = false;
				const revisions = await options.read();
				if (stopped) break;
				for (const row of revisions) {
					if (seen.get(row.table) === row.revision) continue;
					await emit(row.table, {
						op: 'update',
						row: { id: row.table, revision: row.revision }
					});
					seen.set(row.table, row.revision);
				}
			} while (pending && !stopped);
		} catch (error) {
			report(error);
		} finally {
			running = false;
			if (!stopped) timer = setTimeout(() => void drain(), interval);
		}
	};
	return {
		async start(nextEmit) {
			if (!stopped) return;
			stopped = false;
			emit = nextEmit;
			if (options.listen) {
				try {
					unlisten = await options.listen(
						'absolute_sync_revisions',
						() => void drain()
					);
				} catch (error) {
					report(error);
				}
			}
			await drain();
		},
		async stop() {
			stopped = true;
			clearTimeout(timer);
			await unlisten?.();
			unlisten = undefined;
		}
	};
}
