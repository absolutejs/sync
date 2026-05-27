import type { ChangeSource, EmitChange, ParsedChange, RowOp } from './types';

/**
 * A database-agnostic CDC {@link ChangeSource} that tails an append-only
 * changelog (outbox) table and emits its rows into the engine.
 *
 * This is the portable way to catch out-of-band writes on databases without a
 * native push channel — MySQL (no `LISTEN/NOTIFY`) and SQLite (no update hook
 * reachable from the JS runtime). Install per-table triggers that append to the
 * changelog (see `@absolutejs/sync/mysql` and `/sqlite`), then poll it here. It
 * is also a fine fallback for Postgres when you'd rather not run `LISTEN`.
 *
 * Driver-agnostic: you supply `poll(sinceSeq)` — a single
 * `SELECT seq, tbl, op, payload FROM <changelog> WHERE seq > ? ORDER BY seq` run
 * with your client — and the adapter tracks the cursor, parses each row, emits,
 * and advances. Delivery is at-least-once across crashes (a row may re-emit if
 * the process dies mid-batch); the engine's per-key last-write-wins makes that
 * safe. Use `onProcessed` to prune the changelog once a watermark is durable.
 */

const OP_BY_NAME: Record<string, RowOp> = {
	insert: 'insert',
	INSERT: 'insert',
	update: 'update',
	UPDATE: 'update',
	delete: 'delete',
	DELETE: 'delete'
};

/** One changelog row, as returned by your `poll` query. */
export type OutboxRow = {
	/** Monotonic sequence (the cursor advances to the max seen). */
	seq: number;
	/** Source table the change happened on. */
	tbl: string;
	/** `insert` | `update` | `delete` (upper- or lower-case). */
	op: string;
	/** The row's captured values — a JSON string or an already-parsed object. */
	payload: unknown;
};

/**
 * Default changelog-row parser: normalizes `op`, JSON-parses a string `payload`,
 * and returns `{ table, change }`. Returns `undefined` for a malformed row so it
 * is skipped (and its `seq` still advances the cursor) rather than wedging the
 * feed.
 */
export const parseOutboxRow = (row: OutboxRow): ParsedChange | undefined => {
	if (typeof row.tbl !== 'string') {
		return undefined;
	}
	const op = OP_BY_NAME[row.op];
	if (op === undefined) {
		return undefined;
	}
	let payload: unknown = row.payload;
	if (typeof payload === 'string') {
		try {
			payload = JSON.parse(payload);
		} catch {
			return undefined;
		}
	}
	if (typeof payload !== 'object' || payload === null) {
		return undefined;
	}
	return { table: row.tbl, change: { op, row: payload } };
};

export type PollingChangeSourceOptions = {
	/**
	 * Fetch changelog rows with `seq > sinceSeq`, ordered by `seq` ascending.
	 * e.g. `(since) => sql\`SELECT seq, tbl, op, payload FROM absolute_sync_changelog WHERE seq > ${since} ORDER BY seq\``.
	 */
	poll: (sinceSeq: number) => Promise<OutboxRow[]> | OutboxRow[];
	/** Poll interval in ms. Defaults to 1000. */
	intervalMs?: number;
	/** Resume cursor (highest `seq` already processed). Defaults to 0. */
	startSeq?: number;
	/** Override the row parser (defaults to {@link parseOutboxRow}). */
	parse?: (row: OutboxRow) => ParsedChange | undefined;
	/** Called after a non-empty batch with the new watermark — prune here. */
	onProcessed?: (uptoSeq: number) => void | Promise<void>;
	/** Called if a poll throws (the loop keeps running). Defaults to a warning. */
	onError?: (error: unknown) => void;
	/**
	 * Called when an outbox row fails to parse (custom `parse` returned
	 * undefined, malformed JSON in `payload`, unknown `op`, etc). Default:
	 * silently dropped. Provide this to surface skipped rows so you notice
	 * malformed entries in the changelog table.
	 */
	onSkip?: (row: OutboxRow, reason: 'parse-failed') => void;
};

/**
 * Create a polling {@link ChangeSource} over a changelog table. Connect it with
 * `engine.connectSource(...)`; `start` runs an immediate first poll (draining any
 * backlog) and then polls every `intervalMs` until `stop`.
 */
export const createPollingChangeSource = (
	options: PollingChangeSourceOptions
): ChangeSource => {
	const intervalMs = options.intervalMs ?? 1000;
	const parse = options.parse ?? parseOutboxRow;
	const onError =
		options.onError ??
		((error: unknown) => {
			console.warn('[sync] polling change source error:', error);
		});
	const onSkip = options.onSkip;
	let cursor = options.startSeq ?? 0;
	let running = false;
	let timer: ReturnType<typeof setTimeout> | undefined;

	const tick = async (emit: EmitChange): Promise<void> => {
		if (!running) {
			return;
		}
		try {
			const rows = await options.poll(cursor);
			for (const row of rows) {
				const parsed = parse(row);
				if (parsed === undefined) {
					onSkip?.(row, 'parse-failed');
				} else {
					await emit(parsed.table, parsed.change);
				}
				if (typeof row.seq === 'number' && row.seq > cursor) {
					cursor = row.seq;
				}
			}
			if (rows.length > 0) {
				await options.onProcessed?.(cursor);
			}
		} catch (error) {
			onError(error);
		}
		if (running) {
			timer = setTimeout(() => {
				void tick(emit);
			}, intervalMs);
		}
	};

	return {
		start: async (emit) => {
			if (running) {
				return;
			}
			running = true;
			await tick(emit);
		},
		stop: () => {
			running = false;
			if (timer !== undefined) {
				clearTimeout(timer);
				timer = undefined;
			}
		}
	};
};
