/**
 * Tenant migration primitives. Closes G7 from the deep-research
 * audit: "move a tenant from engine A to engine B."
 *
 * The substrate offers three composable verbs:
 *
 * - **`engine.fence({ reason })`** — pause new mutations on the
 *   source so its captured state stops drifting. Subscribers continue
 *   to read; only `runMutation` rejects (with
 *   {@link EngineFencedError}). Returns a {@link FenceHandle} with
 *   `lift()` to undo.
 * - **`engine.exportSnapshot({ tables?, ctx? })`** — walk the
 *   registered readers and return a portable
 *   {@link EngineSnapshot} carrying the source `instanceId`,
 *   `version`, and current rows per table. Cheap and synchronous from
 *   the operator's POV.
 * - **`engine.importSnapshot(snapshot, options?)`** — on the target,
 *   bulk-load the rows via each table's registered writer. Tracks
 *   per-table progress. Returns a {@link MigrationImportResult} with
 *   row counts.
 *
 * The intended choreography for a cross-region tenant move:
 *
 * ```ts
 * // ── on the source ──
 * const fence = source.fence({ reason: 'tenant-7 → us-east-2' });
 * try {
 *   const snapshot = await source.exportSnapshot();
 *   await transport(snapshot); // S3, message bus, etc.
 *   // ── on the target ──
 *   await target.importSnapshot(snapshot, {
 *     onProgress: (table, done, total) =>
 *       console.log(`${table}: ${done}/${total}`)
 *   });
 *   await cutoverDns(); // direct clients at target
 * } finally {
 *   fence.lift();
 * }
 * ```
 *
 * Out of scope: out-of-band writes (CDC drivers, raw SQL) — the
 * caller is responsible for pausing those before fencing, otherwise
 * the captured snapshot drifts.
 *
 * Added in 1.24.0.
 */

/**
 * Portable per-tenant state captured by
 * {@link SyncEngine.exportSnapshot}. Consumed by
 * {@link SyncEngine.importSnapshot} on the target engine.
 */
export type EngineSnapshot = {
	/** The exporting engine's `instanceId` (for audit / forensics). */
	sourceInstanceId: string;
	/** Source engine's monotonic version at snapshot time. */
	version: number;
	/** `Date.now()` at export — used by hosts for staleness checks. */
	exportedAt: number;
	/** Current rows per table, read from each table's registered reader. */
	tables: Record<string, ReadonlyArray<unknown>>;
};

/**
 * Returned by {@link SyncEngine.importSnapshot}.
 */
export type MigrationImportResult = {
	/** Number of tables that had at least one row imported. */
	tablesImported: number;
	/** Total rows inserted across all tables. */
	rowsImported: number;
	/** Rows inserted per table. Tables with zero rows are still listed. */
	perTable: Record<string, number>;
	/**
	 * Tables present in the snapshot that the target engine has no
	 * registered writer for — skipped silently. Surface this to
	 * operators so they can catch "I forgot to register `tasks` on
	 * the new shard" cleanly.
	 */
	skipped: ReadonlyArray<string>;
};

/**
 * Returned by {@link SyncEngine.fence}. Hold this and call `lift()`
 * to re-enable mutations. Holding multiple fences is supported — the
 * engine stays fenced until every handle has been lifted.
 */
export type FenceHandle = {
	/** `Date.now()` at fence time. */
	fencedAt: number;
	/** Human-readable reason — surfaced on {@link EngineFencedError}. */
	reason: string;
	/** Re-enable mutations. Idempotent (later calls are no-ops). */
	lift: () => void;
};

export type ExportSnapshotOptions = {
	/**
	 * Narrow the export to a subset of registered tables. Useful for
	 * per-tenant cuts when readers expose `ctx`-scoped data.
	 */
	tables?: ReadonlyArray<string>;
	/**
	 * Context passed to each reader's `all(ctx)`. The default `{}`
	 * works for engines whose readers ignore context.
	 */
	ctx?: unknown;
};

export type ImportSnapshotOptions = {
	/**
	 * Narrow the import to a subset of tables in the snapshot.
	 * Tables outside the filter are skipped (NOT recorded in
	 * `skipped`; that field is for tables with no writer).
	 */
	tables?: ReadonlyArray<string>;
	/**
	 * Called for each row insertion. Fires synchronously inside the
	 * import loop; keep it cheap or schedule heavy work elsewhere.
	 */
	onProgress?: (table: string, done: number, total: number) => void;
	/**
	 * Context passed to each writer's `insert(data, ctx, tx)`. The
	 * default `{}` works for writers that ignore context.
	 */
	ctx?: unknown;
};

/**
 * Thrown by `runMutation` when the engine is fenced. The reason
 * carries through so operators can correlate denied calls to the
 * fence that caused them.
 */
export class EngineFencedError extends Error {
	readonly reason: string;
	constructor(reason: string) {
		super(`[sync] Engine is fenced for migration: ${reason}`);
		this.name = 'EngineFencedError';
		this.reason = reason;
	}
}
