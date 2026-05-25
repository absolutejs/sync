import type { MutationActions } from './mutation';
import type { ReadHandle } from './reactive';

/**
 * Scheduled functions — server-triggered work whose effects flow through the
 * change feed, so what a schedule writes goes live to subscribers with no extra
 * wiring. The engine owns running them (and making them reactive); the trigger
 * is `@elysiajs/cron` via the `scheduled` plugin — cron decides *when*, the
 * engine makes the effect *live*. For durable, retryable work, a schedule can
 * call into `@absolutejs/queue` (cron enqueues; the queue guarantees it runs).
 */

/** What a scheduled function's `run` receives — read current state, write live. */
export type ScheduleContext = {
	/** Read through registered table readers (unscoped — schedules are trusted). */
	db: ReadHandle;
	/**
	 * Persist + emit changes, exactly like a mutation handler: every
	 * insert/update/delete goes live as one atomic batch. Write permission rules
	 * are not applied (a schedule is trusted server code).
	 */
	actions: MutationActions;
};

export type ScheduleDefinition = {
	/** Schedule name — its identity (the cron job is registered under it). */
	name: string;
	/**
	 * Cron pattern (`@elysiajs/cron` / croner syntax; the optional 6th leading
	 * field is seconds), e.g. `'0 8 * * 1'` (Mondays 08:00) or `'*\/5 * * * * *'`
	 * (every 5 seconds).
	 */
	pattern: string;
	/** The work to run on each fire. Writes via `ctx.actions` go live. */
	run: (ctx: ScheduleContext) => Promise<void> | void;
};

/**
 * Define a scheduled function. Identity at runtime (for type inference). Register
 * it with {@link SyncEngine.registerSchedule} and wire the triggers with the
 * `scheduled` Elysia plugin.
 */
export const defineSchedule = (
	definition: ScheduleDefinition
): ScheduleDefinition => definition;
