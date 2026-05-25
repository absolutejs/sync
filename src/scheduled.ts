import { Elysia } from 'elysia';
import { cron } from '@elysiajs/cron';
import type { SyncEngine } from './engine/syncEngine';

export type ScheduledOptions = {
	/** The engine whose registered schedules (see `registerSchedule`) to run. */
	engine: SyncEngine;
	/** Prefix for the cron job names registered on Elysia's store. Default `sync`. */
	prefix?: string;
	/** Called when a scheduled run throws (the batch is rolled back). */
	onError?: (name: string, error: unknown) => void;
};

/**
 * Elysia plugin that fires the engine's registered scheduled functions on their
 * cron patterns, via `@elysiajs/cron`. Cron decides *when*; the engine runs the
 * schedule and makes its writes go live through the change feed (and a schedule
 * can `enqueue` into `@absolutejs/queue` for durable, retryable work).
 *
 * Register schedules with `engine.registerSchedule(...)` before mounting this, so
 * each one becomes a cron job named `<prefix>:<schedule.name>`. Mount once.
 */
export const scheduled = ({
	engine,
	prefix = 'sync',
	onError
}: ScheduledOptions) => {
	const run = (name: string) => () => {
		void engine.runSchedule(name).catch((error) => {
			if (onError === undefined) {
				throw error;
			}
			onError(name, error);
		});
	};

	// `.use` registers the cron job on this instance (mutating), so a loop builds
	// one plugin carrying every schedule's cron trigger.
	const app = new Elysia({ name: '@absolutejs/sync/scheduled' });
	for (const schedule of engine.listSchedules()) {
		app.use(
			cron({
				name: `${prefix}:${schedule.name}`,
				pattern: schedule.pattern,
				run: run(schedule.name)
			})
		);
	}
	return app;
};
