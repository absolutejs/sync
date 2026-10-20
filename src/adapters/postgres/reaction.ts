import type { ChangeSource } from '../../engine/types';

/** Server work that runs because something happened, never on a timer.
 *
 * A NOTIFY on `channel` means "there may be work": the reaction claims the
 * pending work the host keeps durably (a row with a pending state, written in
 * the same transaction as whatever caused it), runs it, and claims again until
 * nothing is left. The listening connection waking on every (re)connect is the
 * catch-up: work that arrived while the process was down or disconnected runs
 * as soon as it listens again. Nothing polls.
 *
 * Delivery is at-least-once. `claim` must hand each piece of work to one
 * process (e.g. `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)`), and
 * `run` must be idempotent: a crash after an effect but before `run` records
 * completion re-runs it on the next signal or reconnect.
 *
 * A failure is reported and the work is left to `onFailure` (record the
 * attempt, put it back as pending, or give up after N). A pass ends when a
 * claim returns nothing or a batch makes no progress (everything in it
 * failed), so a broken piece of work waits for the next signal instead of
 * spinning; it is retried within a pass at most while other work succeeds.
 *
 * Returned as a ChangeSource so the engine owns its lifecycle
 * (`engine.connectSource(reaction)`); it emits nothing itself — the writes
 * `run` makes reach subscribers through the table revision triggers.
 */
export type PostgresSignalReactionOptions<Work> = {
	/** NOTIFY channel announcing possible work. */
	channel: string;
	/** Subscribe. Call `wake` for each notification AND whenever the
	 *  connection is (re)established — Bun SQL: `sql.listen(channel, wake, wake)`. */
	listen: (
		channel: string,
		wake: () => void
	) => Promise<() => void | Promise<void>>;
	/** Atomically claim up to `limit` pending pieces of work; `[]` when none. */
	claim: (limit: number) => Promise<Work[]>;
	/** Do one piece of work and record that it is done. Throw to fail it. */
	run: (work: Work) => Promise<void>;
	/** Record a failed attempt so a later signal can retry it (or give up). */
	onFailure?: (work: Work, error: unknown) => Promise<void> | void;
	/** How many to claim at once. Default 10. */
	batch?: number;
	onError?: (error: unknown) => void;
};

export function createPostgresSignalReaction<Work>(
	options: PostgresSignalReactionOptions<Work>
): ChangeSource & { drain: () => Promise<void> } {
	const batch = options.batch ?? 10;
	if (!Number.isInteger(batch) || batch < 1)
		throw new TypeError('batch must be a positive integer');
	let stopped = true,
		running = false,
		pending = false;
	let unlisten: (() => void | Promise<void>) | undefined;
	const report = (error: unknown) => options.onError?.(error);

	const pass = async () => {
		for (;;) {
			const claimed = await options.claim(batch);
			if (stopped || claimed.length === 0) return;
			let succeeded = 0;
			for (const work of claimed) {
				if (stopped) return;
				try {
					await options.run(work);
					succeeded++;
				} catch (error) {
					report(error);
					try {
						await options.onFailure?.(work, error);
					} catch (failure) {
						report(failure);
					}
				}
			}
			// Everything claimed failed: wait for the next signal.
			if (succeeded === 0) return;
		}
	};

	/** One pass at a time; a signal during a pass runs one more after it. */
	const drain = async () => {
		if (stopped) return;
		if (running) {
			pending = true;
			return;
		}
		running = true;
		try {
			do {
				pending = false;
				try {
					await pass();
				} catch (error) {
					report(error);
				}
			} while (pending && !stopped);
		} finally {
			running = false;
		}
	};

	return {
		drain,
		async start() {
			if (!stopped) return;
			stopped = false;
			unlisten = await options.listen(
				options.channel,
				() => void drain()
			);
			await drain();
		},
		async stop() {
			stopped = true;
			await unlisten?.();
			unlisten = undefined;
		}
	};
}
