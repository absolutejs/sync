import { expect, test } from 'bun:test';
import { createPostgresSignalReaction } from '../src/adapters/postgres/reaction';
import { createPostgresTableRevisionSource } from '../src/adapters/postgres/revisions';

/* A durable "pending work" store the reaction drains, with SKIP LOCKED-like
 * claiming: claimed work is invisible to further claims until released. */
const store = (ids: string[]) => {
	const pending = new Set(ids);
	const claimed = new Set<string>();
	return {
		add: (id: string) => pending.add(id),
		claim: async (limit: number) => {
			const out = [...pending]
				.filter((id) => !claimed.has(id))
				.slice(0, limit);
			for (const id of out) claimed.add(id);
			return out;
		},
		done: (id: string) => {
			pending.delete(id);
			claimed.delete(id);
		},
		release: (id: string) => claimed.delete(id),
		pending
	};
};

const listener = () => {
	let wake = () => {};
	let listening = false;
	return {
		listen: async (_channel: string, fn: () => void) => {
			wake = fn;
			listening = true;
			return () => {
				listening = false;
			};
		},
		signal: () => wake(),
		get listening() {
			return listening;
		}
	};
};

test('catches up on start, then runs only when signalled', async () => {
	const work = store(['a', 'b', 'c']);
	const ran: string[] = [];
	const channel = listener();
	const reaction = createPostgresSignalReaction({
		batch: 2,
		channel: 'ci_runs',
		claim: work.claim,
		listen: channel.listen,
		run: async (id) => {
			ran.push(id);
			work.done(id);
		}
	});
	await reaction.start(async () => {});
	expect(ran).toEqual(['a', 'b', 'c']);

	work.add('d');
	expect(ran).toEqual(['a', 'b', 'c']);
	channel.signal();
	await reaction.drain();
	expect(ran).toEqual(['a', 'b', 'c', 'd']);
	await reaction.stop();
	expect(channel.listening).toBe(false);
});

test('a failing piece of work waits for the next signal instead of spinning', async () => {
	const work = store(['ok', 'broken']);
	let attempts = 0;
	let healthy = false;
	const failures: string[] = [];
	const channel = listener();
	const reaction = createPostgresSignalReaction({
		channel: 'ci_runs',
		claim: work.claim,
		listen: channel.listen,
		onError: () => {},
		onFailure: (id) => {
			failures.push(id);
			work.release(id);
		},
		run: async (id) => {
			if (id === 'broken') {
				attempts++;
				if (!healthy) throw Error('GitHub 502');
			}
			work.done(id);
		}
	});
	await reaction.start(async () => {});
	// Retried once in the same pass because 'ok' made progress; then the
	// pass ends, since a batch with no success stops it.
	expect(attempts).toBe(2);
	expect(failures).toEqual(['broken', 'broken']);
	expect([...work.pending]).toEqual(['broken']);

	healthy = true;
	channel.signal();
	await reaction.drain();
	expect(attempts).toBe(3);
	expect([...work.pending]).toEqual([]);
	await reaction.stop();
});

test('signals during a pass run one more pass, not one per signal', async () => {
	const work = store(['a']);
	let passes = 0;
	const channel = listener();
	let release: () => void = () => {};
	const reaction = createPostgresSignalReaction({
		channel: 'ci_runs',
		claim: async (limit) => {
			passes++;
			return work.claim(limit);
		},
		listen: channel.listen,
		run: async (id) => {
			await new Promise<void>((resolve) => {
				release = resolve;
			});
			work.done(id);
		}
	});
	const started = reaction.start(async () => {});
	await Bun.sleep(0);
	channel.signal();
	channel.signal();
	channel.signal();
	release();
	await started;
	// start's pass claims [a] then [], then one coalesced extra pass claims [].
	expect(passes).toBe(3);
	await reaction.stop();
});

test('the revision source can run with no timer, catching up when the listener (re)connects', async () => {
	const originalSetTimeout = globalThis.setTimeout;
	let timers = 0;
	globalThis.setTimeout = ((...args: Parameters<typeof setTimeout>) => {
		timers++;
		return originalSetTimeout(...args);
	}) as typeof setTimeout;
	try {
		let wake = () => {};
		let revision = '1';
		const seen: string[] = [];
		const source = createPostgresTableRevisionSource({
			listen: async (_channel, fn) => {
				wake = fn;
				return () => {};
			},
			read: async () => [{ revision, table: 'ci_runs' }],
			reconcileMs: false
		});
		await source.start(async (_table, change) => {
			seen.push((change.row as { revision: string }).revision);
		});
		revision = '2';
		wake();
		await Bun.sleep(0);
		await Bun.sleep(0);
		expect(seen).toEqual(['1', '2']);
		expect(timers).toBe(0);
		await source.stop();
	} finally {
		globalThis.setTimeout = originalSetTimeout;
	}
	expect(() =>
		createPostgresTableRevisionSource({
			read: async () => [],
			reconcileMs: false
		})
	).toThrow('needs a listen');
});
