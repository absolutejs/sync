import { describe, expect, test } from 'bun:test';
import { defineReactiveQuery } from '../src/engine/reactive';
import { defineSchedule } from '../src/engine/schedule';
import { createSyncEngine } from '../src/engine/syncEngine';
import { scheduled } from '../src/scheduled';
import type { ViewDiff } from '../src/engine/types';

type Tick = { id: number; at: number };

const collect = <T>() => {
	const diffs: ViewDiff<T>[] = [];
	return {
		diffs,
		onDiff: (diff: ViewDiff<T>) => {
			diffs.push(diff);
		}
	};
};

// An engine with a `ticks` table (reader + writer) and a reactive collection.
const makeEngine = (options: Parameters<typeof createSyncEngine>[0] = {}) => {
	const ticks = new Map<number, Tick>();
	const engine = createSyncEngine(options);
	engine.registerReader('ticks', { all: () => [...ticks.values()] });
	engine.registerWriter<Tick>('ticks', {
		insert: (data: Tick) => {
			ticks.set(data.id, data);
			return data;
		},
		update: (data: Tick) => {
			ticks.set(data.id, data);
			return data;
		},
		delete: (row: { id: number }) => {
			ticks.delete(row.id);
		}
	});
	engine.registerReactive(
		defineReactiveQuery<Tick>({
			name: 'ticks',
			key: (tick) => tick.id,
			run: ({ db }) => db.all<Tick>('ticks')
		})
	);
	return { engine, ticks };
};

describe('scheduled functions (engine.runSchedule)', () => {
	test("a schedule's writes go live to subscribers", async () => {
		const { engine } = makeEngine();
		engine.registerSchedule(
			defineSchedule({
				name: 'tick',
				pattern: '* * * * *',
				run: ({ actions }) =>
					void actions.insert('ticks', { id: 1, at: 100 })
			})
		);
		const { diffs, onDiff } = collect<Tick>();
		await engine.subscribe<Tick>({
			collection: 'ticks',
			params: undefined,
			ctx: {},
			onDiff
		});

		await engine.runSchedule('tick');
		expect(diffs.at(-1)?.added).toEqual([{ id: 1, at: 100 }]);
	});

	test('a schedule can read current state through ctx.db', async () => {
		const { engine, ticks } = makeEngine();
		ticks.set(1, { id: 1, at: 1 });
		ticks.set(2, { id: 2, at: 2 });
		let seen = 0;
		engine.registerSchedule(
			defineSchedule({
				name: 'count',
				pattern: '* * * * *',
				run: async ({ db, actions }) => {
					const rows = await db.all<Tick>('ticks');
					seen = rows.length;
					await actions.insert('ticks', { id: 99, at: seen });
				}
			})
		);
		await engine.runSchedule('count');
		expect(seen).toBe(2);
		expect(ticks.get(99)).toEqual({ id: 99, at: 2 });
	});

	test('schedule writes bypass write permission rules (trusted)', async () => {
		const { engine } = makeEngine({
			permissions: { ticks: { write: () => false } }
		});
		engine.registerSchedule(
			defineSchedule({
				name: 'tick',
				pattern: '* * * * *',
				run: ({ actions }) =>
					void actions.insert('ticks', { id: 1, at: 1 })
			})
		);
		const { diffs, onDiff } = collect<Tick>();
		await engine.subscribe<Tick>({
			collection: 'ticks',
			params: undefined,
			ctx: {},
			onDiff
		});
		// A write rule of `false` would reject a mutation, but a schedule is trusted.
		await engine.runSchedule('tick');
		expect(diffs.at(-1)?.added).toEqual([{ id: 1, at: 1 }]);
	});

	test('a throwing schedule emits nothing (atomic)', async () => {
		const { engine } = makeEngine();
		engine.registerSchedule(
			defineSchedule({
				name: 'boom',
				pattern: '* * * * *',
				run: async ({ actions }) => {
					await actions.insert('ticks', { id: 1, at: 1 });
					throw new Error('handler failed');
				}
			})
		);
		const { diffs, onDiff } = collect<Tick>();
		await engine.subscribe<Tick>({
			collection: 'ticks',
			params: undefined,
			ctx: {},
			onDiff
		});
		await expect(engine.runSchedule('boom')).rejects.toThrow(
			'handler failed'
		);
		// applyChangeBatch is only reached after the handler resolves, so a throw
		// emits nothing.
		expect(diffs).toHaveLength(0);
	});

	test('listSchedules reports registrations; unknown schedule throws', async () => {
		const { engine } = makeEngine();
		engine.registerSchedule(
			defineSchedule({ name: 'a', pattern: '0 * * * *', run: () => {} })
		);
		engine.registerSchedule(
			defineSchedule({ name: 'b', pattern: '*/5 * * * *', run: () => {} })
		);
		expect(engine.listSchedules().map((s) => s.name)).toEqual(['a', 'b']);
		expect(engine.listSchedules().map((s) => s.pattern)).toEqual([
			'0 * * * *',
			'*/5 * * * *'
		]);
		await expect(engine.runSchedule('missing')).rejects.toThrow(
			'Unknown schedule "missing"'
		);
	});
});

describe('scheduled plugin', () => {
	test('builds a cron-wired plugin from the engine schedules', () => {
		const { engine } = makeEngine();
		engine.registerSchedule(
			defineSchedule({
				name: 'tick',
				pattern: '*/5 * * * * *',
				run: () => {}
			})
		);
		// Constructing the plugin registers a cron job named `sync:tick`.
		const plugin = scheduled({ engine });
		expect(plugin).toBeDefined();
	});
});
