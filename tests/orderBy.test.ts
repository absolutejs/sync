import { describe, expect, test } from 'bun:test';
import { orderByOp, query } from '../src/engine/index';
import { defineGraphCollection } from '../src/engine/graph';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { Change } from '../src/engine/dataflow';
import type { ViewDiff } from '../src/engine/types';

type Score = { id: number; points: number };

const up = (row: Score): Change<Score> => ({
	op: 'upsert',
	key: row.id,
	row
});
const del = (id: number): Change<Score> => ({
	op: 'delete',
	key: id,
	row: { id, points: 0 }
});

// highest points first
const byPoints = (a: Score, b: Score) => b.points - a.points;

describe('orderByOp (top-N window)', () => {
	test('keeps only the top N by comparator', () => {
		const top2 = orderByOp<Score>({
			key: (s) => s.id,
			compare: byPoints,
			limit: 2
		});
		const out = top2.push([
			up({ id: 1, points: 10 }),
			up({ id: 2, points: 30 }),
			up({ id: 3, points: 20 })
		]);
		// window = ids 2 (30) and 3 (20); id 1 (10) excluded
		const upserts = out.filter((c) => c.op === 'upsert').map((c) => c.key);
		expect(upserts.sort()).toEqual([2, 3]);
	});

	test('a higher new entry evicts the row it displaces', () => {
		const top2 = orderByOp<Score>({
			key: (s) => s.id,
			compare: byPoints,
			limit: 2
		});
		top2.push([up({ id: 1, points: 10 }), up({ id: 2, points: 30 })]);
		// window currently {2:30, 1:10}; insert id 3 with 20 -> evicts id 1
		const out = top2.push([up({ id: 3, points: 20 })]);
		expect(out.find((c) => c.key === 1)?.op).toBe('delete'); // displaced
		expect(out.find((c) => c.key === 3)?.op).toBe('upsert'); // entered
	});

	test('deleting a window row pulls the next one in', () => {
		const top2 = orderByOp<Score>({
			key: (s) => s.id,
			compare: byPoints,
			limit: 2
		});
		top2.push([
			up({ id: 1, points: 10 }),
			up({ id: 2, points: 30 }),
			up({ id: 3, points: 20 })
		]);
		// window {2:30, 3:20}; delete 2 -> 3 stays, 1 (10) enters
		const out = top2.push([del(2)]);
		expect(out.find((c) => c.key === 2)?.op).toBe('delete');
		expect(out.find((c) => c.key === 1)?.op).toBe('upsert'); // pulled in
	});

	test('offset paginates the window', () => {
		const page = orderByOp<Score>({
			key: (s) => s.id,
			compare: byPoints,
			offset: 1,
			limit: 1
		});
		const out = page.push([
			up({ id: 1, points: 10 }),
			up({ id: 2, points: 30 }),
			up({ id: 3, points: 20 })
		]);
		// sorted 2(30),3(20),1(10); offset 1 limit 1 -> just id 3
		expect(out.filter((c) => c.op === 'upsert').map((c) => c.key)).toEqual([
			3
		]);
	});
});

describe('orderBy in a graph collection (live top-N)', () => {
	test('derives each live page from subscription params', async () => {
		const scores: Score[] = [
			{ id: 1, points: 10 },
			{ id: 2, points: 30 },
			{ id: 3, points: 20 }
		];
		const engine = createSyncEngine();
		engine.registerGraph(
			defineGraphCollection<Score, { limit: number; offset: number }>({
				name: 'scorePage',
				key: (score) => score.id,
				query: query<Score, { limit: number; offset: number }>({
					table: 'scores',
					hydrate: () => scores,
					key: (score) => score.id
				}).orderBy({
					compare: byPoints,
					key: (score) => score.id,
					limit: (params) => params.limit,
					offset: (params) => params.offset
				})
			})
		);

		const sub = await engine.subscribe<
			Score,
			{ limit: number; offset: number }
		>({
			collection: 'scorePage',
			ctx: {},
			onDiff: () => undefined,
			params: { limit: 1, offset: 1 }
		});
		expect(sub.initial.map((score) => score.id)).toEqual([3]);
	});

	test('maintains a top-2 leaderboard incrementally', async () => {
		const scores: Score[] = [
			{ id: 1, points: 10 },
			{ id: 2, points: 30 }
		];
		const engine = createSyncEngine();
		engine.registerGraph(
			defineGraphCollection<Score>({
				name: 'leaderboard',
				key: (s) => s.id,
				query: query<Score>({
					table: 'scores',
					hydrate: () => scores,
					key: (s) => s.id
				}).orderBy({ key: (s) => s.id, compare: byPoints, limit: 2 })
			})
		);

		const diffs: ViewDiff<Score>[] = [];
		const sub = await engine.subscribe<Score>({
			collection: 'leaderboard',
			params: undefined,
			ctx: {},
			onDiff: (d) => diffs.push(d)
		});
		expect(sub.initial.map((s) => s.id).sort()).toEqual([1, 2]);

		// id 3 scores 20 -> enters top-2, id 1 (10) drops out
		await engine.applyChange<Score>('scores', {
			op: 'insert',
			row: { id: 3, points: 20 }
		});
		const last = diffs[diffs.length - 1];
		expect(last?.added.map((s) => s.id)).toEqual([3]);
		expect(last?.removed.map((s) => s.id)).toEqual([1]);
	});
});
