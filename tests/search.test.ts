import { describe, expect, test } from 'bun:test';
import { defineSearchCollection } from '../src/engine/search';
import { createTextIndex } from '../src/engine/textIndex';
import { createVectorIndex } from '../src/engine/vectorIndex';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Doc = { id: number; title: string; owner?: number };
type Vec = { id: number; embedding: number[] };

const ids = (rows: unknown[]) => rows.map((row) => (row as Doc).id);

const collect = <T>() => {
	const diffs: ViewDiff<T>[] = [];
	return {
		diffs,
		onDiff: (diff: ViewDiff<T>) => {
			diffs.push(diff);
		}
	};
};

describe('createTextIndex (BM25 full-text)', () => {
	const seed = (): Doc[] => [
		{ id: 1, title: 'the quick brown fox' },
		{ id: 2, title: 'quick brown dogs' },
		{ id: 3, title: 'lazy cat sleeps' }
	];

	test('ranks matching docs by relevance, excludes non-matches', () => {
		const index = createTextIndex<Doc>({
			key: (doc) => doc.id,
			fields: ['title']
		});
		seed().forEach(index.add);

		const hits = index.search('quick brown', 10);
		// Both contain "quick brown"; the shorter doc ranks higher (length norm).
		expect(hits.map((hit) => hit.row.id)).toEqual([2, 1]);
		expect(hits[0]!.score).toBeGreaterThan(hits[1]!.score);
		// "lazy cat" doc shares no query terms.
		expect(hits.some((hit) => hit.row.id === 3)).toBe(false);
	});

	test('remove and upsert keep the index current', () => {
		const index = createTextIndex<Doc>({
			key: (doc) => doc.id,
			fields: ['title']
		});
		seed().forEach(index.add);

		index.remove(2);
		expect(index.search('dogs', 10)).toHaveLength(0);
		expect(index.size()).toBe(2);

		// Upsert doc 1 so it no longer matches "fox" but matches "shark".
		index.add({ id: 1, title: 'a hungry shark' });
		expect(index.search('fox', 10)).toHaveLength(0);
		expect(index.search('shark', 10).map((hit) => hit.row.id)).toEqual([1]);
	});

	test('respects the result limit', () => {
		const index = createTextIndex<Doc>({
			key: (doc) => doc.id,
			fields: ['title']
		});
		for (let id = 0; id < 10; id += 1) {
			index.add({ id, title: 'common term here' });
		}
		expect(index.search('common', 3)).toHaveLength(3);
	});
});

describe('createVectorIndex (similarity)', () => {
	const seed: Vec[] = [
		{ id: 1, embedding: [1, 0] },
		{ id: 2, embedding: [0.9, 0.1] },
		{ id: 3, embedding: [0, 1] }
	];

	test('cosine ranks by nearest direction', () => {
		const index = createVectorIndex<Vec>({
			key: (vec) => vec.id,
			embedding: (vec) => vec.embedding
		});
		seed.forEach(index.add);

		const hits = index.search([1, 0], 3);
		expect(hits.map((hit) => hit.row.id)).toEqual([1, 2, 3]);
		expect(hits[0]!.score).toBeCloseTo(1, 5);
	});

	test('euclidean ranks by nearest point and upsert/remove work', () => {
		const index = createVectorIndex<Vec>({
			key: (vec) => vec.id,
			embedding: (vec) => vec.embedding,
			metric: 'euclidean'
		});
		seed.forEach(index.add);

		expect(index.search([0, 1], 1).map((hit) => hit.row.id)).toEqual([3]);

		index.remove(3);
		index.add({ id: 2, embedding: [0, 0.95] }); // move doc 2 near [0,1]
		expect(index.search([0, 1], 1).map((hit) => hit.row.id)).toEqual([2]);
		expect(index.size()).toBe(2);
	});
});

describe('live search collection', () => {
	const makeEngine = () => {
		const docs = new Map<number, Doc>([
			[1, { id: 1, title: 'the quick brown fox' }],
			[2, { id: 2, title: 'quick brown dogs' }],
			[3, { id: 3, title: 'lazy cat sleeps' }]
		]);
		const engine = createSyncEngine();
		engine.registerSearch(
			defineSearchCollection<Doc>({
				name: 'docSearch',
				table: 'docs',
				index: () =>
					createTextIndex<Doc>({
						key: (doc) => doc.id,
						fields: ['title']
					}),
				source: () => [...docs.values()],
				key: (doc) => doc.id
			})
		);
		return { docs, engine };
	};

	test('returns the ranked top-K with a score, live as the corpus changes', async () => {
		const { docs, engine } = makeEngine();
		const { diffs, onDiff } = collect<Doc>();
		const sub = await engine.subscribe<Doc, string>({
			collection: 'docSearch',
			params: 'quick brown',
			ctx: {},
			onDiff
		});
		expect(ids(sub.initial)).toEqual([2, 1]);
		// Each emitted row carries its relevance score.
		expect((sub.initial[0] as { _score?: number })._score).toBeGreaterThan(
			0
		);

		// A new strongly-matching doc enters the results.
		docs.set(4, { id: 4, title: 'quick quick brown' });
		await engine.applyChange('docs', {
			op: 'insert',
			row: { id: 4, title: 'quick quick brown' }
		});
		expect(ids(diffs.at(-1)!.added)).toContain(4);

		// Deleting a result removes it.
		docs.delete(2);
		await engine.applyChange('docs', {
			op: 'delete',
			row: { id: 2, title: 'quick brown dogs' }
		});
		expect(ids(diffs.at(-1)!.removed)).toContain(2);
	});

	test('a change that matches no result query emits nothing', async () => {
		const { engine } = makeEngine();
		const { diffs, onDiff } = collect<Doc>();
		await engine.subscribe<Doc, string>({
			collection: 'docSearch',
			params: 'quick',
			ctx: {},
			onDiff
		});
		// A doc with no overlap with "quick" doesn't enter the result set.
		await engine.applyChange('docs', {
			op: 'insert',
			row: { id: 9, title: 'totally unrelated text' }
		});
		expect(diffs).toHaveLength(0);
	});

	test('a row-level read permission filters search hits', async () => {
		const docs = new Map<number, Doc>([
			[1, { id: 1, title: 'shared report', owner: 1 }],
			[2, { id: 2, title: 'shared report', owner: 2 }]
		]);
		const engine = createSyncEngine({
			permissions: {
				docs: {
					read: (ctx: { userId: number }, row: Doc) =>
						row.owner === ctx.userId
				}
			}
		});
		engine.registerSearch(
			defineSearchCollection<Doc, string, { userId: number }>({
				name: 'docSearch',
				table: 'docs',
				index: () =>
					createTextIndex<Doc>({
						key: (doc) => doc.id,
						fields: ['title']
					}),
				source: () => [...docs.values()],
				key: (doc) => doc.id
			})
		);

		const sub = await engine.subscribe<Doc, string, { userId: number }>({
			collection: 'docSearch',
			params: 'report',
			ctx: { userId: 1 },
			onDiff: () => {}
		});
		// Both docs match "report", but only the caller's own is returned.
		expect(ids(sub.initial)).toEqual([1]);
	});
});
