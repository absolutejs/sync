import { describe, expect, test } from 'bun:test';
import {
	createMaterializedView,
	isEmptyViewDiff
} from '../src/engine/materializedView';

type Order = { id: number; userId: number; status: 'open' | 'closed' };

// "open orders for user 5"
const view = () =>
	createMaterializedView<Order>({
		key: (row) => row.id,
		match: (row) => row.userId === 5 && row.status === 'open'
	});

const open5 = (id: number): Order => ({ id, userId: 5, status: 'open' });

describe('createMaterializedView', () => {
	test('hydrate sets the result set', () => {
		const v = view();
		v.hydrate([open5(1), open5(2)]);

		expect(v.size()).toBe(2);
		expect(v.rows().map((row) => row.id)).toEqual([1, 2]);
	});

	test('hydrate replaces a prior result set', () => {
		const v = view();
		v.hydrate([open5(1)]);
		v.hydrate([open5(2), open5(3)]);

		expect(v.rows().map((row) => row.id)).toEqual([2, 3]);
	});

	test('an inserted matching row is added', () => {
		const v = view();
		const diff = v.apply({ op: 'insert', row: open5(1) });

		expect(diff).toEqual({ added: [open5(1)], removed: [], changed: [] });
		expect(v.size()).toBe(1);
	});

	test('an inserted non-matching row is ignored', () => {
		const v = view();
		const diff = v.apply({
			op: 'insert',
			row: { id: 1, userId: 99, status: 'open' }
		});

		expect(isEmptyViewDiff(diff)).toBe(true);
		expect(v.size()).toBe(0);
	});

	test('an updated row that still matches is changed', () => {
		const v = view();
		v.hydrate([open5(1)]);
		const next: Order = { id: 1, userId: 5, status: 'open' };
		const diff = v.apply({ op: 'update', row: next });

		expect(diff).toEqual({ added: [], removed: [], changed: [next] });
		expect(v.size()).toBe(1);
		expect(v.rows()[0]).toBe(next);
	});

	test('an update that stops matching removes the row (leave)', () => {
		const v = view();
		const original = open5(1);
		v.hydrate([original]);
		const diff = v.apply({
			op: 'update',
			row: { id: 1, userId: 5, status: 'closed' }
		});

		expect(diff).toEqual({ added: [], removed: [original], changed: [] });
		expect(v.size()).toBe(0);
	});

	test('an update that starts matching adds the row (enter)', () => {
		const v = view();
		// Row 1 was not in the set (was closed); now it becomes open.
		const diff = v.apply({ op: 'update', row: open5(1) });

		expect(diff).toEqual({ added: [open5(1)], removed: [], changed: [] });
		expect(v.size()).toBe(1);
	});

	test('deleting a present row removes it', () => {
		const v = view();
		const original = open5(1);
		v.hydrate([original]);
		const diff = v.apply({ op: 'delete', row: { id: 1 } as Order });

		expect(diff).toEqual({ added: [], removed: [original], changed: [] });
		expect(v.size()).toBe(0);
	});

	test('deleting an absent row is a no-op', () => {
		const v = view();
		const diff = v.apply({ op: 'delete', row: { id: 7 } as Order });

		expect(isEmptyViewDiff(diff)).toBe(true);
		expect(v.size()).toBe(0);
	});

	test('updating an absent non-matching row is a no-op', () => {
		const v = view();
		const diff = v.apply({
			op: 'update',
			row: { id: 1, userId: 99, status: 'closed' }
		});

		expect(isEmptyViewDiff(diff)).toBe(true);
		expect(v.size()).toBe(0);
	});

	test('removed carries the value the view last held, not the new one', () => {
		const v = view();
		const original = open5(1);
		v.hydrate([original]);
		const diff = v.apply({
			op: 'update',
			row: { id: 1, userId: 5, status: 'closed' }
		});

		expect(diff.removed[0]).toBe(original);
	});

	test('a sequence of changes keeps the set consistent', () => {
		const v = view();
		v.apply({ op: 'insert', row: open5(1) }); // +1
		v.apply({ op: 'insert', row: open5(2) }); // +2
		v.apply({ op: 'insert', row: { id: 3, userId: 9, status: 'open' } }); // ignored
		v.apply({ op: 'update', row: { id: 1, userId: 5, status: 'closed' } }); // -1
		v.apply({ op: 'delete', row: { id: 2 } as Order }); // -2
		v.apply({ op: 'update', row: open5(4) }); // +4 (enter)

		expect(v.rows().map((row) => row.id)).toEqual([4]);
	});

	test('honours a custom key function', () => {
		const v = createMaterializedView<{ sku: string; live: boolean }>({
			key: (row) => row.sku,
			match: (row) => row.live
		});
		v.apply({ op: 'insert', row: { sku: 'abc', live: true } });
		const diff = v.apply({ op: 'update', row: { sku: 'abc', live: true } });

		expect(diff.changed).toEqual([{ sku: 'abc', live: true }]);
		expect(v.size()).toBe(1);
	});
});

describe('MaterializedView.reset', () => {
	test('diffs a fresh result set against what the view held', () => {
		type Item = { id: number; label: string };
		const v = createMaterializedView<Item>({
			key: (row) => row.id,
			match: () => true
		});
		v.hydrate([
			{ id: 1, label: 'a' },
			{ id: 2, label: 'b' },
			{ id: 3, label: 'c' }
		]);

		// 1 unchanged, 2 dropped, 3 changed value, 4 appeared.
		const changed3 = { id: 3, label: 'C' };
		const diff = v.reset([
			{ id: 1, label: 'a' },
			changed3,
			{ id: 4, label: 'd' }
		]);

		expect(diff.added).toEqual([{ id: 4, label: 'd' }]);
		expect(diff.removed).toEqual([{ id: 2, label: 'b' }]);
		expect(diff.changed).toEqual([changed3]);
		expect(
			v
				.rows()
				.map((row) => row.id)
				.sort()
		).toEqual([1, 3, 4]);
	});

	test('treats a shallow-equal row as unchanged', () => {
		const v = view();
		v.hydrate([open5(1)]);
		const diff = v.reset([open5(1)]); // same values, new object

		expect(diff.added).toEqual([]);
		expect(diff.removed).toEqual([]);
		expect(diff.changed).toEqual([]);
	});

	test('honours a custom equals', () => {
		const v = createMaterializedView<{ id: number; v: number }>({
			key: (row) => row.id,
			match: () => true,
			equals: (a, b) => a.id === b.id // ignore non-key fields
		});
		v.hydrate([{ id: 1, v: 1 }]);
		const diff = v.reset([{ id: 1, v: 999 }]);

		expect(diff.changed).toEqual([]); // equal by id, so not "changed"
	});
});
