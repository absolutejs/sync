import { describe, expect, test } from 'bun:test';
import { createAggregate } from '../src/engine/aggregate';

type Order = { id: number; userId: number; total: number };

const order = (id: number, userId: number, total: number): Order => ({
	id,
	userId,
	total
});

describe('createAggregate', () => {
	test('count/sum/avg over a single group', () => {
		const agg = createAggregate<Order>({
			key: (row) => row.id,
			value: (row) => row.total
		});
		agg.hydrate([order(1, 5, 10), order(2, 5, 30)]);

		const group = agg.group('');
		expect(group?.count).toBe(2);
		expect(group?.sum).toBe(40);
		expect(group?.avg).toBe(20);
		expect(group?.min).toBe(10);
		expect(group?.max).toBe(30);
	});

	test('insert / delete adjust incrementally', () => {
		const agg = createAggregate<Order>({
			key: (row) => row.id,
			value: (row) => row.total
		});
		agg.apply({ op: 'insert', row: order(1, 5, 10) });
		agg.apply({ op: 'insert', row: order(2, 5, 20) });
		agg.apply({ op: 'delete', row: order(1, 5, 10) });

		expect(agg.group('')).toMatchObject({ count: 1, sum: 20, avg: 20 });
	});

	test('update adjusts sum and value without double counting', () => {
		const agg = createAggregate<Order>({
			key: (row) => row.id,
			value: (row) => row.total
		});
		agg.apply({ op: 'insert', row: order(1, 5, 10) });
		agg.apply({ op: 'update', row: order(1, 5, 99) });

		expect(agg.group('')).toMatchObject({ count: 1, sum: 99, max: 99 });
	});

	test('removing the current max recomputes it correctly', () => {
		const agg = createAggregate<Order>({
			key: (row) => row.id,
			value: (row) => row.total
		});
		agg.hydrate([order(1, 5, 10), order(2, 5, 50), order(3, 5, 30)]);
		expect(agg.group('')?.max).toBe(50);

		agg.apply({ op: 'delete', row: order(2, 5, 50) });
		expect(agg.group('')?.max).toBe(30);
		expect(agg.group('')?.min).toBe(10);
	});

	test('groups by a key and reports each group', () => {
		const agg = createAggregate<Order>({
			key: (row) => row.id,
			groupBy: (row) => row.userId,
			value: (row) => row.total
		});
		agg.hydrate([order(1, 5, 10), order(2, 5, 20), order(3, 9, 100)]);

		expect(agg.group(5)).toMatchObject({ count: 2, sum: 30 });
		expect(agg.group(9)).toMatchObject({ count: 1, sum: 100 });
		expect(agg.groups()).toHaveLength(2);
	});

	test('an update moving a row between groups rebalances both', () => {
		const agg = createAggregate<Order>({
			key: (row) => row.id,
			groupBy: (row) => row.userId,
			value: (row) => row.total
		});
		agg.apply({ op: 'insert', row: order(1, 5, 10) });
		agg.apply({ op: 'update', row: order(1, 9, 10) }); // user 5 -> 9

		expect(agg.group(5)).toBeUndefined(); // emptied groups are dropped
		expect(agg.group(9)).toMatchObject({ count: 1, sum: 10 });
	});

	test('an emptied group disappears', () => {
		const agg = createAggregate<Order>({ key: (row) => row.id });
		agg.apply({ op: 'insert', row: order(1, 5, 0) });
		agg.apply({ op: 'delete', row: order(1, 5, 0) });

		expect(agg.group('')).toBeUndefined();
		expect(agg.groups()).toEqual([]);
	});

	test('count-only aggregate (no value extractor)', () => {
		const agg = createAggregate<Order>({ key: (row) => row.id });
		agg.hydrate([order(1, 5, 10), order(2, 5, 20)]);

		expect(agg.group('')).toMatchObject({
			count: 2,
			sum: 0,
			min: undefined,
			max: undefined
		});
	});
});
